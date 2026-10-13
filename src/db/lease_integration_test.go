//go:build integration

package db_test

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/testutil"
)

const (
	testLease = time.Minute
	slack     = 5 * time.Second
)

var errWrites = errors.New("domain writes failed")

type deliveryRow struct {
	ID       uint64 `gorm:"primaryKey"`
	EventID  db.ID  `gorm:"column:event_id"`
	TenantID uint64 `gorm:"column:tenant_id"`
	Body     []byte
}

func (deliveryRow) TableName() string { return "webhook_deliveries" }

// leasedTable describes a table the way the tests see it; queued and failed are the same in both.
type leasedTable struct {
	name, column string
	leased, done string
	newLeases    func(*gorm.DB) *db.Leases
	insert       func(t *testing.T, gdb *gorm.DB, tenantID uint64) uint64
}

func leasedTables() []leasedTable {
	return []leasedTable{
		{
			name: "assets", column: "status", leased: "processing", done: "ready",
			newLeases: db.NewAssetLeases,
			insert: func(t *testing.T, gdb *gorm.DB, tenantID uint64) uint64 {
				t.Helper()
				row := newAsset(tenantID, db.NewID(), nil)
				if err := gdb.Create(row).Error; err != nil {
					t.Fatalf("error inserting an asset: %v", err)
				}
				return row.ID
			},
		},
		{
			name: "webhook_deliveries", column: "state", leased: "sending", done: "delivered",
			newLeases: db.NewWebhookDeliveryLeases,
			insert: func(t *testing.T, gdb *gorm.DB, tenantID uint64) uint64 {
				t.Helper()
				row := &deliveryRow{EventID: db.NewID(), TenantID: tenantID, Body: []byte(`{}`)}
				if err := gdb.Create(row).Error; err != nil {
					t.Fatalf("error inserting a webhook delivery: %v", err)
				}
				return row.ID
			},
		},
	}
}

// storedRow is a leased row's lease columns as stored, comparable with ==.
type storedRow struct {
	State          string
	Attempts       int
	LockedBy       sql.NullString
	LeaseExpiresAt sql.NullString
	RunAfter       string
	LastError      sql.NullString
}

// leaseEnv is a throwaway database with one tenant and a domain_writes table that stands in for
// the writes a repository makes alongside a lease change.
type leaseEnv struct {
	t        *testing.T
	gdb      *gorm.DB
	table    leasedTable
	leases   *db.Leases
	tenantID uint64
}

func newLeaseEnv(t *testing.T, table leasedTable) *leaseEnv {
	t.Helper()
	gdb := open(t, testutil.NewDatabase(t))
	ctxDB := gdb.WithContext(t.Context())
	if err := ctxDB.Exec("CREATE TABLE domain_writes (name VARCHAR(64) NOT NULL PRIMARY KEY)").Error; err != nil {
		t.Fatalf("error creating domain_writes: %v", err)
	}
	return &leaseEnv{
		t:        t,
		gdb:      ctxDB,
		table:    table,
		leases:   table.newLeases(gdb),
		tenantID: createTenant(t, ctxDB, "lease"),
	}
}

// on returns an environment for table in the same database.
func (e *leaseEnv) on(table leasedTable) *leaseEnv {
	return &leaseEnv{t: e.t, gdb: e.gdb, table: table, leases: table.newLeases(e.gdb), tenantID: e.tenantID}
}

func (e *leaseEnv) insert() uint64 {
	e.t.Helper()
	return e.table.insert(e.t, e.gdb, e.tenantID)
}

// exec runs query with %[1]s standing for the table and %[2]s for its state column.
func (e *leaseEnv) exec(query string, args ...any) {
	e.t.Helper()
	if err := e.gdb.Exec(fmt.Sprintf(query, e.table.name, e.table.column), args...).Error; err != nil {
		e.t.Fatalf("error running %q: %v", query, err)
	}
}

func (e *leaseEnv) row(id uint64) storedRow {
	e.t.Helper()
	var row storedRow
	err := e.gdb.Raw(fmt.Sprintf("SELECT %s AS state, attempts, locked_by, lease_expires_at, run_after, last_error "+
		"FROM %s WHERE id = ?", e.table.column, e.table.name), id).Scan(&row).Error
	if err != nil {
		e.t.Fatalf("error reading %s row %d: %v", e.table.name, id, err)
	}
	return row
}

// until returns how far column, a DATETIME(6), lies after NOW(6).
func (e *leaseEnv) until(id uint64, column string) time.Duration {
	e.t.Helper()
	var us sql.NullInt64
	err := e.gdb.Raw(fmt.Sprintf("SELECT TIMESTAMPDIFF(MICROSECOND, NOW(6), %s) FROM %s WHERE id = ?",
		column, e.table.name), id).Scan(&us).Error
	if err != nil || !us.Valid {
		e.t.Fatalf("error reading %s of %s row %d: %v (valid %t)", column, e.table.name, id, err, us.Valid)
	}
	return time.Duration(us.Int64) * time.Microsecond
}

func (e *leaseEnv) expire(id uint64) {
	e.t.Helper()
	e.exec("UPDATE %[1]s SET lease_expires_at = NOW(6) - INTERVAL 1 SECOND WHERE id = ?", id)
}

func (e *leaseEnv) makeDue(id uint64) {
	e.t.Helper()
	e.exec("UPDATE %[1]s SET run_after = NOW(6) WHERE id = ?", id)
}

func (e *leaseEnv) claim(workerID string) db.Lease {
	e.t.Helper()
	lease, ok, err := e.leases.Claim(e.t.Context(), workerID, testLease)
	if err != nil || !ok {
		e.t.Fatalf("Claim() = %v, %t, %v; want a lease", lease, ok, err)
	}
	return lease
}

func (e *leaseEnv) wantNothingDue() {
	e.t.Helper()
	if lease, ok, err := e.leases.Claim(e.t.Context(), "idle", testLease); ok || err != nil {
		e.t.Fatalf("Claim() = %v, %t, %v; want nothing due", lease, ok, err)
	}
}

func (e *leaseEnv) reap(maxAttempts int, writes func(*gorm.DB, []db.Lease) error) (requeued, failed int) {
	e.t.Helper()
	requeued, failed, err := e.leases.Reap(e.t.Context(), maxAttempts, writes)
	if err != nil {
		e.t.Fatalf("Reap() error = %v", err)
	}
	return requeued, failed
}

// write returns domain writes that record name, and fail with err if it isn't nil.
func write(name string, err error) func(*gorm.DB) error {
	return func(tx *gorm.DB) error {
		if dbErr := tx.Exec("INSERT INTO domain_writes (name) VALUES (?)", name).Error; dbErr != nil {
			return dbErr
		}
		return err
	}
}

func (e *leaseEnv) written(name string) bool {
	e.t.Helper()
	var n int64
	if err := e.gdb.Table("domain_writes").Where("name = ?", name).Count(&n).Error; err != nil {
		e.t.Fatalf("error reading domain_writes: %v", err)
	}
	return n > 0
}

// wantLeased checks that row id is leased to workerID on attempt attempts.
func (e *leaseEnv) wantLeased(id uint64, workerID string, attempts int) {
	e.t.Helper()
	row := e.row(id)
	if row.State != e.table.leased || row.Attempts != attempts || !row.LockedBy.Valid ||
		row.LockedBy.String != workerID || !row.LeaseExpiresAt.Valid {
		e.t.Errorf("row %d = %+v, want %s on attempt %d, locked by %q with a lease",
			id, row, e.table.leased, attempts, workerID)
	}
}

// wantUnleased checks that row id is in state with attempts, and that its lease columns are cleared.
func (e *leaseEnv) wantUnleased(id uint64, state string, attempts int) storedRow {
	e.t.Helper()
	row := e.row(id)
	if row.State != state || row.Attempts != attempts || row.LockedBy.Valid || row.LeaseExpiresAt.Valid {
		e.t.Errorf("row %d = %+v, want %s on attempt %d without locked_by or lease_expires_at",
			id, row, state, attempts)
	}
	return row
}

func forEachTable(t *testing.T, test func(t *testing.T, table leasedTable)) {
	for _, table := range leasedTables() {
		t.Run(table.name, func(t *testing.T) { test(t, table) })
	}
}

func TestClaimConcurrently(t *testing.T) {
	const rows, claimers = 40, 8
	forEachTable(t, func(t *testing.T, table leasedTable) {
		e := newLeaseEnv(t, table)
		var ids []uint64
		for range rows {
			ids = append(ids, e.insert())
		}

		claimed := make([][]db.Lease, claimers)
		errs := make([]error, claimers)
		var wg sync.WaitGroup
		for i := range claimers {
			wg.Go(func() {
				for {
					lease, ok, err := e.leases.Claim(t.Context(), fmt.Sprintf("worker-%d", i), testLease)
					if err != nil || !ok {
						errs[i] = err
						return
					}
					claimed[i] = append(claimed[i], lease)
				}
			})
		}
		wg.Wait()

		holders := make(map[uint64]string)
		for i, leases := range claimed {
			if errs[i] != nil {
				t.Errorf("claimer %d: Claim() error = %v", i, errs[i])
			}
			for _, lease := range leases {
				if holder, ok := holders[lease.ID]; ok {
					t.Errorf("row %d claimed by worker-%d and %s", lease.ID, i, holder)
				}
				holders[lease.ID] = fmt.Sprintf("worker-%d", i)
				if lease.TenantID != e.tenantID || lease.Attempt != 1 {
					t.Errorf("lease %+v, want tenant %d on attempt 1", lease, e.tenantID)
				}
			}
		}
		for _, id := range ids {
			holder, ok := holders[id]
			if !ok {
				t.Errorf("row %d wasn't claimed", id)
				continue
			}
			e.wantLeased(id, holder, 1)
		}
		if len(holders) != rows {
			t.Errorf("claimed %d rows, want %d", len(holders), rows)
		}
	})
}

func TestClaimOrder(t *testing.T) {
	forEachTable(t, func(t *testing.T, table leasedTable) {
		e := newLeaseEnv(t, table)
		at := func(runAfter string) uint64 {
			id := e.insert()
			e.exec("UPDATE %[1]s SET run_after = ? WHERE id = ?", runAfter, id)
			return id
		}
		later := at("2020-01-01 00:00:02")
		first := at("2020-01-01 00:00:01")
		tied := at("2020-01-01 00:00:01")
		future := e.insert()
		e.exec("UPDATE %[1]s SET run_after = NOW(6) + INTERVAL 1 HOUR WHERE id = ?", future)
		for _, state := range []string{table.leased, table.done, "failed"} {
			id := at("2019-01-01 00:00:00")
			e.exec("UPDATE %[1]s SET %[2]s = ? WHERE id = ?", state, id)
		}

		var got []uint64
		for range 3 {
			got = append(got, e.claim("worker").ID)
		}
		e.wantNothingDue()

		if want := []uint64{first, tied, later}; !slices.Equal(got, want) {
			t.Errorf("claimed %v, want %v", got, want)
		}
		e.wantUnleased(future, "queued", 0)
	})
}

func TestClaimSetsTheLease(t *testing.T) {
	forEachTable(t, func(t *testing.T, table leasedTable) {
		e := newLeaseEnv(t, table)
		e.wantNothingDue()
		id := e.insert()
		workerID := "worker-" + strings.Repeat("ö", 100)

		lease, ok, err := e.leases.Claim(t.Context(), workerID, 90*time.Second)
		if err != nil || !ok {
			t.Fatalf("Claim() = %v, %t, %v; want a lease", lease, ok, err)
		}

		if want := (db.Lease{ID: id, TenantID: e.tenantID, Attempt: 1}); lease != want {
			t.Errorf("Claim() = %+v, want %+v", lease, want)
		}
		e.wantLeased(id, db.Clip(workerID, 128), 1)
		if d := e.until(id, "lease_expires_at"); d <= 90*time.Second-slack || d > 90*time.Second {
			t.Errorf("lease expires in %v, want about 90s", d)
		}
		e.wantNothingDue()
	})
}

func TestExtend(t *testing.T) {
	forEachTable(t, func(t *testing.T, table leasedTable) {
		e := newLeaseEnv(t, table)
		id := e.insert()
		lease := e.claim("worker")

		if err := e.leases.Extend(t.Context(), lease, 2*time.Hour); err != nil {
			t.Fatalf("Extend() error = %v", err)
		}

		e.wantLeased(id, "worker", 1)
		if d := e.until(id, "lease_expires_at"); d <= 2*time.Hour-slack || d > 2*time.Hour {
			t.Errorf("lease expires in %v, want about 2h", d)
		}
	})
}

// An extension that matches its row but changes nothing must still count the row (pitfall 12).
// Pinning the session clock makes the second extension write the values the first one wrote.
func TestUnchangedFencedUpdateCountsItsRow(t *testing.T) {
	forEachTable(t, func(t *testing.T, table leasedTable) {
		e := newLeaseEnv(t, table)
		id := e.insert()
		lease := e.claim("worker")

		err := e.gdb.Connection(func(conn *gorm.DB) error {
			if err := conn.Exec("SET timestamp = UNIX_TIMESTAMP(NOW(6))").Error; err != nil {
				return err
			}
			defer conn.Exec("SET timestamp = DEFAULT")
			leases := table.newLeases(conn)
			if err := leases.Extend(t.Context(), lease, testLease); err != nil {
				return fmt.Errorf("first Extend(): %w", err)
			}
			before := e.row(id)
			if err := leases.Extend(t.Context(), lease, testLease); err != nil {
				return fmt.Errorf("unchanged Extend(): %w", err)
			}
			if after := e.row(id); after != before {
				return fmt.Errorf("unchanged Extend() changed the row from %+v to %+v", before, after)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestComplete(t *testing.T) {
	tests := []struct {
		name        string
		writes      func(*gorm.DB) error
		wantErr     error
		wantDone    bool
		wantWritten bool
	}{
		{"with writes", write("done", nil), nil, true, true},
		{"without writes", nil, nil, true, false},
		{"writes fail", write("done", errWrites), errWrites, false, false},
	}
	forEachTable(t, func(t *testing.T, table leasedTable) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				e := newLeaseEnv(t, table)
				id := e.insert()
				lease := e.claim("worker")
				before := e.row(id)

				err := e.leases.Complete(t.Context(), lease, tc.writes)

				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Complete() error = %v, want %v", err, tc.wantErr)
				}
				if tc.wantDone {
					e.wantUnleased(id, table.done, 1)
				} else if after := e.row(id); after != before {
					t.Errorf("row changed from %+v to %+v, want it rolled back", before, after)
				}
				if got := e.written("done"); got != tc.wantWritten {
					t.Errorf("domain writes committed: %t, want %t", got, tc.wantWritten)
				}
			})
		}
	})
}

func TestFail(t *testing.T) {
	long := strings.Repeat("é", 1500)
	tests := []struct {
		name        string
		failure     db.Failure
		wantErr     error
		wantFinal   bool
		wantState   string
		wantWritten bool
		wantDelay   time.Duration
		wantMessage string
	}{
		{
			name:        "retry backs off",
			failure:     db.Failure{MaxAttempts: 3, Delay: 30 * time.Second, Message: "timeout", Writes: write("failed", nil)},
			wantState:   "queued",
			wantDelay:   30 * time.Second,
			wantMessage: "timeout",
		},
		{
			name:        "retry without a delay",
			failure:     db.Failure{MaxAttempts: 3, Message: "timeout"},
			wantState:   "queued",
			wantMessage: "timeout",
		},
		{
			name:        "last attempt",
			failure:     db.Failure{MaxAttempts: 1, Delay: time.Hour, Message: "timeout", Writes: write("failed", nil)},
			wantFinal:   true,
			wantState:   "failed",
			wantWritten: true,
			wantMessage: "timeout",
		},
		{
			name:        "permanent",
			failure:     db.Failure{Permanent: true, MaxAttempts: 3, Message: "not audio", Writes: write("failed", nil)},
			wantFinal:   true,
			wantState:   "failed",
			wantWritten: true,
			wantMessage: "not audio",
		},
		{
			name:        "long message is clipped",
			failure:     db.Failure{Permanent: true, MaxAttempts: 3, Message: long},
			wantFinal:   true,
			wantState:   "failed",
			wantMessage: db.Clip(long, 2048),
		},
		{
			name:    "final writes fail",
			failure: db.Failure{Permanent: true, MaxAttempts: 3, Message: "x", Writes: write("failed", errWrites)},
			wantErr: errWrites,
		},
	}
	forEachTable(t, func(t *testing.T, table leasedTable) {
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				e := newLeaseEnv(t, table)
				id := e.insert()
				lease := e.claim("worker")
				before := e.row(id)

				final, err := e.leases.Fail(t.Context(), lease, tc.failure)

				if got := e.written("failed"); got != tc.wantWritten {
					t.Errorf("domain writes committed: %t, want %t", got, tc.wantWritten)
				}
				if tc.wantErr != nil {
					if !errors.Is(err, tc.wantErr) {
						t.Fatalf("Fail() error = %v, want %v", err, tc.wantErr)
					}
					if after := e.row(id); after != before {
						t.Errorf("row changed from %+v to %+v, want it rolled back", before, after)
					}
					return
				}
				if err != nil || final != tc.wantFinal {
					t.Fatalf("Fail() = %t, %v; want %t", final, err, tc.wantFinal)
				}
				row := e.wantUnleased(id, tc.wantState, 1)
				if row.LastError.String != tc.wantMessage {
					t.Errorf("last_error = %q, want %q", row.LastError.String, tc.wantMessage)
				}
				if tc.wantFinal {
					e.wantNothingDue()
					return
				}
				if d := e.until(id, "run_after"); d <= tc.wantDelay-slack || d > tc.wantDelay {
					t.Errorf("due in %v, want about %v", d, tc.wantDelay)
				}
				if tc.wantDelay > 0 {
					e.wantNothingDue()
					e.makeDue(id)
				}
				if again := e.claim("worker"); again.ID != id || again.Attempt != 2 {
					t.Errorf("claim after the retry = %+v, want row %d on attempt 2", again, id)
				}
			})
		}
	})
}

func TestReleaseReturnsTheAttempt(t *testing.T) {
	forEachTable(t, func(t *testing.T, table leasedTable) {
		e := newLeaseEnv(t, table)
		id := e.insert()
		lease := e.claim("worker")

		if err := e.leases.Release(t.Context(), lease); err != nil {
			t.Fatalf("Release() error = %v", err)
		}

		e.wantUnleased(id, "queued", 0)
		if again := e.claim("other"); again != lease {
			t.Errorf("claim after the release = %+v, want %+v", again, lease)
		}
	})
}

func TestReap(t *testing.T) {
	forEachTable(t, func(t *testing.T, table leasedTable) {
		t.Run("requeues with a short delay", func(t *testing.T) {
			e := newLeaseEnv(t, table)
			id := e.insert()
			e.claim("crashed")
			live := e.insert()
			e.claim("alive")
			e.expire(id)

			requeued, failed := e.reap(3, failWrites("reaped"))

			if requeued != 1 || failed != 0 {
				t.Errorf("Reap() = %d requeued, %d failed; want 1, 0", requeued, failed)
			}
			row := e.wantUnleased(id, "queued", 1)
			if row.LastError.String != "lease expired" {
				t.Errorf("last_error = %q, want %q", row.LastError.String, "lease expired")
			}
			if d := e.until(id, "run_after"); d <= 5*time.Second-slack || d > 5*time.Second {
				t.Errorf("due in %v, want about 5s", d)
			}
			e.wantLeased(live, "alive", 1)
			e.wantNothingDue()
			if e.written("reaped") {
				t.Error("reaping a row with attempts to spare ran the failure's writes")
			}
		})

		t.Run("fails at the limit", func(t *testing.T) {
			e := newLeaseEnv(t, table)
			id := e.insert()
			e.claim("crashed")
			e.expire(id)
			if requeued, failed := e.reap(2, failWrites("reaped")); requeued != 1 || failed != 0 {
				t.Fatalf("first Reap() = %d requeued, %d failed; want 1, 0", requeued, failed)
			}
			e.makeDue(id)
			e.claim("crashed again")
			e.expire(id)

			var got []db.Lease
			requeued, failed := e.reap(2, func(tx *gorm.DB, leases []db.Lease) error {
				got = leases
				return failWrites("reaped")(tx, leases)
			})

			if requeued != 0 || failed != 1 {
				t.Errorf("Reap() = %d requeued, %d failed; want 0, 1", requeued, failed)
			}
			if want := []db.Lease{{ID: id, TenantID: e.tenantID, Attempt: 2}}; !slices.Equal(got, want) {
				t.Errorf("writes got %+v, want %+v", got, want)
			}
			e.wantUnleased(id, "failed", 2)
			if !e.written("reaped") {
				t.Error("the failure's writes weren't committed")
			}
		})

		t.Run("writes fail", func(t *testing.T) {
			e := newLeaseEnv(t, table)
			spare, last := e.insert(), e.insert()
			e.claim("crashed")
			e.claim("crashed")
			e.exec("UPDATE %[1]s SET attempts = 3 WHERE id = ?", last)
			e.expire(spare)
			e.expire(last)
			before := []storedRow{e.row(spare), e.row(last)}

			_, _, err := e.leases.Reap(t.Context(), 3, func(*gorm.DB, []db.Lease) error { return errWrites })

			if !errors.Is(err, errWrites) {
				t.Fatalf("Reap() error = %v, want %v", err, errWrites)
			}
			if after := []storedRow{e.row(spare), e.row(last)}; !slices.Equal(after, before) {
				t.Errorf("rows changed from %+v to %+v, want the reap rolled back", before, after)
			}
		})

		t.Run("takes at most 100 rows", func(t *testing.T) {
			e := newLeaseEnv(t, table)
			for range 101 {
				e.insert()
			}
			e.exec("UPDATE %[1]s SET %[2]s = ?, attempts = 1, locked_by = 'crashed', "+
				"lease_expires_at = NOW(6) - INTERVAL 1 SECOND", table.leased)

			for i, want := range []int{100, 1, 0} {
				if requeued, failed := e.reap(3, nil); requeued != want || failed != 0 {
					t.Errorf("Reap() call %d = %d requeued, %d failed; want %d, 0", i, requeued, failed, want)
				}
			}
		})

		t.Run("touches only its own table", func(t *testing.T) {
			e := newLeaseEnv(t, table)
			id := e.insert()
			e.claim("crashed")
			e.expire(id)
			other := e.on(otherTable(t, table))
			otherID := other.insert()
			other.claim("crashed")
			other.expire(otherID)
			before := other.row(otherID)

			if requeued, failed := e.reap(3, nil); requeued != 1 || failed != 0 {
				t.Errorf("Reap() = %d requeued, %d failed; want 1, 0", requeued, failed)
			}
			if after := other.row(otherID); after != before {
				t.Errorf("%s row changed from %+v to %+v", other.table.name, before, after)
			}
		})
	})
}

func otherTable(t *testing.T, table leasedTable) leasedTable {
	t.Helper()
	for _, other := range leasedTables() {
		if other.name != table.name {
			return other
		}
	}
	t.Fatalf("no leased table other than %s", table.name)
	return leasedTable{}
}

func failWrites(name string) func(*gorm.DB, []db.Lease) error {
	return func(tx *gorm.DB, _ []db.Lease) error { return write(name, nil)(tx) }
}

// A holder whose lease was reaped can't write, whether the row waits in the queue or has been claimed again.
func TestReapedHolderIsFencedOut(t *testing.T) {
	ops := []struct {
		name string
		run  func(e *leaseEnv, stale db.Lease) error
	}{
		{"extend", func(e *leaseEnv, stale db.Lease) error {
			return e.leases.Extend(e.t.Context(), stale, testLease)
		}},
		{"complete", func(e *leaseEnv, stale db.Lease) error {
			return e.leases.Complete(e.t.Context(), stale, write("stale", nil))
		}},
		{"retry", func(e *leaseEnv, stale db.Lease) error {
			_, err := e.leases.Fail(e.t.Context(), stale, db.Failure{MaxAttempts: 10, Delay: time.Second, Message: "x"})
			return err
		}},
		{"fail", func(e *leaseEnv, stale db.Lease) error {
			_, err := e.leases.Fail(e.t.Context(), stale,
				db.Failure{Permanent: true, MaxAttempts: 10, Message: "x", Writes: write("stale", nil)})
			return err
		}},
		{"release", func(e *leaseEnv, stale db.Lease) error {
			return e.leases.Release(e.t.Context(), stale)
		}},
	}
	forEachTable(t, func(t *testing.T, table leasedTable) {
		for _, reclaimed := range []bool{false, true} {
			for _, op := range ops {
				name := op.name + " after the reap"
				if reclaimed {
					name = op.name + " after a new claim"
				}
				t.Run(name, func(t *testing.T) {
					e := newLeaseEnv(t, table)
					id := e.insert()
					stale := e.claim("stale")
					e.expire(id)
					if requeued, _ := e.reap(10, nil); requeued != 1 {
						t.Fatalf("Reap() requeued %d rows, want 1", requeued)
					}
					if reclaimed {
						e.makeDue(id)
						e.claim("successor")
					}
					before := e.row(id)

					err := op.run(e, stale)

					if !errors.Is(err, db.ErrLeaseLost) {
						t.Errorf("error = %v, want %v", err, db.ErrLeaseLost)
					}
					if after := e.row(id); after != before {
						t.Errorf("row changed from %+v to %+v", before, after)
					}
					if e.written("stale") {
						t.Error("the stale holder's domain writes were committed")
					}
				})
			}
		}
	})
}
