package webhook

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/worker"
)

var errNoDatabase = errors.New("dry run reached the connection pool")

// noConn fails every query: in a dry run GORM builds statements without sending them.
type noConn struct{}

func (noConn) PrepareContext(context.Context, string) (*sql.Stmt, error) { return nil, errNoDatabase }

func (noConn) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, errNoDatabase
}

func (noConn) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errNoDatabase
}

func (noConn) QueryRowContext(context.Context, string, ...any) *sql.Row { return nil }

// dryRunPool begins transactions without a database and records their isolation levels.
type dryRunPool struct {
	noConn
	isolation []sql.IsolationLevel
}

func (p *dryRunPool) BeginTx(_ context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	level := sql.LevelDefault
	if opts != nil {
		level = opts.Isolation
	}
	p.isolation = append(p.isolation, level)
	return &dryRunTx{}, nil
}

type dryRunTx struct{ noConn }

func (*dryRunTx) Commit() error { return nil }

func (*dryRunTx) Rollback() error { return nil }

type statement struct {
	sql  string
	vars []any
	inTx bool
}

// recorder keeps every statement GORM builds, answers a delivery read with row, and counts every update
// as one matched row.
type recorder struct {
	pool       *dryRunPool
	statements []statement
	row        deliveryRow
}

func (r *recorder) record(gdb *gorm.DB) {
	_, inTx := gdb.Statement.ConnPool.(*dryRunTx)
	r.statements = append(r.statements,
		statement{gdb.Statement.SQL.String(), slices.Clone(gdb.Statement.Vars), inTx})
}

func (r *recorder) query(gdb *gorm.DB) {
	r.record(gdb)
	if dest, ok := gdb.Statement.Dest.(*deliveryRow); ok {
		*dest = r.row
	}
}

func (r *recorder) update(gdb *gorm.DB) {
	r.record(gdb)
	gdb.RowsAffected = 1
}

// dryRun returns a repository whose statements are recorded rather than sent.
func dryRun(t *testing.T) (*Repository, *recorder) {
	t.Helper()
	pool := &dryRunPool{}
	gdb, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: pool, SkipInitializeWithVersion: true}),
		&gorm.Config{
			SkipDefaultTransaction: true,
			TranslateError:         true,
			DryRun:                 true,
			DisableAutomaticPing:   true,
			Logger:                 logger.Discard,
		})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	rec := &recorder{pool: pool}
	cb := gdb.Callback()
	for _, err := range []error{
		cb.Query().After("gorm:query").Register("test:record", rec.query),
		cb.Create().After("gorm:create").Register("test:record", rec.record),
		cb.Update().After("gorm:update").Register("test:record", rec.update),
	} {
		if err != nil {
			t.Fatalf("error registering a recording callback: %v", err)
		}
	}
	return NewRepository(gdb), rec
}

// TestInsertDeliveriesSQL inserts a delivery whose fields are zero wherever the type allows,
// so a dropped zero value would show as a missing value.
func TestInsertDeliveriesSQL(t *testing.T) {
	r, rec := dryRun(t)
	ds := []delivery{
		{EventID: uuid.NewV7(), Body: []byte{}},
		{EventID: uuid.NewV7(), TenantID: 42, Body: []byte(`{"id":"x"}`)},
	}

	if err := r.insertDeliveries(t.Context(), ds); err != nil {
		t.Fatalf("insertDeliveries() error = %v", err)
	}

	if len(rec.statements) != 1 {
		t.Fatalf("ran %d statements, want 1: %v", len(rec.statements), rec.statements)
	}
	got := rec.statements[0]
	const want = "INSERT INTO `webhook_deliveries` (`event_id`,`tenant_id`,`body`) VALUES (?,?,?),(?,?,?) " +
		"ON DUPLICATE KEY UPDATE `id`=`id`"
	if got.sql != want {
		t.Errorf("SQL = %s\nwant  %s", got.sql, want)
	}
	wantVars := []any{db.ID(ds[0].EventID), uint64(0), []byte{}, db.ID(ds[1].EventID), uint64(42), []byte(`{"id":"x"}`)}
	if !reflect.DeepEqual(got.vars, wantVars) {
		t.Errorf("values = %#v\nwant     %#v", got.vars, wantVars)
	}
	checkStatement(t, got)
	if got.inTx || len(rec.pool.isolation) > 0 {
		t.Error("insertDeliveries() opened a transaction")
	}
}

func TestInsertNoDeliveries(t *testing.T) {
	r, rec := dryRun(t)
	if err := r.insertDeliveries(t.Context(), nil); err != nil || len(rec.statements) > 0 {
		t.Errorf("insertDeliveries(nil) = %v after %v, want nil without a statement", err, rec.statements)
	}
}

func TestLeasedDeliverySQL(t *testing.T) {
	r, rec := dryRun(t)
	eventID := uuid.NewV7()
	rec.row = deliveryRow{EventID: db.ID(eventID), TenantID: 42, Body: []byte(`{"id":"x"}`)}

	got, err := r.leasedDelivery(t.Context(), lease{ID: 11, TenantID: 42, Attempt: 3})

	if err != nil {
		t.Fatalf("leasedDelivery() error = %v", err)
	}
	if want := (delivery{EventID: eventID, TenantID: 42, Body: []byte(`{"id":"x"}`)}); !reflect.DeepEqual(got, want) {
		t.Errorf("leasedDelivery() = %+v, want %+v", got, want)
	}
	if len(rec.statements) != 1 {
		t.Fatalf("ran %d statements, want 1: %v", len(rec.statements), rec.statements)
	}
	s := rec.statements[0]
	const want = "SELECT `event_id`,`tenant_id`,`body` FROM `webhook_deliveries` WHERE id = ? AND tenant_id = ? LIMIT ?"
	if s.sql != want || !reflect.DeepEqual(s.vars, []any{uint64(11), uint64(42), 1}) {
		t.Errorf("statement = %s %v\nwant        %s [11 42 1]", s.sql, s.vars, want)
	}
}

// TestFailGivesUpAfterTwelveAttempts fails a delivery on several attempts: a retryable failure requeues it until
// the twelfth attempt, and a permanent one fails it at once.
func TestFailGivesUpAfterTwelveAttempts(t *testing.T) {
	tests := []struct {
		attempt   int
		permanent bool
		wantFinal bool
	}{
		{1, false, false},
		{11, false, false},
		{12, false, true},
		{1, true, true},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("attempt %d, permanent %t", tc.attempt, tc.permanent), func(t *testing.T) {
			r, rec := dryRun(t)

			final, err := r.fail(t.Context(), lease{ID: 11, TenantID: 42, Attempt: tc.attempt},
				failure{Permanent: tc.permanent, Detail: "webhook endpoint answered 503", Delay: time.Minute})

			if err != nil || final != tc.wantFinal {
				t.Fatalf("fail() = %t, %v; want %t", final, err, tc.wantFinal)
			}
			if len(rec.statements) != 1 {
				t.Fatalf("ran %d statements, want 1: %v", len(rec.statements), rec.statements)
			}
			s := rec.statements[0]
			state, retried := "queued", true
			if tc.wantFinal {
				state, retried = "failed", false
			}
			if !strings.HasPrefix(s.sql, "UPDATE `webhook_deliveries` SET ") ||
				!strings.HasSuffix(s.sql, "WHERE id = ? AND state = ? AND attempts = ?") ||
				strings.Contains(s.sql, "`run_after`=") != retried || !slices.Contains(s.vars, any(state)) ||
				!slices.Contains(s.vars, any("webhook endpoint answered 503")) {
				t.Errorf("statement = %s %v, want the delivery %s with its last error", s.sql, s.vars, state)
			}
			checkStatement(t, s)
		})
	}
}

// The deliverer converts between worker.Lease and the repository's lease.
var (
	_ = worker.Lease(lease{})
	_ = lease(worker.Lease{})
)

// checkStatement checks that a statement leaves timestamps to MySQL and binds a value to every placeholder.
func checkStatement(t *testing.T, got statement) {
	t.Helper()
	for _, column := range []string{"created_at", "updated_at"} {
		if strings.Contains(got.sql, column) {
			t.Errorf("%s\nmentions %s", got.sql, column)
		}
	}
	if n := strings.Count(got.sql, "?"); n != len(got.vars) {
		t.Errorf("%s\nhas %d placeholders for %d values: %v", got.sql, n, len(got.vars), got.vars)
	}
	for _, v := range got.vars {
		if _, ok := v.(time.Time); ok {
			t.Errorf("%s\nbinds the Go time %v; times come from NOW(6)", got.sql, v)
		}
	}
}
