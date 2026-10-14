//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"uuid"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/testutil"
)

// stack is the real repository on a database, with a connection pool of its own.
type stack struct {
	gdb  *gorm.DB
	repo *Repository
	pool *sql.DB
}

func newStack(t *testing.T, dsn string) *stack {
	t.Helper()
	gdb, err := db.Open(dsn, testutil.Logger(t))
	if err != nil {
		t.Fatalf("error opening the database: %v", err)
	}
	pool, err := gdb.DB()
	if err != nil {
		t.Fatalf("error getting the pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	return &stack{gdb: gdb, repo: NewRepository(gdb), pool: pool}
}

// holder returns the connection ID of the session holding the relay lock, or false if it's free.
func (s *stack) holder(t *testing.T) (int64, bool) {
	t.Helper()
	var id sql.NullInt64
	if err := s.pool.QueryRowContext(t.Context(), "SELECT IS_USED_LOCK(?)", relayLockName).Scan(&id); err != nil {
		t.Fatalf("error reading the relay lock's holder: %v", err)
	}
	return id.Int64, id.Valid
}

// killHolder ends the session holding the relay lock, as a crash or a lost connection would.
func (s *stack) killHolder(t *testing.T) {
	t.Helper()
	id, ok := s.holder(t)
	if !ok {
		t.Fatal("nobody holds the relay lock")
	}
	s.exec(t, fmt.Sprintf("KILL %d", id))
}

// waitUntilFree returns once no session holds the relay lock. A killed session ends asynchronously,
// so it waits for the lock in MySQL, on a session of its own, and then gives it back.
func (s *stack) waitUntilFree(t *testing.T) {
	t.Helper()
	conn, err := s.pool.Conn(t.Context())
	if err != nil {
		t.Fatalf("error getting a connection: %v", err)
	}
	defer conn.Close()
	var got, released sql.NullInt64
	if err := conn.QueryRowContext(t.Context(), "SELECT GET_LOCK(?, 10)", relayLockName).Scan(&got); err != nil ||
		got.Int64 != 1 {
		t.Fatalf("GET_LOCK() = %v, %v; want the lock within 10s", got, err)
	}
	if err := conn.QueryRowContext(t.Context(), "SELECT RELEASE_LOCK(?)", relayLockName).Scan(&released); err != nil ||
		released.Int64 != 1 {
		t.Fatalf("RELEASE_LOCK() = %v, %v; want 1", released, err)
	}
}

func (s *stack) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := s.pool.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
}

// rows returns every column of every row query reads, to tell whether a write changed any of them.
func (s *stack) rows(t *testing.T, query string, args ...any) []map[string]any {
	t.Helper()
	rows, err := s.pool.QueryContext(t.Context(), query, args...)
	if err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("error reading the columns of %q: %v", query, err)
	}
	var all []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("error reading %q: %v", query, err)
		}
		row := make(map[string]any, len(columns))
		for i, c := range columns {
			row[c] = values[i]
		}
		all = append(all, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error reading %q: %v", query, err)
	}
	return all
}

// storedDelivery is a delivery's row, apart from its lease columns.
type storedDelivery struct {
	EventID  db.ID
	TenantID uint64
	Body     []byte
	State    string
	Attempts int
}

func (s *stack) deliveries(t *testing.T) []storedDelivery {
	t.Helper()
	rows, err := s.pool.QueryContext(t.Context(),
		"SELECT event_id, tenant_id, body, state, attempts FROM webhook_deliveries ORDER BY id")
	if err != nil {
		t.Fatalf("error reading deliveries: %v", err)
	}
	defer rows.Close()
	var out []storedDelivery
	for rows.Next() {
		var d storedDelivery
		if err := rows.Scan(&d.EventID, &d.TenantID, &d.Body, &d.State, &d.Attempts); err != nil {
			t.Fatalf("error reading deliveries: %v", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error reading deliveries: %v", err)
	}
	return out
}

// TestInsertDeliveriesKeepsTheFirst inserts an event's delivery twice, with another body the second time:
// the event keeps one delivery, unchanged down to its timestamps, and the other event in the batch gets its own.
func TestInsertDeliveriesKeepsTheFirst(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t))
	first := delivery{EventID: uuid.NewV7(), TenantID: 7, Body: []byte(`{"title":"Rain"}`)}
	if err := s.repo.insertDeliveries(t.Context(), []delivery{first}); err != nil {
		t.Fatalf("insertDeliveries() error = %v", err)
	}
	const firstRow = "SELECT * FROM webhook_deliveries WHERE event_id = ?"
	before := s.rows(t, firstRow, db.ID(first.EventID))

	again := first
	again.Body = []byte(`{"title":"Changed"}`)
	other := delivery{EventID: uuid.NewV7(), TenantID: 8, Body: []byte(`{"title":"Thunder"}`)}
	if err := s.repo.insertDeliveries(t.Context(), []delivery{again, other}); err != nil {
		t.Fatalf("second insertDeliveries() error = %v", err)
	}

	if after := s.rows(t, firstRow, db.ID(first.EventID)); !reflect.DeepEqual(after, before) {
		t.Errorf("the first delivery changed from %v\nto %v", before, after)
	}
	want := []storedDelivery{
		{EventID: db.ID(first.EventID), TenantID: 7, Body: first.Body, State: "queued"},
		{EventID: db.ID(other.EventID), TenantID: 8, Body: other.Body, State: "queued"},
	}
	if got := s.deliveries(t); !reflect.DeepEqual(got, want) {
		t.Errorf("deliveries = %+v\nwant %+v", got, want)
	}
}

// TestRelayLockSession takes the relay lock on one pool and not on another, keeps it through a cancelled context,
// gives it back on release, and loses it when its session is discarded or killed.
func TestRelayLockSession(t *testing.T) {
	dsn := testutil.NewDatabase(t)
	s, other := newStack(t, dsn), newStack(t, dsn)
	take := func(t *testing.T, s *stack) relayLock {
		t.Helper()
		lock, ok, err := s.repo.lockRelay(t.Context())
		if err != nil || !ok {
			t.Fatalf("lockRelay() = %t, %v; want the lock", ok, err)
		}
		return lock
	}

	lock := take(t, s)
	if _, ok, err := other.repo.lockRelay(t.Context()); ok || err != nil {
		t.Errorf("lockRelay() on another pool = %t, %v; want false, nil", ok, err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := lock.check(cancelled); err != nil {
		t.Errorf("check() with a cancelled context: error = %v", err)
	}
	if err := lock.release(t.Context()); err != nil {
		t.Errorf("release() error = %v", err)
	}
	if id, held := s.holder(t); held {
		t.Errorf("session %d holds the lock after release", id)
	}

	t.Run("discarded", func(t *testing.T) {
		lock := take(t, other)
		discard(lock.(*sessionLock).conn)
		s.waitUntilFree(t)
		if err := lock.check(t.Context()); !errors.Is(err, errLockLost) {
			t.Errorf("check() error = %v, want %v", err, errLockLost)
		}
	})
	t.Run("killed", func(t *testing.T) {
		lock := take(t, s)
		s.killHolder(t)
		if err := lock.check(t.Context()); !errors.Is(err, errLockLost) {
			t.Errorf("check() error = %v, want %v", err, errLockLost)
		}
		s.waitUntilFree(t)
	})
}

func TestInsertDeliveriesErrors(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t))
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	tests := []struct {
		name string
		ctx  context.Context
		ds   []delivery
	}{
		{"context cancelled", cancelled, []delivery{{EventID: uuid.NewV7(), TenantID: 7, Body: []byte(`{}`)}}},
		{"body rejected", t.Context(), []delivery{{EventID: uuid.NewV7(), TenantID: 7, Body: make([]byte, 1<<24)}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := s.repo.insertDeliveries(tc.ctx, tc.ds)
			if !errors.Is(err, errDatabase) {
				t.Fatalf("insertDeliveries() error = %v, want %v", err, errDatabase)
			}
			if _, ok := errors.AsType[*mysql.MySQLError](err); ok {
				t.Errorf("the driver's error is reachable through %v", err)
			}
			if errors.Is(err, context.Canceled) {
				t.Errorf("%v is reachable through %v", context.Canceled, err)
			}
		})
	}
	if got := s.deliveries(t); len(got) != 0 {
		t.Errorf("deliveries = %+v, want none", got)
	}
}
