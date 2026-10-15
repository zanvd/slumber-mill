//go:build integration

package webhook

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"gorm.io/gorm"

	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/testutil"
)

const retentionAge = 30 * 24 * time.Hour

// countingRepo records how many deliveries each retention batch deleted, and runs after, if set, after each batch.
type countingRepo struct {
	*Repository
	deleted []int
	after   func(batch int)
}

func (c *countingRepo) deleteDelivered(ctx context.Context, age time.Duration, after deliveryCursor, limit int) (
	int, deliveryCursor, bool, error) {
	n, next, full, err := c.Repository.deleteDelivered(ctx, age, after, limit)
	c.deleted = append(c.deleted, n)
	if c.after != nil {
		c.after(len(c.deleted))
	}
	return n, next, full, err
}

// retentionDeliverer returns a deliverer over r that deletes batch deliveries at a time.
func retentionDeliverer(t *testing.T, r deliveryRepo, batch int) *Deliverer {
	t.Helper()
	d, err := NewDeliverer(r, &fakeTenants{}, &fakePoster{}, DelivererConfig{WorkerID: "webhook-1", Lease: time.Minute})
	if err != nil {
		t.Fatalf("NewDeliverer() error = %v", err)
	}
	d.retentionBatch = batch
	return d
}

// addDelivery stores a delivery of tenant 7 in state, due at runAfter and last changed at updatedAt,
// both SQL expressions, and returns its ID.
func (s *stack) addDelivery(t *testing.T, state, runAfter, updatedAt string) uint64 {
	t.Helper()
	res, err := s.pool.ExecContext(t.Context(),
		"INSERT INTO webhook_deliveries (event_id, tenant_id, body, state, attempts, run_after, updated_at) "+
			"VALUES (?, 7, ?, ?, 1, "+runAfter+", "+updatedAt+")",
		db.ID(uuid.NewV7()), []byte(`{"id":"event"}`), state)
	if err != nil {
		t.Fatalf("error inserting a %s delivery: %v", state, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("error reading the delivery's ID: %v", err)
	}
	return uint64(id)
}

func (s *stack) deliveryIDs(t *testing.T) []uint64 {
	t.Helper()
	rows, err := s.pool.QueryContext(t.Context(), "SELECT id FROM webhook_deliveries ORDER BY id")
	if err != nil {
		t.Fatalf("error reading deliveries: %v", err)
	}
	defer rows.Close()
	var ids []uint64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("error reading deliveries: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error reading deliveries: %v", err)
	}
	return ids
}

// retentionFixture holds deliveries in every state on either side of the retention age. Read lists whether
// each delivered one that a batch reads, in the order batches read them, is past the age; kept lists the IDs
// of the deliveries that must stay, in order.
type retentionFixture struct {
	read []bool
	kept []uint64
}

func newRetentionFixture(t *testing.T, s *stack) retentionFixture {
	t.Helper()
	var f retentionFixture
	expired := func(runAfter, updatedAt string) {
		s.addDelivery(t, "delivered", runAfter, updatedAt)
		f.read = append(f.read, true)
	}
	keep := func(state, runAfter, updatedAt string) uint64 {
		id := s.addDelivery(t, state, runAfter, updatedAt)
		f.kept = append(f.kept, id)
		return id
	}
	// Deliveries due at the same time are ordered by ID, and a batch can stop between them.
	for range 5 {
		expired("'2020-01-01 00:00:00.000001'", "'2020-01-01 00:00:05'")
	}
	expired("NOW(6) - INTERVAL 40 DAY - INTERVAL 1 MINUTE", "NOW(6) - INTERVAL 40 DAY")
	expired("NOW(6) - INTERVAL 31 DAY - INTERVAL 1 HOUR", "NOW(6) - INTERVAL 31 DAY")
	expired("NOW(6) - INTERVAL 31 DAY", "NOW(6) - INTERVAL 31 DAY")
	// Due long ago but delivered recently, as after a backlog: read, but kept.
	keep("delivered", "NOW(6) - INTERVAL 30 DAY - INTERVAL 2 HOUR", "NOW(6) - INTERVAL 30 DAY + INTERVAL 1 HOUR")
	f.read = append(f.read, false)
	expired("NOW(6) - INTERVAL 30 DAY - INTERVAL 1 HOUR - INTERVAL 1 MINUTE", "NOW(6) - INTERVAL 30 DAY - INTERVAL 1 HOUR")
	keep("delivered", "NOW(6) - INTERVAL 29 DAY", "NOW(6) - INTERVAL 29 DAY")
	keep("delivered", "NOW(6) - INTERVAL 1 MINUTE", "NOW(6)")
	for _, state := range []string{"queued", "sending", "failed"} {
		keep(state, "NOW(6) - INTERVAL 40 DAY", "NOW(6) - INTERVAL 40 DAY")
	}
	return f
}

// expired returns how many of the deliveries read are past the age.
func (f retentionFixture) expired() int {
	n := 0
	for _, e := range f.read {
		if e {
			n++
		}
	}
	return n
}

// TestDeleteDeliveredIntegration deletes, at several batch sizes, only the deliveries delivered longer ago than
// the age: newer ones and those queued, sending or failed stay, however old. Each batch deletes what it read
// that is past the age.
func TestDeleteDeliveredIntegration(t *testing.T) {
	for _, batch := range []int{1, 2, 3, 4, 10, defaultRetentionBatch} {
		t.Run(fmt.Sprintf("batches of %d", batch), func(t *testing.T) {
			s := newStack(t, testutil.NewDatabase(t))
			f := newRetentionFixture(t, s)
			c := &countingRepo{Repository: s.repo}

			deleted, err := retentionDeliverer(t, c, batch).DeleteDelivered(t.Context(), retentionAge)

			if err != nil || deleted != f.expired() {
				t.Fatalf("DeleteDelivered() = %d, %v; want %d", deleted, err, f.expired())
			}
			var want []int
			for chunk := range slices.Chunk(f.read, batch) {
				n := 0
				for _, e := range chunk {
					if e {
						n++
					}
				}
				want = append(want, n)
			}
			if len(f.read)%batch == 0 {
				want = append(want, 0)
			}
			if !slices.Equal(c.deleted, want) {
				t.Errorf("batches deleted %v, want %v", c.deleted, want)
			}
			if got := s.deliveryIDs(t); !slices.Equal(got, f.kept) {
				t.Errorf("deliveries left %v, want %v", got, f.kept)
			}

			again, err := retentionDeliverer(t, s.repo, batch).DeleteDelivered(t.Context(), retentionAge)
			if err != nil || again != 0 {
				t.Errorf("DeleteDelivered() again = %d, %v; want 0", again, err)
			}
		})
	}
}

// TestDeleteDeliveredCancelled cancels the context after the second batch: the loop stops there,
// having deleted the oldest deliveries, and the rest stay until the next run.
func TestDeleteDeliveredCancelled(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t))
	f := newRetentionFixture(t, s)
	before := s.deliveryIDs(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := &countingRepo{Repository: s.repo, after: func(batch int) {
		if batch == 2 {
			cancel()
		}
	}}

	deleted, err := retentionDeliverer(t, c, 2).DeleteDelivered(ctx, retentionAge)

	if deleted != 4 || !errors.Is(err, context.Canceled) {
		t.Errorf("DeleteDelivered() = %d, %v; want 4, %v", deleted, err, context.Canceled)
	}
	if !slices.Equal(c.deleted, []int{2, 2}) {
		t.Errorf("batches deleted %v, want [2 2]", c.deleted)
	}
	if got, want := s.deliveryIDs(t), before[4:]; !slices.Equal(got, want) {
		t.Errorf("deliveries left %v, want %v", got, want)
	}
	if left := len(s.deliveryIDs(t)) - len(f.kept); left != f.expired()-4 {
		t.Errorf("%d deliveries past the age left, want %d", left, f.expired()-4)
	}
}

type recorded struct {
	sql  string
	vars []any
}

// recordStatements records the SQL and values of every query and delete GORM runs on gdb from now on.
func recordStatements(t *testing.T, gdb *gorm.DB) *[]recorded {
	t.Helper()
	var list []recorded
	record := func(tx *gorm.DB) {
		list = append(list, recorded{tx.Statement.SQL.String(), slices.Clone(tx.Statement.Vars)})
	}
	cb := gdb.Callback()
	for _, err := range []error{
		cb.Query().After("gorm:query").Register("test:explain", record),
		cb.Delete().After("gorm:delete").Register("test:explain", record),
	} {
		if err != nil {
			t.Fatalf("error registering a recording callback: %v", err)
		}
	}
	return &list
}

// explain returns EXPLAIN's rows for the statement, each column as text, and logs them.
func (s *stack) explain(t *testing.T, r recorded) []map[string]string {
	t.Helper()
	var plan []map[string]string
	for _, row := range s.rows(t, "EXPLAIN "+r.sql, r.vars...) {
		text := make(map[string]string, len(row))
		for k, v := range row {
			if b, ok := v.([]byte); ok {
				text[k] = string(b)
			} else if v != nil {
				text[k] = fmt.Sprint(v)
			}
		}
		plan = append(plan, text)
	}
	t.Logf("EXPLAIN %s %v\n%v", r.sql, r.vars, plan)
	return plan
}

// TestDeleteDeliveredUsesTheIndex runs retention over a thousand deliveries of 2 KiB, in every state and mostly
// newer than the age, then refills the table as it was and explains each statement the retention ran: every batch
// reads a range of ix_webhook_deliveries_claim from the index alone, in its order, and deletes by primary key.
func TestDeleteDeliveredUsesTheIndex(t *testing.T) {
	s := newStack(t, testutil.NewDatabase(t))
	fill := func() {
		s.exec(t, "INSERT INTO webhook_deliveries (event_id, tenant_id, body, state, attempts, run_after, updated_at) "+
			"WITH RECURSIVE n (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1000) "+
			"SELECT UUID_TO_BIN(UUID()), 7, REPEAT('x', 2048), ELT(i % 5 + 1, 'queued', 'failed', 'sending', "+
			"'delivered', 'delivered'), 1, NOW(6) - INTERVAL i HOUR - INTERVAL 1 MINUTE, NOW(6) - INTERVAL i HOUR "+
			"FROM n ORDER BY i")
		s.exec(t, "ANALYZE TABLE webhook_deliveries")
	}
	fill()
	var expired int
	if err := s.pool.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM webhook_deliveries "+
		"WHERE state = 'delivered' AND updated_at < NOW(6) - INTERVAL 30 DAY").Scan(&expired); err != nil {
		t.Fatalf("error counting expired deliveries: %v", err)
	}
	st := recordStatements(t, s.gdb)
	const batch = 50

	deleted, err := retentionDeliverer(t, s.repo, batch).DeleteDelivered(t.Context(), retentionAge)

	if err != nil || deleted != expired || expired < 2*batch {
		t.Fatalf("DeleteDelivered() = %d, %v; want all %d expired, at least %d", deleted, err, expired, 2*batch)
	}
	s.exec(t, "TRUNCATE TABLE webhook_deliveries")
	fill()
	var reads, deletes int
	for _, r := range *st {
		plan := s.explain(t, r)
		if len(plan) != 1 {
			t.Errorf("EXPLAIN %s has %d rows, want 1", r.sql, len(plan))
			continue
		}
		p := plan[0]
		switch {
		case strings.HasPrefix(r.sql, "SELECT "):
			reads++
			if p["type"] != "range" || p["key"] != "ix_webhook_deliveries_claim" ||
				!strings.Contains(p["Extra"], "Using index") || strings.Contains(p["Extra"], "filesort") {
				t.Errorf("%s\nplan %v, want an index-only range scan of ix_webhook_deliveries_claim without a sort",
					r.sql, p)
			}
		case strings.HasPrefix(r.sql, "DELETE "):
			deletes++
			if p["type"] != "range" || p["key"] != "PRIMARY" {
				t.Errorf("%s\nplan %v, want a range of the primary key", r.sql, p)
			}
		default:
			t.Errorf("unexpected statement %s", r.sql)
		}
	}
	if wantReads, wantDeletes := expired/batch+1, (expired+batch-1)/batch; reads != wantReads || deletes != wantDeletes {
		t.Errorf("ran %d reads and %d deletes, want %d and %d", reads, deletes, wantReads, wantDeletes)
	}
}
