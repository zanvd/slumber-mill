//go:build integration

package asset

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/zanvd/slumber-mill/src/db"
)

const retentionAge = 30 * 24 * time.Hour

// countingRepo records how many events each retention batch deleted, and runs after, if set, after each batch.
type countingRepo struct {
	*Repository
	deleted []int
	after   func(batch int)
}

func (c *countingRepo) deleteRelayedEvents(ctx context.Context, age time.Duration, after eventCursor, limit int) (
	int, eventCursor, bool, error) {
	n, next, full, err := c.Repository.deleteRelayedEvents(ctx, age, after, limit)
	c.deleted = append(c.deleted, n)
	if c.after != nil {
		c.after(len(c.deleted))
	}
	return n, next, full, err
}

// retentionService returns a service over c that deletes batch events at a time.
func (s *stack) retentionService(c *countingRepo, batch int) *Service {
	svc := NewService(c, s.store)
	svc.retentionBatch = batch
	return svc
}

// addEvent records an event of the asset relayed at relayedAt, an SQL expression, and returns its ID.
func (s *stack) addEvent(t *testing.T, assetID uint64, relayedAt string) uint64 {
	t.Helper()
	res, err := s.pool.ExecContext(t.Context(),
		"INSERT INTO asset_events (event_id, tenant_id, asset_id, type, relayed_at) "+
			"SELECT ?, tenant_id, id, 'asset.ready', "+relayedAt+" FROM assets WHERE id = ?", db.NewID(), assetID)
	if err != nil {
		t.Fatalf("error inserting an event relayed at %s: %v", relayedAt, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("error reading the event's ID: %v", err)
	}
	return uint64(id)
}

// eventIDs returns the IDs of the events left, oldest relayed first and unrelayed last.
func (s *stack) eventIDs(t *testing.T) []uint64 {
	t.Helper()
	rows, err := s.pool.QueryContext(t.Context(),
		"SELECT id FROM asset_events ORDER BY relayed_at IS NULL, relayed_at, id")
	if err != nil {
		t.Fatalf("error reading events: %v", err)
	}
	defer rows.Close()
	var ids []uint64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("error reading events: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error reading events: %v", err)
	}
	return ids
}

// retentionFixture holds events of two tenants on either side of the retention age. Expired lists the events
// past it, in the order they are deleted, and kept the rest, in the order eventIDs lists them.
type retentionFixture struct {
	expired, kept []uint64
}

func newRetentionFixture(t *testing.T, s *stack) retentionFixture {
	t.Helper()
	_, a := s.queue(t, s.tenant(t, "owner"))
	_, b := s.queue(t, s.tenant(t, "other"))
	var f retentionFixture
	// Events relayed at the same time are ordered by ID, and a batch can stop between them.
	for range 5 {
		f.expired = append(f.expired, s.addEvent(t, a, "'2020-01-01 00:00:00.000001'"))
	}
	f.expired = append(f.expired,
		s.addEvent(t, b, "NOW(6) - INTERVAL 40 DAY"),
		s.addEvent(t, a, "NOW(6) - INTERVAL 31 DAY"),
		s.addEvent(t, b, "NOW(6) - INTERVAL 31 DAY"),
		s.addEvent(t, a, "NOW(6) - INTERVAL 30 DAY - INTERVAL 1 HOUR"),
	)
	f.kept = append(f.kept,
		s.addEvent(t, b, "NOW(6) - INTERVAL 30 DAY + INTERVAL 1 HOUR"),
		s.addEvent(t, a, "NOW(6)"),
		s.addEvent(t, a, "NULL"),
		s.addEvent(t, b, "NULL"),
	)
	s.exec(t, "UPDATE asset_events SET created_at = NOW(6) - INTERVAL 400 DAY WHERE relayed_at IS NULL")
	return f
}

// TestDeleteRelayedEventsIntegration deletes, across tenants and at several batch sizes, only the events relayed
// longer ago than the age: newer and unrelayed ones stay, however old. Every full batch deletes a whole batch.
func TestDeleteRelayedEventsIntegration(t *testing.T) {
	for _, batch := range []int{1, 2, 3, 4, 9, defaultRetentionBatch} {
		t.Run(fmt.Sprintf("batches of %d", batch), func(t *testing.T) {
			s := newStack(t)
			f := newRetentionFixture(t, s)
			c := &countingRepo{Repository: s.repo}

			deleted, err := s.retentionService(c, batch).DeleteRelayedEvents(t.Context(), retentionAge)

			if err != nil || deleted != len(f.expired) {
				t.Fatalf("DeleteRelayedEvents() = %d, %v; want %d", deleted, err, len(f.expired))
			}
			var want []int
			for rest := len(f.expired); ; rest -= batch {
				want = append(want, min(rest, batch))
				if rest < batch {
					break
				}
			}
			if !slices.Equal(c.deleted, want) {
				t.Errorf("batches deleted %v, want %v", c.deleted, want)
			}
			if got := s.eventIDs(t); !slices.Equal(got, f.kept) {
				t.Errorf("events left %v, want %v", got, f.kept)
			}

			again, err := s.svc.DeleteRelayedEvents(t.Context(), retentionAge)
			if err != nil || again != 0 {
				t.Errorf("DeleteRelayedEvents() again = %d, %v; want 0", again, err)
			}
		})
	}
}

// TestDeleteRelayedEventsCancelled cancels the context after the second batch: the loop stops there,
// having deleted the oldest events, and the rest stay until the next run.
func TestDeleteRelayedEventsCancelled(t *testing.T) {
	s := newStack(t)
	f := newRetentionFixture(t, s)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := &countingRepo{Repository: s.repo, after: func(batch int) {
		if batch == 2 {
			cancel()
		}
	}}

	deleted, err := s.retentionService(c, 2).DeleteRelayedEvents(ctx, retentionAge)

	if deleted != 4 || !errors.Is(err, context.Canceled) {
		t.Errorf("DeleteRelayedEvents() = %d, %v; want 4, %v", deleted, err, context.Canceled)
	}
	if !slices.Equal(c.deleted, []int{2, 2}) {
		t.Errorf("batches deleted %v, want [2 2]", c.deleted)
	}
	if got, want := s.eventIDs(t), append(slices.Clone(f.expired[4:]), f.kept...); !slices.Equal(got, want) {
		t.Errorf("events left %v, want %v", got, want)
	}
}

// statements records the SQL and values of every query and delete GORM runs on gdb from now on.
type statements struct {
	list []recorded
}

type recorded struct {
	sql  string
	vars []any
}

func recordStatements(t *testing.T, gdb *gorm.DB) *statements {
	t.Helper()
	st := &statements{}
	record := func(tx *gorm.DB) {
		st.list = append(st.list, recorded{tx.Statement.SQL.String(), slices.Clone(tx.Statement.Vars)})
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
	return st
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

// TestDeleteRelayedEventsUsesTheIndex runs retention over a thousand events, mostly unrelayed or newer than the age,
// then refills the table as it was and explains each statement the retention ran: every batch reads a range of
// ix_asset_events_pending from the index alone, in its order, and deletes by primary key.
func TestDeleteRelayedEventsUsesTheIndex(t *testing.T) {
	s := newStack(t)
	_, a := s.queue(t, s.tenant(t, "owner"))
	fill := func() {
		s.exec(t, "INSERT INTO asset_events (event_id, tenant_id, asset_id, type, relayed_at) "+
			"WITH RECURSIVE n (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1000) "+
			"SELECT UUID_TO_BIN(UUID()), tenant_id, id, 'asset.ready', "+
			"IF(i % 4 = 0, NULL, NOW(6) - INTERVAL i HOUR) FROM assets, n WHERE assets.id = ? ORDER BY i", a)
		s.exec(t, "ANALYZE TABLE asset_events")
	}
	fill()
	var expired int
	if err := s.pool.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM asset_events "+
		"WHERE relayed_at < NOW(6) - INTERVAL 30 DAY").Scan(&expired); err != nil {
		t.Fatalf("error counting expired events: %v", err)
	}
	st := recordStatements(t, s.gdb)
	svc := NewService(s.repo, s.store)
	svc.retentionBatch = 100

	deleted, err := svc.DeleteRelayedEvents(t.Context(), retentionAge)

	if err != nil || deleted != expired || expired < 200 {
		t.Fatalf("DeleteRelayedEvents() = %d, %v; want all %d expired, at least 200", deleted, err, expired)
	}
	s.exec(t, "TRUNCATE TABLE asset_events")
	fill()
	var reads, deletes int
	for _, r := range st.list {
		plan := s.explain(t, r)
		if len(plan) != 1 {
			t.Errorf("EXPLAIN %s has %d rows, want 1", r.sql, len(plan))
			continue
		}
		p := plan[0]
		switch {
		case strings.HasPrefix(r.sql, "SELECT "):
			reads++
			if p["type"] != "range" || p["key"] != "ix_asset_events_pending" ||
				!strings.Contains(p["Extra"], "Using index") || strings.Contains(p["Extra"], "filesort") {
				t.Errorf("%s\nplan %v, want an index-only range scan of ix_asset_events_pending without a sort", r.sql, p)
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
	if wantReads := expired/100 + 1; reads != wantReads || deletes != (expired+99)/100 {
		t.Errorf("ran %d reads and %d deletes, want %d and %d", reads, deletes, wantReads, (expired+99)/100)
	}
}
