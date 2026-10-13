//go:build soak

package worker_test

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/worker"
)

const maxSamples = 5

type errorKind string

const (
	kindDeadlock   errorKind = "deadlock"
	kindLockWait   errorKind = "lock wait timeout"
	kindUnexpected errorKind = "unexpected"
	kindLeaseLost  errorKind = "lease lost"
	kindInjected   errorKind = "injected"
	kindTimedOut   errorKind = "timed out"
	kindCancelled  errorKind = "cancelled"
	kindSlowQuery  errorKind = "slow query"
)

// errorLog is a slog.Handler that sorts every record carrying an error, and every warning, by kind.
// The pools, GORM and the driver all log through it, so it sees every error the soak runs into.
type errorLog struct {
	*errorStore
	attrs []slog.Attr
}

type errorStore struct {
	mu      sync.Mutex
	counts  map[errorKind]int
	samples map[errorKind][]string
}

func newErrorLog() errorLog {
	return errorLog{errorStore: &errorStore{counts: map[errorKind]int{}, samples: map[errorKind][]string{}}}
}

func (errorLog) Enabled(context.Context, slog.Level) bool { return true }

func (h errorLog) Handle(ctx context.Context, r slog.Record) error {
	var cause any
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "error" {
			cause = a.Value.Any()
			return false
		}
		return true
	})
	if cause == nil && r.Level < slog.LevelWarn {
		return nil
	}
	kind := classify(ctx, r.Message, cause)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counts[kind]++
	if len(h.samples[kind]) < maxSamples {
		h.samples[kind] = append(h.samples[kind], fmt.Sprintf("%s %v: %v", r.Message, h.attrs, cause))
	}
	return nil
}

func (h errorLog) WithAttrs(attrs []slog.Attr) slog.Handler {
	return errorLog{errorStore: h.errorStore, attrs: append(slices.Clip(h.attrs), attrs...)}
}

func (h errorLog) WithGroup(string) slog.Handler { return h }

// classify works on the message as well as the error, because GORM logs failed queries with the error as a string.
// Whatever it doesn't recognise is unexpected. ctx is the record's, which for a failed query is the query's.
func classify(ctx context.Context, msg string, cause any) errorKind {
	text := msg
	if cause != nil {
		text = fmt.Sprintf("%s: %v", msg, cause)
	}
	err, _ := cause.(error)
	has := func(s string) bool { return strings.Contains(text, s) }
	switch {
	case has("Error 1213"):
		return kindDeadlock
	case has("Error 1205"):
		return kindLockWait
	case errors.Is(err, worker.ErrLeaseLost), errors.Is(err, db.ErrLeaseLost), has("lease lost"):
		return kindLeaseLost
	case errors.Is(err, errRetryable), errors.Is(err, errRejected):
		return kindInjected
	// The DSN sets no I/O timeouts, so an i/o timeout is a context's deadline passing while a connection is dialled,
	// which the net package reports that way; and a cancelled dial is an operation that was canceled.
	case errors.Is(err, context.DeadlineExceeded), has(context.DeadlineExceeded.Error()), has("i/o timeout"):
		return kindTimedOut
	// A transaction whose context is cancelled is rolled back, so its next statement finds it done.
	case errors.Is(err, context.Canceled), has(context.Canceled.Error()), has("operation was canceled"),
		has(sql.ErrTxDone.Error()), has("shutdown grace expired"):
		return kindCancelled
	// The driver closes a connection when its context ends, even as a statement on it finishes,
	// so the transaction's next statement finds the connection bad.
	case ctx.Err() != nil && has(driver.ErrBadConn.Error()):
		return kindCancelled
	case cause == nil && msg == "slow query":
		return kindSlowQuery
	default:
		return kindUnexpected
	}
}

func (s *errorStore) count(kind errorKind) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[kind]
}

func (s *errorStore) examples(kind errorKind) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.samples[kind])
}

// String lists the counts by kind, most frequent first.
func (s *errorStore) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	kinds := slices.SortedFunc(maps.Keys(s.counts), func(a, b errorKind) int {
		return cmp.Or(cmp.Compare(s.counts[b], s.counts[a]), cmp.Compare(a, b))
	})
	if len(kinds) == 0 {
		return "none"
	}
	parts := make([]string, len(kinds))
	for i, k := range kinds {
		parts[i] = fmt.Sprintf("%s %d", k, s.counts[k])
	}
	return strings.Join(parts, ", ")
}
