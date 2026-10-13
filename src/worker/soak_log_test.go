//go:build soak

package worker_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/zanvd/slumber-mill/src/worker"
)

const maxSamples = 5

type errorKind string

const (
	kindDeadlock   errorKind = "deadlock"
	kindLockWait   errorKind = "lock wait timeout"
	kindUnexpected errorKind = "unexpected"
	kindLeaseLost  errorKind = "lease lost"
	kindInjected   errorKind = "injected failure"
	kindHeartbeat  errorKind = "heartbeat timed out"
	kindShutdown   errorKind = "cancelled by shutdown"
	kindSlowQuery  errorKind = "slow query"
)

// errorLog is a slog.Handler that sorts every record carrying an error, and every warning, by kind.
// The pools, GORM and the driver all log through it, so it sees every error the soak runs into.
type errorLog struct {
	*errorStore
	attrs []slog.Attr
}

type errorStore struct {
	stopping atomic.Bool // set while the pools shut down

	mu      sync.Mutex
	counts  map[errorKind]int
	samples map[errorKind][]string
}

// record is what classify needs from a log record: the pool's attributes, and GORM's for a failed query.
type record struct {
	msg, sql, outcome string
	cause             any
}

func newErrorLog() errorLog {
	return errorLog{errorStore: &errorStore{counts: map[errorKind]int{}, samples: map[errorKind][]string{}}}
}

func (errorLog) Enabled(context.Context, slog.Level) bool { return true }

func (h errorLog) Handle(ctx context.Context, r slog.Record) error {
	rec := record{msg: r.Message}
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "error":
			rec.cause = a.Value.Any()
		case "sql":
			rec.sql = a.Value.String()
		case "outcome":
			rec.outcome = a.Value.String()
		}
		return true
	})
	if rec.cause == nil && r.Level < slog.LevelWarn {
		return nil
	}
	kind := h.classify(ctx, rec)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counts[kind]++
	if len(h.samples[kind]) < maxSamples {
		sample := fmt.Sprintf("%s %v: %v", rec.msg, h.attrs, rec.cause)
		if rec.sql != "" {
			sample += " in " + rec.sql
		}
		h.samples[kind] = append(h.samples[kind], sample)
	}
	return nil
}

func (h errorLog) WithAttrs(attrs []slog.Attr) slog.Handler {
	return errorLog{errorStore: h.errorStore, attrs: append(slices.Clip(h.attrs), attrs...)}
}

func (h errorLog) WithGroup(string) slog.Handler { return h }

// classify recognises the records the soak expects by where they come from as well as by their error,
// so a timeout or cancellation anywhere else is unexpected. ctx is the record's, which for a failed query
// is the query's own.
func (h errorLog) classify(ctx context.Context, rec record) errorKind {
	text := rec.msg
	if rec.cause != nil {
		text = fmt.Sprintf("%s: %v", rec.msg, rec.cause)
	}
	switch {
	case strings.Contains(text, "Error 1213"):
		return kindDeadlock
	case strings.Contains(text, "Error 1205"):
		return kindLockWait
	}
	err, _ := rec.cause.(error)
	stopping := h.stopping.Load()
	switch rec.msg {
	case "lease lost, cancelling the work", "lease lost before the outcome was written":
		if errors.Is(err, worker.ErrLeaseLost) {
			return kindLeaseLost
		}
	case "heartbeat failed":
		// The pool logs this only while the work goes on, so the deadline is the beat's own, a third of the lease.
		if errors.Is(err, context.DeadlineExceeded) {
			return kindHeartbeat
		}
	case "work finished":
		switch rec.outcome {
		case "discarded":
			// The pool discards only work whose lease was lost, whatever error the work stopped with.
			return kindLeaseLost
		case "released":
			// The pool releases work only once its context or the work's is cancelled, which only shutdown does.
			if stopping {
				return kindShutdown
			}
		case "failed":
			if errors.Is(err, errRetryable) || errors.Is(err, errRejected) {
				return kindInjected
			}
		}
	case "shutdown grace expired, cancelling the work":
		if stopping {
			return kindShutdown
		}
	case "query failed":
		switch {
		// A heartbeat that finds the lease lost cancels the work, interrupting the statement completing it.
		case errors.Is(context.Cause(ctx), worker.ErrLeaseLost):
			return kindLeaseLost
		case isExtend(rec.sql) && errors.Is(ctx.Err(), context.DeadlineExceeded):
			return kindHeartbeat
		// Shutdown cancels the claims, the reaps and, after the grace, the work. Whatever the driver then reports
		// for a statement whose context has ended is the cancellation's doing: the context's error, a transaction
		// rolled back under its next statement, or a connection the driver closed as a statement finished.
		case stopping && ctx.Err() != nil:
			return kindShutdown
		}
	case "slow query":
		if rec.cause == nil {
			return kindSlowQuery
		}
	}
	return kindUnexpected
}

// isExtend reports whether sql is a heartbeat's: the only update that sets lease_expires_at alone.
func isExtend(sql string) bool {
	return strings.HasPrefix(sql, "UPDATE ") &&
		strings.Contains(sql, " SET `lease_expires_at`=NOW(6) + INTERVAL ? MICROSECOND WHERE ")
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
