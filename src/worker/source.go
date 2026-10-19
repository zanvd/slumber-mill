package worker

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/trace"
)

// ErrLeaseLost means a fenced write matched no row: the lease expired and was reaped, or another worker holds it.
var ErrLeaseLost = errors.New("lease lost")

// source is a table of leased work, which a Pool claims from and drives through each lease to its outcome.
type source interface {
	Claim(ctx context.Context) (Lease, bool, error)       // false: nothing is due
	Extend(ctx context.Context, l Lease) error            // heartbeat; ErrLeaseLost when fenced out
	Process(ctx context.Context, l Lease) error           // does the work, then completes it, fenced
	Fail(ctx context.Context, l Lease, cause error) error // retry with backoff, or give up
	Release(ctx context.Context, l Lease) error
	Reap(ctx context.Context) error
}

// tracedSource is a source whose work may continue a trace stored with it.
type tracedSource interface {
	// Trace returns ctx with the span of the lease's work, or a nil span if it has none.
	// The pool ends the span, with the lease's outcome, once the outcome is written.
	Trace(ctx context.Context, l Lease) (context.Context, trace.Span)
}

type Lease struct {
	ID, TenantID uint64
	Attempt      int
}

type leaseLostError struct {
	err error
}

func (e *leaseLostError) Error() string { return e.err.Error() }
func (e *leaseLostError) Unwrap() error { return ErrLeaseLost }

// LeaseLost wraps a repository's lease-lost error so that it matches ErrLeaseLost and keeps its message.
// LeaseLost(nil) is nil.
func LeaseLost(err error) error {
	if err == nil {
		return nil
	}
	return &leaseLostError{err: err}
}

type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps errors that retrying can't fix. Permanent(nil) is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or any error it wraps, came from Permanent.
func IsPermanent(err error) bool {
	_, ok := errors.AsType[*permanentError](err)
	return ok
}
