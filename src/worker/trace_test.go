package worker

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// tracedFake is a fakeSource whose Trace starts each lease's span from tracer, or gives it none when tracer is nil.
type tracedFake struct {
	*fakeSource
	tracer trace.Tracer
}

func (f *tracedFake) Trace(ctx context.Context, l Lease) (context.Context, trace.Span) {
	if f.tracer == nil {
		return ctx, nil
	}
	return f.tracer.Start(ctx, "work", trace.WithAttributes(attribute.Int("attempt", l.Attempt)))
}

// TestTracedOutcomes runs a lease of a traced source to each outcome: the pool ends the span the source started
// with the outcome, after writing it, and an attempt that failed as an error. The work, the heartbeats and the
// outcome's write run under the span, and the lease's log lines carry its IDs.
func TestTracedOutcomes(t *testing.T) {
	lost := fmt.Errorf("error completing: %w", ErrLeaseLost)
	tests := []struct {
		name      string
		drain     bool
		err       error
		want      outcome
		wantError bool
	}{
		{"done", false, nil, outcomeDone, false},
		{"failed", false, errBoom, outcomeFail, true},
		{"lease lost", false, lost, outcomeDiscard, false},
		{"released while draining", true, errBoom, outcomeRelease, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := tracetest.NewSpanRecorder()
				tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
				cfg, logs := testConfig(t)
				src := &tracedFake{fakeSource: newFakeSource(lease), tracer: tp.Tracer("test")}
				proceed := make(chan struct{})
				src.process = func(ctx context.Context, _ Lease) error {
					if err := busy(ctx, 15*time.Second); err != nil {
						return err
					}
					<-proceed
					return tc.err
				}
				r := startPool(t, src, cfg)
				synctest.Sleep(20 * time.Second)
				if n := len(rec.Ended()); n != 0 {
					t.Errorf("%d spans ended while the work ran, want none", n)
				}
				if tc.drain {
					r.cancel()
					synctest.Wait()
					close(proceed)
					r.wait()
				} else {
					close(proceed)
					synctest.Sleep(time.Minute)
					r.stop()
				}

				spans := rec.Ended()
				if len(spans) != 1 {
					t.Fatalf("ended %d spans, want the lease's", len(spans))
				}
				span := spans[0]
				var got string
				for _, kv := range span.Attributes() {
					if kv.Key == attrOutcome {
						got = kv.Value.AsString()
					}
				}
				if got != string(tc.want) {
					t.Errorf("%s = %q, want %q", attrOutcome, got, tc.want)
				}
				if isError := span.Status().Code == codes.Error; isError != tc.wantError {
					t.Errorf("status = %+v, want an error %t", span.Status(), tc.wantError)
				}

				calls := src.calls()
				underSpan := append(append(append(calls.processed, calls.beats...), calls.fails...), calls.releases...)
				if len(calls.beats) == 0 {
					t.Error("no heartbeats, want one while the work ran")
				}
				for _, c := range underSpan {
					if !c.span.Equal(span.SpanContext()) {
						t.Errorf("a call at %v ran under span %s, want the lease's %s", c.at, c.span.SpanID(),
							span.SpanContext().SpanID())
					}
				}
				sc := span.SpanContext()
				for _, msg := range []string{"work claimed", "work finished"} {
					recs := logs.find(t, msg)
					if len(recs) != 1 || recs[0]["trace_id"] != sc.TraceID().String() ||
						recs[0]["span_id"] != sc.SpanID().String() {
						t.Errorf("%q records = %v, want one with trace %s and span %s", msg, recs, sc.TraceID(),
							sc.SpanID())
					}
				}
			})
		})
	}
}

// TestUntracedLeases runs a lease whose span isn't sampled, and one the source gives no span: nothing is recorded,
// and the lease's log lines carry no trace IDs.
func TestUntracedLeases(t *testing.T) {
	tests := []struct {
		name    string
		sampler sdktrace.Sampler
		noSpan  bool
	}{
		{name: "not sampled", sampler: sdktrace.NeverSample()},
		{name: "no span", noSpan: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := tracetest.NewSpanRecorder()
				tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(tc.sampler), sdktrace.WithSpanProcessor(rec))
				cfg, logs := testConfig(t)
				src := &tracedFake{fakeSource: newFakeSource(lease)}
				if !tc.noSpan {
					src.tracer = tp.Tracer("test")
				}
				src.process = func(context.Context, Lease) error { return errBoom }
				r := startPool(t, src, cfg)
				synctest.Sleep(time.Minute)
				r.stop()

				if n := len(rec.Started()); n != 0 {
					t.Errorf("recorded %d spans, want none", n)
				}
				if calls := src.calls(); len(calls.fails) != 1 {
					t.Errorf("failed %d times, want once", len(calls.fails))
				}
				for _, msg := range []string{"work claimed", "work finished"} {
					recs := logs.find(t, msg)
					if len(recs) != 1 {
						t.Fatalf("%q records = %v, want one", msg, recs)
					}
					if _, ok := recs[0]["trace_id"]; ok {
						t.Errorf("%q record = %v, want no trace ID", msg, recs[0])
					}
				}
			})
		})
	}
}
