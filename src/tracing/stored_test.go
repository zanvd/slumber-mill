package tracing

import (
	"context"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestStoredContext(t *testing.T) {
	long := func(key string) string { return key + "=" + strings.Repeat("v", 250) }
	tests := []struct {
		name       string
		tp         trace.TracerProvider
		headers    map[string]string
		wantStored bool
		// wantState is the tracestate stored after the traceparent.
		wantState string
	}{
		{
			name:       "Checkly's request",
			headers:    map[string]string{"traceparent": traceparent("01"), "tracestate": "checkly=true"},
			wantStored: true,
			wantState:  "checkly=true",
		},
		{
			name:       "other vendors' members are kept",
			headers:    map[string]string{"traceparent": traceparent("01"), "tracestate": "vendor=abc,checkly=true"},
			wantStored: true,
			wantState:  "vendor=abc,checkly=true",
		},
		{
			name: "too long: the last members go first",
			headers: map[string]string{"traceparent": traceparent("01"),
				"tracestate": long("a") + ",checkly=true," + long("b")},
			wantStored: true,
			wantState:  long("a") + ",checkly=true",
		},
		{
			name: "too long: Checkly's member stays",
			headers: map[string]string{"traceparent": traceparent("01"),
				"tracestate": long("a") + "," + long("b") + ",checkly=true"},
			wantStored: true,
			wantState:  long("a") + ",checkly=true",
		},
		{
			name:    "not sampled",
			headers: map[string]string{"traceparent": traceparent("01"), "tracestate": "vendor=abc"},
		},
		{name: "no parent"},
		{
			name:    "tracing off",
			tp:      noop.NewTracerProvider(),
			headers: map[string]string{"traceparent": traceparent("01"), "tracestate": "checkly=true"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tp := tt.tp
			if tp == nil {
				tp = sdktrace.NewTracerProvider(sdktrace.WithSampler(Sampler()))
			}
			ctx, span := startFromHeaders(t, tp, tt.headers)
			defer span.End()

			got := StoredContext(ctx)

			if !tt.wantStored {
				if got != "" {
					t.Errorf("StoredContext() = %q, want empty", got)
				}
				return
			}
			want := "00-" + traceID + "-" + span.SpanContext().SpanID().String() + "-01 " + tt.wantState
			if got != want {
				t.Errorf("StoredContext() = %q\nwant %q", got, want)
			}
			if len(got) > maxStoredContext {
				t.Errorf("StoredContext() is %d bytes, more than %d", len(got), maxStoredContext)
			}
		})
	}
	t.Run("no span", func(t *testing.T) {
		if got := StoredContext(t.Context()); got != "" {
			t.Errorf("StoredContext() = %q, want empty", got)
		}
	})
}

// TestContinueStored continues a stored context in a fresh context, as a worker pool does: a span started from
// it is the stored span's child in its trace, and the sampler keeps only a context Checkly started.
func TestContinueStored(t *testing.T) {
	const storedSpanID = "a3ce929d0e0e4736"
	stored := "00-" + traceID + "-" + storedSpanID + "-01"
	tests := []struct {
		name        string
		stored      string
		wantParent  bool
		wantSampled bool
	}{
		{"Checkly's", stored + " checkly=true", true, true},
		{"among other members", stored + " vendor=abc,checkly=true", true, true},
		{"without Checkly's member", stored + " vendor=abc", true, false},
		{"without a tracestate", stored, true, false},
		{"not sampled", "00-" + traceID + "-" + storedSpanID + "-00 checkly=true", true, false},
		{"empty", "", false, false},
		{"malformed", "00-" + traceID + "-zz-01 checkly=true", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(Sampler()), sdktrace.WithSpanProcessor(rec))

			ctx := ContinueStored(context.Background(), tt.stored)
			parent := trace.SpanContextFromContext(ctx)
			_, span := tp.Tracer("test").Start(ctx, "work")
			span.End()

			if parent.IsValid() != tt.wantParent {
				t.Fatalf("parent = %+v, want valid %t", parent, tt.wantParent)
			}
			if tt.wantParent && (!parent.IsRemote() || parent.TraceID().String() != traceID ||
				parent.SpanID().String() != storedSpanID) {
				t.Errorf("parent = %+v, want the remote span %s of trace %s", parent, storedSpanID, traceID)
			}
			if !tt.wantSampled {
				if n := len(rec.Ended()); n != 0 {
					t.Errorf("recorded %d spans, want none", n)
				}
				return
			}
			if n := len(rec.Ended()); n != 1 {
				t.Fatalf("recorded %d spans, want 1", n)
			}
			got := rec.Ended()[0]
			if got.SpanContext().TraceID().String() != traceID || got.Parent().SpanID().String() != storedSpanID {
				t.Errorf("span in trace %s under %s, want trace %s under %s", got.SpanContext().TraceID(),
					got.Parent().SpanID(), traceID, storedSpanID)
			}
		})
	}
}

// TestStoredContextRoundTrip stores a Checkly request's context and continues it: the work's span is the request
// span's child.
func TestStoredContextRoundTrip(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(Sampler()), sdktrace.WithSpanProcessor(rec))
	ctx, request := startFromHeaders(t, tp, map[string]string{
		"traceparent": traceparent("01"), "tracestate": "vendor=abc,checkly=true",
	})
	stored := StoredContext(ctx)
	request.End()

	_, work := tp.Tracer("test").Start(ContinueStored(context.Background(), stored), "work")
	work.End()

	spans := rec.Ended()
	if len(spans) != 2 {
		t.Fatalf("recorded %d spans, want the request's and the work's", len(spans))
	}
	got := spans[1]
	if got.SpanContext().TraceID() != request.SpanContext().TraceID() ||
		got.Parent().SpanID() != request.SpanContext().SpanID() {
		t.Errorf("work span in trace %s under %s, want the request's child in %s", got.SpanContext().TraceID(),
			got.Parent().SpanID(), request.SpanContext().TraceID())
	}
	if state := got.SpanContext().TraceState().String(); state != "vendor=abc,checkly=true" {
		t.Errorf("work span's tracestate = %q, want the request's", state)
	}
}
