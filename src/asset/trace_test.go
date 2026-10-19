package asset

import (
	"context"
	"io"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/zanvd/slumber-mill/src/tracing"
)

const (
	checklyTraceID  = "4bf92f3577b34da6a3ce929d0e0e4736"
	checklyParentID = "00f067aa0ba902b7"
)

// checklyRequest returns the context of a request's server span, as httpsrv starts it from the request's headers.
func checklyRequest(t *testing.T, tp trace.TracerProvider, tracestate string) (context.Context, trace.Span) {
	t.Helper()
	carrier := propagation.MapCarrier{"traceparent": "00-" + checklyTraceID + "-" + checklyParentID + "-01"}
	if tracestate != "" {
		carrier["tracestate"] = tracestate
	}
	ctx := propagation.TraceContext{}.Extract(t.Context(), carrier)
	return tp.Tracer("test").Start(ctx, "POST /v1/assets", trace.WithSpanKind(trace.SpanKindServer))
}

// TestCreateStoresTraceContext uploads under the server span of a request: only a request Checkly traced stores
// its trace context with the asset.
func TestCreateStoresTraceContext(t *testing.T) {
	checkly := sdktrace.NewTracerProvider(sdktrace.WithSampler(tracing.Sampler()))
	tests := []struct {
		name       string
		tp         trace.TracerProvider
		tracestate string
		wantStored bool
	}{
		{"Checkly's request", checkly, "checkly=true", true},
		{"a tenant's request", checkly, "", false},
		{"tracing off", noop.NewTracerProvider(), "checkly=true", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			ctx, span := checklyRequest(t, tc.tp, tc.tracestate)
			defer span.End()

			if _, _, err := f.svc.Create(ctx, f.params(t, "Rain", "audio", "")); err != nil {
				t.Fatalf("Create() error = %v", err)
			}

			want := ""
			if tc.wantStored {
				want = "00-" + checklyTraceID + "-" + span.SpanContext().SpanID().String() + "-01 checkly=true"
			}
			if !slices.Equal(f.repo.traceContexts, []string{want}) {
				t.Errorf("insertAsset() got trace contexts %q, want [%q]", f.repo.traceContexts, want)
			}
		})
	}
}

func TestStageSpan(t *testing.T) {
	tests := []struct {
		name      string
		reader    io.Reader
		sampled   bool
		wantSpan  bool
		wantBytes int64
		wantError bool
	}{
		{name: "sampled", reader: strings.NewReader("rain"), sampled: true, wantSpan: true, wantBytes: 4},
		{name: "sampled, failing", reader: io.MultiReader(strings.NewReader("ra"), iotest.ErrReader(errBoom)),
			sampled: true, wantSpan: true, wantBytes: 2, wantError: true},
		{name: "unsampled", reader: strings.NewReader("rain")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			rec := tracetest.NewSpanRecorder()
			sampler := sdktrace.NeverSample()
			if tt.sampled {
				sampler = sdktrace.AlwaysSample()
			}
			tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.ParentBased(sampler)),
				sdktrace.WithSpanProcessor(rec))
			ctx, parent := tp.Tracer("test").Start(context.Background(), "request")
			u, err := f.svc.Stage(ctx, tt.reader)
			parent.End()
			if (err != nil) != tt.wantError {
				t.Fatalf("Stage() error = %v, want error %v", err, tt.wantError)
			}
			f.svc.Discard(ctx, u)

			ended := rec.Ended()
			if !tt.wantSpan {
				if len(ended) != 0 {
					t.Errorf("recorded %d spans, want none", len(ended))
				}
				return
			}
			if len(ended) != 2 {
				t.Fatalf("recorded %d spans, want the stage's and its parent", len(ended))
			}
			span := ended[0]
			if span.Name() != "asset.Stage" || span.Parent().SpanID() != parent.SpanContext().SpanID() {
				t.Errorf("span %q with parent %s, want asset.Stage under %s",
					span.Name(), span.Parent().SpanID(), parent.SpanContext().SpanID())
			}
			var bytes int64 = -1
			for _, kv := range span.Attributes() {
				if kv.Key == "slumber_mill.upload.bytes" {
					bytes = kv.Value.AsInt64()
				}
			}
			if bytes != tt.wantBytes {
				t.Errorf("slumber_mill.upload.bytes = %d, want %d", bytes, tt.wantBytes)
			}
			if got := span.Status().Code == codes.Error; got != tt.wantError {
				t.Errorf("status = %v, want error %v", span.Status(), tt.wantError)
			}
		})
	}
}
