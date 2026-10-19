package asset

import (
	"context"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

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
