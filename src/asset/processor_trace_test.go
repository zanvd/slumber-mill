package asset

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/zanvd/slumber-mill/src/audio"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/tracing"
	"github.com/zanvd/slumber-mill/src/worker"
)

// storedChecklyContext returns what an upload Checkly traced stores, and the span ID of its request's span.
func storedChecklyContext(t *testing.T) (stored string, requestSpanID trace.SpanID) {
	t.Helper()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(tracing.Sampler()))
	ctx, span := checklyRequest(t, tp, "checkly=true")
	defer span.End()
	return tracing.StoredContext(ctx), span.SpanContext().SpanID()
}

// tracedProcessor returns a processor on f's fakes that continues traces with a Checkly-only provider recording
// to the returned recorder, and hands out f's due lease to one claim only.
func tracedProcessor(t *testing.T, f *processorFixture) (*Processor, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(tracing.Sampler()), sdktrace.WithSpanProcessor(rec))
	p, err := NewProcessor(&onceRepo{fakeLeaseRepo: f.repo}, f.files, f.tc,
		ProcessorConfig{WorkerID: "encode-1", Lease: processorLease, Tracer: tp})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	return p, rec
}

// onceRepo hands out its fake's due lease to the first claim only, as a table would.
type onceRepo struct {
	*fakeLeaseRepo
	claimed bool
}

func (r *onceRepo) claim(ctx context.Context, workerID string, d time.Duration) (lease, bool, error) {
	if r.claimed {
		return lease{}, false, nil
	}
	r.claimed = true
	return r.fakeLeaseRepo.claim(ctx, workerID, d)
}

func attributes(s sdktrace.ReadOnlySpan) map[attribute.Key]string {
	a := map[attribute.Key]string{}
	for _, kv := range s.Attributes() {
		a[kv.Key] = kv.Value.Emit()
	}
	return a
}

// TestProcessorTrace starts an attempt's span: an asset whose upload Checkly traced gets an asset.encode span that
// continues the upload's trace under the request's span, and any other gets none that records.
func TestProcessorTrace(t *testing.T) {
	stored, requestSpanID := storedChecklyContext(t)
	tests := []struct {
		name         string
		traceContext string
		wantSampled  bool
	}{
		{"uploaded by Checkly", stored, true},
		{"uploaded by a tenant", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newProcessorFixture(t)
			f.repo.traceContext = tc.traceContext
			p, rec := tracedProcessor(t, f)

			ctx, span := p.Trace(t.Context(), claimed)
			if span == nil {
				t.Fatal("Trace() returned no span")
			}
			if !trace.SpanContextFromContext(ctx).Equal(span.SpanContext()) {
				t.Errorf("Trace() returned a context without its span")
			}
			span.End()

			if want := []lease{lease(claimed)}; !slices.Equal(f.repo.leases, want) {
				t.Errorf("leasedTrace() got %+v, want %+v", f.repo.leases, want)
			}
			if !tc.wantSampled {
				if n := len(rec.Started()); n != 0 {
					t.Errorf("recorded %d spans, want none", n)
				}
				return
			}
			ended := rec.Ended()
			if len(ended) != 1 {
				t.Fatalf("recorded %d spans, want 1", len(ended))
			}
			s := ended[0]
			if s.Name() != "asset.encode" || s.SpanKind() != trace.SpanKindConsumer ||
				s.SpanContext().TraceID().String() != checklyTraceID || s.Parent().SpanID() != requestSpanID {
				t.Errorf("span %q (%v) in trace %s under %s, want asset.encode under the upload's %s in Checkly's trace",
					s.Name(), s.SpanKind(), s.SpanContext().TraceID(), s.Parent().SpanID(), requestSpanID)
			}
			a := attributes(s)
			if a[attrAssetID] != f.repo.assetID.String() || a[attrAttempt] != "2" {
				t.Errorf("attributes = %v, want asset %s on attempt 2", a, f.repo.assetID)
			}
		})
	}
}

func TestProcessorTraceOff(t *testing.T) {
	f := newProcessorFixture(t)
	f.repo.traceContext, _ = storedChecklyContext(t)

	ctx, span := f.p.Trace(t.Context(), claimed)

	if span != nil || ctx != t.Context() {
		t.Errorf("Trace() = %v, %v; want the context as it was and no span", ctx, span)
	}
	if len(f.calls.list) != 0 {
		t.Errorf("calls = %v, want none: a processor without a tracer reads no trace context", f.calls.list)
	}
}

func TestProcessorTraceReadFails(t *testing.T) {
	f := newProcessorFixture(t)
	f.repo.errs["leasedTrace"] = errBoom
	p, rec := tracedProcessor(t, f)
	var logs bytes.Buffer
	ctx := logging.CtxWithLogger(t.Context(), slog.New(slog.NewJSONHandler(&logs, nil)))

	if _, span := p.Trace(ctx, claimed); span != nil {
		t.Errorf("Trace() returned span %v, want none", span)
	}
	if n := len(rec.Started()); n != 0 {
		t.Errorf("recorded %d spans, want none", n)
	}
	if !strings.Contains(logs.String(), `"level":"WARN","msg":"error reading the asset's trace context","error":"boom"`) {
		t.Errorf("logs = %s, want a warning", &logs)
	}
}

// TestProcessorTraceInThePool runs an attempt through the encode pool: an upload Checkly traced gets one
// asset.encode span, ended with the attempt's outcome, and the work's log lines carry its trace ID. An upload
// that stored no trace context records nothing.
func TestProcessorTraceInThePool(t *testing.T) {
	stored, _ := storedChecklyContext(t)
	tests := []struct {
		name         string
		traceContext string
		render       func(ctx context.Context, outputs []audio.Output) (audio.Result, error)
		wantOutcome  string
		wantError    bool
	}{
		{name: "ready", traceContext: stored, wantOutcome: "done"},
		{name: "rejected", traceContext: stored, wantOutcome: "failed", wantError: true,
			render: func(context.Context, []audio.Output) (audio.Result, error) {
				return audio.Result{}, &audio.Rejection{Message: audio.MessageNotAudio}
			}},
		{name: "uploaded by a tenant", traceContext: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newProcessorFixture(t)
			f.repo.due = new(lease(claimed))
			f.repo.traceContext = tc.traceContext
			f.tc.render = tc.render
			p, rec := tracedProcessor(t, f)
			var logs bytes.Buffer
			pool, err := worker.NewPool(p, worker.Config{
				WorkerID: "encode-1", Pool: "encode", Concurrency: 1, Lease: processorLease, PollInterval: time.Second,
				Log: slog.New(slog.NewJSONHandler(&logs, nil)),
			})
			if err != nil {
				t.Fatalf("NewPool() error = %v", err)
			}

			if err := pool.RunUntilIdle(t.Context()); err != nil {
				t.Fatalf("RunUntilIdle() error = %v", err)
			}

			ended := rec.Ended()
			if tc.wantOutcome == "" {
				if n := len(rec.Started()); n != 0 {
					t.Errorf("recorded %d spans, want none", n)
				}
				if strings.Contains(logs.String(), "trace_id") {
					t.Errorf("logs = %s, want no trace IDs", &logs)
				}
				return
			}
			if len(ended) != 1 || ended[0].Name() != "asset.encode" {
				t.Fatalf("recorded %d spans, want the asset.encode span", len(ended))
			}
			s := ended[0]
			if got := attributes(s)["slumber_mill.outcome"]; got != tc.wantOutcome {
				t.Errorf("outcome = %q, want %q", got, tc.wantOutcome)
			}
			if isError := s.Status().Code == codes.Error; isError != tc.wantError {
				t.Errorf("status = %+v, want an error %t", s.Status(), tc.wantError)
			}
			if want := `"trace_id":"` + checklyTraceID + `"`; strings.Count(logs.String(), want) != 2 {
				t.Errorf("logs = %s, want the claim and the outcome with %s", &logs, want)
			}
		})
	}
}
