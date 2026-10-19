package webhook

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/zanvd/slumber-mill/src/tracing"
	"github.com/zanvd/slumber-mill/src/worker"
)

// tracedDeliverer returns a deliverer on f's fakes that continues traces with a Checkly-only provider recording
// to the returned recorder.
func tracedDeliverer(t *testing.T, f *deliveryFixture) (*Deliverer, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(tracing.Sampler()), sdktrace.WithSpanProcessor(rec))
	d, err := NewDeliverer(f.repo, f.tenants, f.client,
		DelivererConfig{WorkerID: "webhook-1", Lease: time.Minute, Tracer: tp})
	if err != nil {
		t.Fatalf("NewDeliverer() error = %v", err)
	}
	return d, rec
}

func spanAttributes(s sdktrace.ReadOnlySpan) map[attribute.Key]string {
	a := map[attribute.Key]string{}
	for _, kv := range s.Attributes() {
		a[kv.Key] = kv.Value.Emit()
	}
	return a
}

// TestDelivererTrace starts an attempt's span: a delivery whose event carried Checkly's trace gets a
// webhook.deliver span that continues it, and any other gets none that records.
func TestDelivererTrace(t *testing.T) {
	tests := []struct {
		name         string
		traceContext string
		wantSampled  bool
	}{
		{"an asset Checkly uploaded", checklyTraceContext, true},
		{"a tenant's asset", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			f.repo.pending.TraceContext = tc.traceContext
			d, rec := tracedDeliverer(t, f)

			ctx, span := d.Trace(t.Context(), claimed)
			if span == nil {
				t.Fatal("Trace() returned no span")
			}
			if !trace.SpanContextFromContext(ctx).Equal(span.SpanContext()) {
				t.Error("Trace() returned a context without its span")
			}
			span.End()

			if want := []lease{lease(claimed)}; !slices.Equal(f.repo.leases, want) {
				t.Errorf("leasedTraceContext() got %+v, want %+v", f.repo.leases, want)
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
			if s.Name() != "webhook.deliver" || s.SpanKind() != trace.SpanKindConsumer ||
				s.SpanContext().TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" ||
				s.Parent().SpanID().String() != "00f067aa0ba902b7" {
				t.Errorf("span %q (%v) in trace %s under %s, want webhook.deliver under the stored span",
					s.Name(), s.SpanKind(), s.SpanContext().TraceID(), s.Parent().SpanID())
			}
			if got := spanAttributes(s)[attrAttempt]; got != "2" {
				t.Errorf("%s = %q, want 2", attrAttempt, got)
			}
		})
	}
}

func TestDelivererTraceOff(t *testing.T) {
	f := newDeliveryFixture(t)
	f.repo.pending.TraceContext = checklyTraceContext

	ctx, span := f.d.Trace(t.Context(), claimed)

	if span != nil || ctx != t.Context() {
		t.Errorf("Trace() = %v, %v; want the context as it was and no span", ctx, span)
	}
	if len(f.repo.leases) != 0 {
		t.Errorf("repository calls got %+v, want none: a deliverer without a tracer reads no trace context",
			f.repo.leases)
	}
}

func TestDelivererTraceReadFails(t *testing.T) {
	f := newDeliveryFixture(t)
	f.repo.errs["leasedTraceContext"] = errBoom
	d, rec := tracedDeliverer(t, f)
	var logs logBuffer

	if _, span := d.Trace(logs.ctx(t.Context()), claimed); span != nil {
		t.Errorf("Trace() returned span %v, want none", span)
	}
	if n := len(rec.Started()); n != 0 {
		t.Errorf("recorded %d spans, want none", n)
	}
	if got := logs.messages(t); !slices.Equal(got, []string{"WARN error reading the delivery's trace context"}) {
		t.Errorf("logs = %v, want one warning", got)
	}
}

// TestDelivererTraceInThePool delivers through the webhook pool: a delivery that carried Checkly's trace gets one
// webhook.deliver span with the attempt, the endpoint's status and host, and the outcome, and no span attribute
// or log line holds the webhook URL's path or query. A delivery that carried none records nothing.
func TestDelivererTraceInThePool(t *testing.T) {
	tests := []struct {
		name         string
		traceContext string
		status       int
		postErr      error
		wantOutcome  string
		wantStatus   string
		wantError    bool
	}{
		{name: "delivered", traceContext: checklyTraceContext, status: http.StatusNoContent,
			wantOutcome: "done", wantStatus: "204"},
		{name: "endpoint failing", traceContext: checklyTraceContext, status: http.StatusServiceUnavailable,
			wantOutcome: "failed", wantStatus: "503", wantError: true},
		{name: "no answer", traceContext: checklyTraceContext, postErr: errBoom,
			wantOutcome: "failed", wantError: true},
		{name: "a tenant's delivery", status: http.StatusNoContent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			f.repo.due = new(lease(claimed))
			f.repo.pending.TraceContext = tc.traceContext
			f.client.status, f.client.err = tc.status, tc.postErr
			d, rec := tracedDeliverer(t, f)
			var logs bytes.Buffer
			pool, err := worker.NewPool(d, worker.Config{
				WorkerID: "webhook-1", Pool: "webhook", Concurrency: 1, Lease: time.Minute, PollInterval: time.Second,
				Log: slog.New(slog.NewJSONHandler(&logs, nil)),
			})
			if err != nil {
				t.Fatalf("NewPool() error = %v", err)
			}

			if err := pool.RunUntilIdle(t.Context()); err != nil {
				t.Fatalf("RunUntilIdle() error = %v", err)
			}

			if len(f.client.posts) != 1 {
				t.Fatalf("posted %d times, want once", len(f.client.posts))
			}
			for _, secret := range []string{"/slumber", "tenant=7"} {
				if strings.Contains(logs.String(), secret) {
					t.Errorf("logs hold %q of the webhook URL: %s", secret, &logs)
				}
			}
			if tc.wantOutcome == "" {
				if n := len(rec.Started()); n != 0 {
					t.Errorf("recorded %d spans, want none", n)
				}
				return
			}
			ended := rec.Ended()
			if len(ended) != 1 || ended[0].Name() != "webhook.deliver" {
				t.Fatalf("recorded %d spans, want the webhook.deliver span", len(ended))
			}
			s := ended[0]
			want := map[attribute.Key]string{
				attrAttempt:            "2",
				attrServerAddress:      "hooks.example.com",
				"slumber_mill.outcome": tc.wantOutcome,
			}
			if tc.wantStatus != "" {
				want[attrStatusCode] = tc.wantStatus
			}
			if got := spanAttributes(s); !reflect.DeepEqual(got, want) {
				t.Errorf("attributes = %v, want %v", got, want)
			}
			if isError := s.Status().Code == codes.Error; isError != tc.wantError || s.Status().Description != "" {
				t.Errorf("status = %+v, want an error %t without a message", s.Status(), tc.wantError)
			}
		})
	}
}

// TestPostSendsNoTraceContext posts under a sampled span: the request that arrives is the same as without one,
// and carries no trace context.
func TestPostSendsNoTraceContext(t *testing.T) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	traced, span := tp.Tracer("test").Start(t.Context(), "webhook.deliver")
	defer span.End()

	var got []request
	for _, ctx := range []context.Context{t.Context(), traced} {
		r := newReceiver(t, "tcp4", respondStatus(http.StatusNoContent))
		c := NewClient(true)
		if status, err := c.Post(ctx, r.urlFor("127.0.0.1"), testEventID, testSignature, testBody); err != nil ||
			status != http.StatusNoContent {
			t.Fatalf("Post() = %d, %v; want 204", status, err)
		}
		c.CloseIdleConnections()
		_, requests := r.stop()
		if len(requests) != 1 {
			t.Fatalf("receiver got %d requests, want 1", len(requests))
		}
		checkDelivered(t, requests[0])
		got = append(got, requests[0])
	}

	if !reflect.DeepEqual(got[1], got[0]) {
		t.Errorf("a post under a span sent %+v\nwant what one without sends: %+v", got[1], got[0])
	}
	for _, name := range []string{"Traceparent", "Tracestate", "Baggage"} {
		if v := got[1].header.Values(name); len(v) > 0 {
			t.Errorf("header %s = %q, want none", name, v)
		}
	}
}
