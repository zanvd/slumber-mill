package httpsrv

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"uuid"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/zanvd/slumber-mill/src/tracing"
)

const (
	checklyTraceID  = "4bf92f3577b34da6a3ce929d0e0e4736"
	checklyParentID = "00f067aa0ba902b7"
)

func withTraceHeaders(r *http.Request, flags, tracestate string) *http.Request {
	r.Header.Set("traceparent", "00-"+checklyTraceID+"-"+checklyParentID+"-"+flags)
	if tracestate != "" {
		r.Header.Set("tracestate", tracestate)
	}
	return r
}

func fromCheckly(r *http.Request) *http.Request {
	return withTraceHeaders(r, "01", "checkly=true")
}

// newTracedFixture serves with the production sampler and records every span it starts.
func newTracedFixture(t *testing.T) (*fixture, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(tracing.Sampler()), sdktrace.WithSpanProcessor(rec))
	return newFixture(t, Config{Tracer: tp}), rec
}

func onlySpan(t *testing.T, rec *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()
	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("got %d spans, want 1", len(ended))
	}
	return ended[0]
}

func attrs(s sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	m := map[attribute.Key]attribute.Value{}
	for _, kv := range s.Attributes() {
		m[kv.Key] = kv.Value
	}
	return m
}

func TestTraceRequests(t *testing.T) {
	id := uuid.NewV7().String()
	tests := []struct {
		name       string
		req        *http.Request
		setup      func(f *fixture)
		wantName   string
		wantRoute  string
		wantStatus int
		wantTenant string
		wantError  bool
	}{
		{name: "authenticated", req: authed("/v1/assets/" + id), wantName: "GET /v1/assets/:id",
			wantRoute: "/v1/assets/:id", wantStatus: 200, wantTenant: "acme"},
		{name: "health", req: unauthed("/healthz"), wantName: "GET /healthz", wantRoute: "/healthz", wantStatus: 200},
		{name: "unauthenticated", req: unauthed("/v1/assets"), wantName: "GET /v1/assets",
			wantRoute: "/v1/assets", wantStatus: 401},
		{name: "unknown route", req: unauthed("/nope"), wantName: "GET", wantStatus: 404},
		{name: "trailing-slash redirect", req: httptest.NewRequest(http.MethodPost, "/v1/assets/", nil),
			wantName: "POST", wantStatus: 307},
		{name: "server error", req: authed("/v1/assets/" + id), setup: func(f *fixture) { f.assets.err = errBoom },
			wantName: "GET /v1/assets/:id", wantRoute: "/v1/assets/:id", wantStatus: 500, wantTenant: "acme",
			wantError: true},
		{name: "panic", req: authed("/v1/assets/" + id), setup: func(f *fixture) { f.assets.panicOnce = "kaboom" },
			wantName: "GET /v1/assets/:id", wantRoute: "/v1/assets/:id", wantStatus: 500, wantTenant: "acme",
			wantError: true},
		{name: "not ready", req: unauthed("/readyz"), setup: func(f *fixture) { f.srv.BeginShutdown() },
			wantName: "GET /readyz", wantRoute: "/readyz", wantStatus: 503, wantError: true},
		{name: "undefined method", req: httptest.NewRequest("BREW", "/healthz", nil), wantName: "HTTP",
			wantStatus: 404},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, rec := newTracedFixture(t)
			if tt.setup != nil {
				tt.setup(f)
			}
			if w := f.serve(fromCheckly(tt.req)); w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body %s", w.Code, tt.wantStatus, w.Body)
			}

			span := onlySpan(t, rec)
			sc := span.SpanContext()
			if sc.TraceID().String() != checklyTraceID || !sc.IsSampled() {
				t.Errorf("trace ID = %s (sampled %v), want Checkly's %s", sc.TraceID(), sc.IsSampled(), checklyTraceID)
			}
			if p := span.Parent(); p.SpanID().String() != checklyParentID || !p.IsRemote() {
				t.Errorf("parent = %s (remote %v), want Checkly's span %s", p.SpanID(), p.IsRemote(), checklyParentID)
			}
			if span.SpanKind() != trace.SpanKindServer {
				t.Errorf("kind = %v, want server", span.SpanKind())
			}
			if span.Name() != tt.wantName {
				t.Errorf("name = %q, want %q", span.Name(), tt.wantName)
			}
			a := attrs(span)
			wantMethod := tt.req.Method
			if wantMethod == "BREW" {
				wantMethod = "_OTHER"
			}
			if got := a[attrMethod].AsString(); got != wantMethod {
				t.Errorf("%s = %q, want %q", attrMethod, got, wantMethod)
			}
			if got := a[attrRoute].AsString(); got != tt.wantRoute {
				t.Errorf("%s = %q, want %q", attrRoute, got, tt.wantRoute)
			}
			if got := a[attrStatus].AsInt64(); got != int64(tt.wantStatus) {
				t.Errorf("%s = %d, want %d", attrStatus, got, tt.wantStatus)
			}
			if got := a[attrTenant].AsString(); got != tt.wantTenant {
				t.Errorf("%s = %q, want %q", attrTenant, got, tt.wantTenant)
			}
			if got := span.Status().Code == codes.Error; got != tt.wantError {
				t.Errorf("status = %v, want error %v", span.Status(), tt.wantError)
			}
		})
	}
}

func TestTraceAbortedResponse(t *testing.T) {
	f, rec := newTracedFixture(t)
	f.srv.engine.GET("/abort", func(*gin.Context) { panic(http.ErrAbortHandler) })
	got := func() (v any) {
		defer func() { v = recover() }()
		f.serve(fromCheckly(unauthed("/abort")))
		return nil
	}()
	if got != http.ErrAbortHandler {
		t.Errorf("ServeHTTP panicked with %v, want http.ErrAbortHandler", got)
	}
	span := onlySpan(t, rec)
	if span.Status().Code != codes.Error || span.Name() != "GET /abort" {
		t.Errorf("span %q status = %v, want an error", span.Name(), span.Status())
	}
	if _, ok := attrs(span)[attrStatus]; ok {
		t.Errorf("aborted span has %s, want none", attrStatus)
	}
}

func TestTraceOnlyChecklyRequests(t *testing.T) {
	tests := []struct {
		name string
		req  *http.Request
	}{
		{name: "no trace headers", req: authed("/v1/assets")},
		{name: "another vendor's trace", req: withTraceHeaders(authed("/v1/assets"), "01", "vendor=x")},
		{name: "no tracestate", req: withTraceHeaders(authed("/v1/assets"), "01", "")},
		{name: "unsampled checkly trace", req: withTraceHeaders(authed("/v1/assets"), "00", "checkly=true")},
		{name: "trailing-slash redirect", req: httptest.NewRequest(http.MethodPost, "/v1/assets/", nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, rec := newTracedFixture(t)
			f.serve(tt.req)
			if n := len(rec.Started()); n != 0 {
				t.Errorf("recorded %d spans, want none", n)
			}
			line := f.logLine(t, "request")
			if _, ok := line["trace_id"]; ok {
				t.Errorf("request line has trace_id %v, want none", line["trace_id"])
			}
			for _, c := range f.assets.recorded() {
				if trace.SpanContextFromContext(c.ctx).IsSampled() {
					t.Errorf("%s got a sampled span", c.method)
				}
			}
		})
	}
}

func TestTraceIDsInLogs(t *testing.T) {
	f, rec := newTracedFixture(t)
	if w := f.serve(fromCheckly(authed("/v1/assets"))); w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body)
	}
	span := onlySpan(t, rec)
	line := f.logLine(t, "request")
	if line["trace_id"] != checklyTraceID || line["span_id"] != span.SpanContext().SpanID().String() {
		t.Errorf("request line trace_id %v span_id %v, want %s %s",
			line["trace_id"], line["span_id"], checklyTraceID, span.SpanContext().SpanID())
	}
	calls := f.assets.recorded()
	if len(calls) != 1 || trace.SpanContextFromContext(calls[0].ctx).SpanID() != span.SpanContext().SpanID() {
		t.Error("the service's context doesn't carry the request's span")
	}
}

// TestTraceKeepsSecretsOut sends secrets in every part of a request a client controls, and checks that no span
// carries any of them.
func TestTraceKeepsSecretsOut(t *testing.T) {
	const (
		querySecret  = "query_secret_value"
		cookieSecret = "cookie_secret_value"
		idemSecret   = "idempotency_secret_value"
		titleSecret  = "title_secret_value"
		bodySecret   = "body_secret_value"
	)
	secrets := []string{validKey, "sm_wrong_key_value", "Bearer", cookieSecret, querySecret, idemSecret,
		titleSecret, bodySecret}
	upload := func(key string) *http.Request {
		body := multipartBody(t, titlePart(titleSecret), filePart(bodySecret))
		r := httptest.NewRequest(http.MethodPost, "/v1/assets?key="+querySecret, bytes.NewReader(body))
		r.Header.Set("Content-Type", formContentType)
		r.Header.Set("Authorization", "Bearer "+key)
		r.Header.Set("Idempotency-Key", idemSecret)
		return r
	}
	for _, tt := range []struct {
		name string
		req  *http.Request
	}{
		{name: "upload", req: upload(validKey)},
		{name: "unknown key", req: upload("sm_wrong_key_value")},
		{name: "list", req: authed("/v1/assets?cursor=" + querySecret)},
		{name: "unknown route", req: authed("/nope/" + querySecret + "?q=" + querySecret)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, rec := newTracedFixture(t)
			tt.req.Header.Set("Cookie", "session="+cookieSecret)
			f.serve(fromCheckly(tt.req))
			span := onlySpan(t, rec)
			var dump strings.Builder
			fmt.Fprintln(&dump, span.Name(), span.Status().Description)
			for _, kv := range span.Attributes() {
				fmt.Fprintln(&dump, kv.Key, kv.Value.Emit())
			}
			for _, e := range span.Events() {
				fmt.Fprintln(&dump, e.Name, e.Attributes)
			}
			for _, s := range secrets {
				if strings.Contains(dump.String(), s) {
					t.Errorf("span contains %q:\n%s", s, dump.String())
				}
			}
		})
	}
}
