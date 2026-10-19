package tracing

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	traceID      = "4bf92f3577b34da6a3ce929d0e0e4736"
	parentSpanID = "00f067aa0ba902b7"
	testKey      = "test-only-otlp-key"
)

func traceparent(flags string) string {
	return "00-" + traceID + "-" + parentSpanID + "-" + flags
}

// startFromHeaders starts a span as a server would for a request with these headers.
func startFromHeaders(t *testing.T, tp trace.TracerProvider, headers map[string]string) (context.Context, trace.Span) {
	t.Helper()
	h := http.Header{}
	for k, v := range headers {
		h.Set(k, v)
	}
	ctx := propagation.TraceContext{}.Extract(t.Context(), propagation.HeaderCarrier(h))
	return tp.Tracer("test").Start(ctx, "request", trace.WithSpanKind(trace.SpanKindServer))
}

func TestSampler(t *testing.T) {
	tests := []struct {
		name        string
		headers     map[string]string
		wantSampled bool
	}{
		{name: "no parent"},
		{name: "parent without checkly", headers: map[string]string{
			"traceparent": traceparent("01"), "tracestate": "vendor=abc",
		}},
		{name: "parent without a tracestate", headers: map[string]string{"traceparent": traceparent("01")}},
		{name: "checkly=true, sampled", headers: map[string]string{
			"traceparent": traceparent("01"), "tracestate": "checkly=true",
		}, wantSampled: true},
		{name: "checkly=true among other members, sampled", headers: map[string]string{
			"traceparent": traceparent("01"), "tracestate": "vendor=abc,checkly=true",
		}, wantSampled: true},
		{name: "checkly=true, unsampled", headers: map[string]string{
			"traceparent": traceparent("00"), "tracestate": "checkly=true",
		}},
		{name: "checkly=false, sampled", headers: map[string]string{
			"traceparent": traceparent("01"), "tracestate": "checkly=false",
		}},
		{name: "malformed traceparent", headers: map[string]string{
			"traceparent": "00-" + traceID + "-zz-01", "tracestate": "checkly=true",
		}},
		{name: "all-zero trace ID", headers: map[string]string{
			"traceparent": "00-00000000000000000000000000000000-" + parentSpanID + "-01", "tracestate": "checkly=true",
		}},
		{name: "tracestate without a traceparent", headers: map[string]string{"tracestate": "checkly=true"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(Sampler()), sdktrace.WithSpanProcessor(rec))
			ctx, span := startFromHeaders(t, tp, tt.headers)
			_, child := tp.Tracer("test").Start(ctx, "child")
			child.End()
			span.End()

			sc := span.SpanContext()
			if sc.IsSampled() != tt.wantSampled {
				t.Errorf("sampled = %v, want %v", sc.IsSampled(), tt.wantSampled)
			}
			if child.SpanContext().IsSampled() != tt.wantSampled {
				t.Errorf("local child sampled = %v, want %v", child.SpanContext().IsSampled(), tt.wantSampled)
			}
			wantEnded := 0
			if tt.wantSampled {
				wantEnded = 2
				if sc.TraceID().String() != traceID {
					t.Errorf("trace ID = %s, want the parent's %s", sc.TraceID(), traceID)
				}
				if got := sc.TraceState().Get("checkly"); got != "true" {
					t.Errorf("tracestate checkly = %q, want it kept", got)
				}
			}
			if got := len(rec.Ended()); got != wantEnded {
				t.Errorf("recorded %d spans, want %d", got, wantEnded)
			}
		})
	}
}

type otlpRequest struct {
	method, path, contentType, authorization string
	body                                     []byte
}

// otlpSink is an OTLP/HTTP receiver that keeps every request.
type otlpSink struct {
	mu       sync.Mutex
	requests []otlpRequest
}

func (s *otlpSink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	s.requests = append(s.requests, otlpRequest{
		method: r.Method, path: r.URL.Path, contentType: r.Header.Get("Content-Type"),
		authorization: r.Header.Get("Authorization"), body: body,
	})
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
}

func (s *otlpSink) received() []otlpRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]otlpRequest(nil), s.requests...)
}

func TestExporter(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		wantExport bool
	}{
		{name: "started by checkly", headers: map[string]string{
			"traceparent": traceparent("01"), "tracestate": "checkly=true",
		}, wantExport: true},
		{name: "not started by checkly", headers: map[string]string{"traceparent": traceparent("01")}},
		{name: "no parent"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := &otlpSink{}
			srv := httptest.NewServer(sink)
			defer srv.Close()
			cfg, err := LoadConfig(env(map[string]string{
				EnvEndpoint: srv.URL, EnvHeaders: "authorization=" + testKey,
				EnvProtocol: "http/protobuf", EnvServiceName: "exporter-test",
			}))
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			tp, err := Setup(t.Context(), cfg)
			if err != nil {
				t.Fatalf("Setup() error = %v", err)
			}
			if !tp.Enabled() {
				t.Fatal("Enabled() = false with an endpoint")
			}
			_, span := startFromHeaders(t, tp, tt.headers)
			span.SetName("GET /exported-route")
			span.End()
			if err := tp.Shutdown(t.Context()); err != nil {
				t.Fatalf("Shutdown() error = %v", err)
			}

			got := sink.received()
			if !tt.wantExport {
				if len(got) != 0 {
					t.Fatalf("got %d exports, want none", len(got))
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("got %d exports, want 1", len(got))
			}
			r := got[0]
			if r.method != http.MethodPost || r.path != "/v1/traces" {
				t.Errorf("request = %s %s, want POST /v1/traces", r.method, r.path)
			}
			if r.contentType != "application/x-protobuf" {
				t.Errorf("Content-Type = %q, want application/x-protobuf", r.contentType)
			}
			if r.authorization != testKey {
				t.Errorf("Authorization = %q, want the configured key", r.authorization)
			}
			for _, want := range []string{"GET /exported-route", "exporter-test"} {
				if !bytes.Contains(r.body, []byte(want)) {
					t.Errorf("body lacks %q", want)
				}
			}
			tid := span.SpanContext().TraceID()
			if !bytes.Contains(r.body, tid[:]) || tid.String() != traceID {
				t.Errorf("body lacks the continued trace ID %s", traceID)
			}
		})
	}
}

func TestSetupWithoutEndpoint(t *testing.T) {
	tp, err := Setup(t.Context(), Config{})
	if err != nil {
		t.Fatalf("Setup() error = %v", err)
	}
	if tp.Enabled() {
		t.Error("Enabled() = true without an endpoint")
	}
	_, span := startFromHeaders(t, tp, map[string]string{
		"traceparent": traceparent("01"), "tracestate": "checkly=true",
	})
	if span.IsRecording() {
		t.Error("span is recording without an endpoint")
	}
	span.End()
	if err := tp.Shutdown(t.Context()); err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}
}

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadConfig(t *testing.T) {
	const endpoint = "https://otel.eu-west-1.checklyhq.com"
	tests := []struct {
		name        string
		env         map[string]string
		wantOff     bool
		wantName    string
		wantHeaders map[string]string
		wantErr     []string
	}{
		{name: "nothing set", env: map[string]string{}, wantOff: true},
		{name: "no endpoint ignores the rest", env: map[string]string{
			EnvHeaders: "authorization=", EnvProtocol: "grpc",
		}, wantOff: true},
		{name: "checkly", env: map[string]string{
			EnvEndpoint: endpoint, EnvHeaders: "authorization=" + testKey, EnvProtocol: "http/protobuf",
			EnvServiceName: "slumber-mill-api",
		}, wantName: "slumber-mill-api", wantHeaders: map[string]string{"authorization": testKey}},
		{name: "defaults", env: map[string]string{EnvEndpoint: endpoint},
			wantName: "slumber-mill", wantHeaders: map[string]string{}},
		{name: "several percent-encoded headers", env: map[string]string{
			EnvEndpoint: endpoint, EnvHeaders: " a = x%20y , b=z=1, c=%20d%20",
		}, wantName: "slumber-mill", wantHeaders: map[string]string{"a": "x y", "b": "z=1", "c": "d"}},
		{name: "token characters in a name", env: map[string]string{
			EnvEndpoint: endpoint, EnvHeaders: "X-Api_Key.1!#$%&'*+^`|~=v",
		}, wantName: "slumber-mill", wantHeaders: map[string]string{"X-Api_Key.1!#$%&'*+^`|~": "v"}},
		{name: "blank headers", env: map[string]string{EnvEndpoint: endpoint, EnvHeaders: "  "},
			wantName: "slumber-mill", wantHeaders: map[string]string{}},
		// The exporter parses the variable again and logs, as plain text, every member it rejects.
		{name: "trailing comma", env: map[string]string{
			EnvEndpoint: endpoint, EnvHeaders: "authorization=" + testKey + ",",
		}, wantErr: []string{EnvHeaders, "pair 2"}},
		{name: "leading comma", env: map[string]string{
			EnvEndpoint: endpoint, EnvHeaders: ",authorization=" + testKey,
		}, wantErr: []string{EnvHeaders, "pair 1"}},
		{name: "empty member between pairs", env: map[string]string{
			EnvEndpoint: endpoint, EnvHeaders: "a=1, ,authorization=" + testKey,
		}, wantErr: []string{EnvHeaders, "pair 2"}},
		{name: "only a comma", env: map[string]string{EnvEndpoint: endpoint, EnvHeaders: ","},
			wantErr: []string{EnvHeaders, "pair 1"}},
		{name: "name without a value's '='", env: map[string]string{
			EnvEndpoint: endpoint, EnvHeaders: "authorization",
		}, wantErr: []string{EnvHeaders, "pair 1"}},
		{name: "empty name", env: map[string]string{EnvEndpoint: endpoint, EnvHeaders: " =" + testKey},
			wantErr: []string{EnvHeaders, "pair 1"}},
		{name: "separator in a name", env: map[string]string{EnvEndpoint: endpoint, EnvHeaders: "auth:x=" + testKey},
			wantErr: []string{EnvHeaders, "pair 1"}},
		{name: "space in a name", env: map[string]string{EnvEndpoint: endpoint, EnvHeaders: "auth x=" + testKey},
			wantErr: []string{EnvHeaders, "pair 1"}},
		{name: "non-ASCII name", env: map[string]string{EnvEndpoint: endpoint, EnvHeaders: "autorización=" + testKey},
			wantErr: []string{EnvHeaders, "pair 1"}},
		{name: "value only spaces once decoded", env: map[string]string{
			EnvEndpoint: endpoint, EnvHeaders: "authorization=%20%20",
		}, wantErr: []string{EnvHeaders, "authorization", "empty"}},
		{name: "unsupported protocol", env: map[string]string{EnvEndpoint: endpoint, EnvProtocol: "grpc"},
			wantErr: []string{EnvProtocol}},
		{name: "endpoint without a scheme", env: map[string]string{EnvEndpoint: "otel.example.com"},
			wantErr: []string{EnvEndpoint}},
		{name: "endpoint with credentials", env: map[string]string{EnvEndpoint: "https://user:" + testKey + "@otel.test"},
			wantErr: []string{EnvEndpoint}},
		{name: "endpoint with a query", env: map[string]string{EnvEndpoint: endpoint + "?key=1"},
			wantErr: []string{EnvEndpoint}},
		{name: "empty header value", env: map[string]string{EnvEndpoint: endpoint, EnvHeaders: "authorization="},
			wantErr: []string{EnvHeaders, "authorization", "empty"}},
		{name: "header without '='", env: map[string]string{EnvEndpoint: endpoint, EnvHeaders: testKey},
			wantErr: []string{EnvHeaders}},
		{name: "bad percent-encoding", env: map[string]string{
			EnvEndpoint: endpoint, EnvHeaders: "authorization=" + testKey + "%zz",
		}, wantErr: []string{EnvHeaders, "authorization"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadConfig(env(tt.env))
			if len(tt.wantErr) > 0 {
				if err == nil {
					t.Fatal("LoadConfig() error = nil")
				}
				for _, want := range tt.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q lacks %q", err, want)
					}
				}
				if strings.Contains(err.Error(), testKey) {
					t.Errorf("error %q quotes a secret", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			if tt.wantOff {
				if cfg.Endpoint != "" {
					t.Errorf("Endpoint = %q, want tracing off", cfg.Endpoint)
				}
				return
			}
			if cfg.Endpoint != tt.env[EnvEndpoint] || cfg.ServiceName != tt.wantName {
				t.Errorf("config = %q %q, want %q %q", cfg.Endpoint, cfg.ServiceName, tt.env[EnvEndpoint], tt.wantName)
			}
			if len(cfg.Headers) != len(tt.wantHeaders) {
				t.Errorf("got %d headers, want %d", len(cfg.Headers), len(tt.wantHeaders))
			}
			for k, v := range tt.wantHeaders {
				if got := string(cfg.Headers[k].Bytes()); got != v {
					t.Errorf("header %s = %q, want %q", k, got, v)
				}
			}
		})
	}
}
