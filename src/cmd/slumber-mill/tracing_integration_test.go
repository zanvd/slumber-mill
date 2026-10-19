//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/httpsrv"
	"github.com/zanvd/slumber-mill/src/storage"
	"github.com/zanvd/slumber-mill/src/tenant"
	"github.com/zanvd/slumber-mill/src/testutil"
	"github.com/zanvd/slumber-mill/src/tracing"
)

const (
	checklyTraceID  = "4bf92f3577b34da6a3ce929d0e0e4736"
	checklyParentID = "00f067aa0ba902b7"
	otlpTestKey     = "test-only-otlp-key"
)

func setChecklyHeaders(r *http.Request) {
	r.Header.Set("traceparent", "00-"+checklyTraceID+"-"+checklyParentID+"-01")
	r.Header.Set("tracestate", "checkly=true")
}

// notAudio is an upload only the encode pool finds isn't audio.
var notAudio = []byte("not audio, which only the encode pool checks")

func uploadRequest(t *testing.T, base, apiKey string, content []byte) *http.Request {
	t.Helper()
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	if err := w.WriteField("title", "Traced rain"); err != nil {
		t.Fatal(err)
	}
	fw, err := w.CreateFormFile("file", "rain.wav")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fw.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+"/v1/assets", &body)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", w.FormDataContentType())
	r.Header.Set("Authorization", "Bearer "+apiKey)
	return r
}

// serveOnce sends r to a server of h, which it closes before returning, so every span of the request has ended.
func serveOnce(t *testing.T, h http.Handler, build func(base string) *http.Request) *http.Response {
	t.Helper()
	srv := httptest.NewServer(h)
	resp, err := srv.Client().Do(build(srv.URL))
	if err != nil {
		srv.Close()
		t.Fatalf("request error = %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	srv.Close()
	return resp
}

// TestAPITraceNesting runs the API's handler on the real services and MySQL: the spans of a request Checkly traces
// nest under its server span, which continues Checkly's trace, and a request Checkly didn't start leaves no span.
func TestAPITraceNesting(t *testing.T) {
	httpsrv.ReleaseMode()
	dsn := testutil.NewDatabase(t)
	created := runCommand(t, map[string]string{envDSN: dsn, envSecretKeys: newKeyRing()},
		"tenant", "create", "-slug", "traced")
	if created.code != exitOK {
		t.Fatalf("tenant create: exit code %d; stderr:\n%s", created.code, created.stderr)
	}
	apiKey, _ := parseCredentials(t, created.stdout)

	gdb, err := db.Open(dsn, testutil.Logger(t))
	if err != nil {
		t.Fatalf("db.Open() error = %v", err)
	}
	pool, err := gdb.DB()
	if err != nil {
		t.Fatalf("gdb.DB() error = %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(tracing.Sampler()), sdktrace.WithSpanProcessor(rec))
	srv, err := httpsrv.NewServer(testutil.Logger(t),
		tenant.NewService(tenant.NewRepository(gdb), nil, nil),
		asset.NewService(asset.NewRepository(gdb), storage.NewLocal(t.TempDir())),
		httpsrv.Config{Readiness: pool, Tracer: tp})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	resp := serveOnce(t, srv.Handler(), func(base string) *http.Request {
		r := uploadRequest(t, base, apiKey, notAudio)
		setChecklyHeaders(r)
		return r
	})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("upload status = %d, want 202", resp.StatusCode)
	}
	location := resp.Header.Get("Location")
	checkNesting(t, rec.Ended(), "POST /v1/assets", []string{"SELECT tenants", "asset.Stage", "INSERT assets",
		"SELECT assets"})

	rec.Reset()
	resp = serveOnce(t, srv.Handler(), func(base string) *http.Request {
		r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+location, nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer "+apiKey)
		setChecklyHeaders(r)
		return r
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d, want 200", location, resp.StatusCode)
	}
	checkNesting(t, rec.Ended(), "GET /v1/assets/:id", []string{"SELECT tenants", "SELECT assets"})

	rec.Reset()
	resp = serveOnce(t, srv.Handler(), func(base string) *http.Request {
		r := uploadRequest(t, base, apiKey, notAudio)
		r.Header.Set("traceparent", "00-"+checklyTraceID+"-"+checklyParentID+"-01")
		return r
	})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("untraced upload status = %d, want 202", resp.StatusCode)
	}
	if n := len(rec.Started()); n != 0 {
		t.Errorf("a request Checkly didn't start recorded %d spans, want none", n)
	}
}

// checkNesting finds the one server span, named route and continuing Checkly's trace, and checks that every other
// span is its child, with at least the names in want.
func checkNesting(t *testing.T, spans []sdktrace.ReadOnlySpan, route string, want []string) {
	t.Helper()
	var server sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.SpanKind() == trace.SpanKindServer {
			if server != nil {
				t.Fatalf("%s: more than one server span", route)
			}
			server = s
		}
	}
	if server == nil {
		t.Fatalf("%s: no server span among %d spans", route, len(spans))
	}
	if server.Name() != route || server.SpanContext().TraceID().String() != checklyTraceID ||
		server.Parent().SpanID().String() != checklyParentID {
		t.Errorf("server span %q in trace %s under %s, want %q in Checkly's trace", server.Name(),
			server.SpanContext().TraceID(), server.Parent().SpanID(), route)
	}
	names := map[string]bool{}
	for _, s := range spans {
		if s == server {
			continue
		}
		names[s.Name()] = true
		if s.Parent().SpanID() != server.SpanContext().SpanID() {
			t.Errorf("%s: span %q has parent %s, want the server span %s", route, s.Name(), s.Parent().SpanID(),
				server.SpanContext().SpanID())
		}
		if s.SpanKind() == trace.SpanKindClient {
			a := map[attribute.Key]string{}
			for _, kv := range s.Attributes() {
				a[kv.Key] = kv.Value.Emit()
			}
			if a["db.system"] != "mysql" || !strings.Contains(a["db.query.text"], "?") {
				t.Errorf("%s: DB span %q has attributes %v", route, s.Name(), a)
			}
		}
	}
	for _, n := range want {
		if !names[n] {
			t.Errorf("%s: no %q span among %v", route, n, names)
		}
	}
}

// otlpSink is an OTLP/HTTP receiver that keeps every request.
type otlpSink struct {
	mu       sync.Mutex
	requests []otlpRequest
}

type otlpRequest struct {
	path, contentType, authorization string
	body                             []byte
}

func (s *otlpSink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	s.requests = append(s.requests, otlpRequest{
		path: r.URL.Path, contentType: r.Header.Get("Content-Type"), authorization: r.Header.Get("Authorization"),
		body: body,
	})
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
}

// TestAPIExportsOnShutdown runs the api command with an OTLP endpoint: the span of a request Checkly started has
// been exported by the time the command exits, and a request it didn't start isn't.
func TestAPIExportsOnShutdown(t *testing.T) {
	dsn := testutil.NewDatabase(t)
	sink := &otlpSink{}
	collector := httptest.NewServer(sink)
	defer collector.Close()

	env := envFrom(map[string]string{
		envDSN:                 dsn,
		envHTTPAddr:            "127.0.0.1:0",
		envDataDir:             t.TempDir(),
		envDrainDelay:          "0s",
		envShutdownGrace:       "5s",
		tracing.EnvEndpoint:    collector.URL,
		tracing.EnvHeaders:     "authorization=" + otlpTestKey,
		tracing.EnvProtocol:    "http/protobuf",
		tracing.EnvServiceName: "slumber-mill-api",
	})
	var logs bytes.Buffer
	stderr, records := logRecords(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"api"}, env, io.Discard, io.MultiWriter(stderr, &syncWriter{w: &logs}))
		stderr.Close()
	}()

	started := waitForLog(t, records, "exporting traces started by Checkly")
	if started["endpoint"] != collector.URL || started["service"] != "slumber-mill-api" {
		t.Errorf("startup line = %v", started)
	}
	addr, _ := waitForLog(t, records, "serving HTTP")["addr"].(string)
	client := &http.Client{Timeout: 10 * time.Second}
	get := func(path string, checkly bool) {
		t.Helper()
		r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if checkly {
			setChecklyHeaders(r)
		}
		resp, err := client.Do(r)
		if err != nil {
			t.Fatalf("GET %s error = %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, resp.StatusCode)
		}
	}
	get("/readyz", true)
	get("/healthz", false)
	client.CloseIdleConnections()

	readyz := waitForLog(t, records, "request")
	if readyz["route"] != "/readyz" || readyz["trace_id"] != checklyTraceID {
		t.Errorf("request line = %v, want /readyz with Checkly's trace ID", readyz)
	}
	cancel()
	select {
	case code := <-done:
		if code != exitOK {
			t.Fatalf("api exit code %d, want 0", code)
		}
	case <-time.After(waitTimeout):
		t.Fatalf("api didn't exit within %v", waitTimeout)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.requests) != 1 {
		t.Fatalf("collector got %d exports, want 1", len(sink.requests))
	}
	r := sink.requests[0]
	if r.path != "/v1/traces" || r.contentType != "application/x-protobuf" || r.authorization != otlpTestKey {
		t.Errorf("export = %s %s with the key %v, want /v1/traces protobuf with the key", r.path, r.contentType,
			r.authorization == otlpTestKey)
	}
	if !bytes.Contains(r.body, []byte("GET /readyz")) || bytes.Contains(r.body, []byte("/healthz")) {
		t.Errorf("export doesn't hold exactly the /readyz span")
	}
	for line := range strings.Lines(logs.String()) {
		if !json.Valid([]byte(line)) {
			t.Errorf("stderr line isn't JSON: %q", line)
		}
		if strings.Contains(line, otlpTestKey) {
			t.Errorf("log line holds the OTLP key: %q", line)
		}
	}
}

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}
