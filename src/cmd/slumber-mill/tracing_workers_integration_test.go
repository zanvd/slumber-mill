//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/audio"
	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/httpsrv"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/storage"
	"github.com/zanvd/slumber-mill/src/tenant"
	"github.com/zanvd/slumber-mill/src/testutil"
	"github.com/zanvd/slumber-mill/src/tracing"
	"github.com/zanvd/slumber-mill/src/webhook"
	"github.com/zanvd/slumber-mill/src/worker"
)

// webhookPath is the path and query of the test tenant's webhook URL, which stand in for a token.
const webhookPath = "/hooks/ping-token?key=hook-secret"

// hookHeaders keeps the headers of every webhook a receiver gets.
type hookHeaders struct {
	mu      sync.Mutex
	headers []http.Header
}

func (h *hookHeaders) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	h.mu.Lock()
	h.headers = append(h.headers, r.Header.Clone())
	h.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// checkNoTraceContext checks that n webhooks arrived, none of them with a trace context.
func (h *hookHeaders) checkNoTraceContext(t *testing.T, n int) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.headers) != n {
		t.Errorf("receiver got %d webhooks, want %d", len(h.headers), n)
	}
	for _, header := range h.headers {
		for _, name := range []string{"Traceparent", "Tracestate"} {
			if v := header.Values(name); len(v) > 0 {
				t.Errorf("a webhook arrived with %s %q, want none", name, v)
			}
		}
	}
}

// TestWorkerTraceNesting runs the API's handler and both pools on the real services, MySQL and ffmpeg: an upload
// Checkly traced continues into an asset.encode span and a webhook.deliver span under the upload request's span,
// the encode's steps and statements nest under asset.encode, and the outcome, the endpoint's host and status are
// on the spans, but no part of the webhook URL's path or query. An upload Checkly didn't trace leaves no span
// in either pool, and no webhook carries a trace context.
func TestWorkerTraceNesting(t *testing.T) {
	httpsrv.ReleaseMode()
	dsn := testutil.NewDatabase(t)
	hooks := &hookHeaders{}
	receiver := httptest.NewServer(hooks)
	defer receiver.Close()
	keys := newKeyRing()
	created := runCommand(t, map[string]string{envDSN: dsn, envSecretKeys: keys, envWebhookAllowPrivate: "true"},
		"tenant", "create", "-slug", "traced", "-webhook-url", receiver.URL+webhookPath)
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
	box, err := secret.NewBox(keys)
	if err != nil {
		t.Fatalf("NewBox() error = %v", err)
	}
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(tracing.Sampler()), sdktrace.WithSpanProcessor(rec))
	files := storage.NewLocal(t.TempDir())
	tenants := tenant.NewService(tenant.NewRepository(gdb), box, nil)
	assets := asset.NewService(asset.NewRepository(gdb), files)
	srv, err := httpsrv.NewServer(testutil.Logger(t), tenants, assets, httpsrv.Config{Readiness: pool, Tracer: tp})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	audioFile, err := os.ReadFile(testutil.MonoPink(t))
	if err != nil {
		t.Fatal(err)
	}
	resp := serveOnce(t, srv.Handler(), func(base string) *http.Request {
		r := uploadRequest(t, base, apiKey, audioFile)
		setChecklyHeaders(r)
		return r
	})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("traced upload status = %d, want 202", resp.StatusCode)
	}
	var upload sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "POST /v1/assets" {
			upload = s
		}
	}
	if upload == nil {
		t.Fatal("no span for the traced upload")
	}
	if resp := serveOnce(t, srv.Handler(), func(base string) *http.Request {
		return uploadRequest(t, base, apiKey, notAudio)
	}); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("untraced upload status = %d, want 202", resp.StatusCode)
	}
	rec.Reset()

	ctx := logging.CtxWithLogger(t.Context(), testutil.Logger(t))
	processor, err := asset.NewProcessor(asset.NewRepository(gdb), files, audio.New(),
		asset.ProcessorConfig{WorkerID: "encode-1", Lease: time.Minute, Tracer: tp})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	runPool(t, processor, "encode")
	relay, err := webhook.NewRelay(webhook.NewRepository(gdb), assets, tenants)
	if err != nil {
		t.Fatalf("NewRelay() error = %v", err)
	}
	if held, relayed, err := relay.Once(ctx); err != nil || !held || relayed != 2 {
		t.Fatalf("relay Once() = %t, %d, %v; want both events relayed", held, relayed, err)
	}
	if err := relay.Close(ctx); err != nil {
		t.Fatalf("relay Close() error = %v", err)
	}
	client := webhook.NewClient(true)
	defer client.CloseIdleConnections()
	deliverer, err := webhook.NewDeliverer(webhook.NewRepository(gdb), tenants, client,
		webhook.DelivererConfig{WorkerID: "webhook-1", Lease: time.Minute, Tracer: tp})
	if err != nil {
		t.Fatalf("NewDeliverer() error = %v", err)
	}
	runPool(t, deliverer, "webhook")
	hooks.checkNoTraceContext(t, 2)

	spans := rec.Ended()
	byName := map[string][]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byName[s.Name()] = append(byName[s.Name()], s)
		if s.SpanContext().TraceID().String() != checklyTraceID {
			t.Errorf("span %q is in trace %s, want only Checkly's", s.Name(), s.SpanContext().TraceID())
		}
		for _, kv := range s.Attributes() {
			if v := kv.Value.Emit(); strings.Contains(v, "ping-token") || strings.Contains(v, "hook-secret") {
				t.Errorf("span %q has %s = %q, which holds the webhook URL's path or query", s.Name(), kv.Key, v)
			}
		}
	}
	encode, deliver := only(t, byName, "asset.encode"), only(t, byName, "webhook.deliver")
	for _, s := range []sdktrace.ReadOnlySpan{encode, deliver} {
		if s.Parent().SpanID() != upload.SpanContext().SpanID() {
			t.Errorf("%s is under %s, want the upload request's span %s", s.Name(), s.Parent().SpanID(),
				upload.SpanContext().SpanID())
		}
	}
	checkAttrs(t, encode, map[attribute.Key]string{"slumber_mill.attempt": "1", "slumber_mill.outcome": "done"})
	checkAttrs(t, deliver, map[attribute.Key]string{
		"slumber_mill.attempt": "1", "slumber_mill.outcome": "done", "server.address": "127.0.0.1",
		"http.response.status_code": "204",
	})
	steps := map[string]int{"audio.probe": 1, "audio.measure": 1, "audio.render": 1, "audio.verify": 2}
	for name, n := range steps {
		if got := len(byName[name]); got != n {
			t.Errorf("%d %s spans, want %d", got, name, n)
		}
		for _, s := range byName[name] {
			if s.Parent().SpanID() != encode.SpanContext().SpanID() {
				t.Errorf("%s is under %s, want asset.encode's %s", name, s.Parent().SpanID(),
					encode.SpanContext().SpanID())
			}
		}
	}
	var statements []string
	for _, s := range spans {
		if s.SpanKind() == trace.SpanKindClient && s.Parent().SpanID() == encode.SpanContext().SpanID() {
			statements = append(statements, s.Name())
		}
	}
	for _, want := range []string{"SELECT assets", "INSERT renditions", "INSERT asset_events"} {
		if !strings.Contains(strings.Join(statements, ","), want) {
			t.Errorf("statements under asset.encode = %v, want %q among them", statements, want)
		}
	}
}

// leaseSource is what a worker pool drives: the processor or the deliverer.
type leaseSource interface {
	Claim(ctx context.Context) (worker.Lease, bool, error)
	Extend(ctx context.Context, l worker.Lease) error
	Process(ctx context.Context, l worker.Lease) error
	Fail(ctx context.Context, l worker.Lease, cause error) error
	Release(ctx context.Context, l worker.Lease) error
	Reap(ctx context.Context) error
}

// runPool runs a pool of one slot on src until nothing is due.
func runPool(t *testing.T, src leaseSource, name string) {
	t.Helper()
	p, err := worker.NewPool(src, worker.Config{
		WorkerID: name + "-1", Pool: name, Concurrency: 1, Lease: time.Minute, PollInterval: time.Second,
		Log: testutil.Logger(t),
	})
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	if err := p.RunUntilIdle(t.Context()); err != nil {
		t.Fatalf("the %s pool's RunUntilIdle() error = %v", name, err)
	}
}

// only returns the one span called name.
func only(t *testing.T, byName map[string][]sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	if n := len(byName[name]); n != 1 {
		t.Fatalf("%d %s spans, want 1", n, name)
	}
	return byName[name][0]
}

func checkAttrs(t *testing.T, s sdktrace.ReadOnlySpan, want map[attribute.Key]string) {
	t.Helper()
	got := map[attribute.Key]string{}
	for _, kv := range s.Attributes() {
		got[kv.Key] = kv.Value.Emit()
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s has %s = %q, want %q", s.Name(), k, got[k], v)
		}
	}
}

// TestTraceAcrossTheQueue runs the three processes with an OTLP endpoint, each as its own service: an upload Checkly
// traced has been exported by all three by the time they exit, the pools' spans in the upload's trace, and an
// upload Checkly didn't trace by none. Neither webhook carries a trace context, and no export or log line
// holds the webhook URL's path.
func TestTraceAcrossTheQueue(t *testing.T) {
	sink := &otlpSink{}
	collector := httptest.NewServer(sink)
	defer collector.Close()
	e := startE2E(t, e2eConfig{otlpEndpoint: collector.URL})
	for process, service := range map[string]string{
		"api": "slumber-mill-api", "encode pool": "slumber-mill-worker-encode", "webhook pool": "slumber-mill-worker-webhook",
	} {
		e.logs[process].wait(t, "exporting traces started by Checkly", "endpoint", collector.URL, "service", service)
	}

	nextHook := func(want apiv1.Asset) {
		t.Helper()
		h := waitFor(t, e.hooks, "the webhook of "+want.Title)
		if ev := decode[apiv1.Event](t, h.body); ev.Type != "asset.ready" || ev.Data.ID != want.ID {
			t.Fatalf("got a %s webhook for %s, want asset.ready for %s", ev.Type, ev.Data.ID, want.ID)
		}
		for _, name := range []string{"Traceparent", "Tracestate"} {
			if v := h.header.Values(name); len(v) > 0 {
				t.Errorf("the webhook of %s has %s %q, want none", want.Title, name, v)
			}
		}
	}
	untraced := e.upload(t, "Thunder", testutil.MonoPink(t))
	nextHook(untraced)
	traced := e.uploadWith(t, "Rain", testutil.MonoPink(t), http.Header{
		"Traceparent": {"00-" + checklyTraceID + "-" + checklyParentID + "-01"},
		"Tracestate":  {"checkly=true"},
	})
	nextHook(traced)
	// At once, well within the batch's 5 s delay, so the pools' spans go out only as they stop.
	e.stop(t)

	traceID, err := hex.DecodeString(checklyTraceID)
	if err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	exported := map[string]bool{}
	for _, r := range sink.requests {
		if r.path != "/v1/traces" || r.authorization != otlpTestKey {
			t.Errorf("export to %s with the key %t, want /v1/traces with it", r.path, r.authorization == otlpTestKey)
		}
		if bytes.Contains(r.body, []byte(untraced.ID)) {
			t.Error("an export holds the untraced upload's asset ID")
		}
		if bytes.Contains(r.body, []byte("/hooks")) {
			t.Error("an export holds the webhook URL's path")
		}
		for _, span := range []struct{ service, name string }{
			{"slumber-mill-api", "POST /v1/assets"},
			{"slumber-mill-worker-encode", "asset.encode"},
			{"slumber-mill-worker-webhook", "webhook.deliver"},
		} {
			if bytes.Contains(r.body, []byte(span.service)) && bytes.Contains(r.body, []byte(span.name)) &&
				bytes.Contains(r.body, traceID) {
				exported[span.name] = true
			}
		}
	}
	for _, name := range []string{"POST /v1/assets", "asset.encode", "webhook.deliver"} {
		if !exported[name] {
			t.Errorf("no export holds the %s span of Checkly's trace from its service", name)
		}
	}
	for process, logs := range e.logs {
		logs.mu.Lock()
		for _, rec := range logs.records {
			if line := fmt.Sprint(rec); strings.Contains(line, "/hooks") || strings.Contains(line, otlpTestKey) {
				t.Errorf("%s logged %v", process, rec)
			}
		}
		logs.mu.Unlock()
	}
}
