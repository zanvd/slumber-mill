//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/audio"
	"github.com/zanvd/slumber-mill/src/testutil"
	"github.com/zanvd/slumber-mill/src/webhook"
)

const (
	publishedCeilingDBTP = -1.5
	loudnessToleranceLU  = 1.0
	signatureTolerance   = 5 * time.Minute
)

// e2e is the whole service as Compose runs it: the api command and both worker pools, side by side on one
// freshly migrated database and one data directory, with a tenant whose webhooks go to an httptest receiver.
type e2e struct {
	*testStack
	tenantID uint64
	base     string
	apiKey   string
	secret   []byte
	client   *http.Client
	// hooks carries every request the receiver gets, in arrival order.
	hooks   chan receivedHook
	cancel  context.CancelFunc
	procs   []*e2eProcess
	stopped bool
}

type e2eProcess struct {
	name   string
	done   chan int
	stdout bytes.Buffer
}

// receivedHook is a webhook request as the receiver got it, kept whole so a test can check any part of it.
type receivedHook struct {
	at     time.Time
	header http.Header
	body   []byte
}

type e2eConfig struct {
	encodeConcurrency int
}

// startE2E starts the stack and waits until the API serves and both pools have started.
// The stack is stopped when t ends, unless the test stops it first.
func startE2E(t *testing.T, cfg e2eConfig) *e2e {
	t.Helper()
	if cfg.encodeConcurrency == 0 {
		cfg.encodeConcurrency = 1
	}
	dsn := testutil.NewEmptyDatabase(t)
	if out := runCommand(t, map[string]string{envDSN: dsn}, "migrate"); out.code != exitOK {
		t.Fatalf("migrate: exit code %d; stderr:\n%s", out.code, out.stderr)
	}
	dataDir := t.TempDir()
	e := &e2e{
		testStack: newTestStack(t, dsn, dataDir),
		client:    &http.Client{Timeout: waitTimeout},
		hooks:     make(chan receivedHook, 64),
	}

	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("error reading a webhook: %v", err)
		}
		select {
		case e.hooks <- receivedHook{at: time.Now(), header: r.Header.Clone(), body: body}:
		default:
			t.Errorf("webhook receiver is full; dropped %s", body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(receiver.Close)

	keys := newKeyRing()
	created := runCommand(t, map[string]string{envDSN: dsn, envSecretKeys: keys, envWebhookAllowPrivate: "true"},
		"tenant", "create", "-slug", "e2e", "-webhook-url", receiver.URL+"/hooks")
	if created.code != exitOK {
		t.Fatalf("tenant create: exit code %d; stderr:\n%s", created.code, created.stderr)
	}
	var webhookSecret string
	e.apiKey, webhookSecret = parseCredentials(t, created.stdout)
	e.secret = []byte(webhookSecret)
	tn, err := e.tenants.Authenticate(t.Context(), e.apiKey)
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	e.tenantID = tn.ID

	common := map[string]string{
		envDSN:           dsn,
		envDataDir:       dataDir,
		envShutdownGrace: "5s",
	}
	with := func(extra map[string]string) map[string]string {
		env := maps.Clone(common)
		maps.Copy(env, extra)
		return env
	}

	ctx, cancel := context.WithCancel(t.Context())
	e.cancel = cancel
	api := e.start(ctx, t, "api", with(map[string]string{envHTTPAddr: "127.0.0.1:0", envDrainDelay: "0s"}), "api")
	encode := e.start(ctx, t, "encode pool",
		with(map[string]string{envEncodeConcurrency: strconv.Itoa(cfg.encodeConcurrency)}),
		"worker", "-pool", "encode")
	hooks := e.start(ctx, t, "webhook pool",
		with(map[string]string{envSecretKeys: keys, envWebhookAllowPrivate: "true"}),
		"worker", "-pool", "webhook")
	t.Cleanup(func() { e.stop(t) })

	_, serving := api.wait(t, "serving HTTP")
	addr, _ := serving["addr"].(string)
	e.base = "http://" + addr
	encode.wait(t, "worker pool started")
	hooks.wait(t, "worker pool started")
	return e
}

func (e *e2e) start(ctx context.Context, t *testing.T, name string, env map[string]string, args ...string) *logWatch {
	t.Helper()
	stderr, logs := watchLog(t)
	p := &e2eProcess{name: name, done: make(chan int, 1)}
	e.procs = append(e.procs, p)
	go func() {
		p.done <- run(ctx, args, envFrom(env), &p.stdout, stderr)
		stderr.Close()
	}()
	return logs
}

// stop signals every process and waits for each to exit 0. Nothing reaches the receiver after it returns.
func (e *e2e) stop(t *testing.T) {
	t.Helper()
	if e.stopped {
		return
	}
	e.stopped = true
	e.cancel()
	for _, p := range e.procs {
		if code := waitFor(t, p.done, p.name+" exiting"); code != exitOK {
			t.Errorf("%s: exit code %d, want 0", p.name, code)
		}
		if p.stdout.Len() > 0 {
			t.Errorf("%s wrote to stdout: %q", p.name, p.stdout.String())
		}
	}
}

// do sends req with the tenant's API key and returns the response with its body read.
func (e *e2e) do(t *testing.T, req *http.Request) (*http.Response, []byte) {
	t.Helper()
	req.Header.Set("Authorization", "Bearer "+e.apiKey)
	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatalf("%s %s error = %v", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("error reading the response to %s %s: %v", req.Method, req.URL.Path, err)
	}
	return resp, body
}

func (e *e2e) get(t *testing.T, path string, header http.Header) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, e.base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	return e.do(t, req)
}

// upload posts the file at path through the API, checks that it was accepted, and returns the asset.
func (e *e2e) upload(t *testing.T, title, path string) apiv1.Asset {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	if err := mw.WriteField("title", title); err != nil {
		t.Fatal(err)
	}
	part, err := mw.CreateFormFile("file", filepath.Base(path))
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := io.Copy(part, f); err != nil {
		t.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, e.base+"/v1/assets", &body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, respBody := e.do(t, req)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST /v1/assets = %d %s, want 202", resp.StatusCode, respBody)
	}
	a := decode[apiv1.Asset](t, respBody)
	if loc := resp.Header.Get("Location"); loc != "/v1/assets/"+a.ID {
		t.Errorf("Location = %q, want /v1/assets/%s", loc, a.ID)
	}
	return a
}

func (e *e2e) asset(t *testing.T, id string) apiv1.Asset {
	t.Helper()
	resp, body := e.get(t, "/v1/assets/"+id, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/assets/%s = %d %s, want 200", id, resp.StatusCode, body)
	}
	return decode[apiv1.Asset](t, body)
}

// waitStatus polls the asset until it has status.
func (e *e2e) waitStatus(t *testing.T, id, status string) {
	t.Helper()
	deadline := time.NewTimer(waitTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		a := e.asset(t, id)
		if a.Status == status {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("asset %s is %s after %v, want %s", id, a.Status, waitTimeout, status)
		}
	}
}

// nextHook waits for the next webhook, checks its headers and signature, and returns its event.
func (e *e2e) nextHook(t *testing.T) apiv1.Event {
	t.Helper()
	return e.nextHookWithin(t, waitTimeout)
}

func (e *e2e) nextHookWithin(t *testing.T, timeout time.Duration) apiv1.Event {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	var h receivedHook
	select {
	case h = <-e.hooks:
	case <-deadline.C:
		t.Fatalf("no webhook arrived within %v", timeout)
	}
	if got := h.header.Get("Content-Type"); got != "application/json" {
		t.Errorf("webhook Content-Type = %q, want application/json", got)
	}
	if got := h.header.Get("User-Agent"); got != "slumber-mill-webhooks/1" {
		t.Errorf("webhook User-Agent = %q, want slumber-mill-webhooks/1", got)
	}
	if err := webhook.Verify(e.secret, h.header.Get(webhook.SignatureHeader), h.body, h.at,
		signatureTolerance); err != nil {
		t.Errorf("webhook signature doesn't verify: %v", err)
	}
	ev := decode[apiv1.Event](t, h.body)
	if got := h.header.Get(webhook.EventIDHeader); got != ev.ID {
		t.Errorf("webhook %s header = %q, want the event's ID %q", webhook.EventIDHeader, got, ev.ID)
	}
	return ev
}

// noMoreHooks checks that nothing else reached the receiver. Call it after stop, when nothing more can.
func (e *e2e) noMoreHooks(t *testing.T) {
	t.Helper()
	for {
		select {
		case h := <-e.hooks:
			t.Errorf("unexpected webhook: %s", h.body)
		default:
			return
		}
	}
}

func (e *e2e) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRowContext(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
	return n
}

func decode[T any](t *testing.T, body []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("error decoding %T from %s: %v", v, body, err)
	}
	return v
}

// checkInSpec checks that a ready asset has both renditions, shaped for a source with sourceChannels channels,
// and that each one's measured loudness is within tolerance of the target and its true peak under the ceiling.
func checkInSpec(t *testing.T, a apiv1.Asset, sourceChannels uint8) {
	t.Helper()
	if a.Status != "ready" || a.Error != nil {
		t.Fatalf("asset is %s with error %v, want ready", a.Status, a.Error)
	}
	want := map[string]struct {
		channels uint8
		kbps     uint32
	}{
		"aac_128k": {min(sourceChannels, 2), 128},
		"aac_64k":  {1, 64},
	}
	if len(a.Renditions) != len(want) {
		t.Fatalf("asset has %d renditions, want %d", len(a.Renditions), len(want))
	}
	for _, r := range a.Renditions {
		w, ok := want[r.Profile]
		if !ok {
			t.Errorf("unexpected rendition %q", r.Profile)
			continue
		}
		delete(want, r.Profile)
		if r.Codec != "aac" || r.Channels != w.channels || r.BitrateKbps != w.kbps {
			t.Errorf("%s is %s, %d channels, %d kbps; want aac, %d channels, %d kbps",
				r.Profile, r.Codec, r.Channels, r.BitrateKbps, w.channels, w.kbps)
		}
		if math.Abs(r.LUFS-a.TargetLUFS) > loudnessToleranceLU {
			t.Errorf("%s measures %.2f LUFS, want %.1f ± %.0f", r.Profile, r.LUFS, a.TargetLUFS, loudnessToleranceLU)
		}
		if r.TruePeakDBTP > publishedCeilingDBTP {
			t.Errorf("%s peaks at %.2f dBTP, want at most %.1f", r.Profile, r.TruePeakDBTP, publishedCeilingDBTP)
		}
	}
}

// checkDownload downloads the rendition whole, checks it against its recorded size and hash,
// then checks that a ranged request returns 206 with exactly those bytes of it.
func (e *e2e) checkDownload(t *testing.T, assetID string, r apiv1.Rendition) {
	t.Helper()
	path := "/v1/assets/" + assetID + "/renditions/" + r.Profile
	resp, full := e.get(t, path, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200", path, resp.StatusCode)
	}
	sum := sha256.Sum256(full)
	if uint64(len(full)) != r.Bytes || hex.EncodeToString(sum[:]) != r.SHA256 {
		t.Fatalf("GET %s returned %d bytes hashing to %x, want the recorded %d bytes hashing to %s",
			path, len(full), sum, r.Bytes, r.SHA256)
	}
	if etag := resp.Header.Get("ETag"); etag != `"`+r.SHA256+`"` {
		t.Errorf("GET %s ETag = %s, want the quoted sha256", path, etag)
	}

	first, last := len(full)/3, len(full)/3+4095
	resp, part := e.get(t, path, http.Header{"Range": {fmt.Sprintf("bytes=%d-%d", first, last)}})
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("ranged GET %s = %d, want 206", path, resp.StatusCode)
	}
	if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/%d", first, last, len(full)); got != want {
		t.Errorf("Content-Range = %q, want %q", got, want)
	}
	if !bytes.Equal(part, full[first:last+1]) {
		t.Errorf("ranged GET %s returned %d bytes that aren't bytes %d-%d of the rendition", path, len(part), first, last)
	}
}

// TestEndToEndReady uploads a stereo recording through the API and follows it to the tenant: it becomes ready
// with every rendition in spec, a ranged download returns the right bytes, and exactly one signed asset.ready
// arrives.
func TestEndToEndReady(t *testing.T) {
	e := startE2E(t, e2eConfig{})
	uploaded := e.upload(t, "Rain", testutil.DualMonoPink(t))

	ev := e.nextHook(t)
	if ev.Type != "asset.ready" || ev.Data.ID != uploaded.ID {
		t.Fatalf("got a %s webhook for asset %s (error %v), want asset.ready for %s",
			ev.Type, ev.Data.ID, ev.Data.Error, uploaded.ID)
	}
	got := e.asset(t, uploaded.ID)
	checkInSpec(t, got, 2)
	if !reflect.DeepEqual(ev.Data, got) {
		t.Errorf("the webhook's asset differs from the API's:\nwebhook: %+v\nAPI:     %+v", ev.Data, got)
	}
	for _, r := range got.Renditions {
		e.checkDownload(t, got.ID, r)
	}

	e.stop(t)
	e.checkSingleDelivery(t)
}

// TestEndToEndRejected uploads a text file named like a WAV file: the asset fails on its first attempt with the
// tenant-safe message and nothing else, and exactly one signed asset.failed arrives.
func TestEndToEndRejected(t *testing.T) {
	e := startE2E(t, e2eConfig{})
	uploaded := e.upload(t, "Not audio", testutil.NotAudio(t))

	ev := e.nextHook(t)
	if ev.Type != "asset.failed" || ev.Data.ID != uploaded.ID {
		t.Fatalf("got a %s webhook for asset %s, want asset.failed for %s", ev.Type, ev.Data.ID, uploaded.ID)
	}
	got := e.asset(t, uploaded.ID)
	if got.Status != "failed" || got.Error == nil || *got.Error != audio.MessageNotAudio {
		t.Errorf("asset is %s with error %v, want failed with %q", got.Status, got.Error, audio.MessageNotAudio)
	}
	if len(got.Renditions) != 0 {
		t.Errorf("failed asset has %d renditions, want none", len(got.Renditions))
	}
	if !reflect.DeepEqual(ev.Data, got) {
		t.Errorf("the webhook's asset differs from the API's:\nwebhook: %+v\nAPI:     %+v", ev.Data, got)
	}

	e.stop(t)
	if n := e.count(t, "SELECT COUNT(*) FROM assets WHERE tenant_id = ? AND status = 'failed' AND attempts = 1",
		e.tenantID); n != 1 {
		t.Errorf("tenant has %d assets failed on the first attempt, want 1", n)
	}
	e.checkSingleDelivery(t)
}

// TestEndToEndShortBeforeLong runs two encodes side by side: a short asset uploaded while a long one is encoding
// becomes ready, and its signed asset.ready arrives, while the long one is still processing.
// Then the long one becomes ready and its own signed asset.ready arrives.
func TestEndToEndShortBeforeLong(t *testing.T) {
	const longSource = 2 * time.Minute
	e := startE2E(t, e2eConfig{encodeConcurrency: 2})
	long := e.upload(t, "Ocean, all night", testutil.PinkClip(t, longSource))
	e.waitStatus(t, long.ID, "processing")
	longStarted := time.Now()
	short := e.upload(t, "Chime", testutil.MonoPink(t))

	ev := e.nextHook(t)
	if ev.Type != "asset.ready" || ev.Data.ID != short.ID {
		t.Fatalf("first webhook is %s for asset %s, want asset.ready for the short asset %s",
			ev.Type, ev.Data.ID, short.ID)
	}
	if got := e.asset(t, long.ID); got.Status != "processing" {
		t.Fatalf("long asset is %s when the short asset's webhook arrived, want processing", got.Status)
	}
	t.Logf("short asset's webhook arrived %v after the long encode started", time.Since(longStarted).Round(time.Millisecond))
	gotShort := e.asset(t, short.ID)
	checkInSpec(t, gotShort, 1)
	if !reflect.DeepEqual(ev.Data, gotShort) {
		t.Errorf("the webhook's asset differs from the API's:\nwebhook: %+v\nAPI:     %+v", ev.Data, gotShort)
	}

	ev = e.nextHookWithin(t, 2*time.Minute)
	if ev.Type != "asset.ready" || ev.Data.ID != long.ID {
		t.Fatalf("second webhook is %s for asset %s, want asset.ready for the long asset %s",
			ev.Type, ev.Data.ID, long.ID)
	}
	t.Logf("long asset's webhook arrived %v after its encode started", time.Since(longStarted).Round(time.Millisecond))
	gotLong := e.asset(t, long.ID)
	checkInSpec(t, gotLong, 1)
	if !reflect.DeepEqual(ev.Data, gotLong) {
		t.Errorf("the webhook's asset differs from the API's:\nwebhook: %+v\nAPI:     %+v", ev.Data, gotLong)
	}

	e.stop(t)
	e.noMoreHooks(t)
	if n := e.count(t, "SELECT COUNT(*) FROM assets WHERE tenant_id = ? AND status = 'ready' AND attempts = 1",
		e.tenantID); n != 2 {
		t.Errorf("tenant has %d assets ready on the first attempt, want 2", n)
	}
	if n := e.count(t, "SELECT COUNT(*) FROM asset_events WHERE tenant_id = ?", e.tenantID); n != 2 {
		t.Errorf("tenant has %d asset events, want 2", n)
	}
	if n := e.count(t, "SELECT COUNT(*) FROM webhook_deliveries WHERE tenant_id = ? AND state = 'delivered' "+
		"AND attempts = 1", e.tenantID); n != 2 {
		t.Errorf("tenant has %d deliveries delivered on the first attempt, want 2", n)
	}
	if n := e.count(t, "SELECT COUNT(*) FROM webhook_deliveries WHERE tenant_id = ?", e.tenantID); n != 2 {
		t.Errorf("tenant has %d deliveries, want 2", n)
	}
}

// checkSingleDelivery checks, once the stack has stopped, that the tenant has one event and one delivery,
// delivered on the first attempt, and that nothing else reached the receiver.
func (e *e2e) checkSingleDelivery(t *testing.T) {
	t.Helper()
	e.noMoreHooks(t)
	if n := e.count(t, "SELECT COUNT(*) FROM asset_events WHERE tenant_id = ?", e.tenantID); n != 1 {
		t.Errorf("tenant has %d asset events, want 1", n)
	}
	if n := e.count(t, "SELECT COUNT(*) FROM webhook_deliveries WHERE tenant_id = ? AND state = 'delivered' "+
		"AND attempts = 1", e.tenantID); n != 1 {
		t.Errorf("tenant has %d deliveries delivered on the first attempt, want 1", n)
	}
	if n := e.count(t, "SELECT COUNT(*) FROM webhook_deliveries WHERE tenant_id = ?", e.tenantID); n != 1 {
		t.Errorf("tenant has %d deliveries, want 1", n)
	}
}
