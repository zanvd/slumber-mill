//go:build integration

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/storage"
	"github.com/zanvd/slumber-mill/src/tenant"
	"github.com/zanvd/slumber-mill/src/testutil"
	"github.com/zanvd/slumber-mill/src/webhook"
)

const waitTimeout = 30 * time.Second

func newKeyRing() string {
	key := make([]byte, 32)
	rand.Read(key)
	return "1:" + base64.StdEncoding.EncodeToString(key)
}

type output struct {
	code           int
	stdout, stderr string
}

func runCommand(t *testing.T, env map[string]string, args ...string) output {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), args, envFrom(env), &stdout, &stderr)
	return output{code: code, stdout: stdout.String(), stderr: stderr.String()}
}

func assertJSONLines(t *testing.T, stderr string) {
	t.Helper()
	for line := range strings.Lines(stderr) {
		if !json.Valid([]byte(line)) {
			t.Errorf("stderr line isn't JSON: %q", line)
		}
	}
}

// parseCredentials splits tenant create's stdout into the API key and the webhook secret, which may be empty.
func parseCredentials(t *testing.T, stdout string) (apiKey, webhookSecret string) {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	if len(lines) == 0 || len(lines) > 2 {
		t.Fatalf("stdout = %q, want one or two lines", stdout)
	}
	apiKey, ok := strings.CutPrefix(lines[0], "api_key: ")
	if !ok || !strings.HasPrefix(apiKey, "sm_") {
		t.Fatalf("stdout line 1 = %q, want api_key: sm_...", lines[0])
	}
	if len(lines) == 2 {
		webhookSecret, ok = strings.CutPrefix(lines[1], "webhook_secret: ")
		if !ok || !strings.HasPrefix(webhookSecret, "whsec_") {
			t.Fatalf("stdout line 2 = %q, want webhook_secret: whsec_...", lines[1])
		}
	}
	return apiKey, webhookSecret
}

func TestMigrateThenTenantCreate(t *testing.T) {
	dsn := testutil.NewEmptyDatabase(t)
	keys := newKeyRing()
	env := map[string]string{envDSN: dsn, envSecretKeys: keys}

	for _, name := range []string{"first migrate", "second migrate"} {
		out := runCommand(t, env, "migrate")
		if out.code != exitOK {
			t.Fatalf("%s: exit code %d, want 0; stderr:\n%s", name, out.code, out.stderr)
		}
		if out.stdout != "" {
			t.Errorf("%s wrote to stdout: %q", name, out.stdout)
		}
		assertJSONLines(t, out.stderr)
	}

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
		t.Fatalf("secret.NewBox() error = %v", err)
	}
	tenants := tenant.NewService(tenant.NewRepository(gdb), box, nil)

	tests := []struct {
		name           string
		args           []string
		wantCode       int
		wantStderr     string
		wantWebhookURL string
		wantLUFS       float64
	}{
		{
			name:           "with a webhook",
			args:           []string{"-slug", "with-webhook", "-webhook-url", "https://93.184.215.14/hooks"},
			wantWebhookURL: "https://93.184.215.14/hooks",
			wantLUFS:       -16,
		},
		{
			name:     "without a webhook",
			args:     []string{"-slug", "plain", "-target-lufs", "-14"},
			wantLUFS: -14,
		},
		{
			name:       "private webhook host",
			args:       []string{"-slug", "private", "-webhook-url", "http://127.0.0.1:8080/hook"},
			wantCode:   exitError,
			wantStderr: "webhook_url: host must be public",
		},
		{
			name:       "slug taken",
			args:       []string{"-slug", "plain"},
			wantCode:   exitError,
			wantStderr: "slug already taken",
		},
	}
	var printed []string
	var stderrs strings.Builder
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := runCommand(t, env, append([]string{"tenant", "create"}, tt.args...)...)
			stderrs.WriteString(out.stderr)
			assertJSONLines(t, out.stderr)
			if out.code != tt.wantCode {
				t.Fatalf("exit code %d, want %d; stderr:\n%s", out.code, tt.wantCode, out.stderr)
			}
			if tt.wantCode != exitOK {
				if out.stdout != "" {
					t.Errorf("a failed tenant create wrote to stdout: %q", out.stdout)
				}
				if !strings.Contains(out.stderr, tt.wantStderr) {
					t.Errorf("stderr = %q, want it to contain %q", out.stderr, tt.wantStderr)
				}
				return
			}

			apiKey, webhookSecret := parseCredentials(t, out.stdout)
			printed = append(printed, apiKey)
			got, err := tenants.Authenticate(t.Context(), apiKey)
			if err != nil {
				t.Fatalf("the printed API key doesn't authenticate: %v", err)
			}
			if got.WebhookURL != tt.wantWebhookURL || got.TargetLUFS != tt.wantLUFS {
				t.Errorf("tenant = {WebhookURL: %q, TargetLUFS: %v}, want {%q, %v}",
					got.WebhookURL, got.TargetLUFS, tt.wantWebhookURL, tt.wantLUFS)
			}
			if tt.wantWebhookURL == "" {
				if webhookSecret != "" {
					t.Error("a tenant without a webhook got a webhook secret")
				}
				return
			}
			printed = append(printed, webhookSecret)
			target, err := tenants.WebhookTarget(t.Context(), got.ID)
			if err != nil {
				t.Fatalf("WebhookTarget() error = %v", err)
			}
			if string(target.Secret.Bytes()) != webhookSecret {
				t.Error("the stored webhook secret isn't the printed one")
			}
		})
	}

	if len(printed) != 3 {
		t.Fatalf("got %d printed secrets, want 3", len(printed))
	}
	for _, s := range printed {
		if strings.Contains(stderrs.String(), s) {
			t.Error("a printed secret appears on stderr")
		}
	}
}

// logRecords decodes the JSON log lines written to the returned writer, which the caller closes.
func logRecords(t *testing.T) (io.WriteCloser, <-chan map[string]any) {
	t.Helper()
	r, w := io.Pipe()
	records := make(chan map[string]any, 1024)
	go func() {
		defer close(records)
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			var rec map[string]any
			if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
				t.Errorf("stderr line isn't JSON: %q", sc.Text())
				continue
			}
			records <- rec
		}
	}()
	return w, records
}

func waitForLog(t *testing.T, records <-chan map[string]any, msg string) map[string]any {
	t.Helper()
	timeout := time.NewTimer(waitTimeout)
	defer timeout.Stop()
	for {
		select {
		case rec, ok := <-records:
			if !ok {
				t.Fatalf("the log ended before %q", msg)
			}
			if rec["msg"] == msg {
				return rec
			}
		case <-timeout.C:
			t.Fatalf("no %q logged within %v", msg, waitTimeout)
		}
	}
}

func TestAPIServesUntilDrained(t *testing.T) {
	dsn := testutil.NewDatabase(t)
	created := runCommand(t, map[string]string{envDSN: dsn, envSecretKeys: newKeyRing()},
		"tenant", "create", "-slug", "api-test")
	if created.code != exitOK {
		t.Fatalf("tenant create: exit code %d; stderr:\n%s", created.code, created.stderr)
	}
	apiKey, _ := parseCredentials(t, created.stdout)

	// Older and younger than the default encode lease of 2m, which the sweep's age follows.
	dataDir := t.TempDir()
	stale := plantTempFile(t, dataDir, "part-stale", time.Now().Add(-3*time.Minute))
	fresh := plantTempFile(t, dataDir, "part-fresh", time.Now().Add(-time.Minute))

	env := envFrom(map[string]string{
		envDSN:           dsn,
		envHTTPAddr:      "127.0.0.1:0",
		envDataDir:       dataDir,
		envDrainDelay:    "2s",
		envShutdownGrace: "5s",
	})
	stderr, records := logRecords(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var stdout bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"api"}, env, &stdout, stderr)
		stderr.Close()
	}()

	checkSwept(t, waitForLog(t, records, "swept temp files"), "2m0s", stale, fresh)
	addr, _ := waitForLog(t, records, "serving HTTP")["addr"].(string)
	base := "http://" + addr
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 10 * time.Second}
	get := func(path, key string) (int, error) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+path, nil)
		if err != nil {
			return 0, err
		}
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, nil
	}
	check := func(phase, path, key string, want int) {
		t.Helper()
		got, err := get(path, key)
		if err != nil {
			t.Fatalf("%s: GET %s error = %v", phase, path, err)
		}
		if got != want {
			t.Errorf("%s: GET %s = %d, want %d", phase, path, got, want)
		}
	}

	check("serving", "/healthz", "", http.StatusOK)
	check("serving", "/readyz", "", http.StatusOK)
	check("serving", "/v1/assets", "", http.StatusUnauthorized)
	check("serving", "/v1/assets", apiKey, http.StatusOK)

	cancel()
	waitForLog(t, records, "draining: readiness fails, still serving")
	check("draining", "/readyz", "", http.StatusServiceUnavailable)
	check("draining", "/healthz", "", http.StatusOK)
	check("draining", "/v1/assets", apiKey, http.StatusOK)

	timeout := time.NewTimer(waitTimeout)
	defer timeout.Stop()
	select {
	case code := <-done:
		if code != exitOK {
			t.Errorf("api exit code %d, want 0", code)
		}
	case <-timeout.C:
		t.Fatalf("api didn't exit within %v of the signal", waitTimeout)
	}
	if _, err := get("/healthz", ""); err == nil {
		t.Error("GET /healthz succeeded after the API exited")
	}
	if stdout.Len() > 0 {
		t.Errorf("api wrote to stdout: %q", stdout.String())
	}
}

// checkSwept checks a "swept temp files" record, and that the sweep removed stale and kept fresh.
func checkSwept(t *testing.T, rec map[string]any, olderThan, stale, fresh string) {
	t.Helper()
	if rec["removed"] != float64(1) || rec["older_than"] != olderThan {
		t.Errorf("sweep logged %v, want 1 file removed older than %s", rec, olderThan)
	}
	if exists(t, stale) {
		t.Error("the stale temp file survived the startup sweep")
	}
	if !exists(t, fresh) {
		t.Error("the startup sweep removed a temp file younger than the encode lease")
	}
}

// testStack is the services a test drives the database through, beside the command it runs.
type testStack struct {
	pool    *sql.DB
	files   *storage.Local
	tenants *tenant.Service
	assets  *asset.Service
	relay   *webhook.Relay
}

func newTestStack(t *testing.T, dsn, dataDir string) *testStack {
	t.Helper()
	gdb, err := db.Open(dsn, testutil.Logger(t))
	if err != nil {
		t.Fatalf("db.Open() error = %v", err)
	}
	pool, err := gdb.DB()
	if err != nil {
		t.Fatalf("gdb.DB() error = %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	s := &testStack{
		pool:    pool,
		files:   storage.NewLocal(dataDir),
		tenants: tenant.NewService(tenant.NewRepository(gdb), nil, nil),
	}
	s.assets = asset.NewService(asset.NewRepository(gdb), s.files)
	if s.relay, err = webhook.NewRelay(webhook.NewRepository(gdb), s.assets, s.tenants); err != nil {
		t.Fatalf("webhook.NewRelay() error = %v", err)
	}
	t.Cleanup(func() {
		if err := s.relay.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return s
}

// createTenant runs tenant create and returns the tenant's ID and its webhook secret, if it has a webhook.
func (s *testStack) createTenant(t *testing.T, env map[string]string, args ...string) (uint64, string) {
	t.Helper()
	out := runCommand(t, env, append([]string{"tenant", "create"}, args...)...)
	if out.code != exitOK {
		t.Fatalf("tenant create: exit code %d; stderr:\n%s", out.code, out.stderr)
	}
	apiKey, webhookSecret := parseCredentials(t, out.stdout)
	tn, err := s.tenants.Authenticate(t.Context(), apiKey)
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	return tn.ID, webhookSecret
}

// upload queues source as the tenant's asset, as the API would.
func (s *testStack) upload(t *testing.T, tenantID uint64, title, source string) asset.Asset {
	t.Helper()
	f, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	staged, err := s.assets.Stage(t.Context(), f)
	if err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	a, _, err := s.assets.Create(t.Context(), asset.CreateParams{
		TenantID: tenantID, TargetLUFS: -16, Title: title, Upload: staged,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return a
}

// addEvent records a failed asset of the tenant and its asset.failed event, as the encode pool would,
// and returns the event's ID.
func (s *testStack) addEvent(t *testing.T, tenantID uint64, title string) uuid.UUID {
	t.Helper()
	eventID, assetID := uuid.NewV7(), uuid.NewV7()
	sum := sha256.Sum256([]byte(title))
	s.exec(t, "INSERT INTO assets (public_id, tenant_id, title, status, attempts, source_sha256, source_bytes, "+
		"source_codec, target_lufs, error) VALUES (?, ?, ?, 'failed', 1, ?, 5, 'mp3', -16, 'not audio')",
		db.ID(assetID), tenantID, title, sum[:])
	s.exec(t, "INSERT INTO asset_events (event_id, tenant_id, asset_id, type) "+
		"SELECT ?, tenant_id, id, 'asset.failed' FROM assets WHERE public_id = ?", db.ID(eventID), db.ID(assetID))
	return eventID
}

func (s *testStack) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := s.pool.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
}

// leaseState is a row of leased work as the lease columns describe it.
type leaseState struct {
	state    string
	attempts int
	leased   bool
}

func (s *testStack) leaseState(t *testing.T, query string, args ...any) leaseState {
	t.Helper()
	var st leaseState
	if err := s.pool.QueryRowContext(t.Context(), query, args...).Scan(&st.state, &st.attempts, &st.leased); err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
	return st
}

// openFIFO makes path a FIFO and returns a channel that is closed once a reader opens it.
// Nothing is ever written to it, so the reader waits until it is killed.
func openFIFO(t *testing.T, path string) <-chan struct{} {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	opened := make(chan struct{})
	writer := make(chan *os.File, 1)
	go func() {
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			t.Errorf("error opening %s for writing: %v", path, err)
		} else {
			close(opened)
		}
		writer <- f
	}()
	t.Cleanup(func() {
		// Opening the FIFO for reading lets a writer still waiting for a reader return.
		if r, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0); err == nil {
			defer r.Close()
		}
		if f := <-writer; f != nil {
			f.Close()
		}
	})
	return opened
}

func waitFor[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	timeout := time.NewTimer(waitTimeout)
	defer timeout.Stop()
	select {
	case v := <-ch:
		return v
	case <-timeout.C:
		t.Fatalf("%s didn't happen within %v", what, waitTimeout)
	}
	var zero T
	return zero
}

// exitsWithinGrace waits for the command's exit code on done after its context was cancelled at cancelled.
// It checks that the command exited 0, and that it waited out the grace for work in flight, but not much longer.
func exitsWithinGrace(t *testing.T, done <-chan int, cancelled time.Time, grace time.Duration) {
	t.Helper()
	code := waitFor(t, done, "exit")
	elapsed := time.Since(cancelled)
	if code != exitOK {
		t.Errorf("exit code %d, want 0", code)
	}
	if elapsed < grace || elapsed > grace+5*time.Second {
		t.Errorf("exited %v after the signal, want the grace of %v and at most 5s more", elapsed, grace)
	}
}

// TestEncodeWorker runs the encode pool on the real ffmpeg. At startup it logs the build and sweeps stale temp files;
// it encodes an upload; and when it is cancelled with an asset in flight, it releases that asset once the grace
// runs out, so the attempt isn't used, and exits 0.
func TestEncodeWorker(t *testing.T) {
	const grace = time.Second
	dsn := testutil.NewDatabase(t)
	dataDir := t.TempDir()
	stale := plantTempFile(t, dataDir, "part-stale", time.Now().Add(-2*time.Minute))
	fresh := plantTempFile(t, dataDir, "part-fresh", time.Now().Add(-30*time.Second))
	s := newTestStack(t, dsn, dataDir)
	tenantID, _ := s.createTenant(t, map[string]string{envDSN: dsn, envSecretKeys: newKeyRing()}, "-slug", "encode-test")
	ready := s.upload(t, tenantID, "Rain", testutil.DualMonoPink(t))
	inFlight := s.upload(t, tenantID, "Stalled", testutil.MonoPink(t))
	probing := openFIFO(t, s.files.SourcePath(tenantID, inFlight.ID))

	env := envFrom(map[string]string{
		envDSN:               dsn,
		envDataDir:           dataDir,
		envEncodeConcurrency: "1",
		envEncodeLease:       "1m",
		envShutdownGrace:     grace.String(),
	})
	stderr, records := logRecords(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var stdout bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"worker", "-pool", "encode"}, env, &stdout, stderr)
		stderr.Close()
	}()

	found := waitForLog(t, records, "found ffmpeg")
	if v, _ := found["ffmpeg"].(string); !strings.HasPrefix(v, "ffmpeg version 6.1.1") {
		t.Errorf("logged ffmpeg build %q, want ffmpeg version 6.1.1", v)
	}
	if v, _ := found["ffprobe"].(string); !strings.HasPrefix(v, "ffprobe version 6.1.1") {
		t.Errorf("logged ffprobe build %q, want ffprobe version 6.1.1", v)
	}
	checkSwept(t, waitForLog(t, records, "swept temp files"), "1m0s", stale, fresh)
	started := waitForLog(t, records, "worker pool started")
	if started["pool"] != "encode" || started["concurrency"] != float64(1) || started["lease"] != "1m0s" ||
		started["grace"] != "1s" {
		t.Errorf("logged %v, want the encode pool with concurrency 1, lease 1m0s and grace 1s", started)
	}

	finished := waitForLog(t, records, "work finished")
	if finished["outcome"] != "done" || finished["attempt"] != float64(1) {
		t.Errorf("logged %v, want the first asset done on attempt 1", finished)
	}
	got, err := s.assets.Get(t.Context(), tenantID, ready.ID.String())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Status != asset.StatusReady || len(got.Renditions) != 2 {
		t.Errorf("asset is %s with %d renditions, want ready with 2", got.Status, len(got.Renditions))
	}

	waitFor(t, probing, "the probe of the second asset")
	cancelled := time.Now()
	cancel()
	exitsWithinGrace(t, done, cancelled, grace)
	waitForLog(t, records, "shutdown grace expired, cancelling the work")
	if released := waitForLog(t, records, "work finished"); released["outcome"] != "released" {
		t.Errorf("logged %v, want the asset in flight released", released)
	}
	waitForLog(t, records, "worker pool stopped")

	st := s.leaseState(t, "SELECT status, attempts, locked_by IS NOT NULL FROM assets WHERE public_id = ?",
		db.ID(inFlight.ID))
	if st != (leaseState{state: "queued", attempts: 0}) {
		t.Errorf("asset in flight is %+v after shutdown, want queued with no attempts and no lease", st)
	}
	if entries, err := os.ReadDir(filepath.Join(dataDir, "tmp")); err != nil || len(entries) != 1 {
		t.Errorf("temp files after shutdown = %v, %v; want only the fresh one", entries, err)
	}
	if stdout.Len() > 0 {
		t.Errorf("wrote to stdout: %q", stdout.String())
	}
}

// receivedWebhook is a webhook as the test receiver got it.
type receivedWebhook struct {
	eventID   string
	verifyErr error
	// aborted means the client went away before the receiver answered.
	aborted bool
}

// TestWebhookWorker runs the webhook pool against a receiver that answers the first delivery and holds the second.
// At startup the pool sweeps stale temp files and takes the relay lock; it relays both events and delivers the first,
// signed; and when it is cancelled with the second in flight, it releases that delivery once the grace runs out,
// gives up the relay lock and exits 0.
func TestWebhookWorker(t *testing.T) {
	const grace = time.Second
	dsn := testutil.NewDatabase(t)
	dataDir := t.TempDir()
	stale := plantTempFile(t, dataDir, "part-stale", time.Now().Add(-2*time.Minute))
	fresh := plantTempFile(t, dataDir, "part-fresh", time.Now().Add(-30*time.Second))
	s := newTestStack(t, dsn, dataDir)

	var (
		mu       sync.Mutex
		key      []byte
		hold     string
		received []receivedWebhook
	)
	holding := make(chan struct{})
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("error reading the webhook: %v", err)
		}
		mu.Lock()
		got := receivedWebhook{
			eventID:   r.Header.Get(webhook.EventIDHeader),
			verifyErr: webhook.Verify(key, r.Header.Get(webhook.SignatureHeader), body, time.Now(), 5*time.Minute),
		}
		holdIt := got.eventID == hold
		mu.Unlock()
		if holdIt {
			close(holding)
			<-r.Context().Done()
			got.aborted = true
		}
		mu.Lock()
		received = append(received, got)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	keys := newKeyRing()
	tenantID, webhookSecret := s.createTenant(t,
		map[string]string{envDSN: dsn, envSecretKeys: keys, envWebhookAllowPrivate: "true"},
		"-slug", "hooked", "-webhook-url", receiver.URL+"/hooks")
	answered := s.addEvent(t, tenantID, "Rain")
	inFlight := s.addEvent(t, tenantID, "Thunder")
	mu.Lock()
	key, hold = []byte(webhookSecret), inFlight.String()
	mu.Unlock()

	env := envFrom(map[string]string{
		envDSN:                 dsn,
		envDataDir:             dataDir,
		envSecretKeys:          keys,
		envWebhookAllowPrivate: "true",
		envWebhookConcurrency:  "1",
		envEncodeLease:         "1m",
		envShutdownGrace:       grace.String(),
	})
	stderr, records := logRecords(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var stdout bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"worker", "-pool", "webhook"}, env, &stdout, stderr)
		stderr.Close()
	}()

	checkSwept(t, waitForLog(t, records, "swept temp files"), "1m0s", stale, fresh)
	started := waitForLog(t, records, "worker pool started")
	if started["pool"] != "webhook" || started["concurrency"] != float64(1) || started["lease"] != "1m0s" ||
		started["grace"] != "1s" {
		t.Errorf("logged %v, want the webhook pool with concurrency 1, lease 1m0s and grace 1s", started)
	}
	if locked := waitForLog(t, records, "took the relay lock"); locked["pool"] != "webhook" {
		t.Errorf("logged %v, want the relay's records to name the pool", locked)
	}
	if finished := waitForLog(t, records, "work finished"); finished["outcome"] != "done" {
		t.Errorf("logged %v, want the first delivery done", finished)
	}

	waitFor(t, holding, "the second delivery")
	cancelled := time.Now()
	cancel()
	exitsWithinGrace(t, done, cancelled, grace)
	// The relay stops at once, while the delivery in flight has the grace.
	waitForLog(t, records, "released the relay lock")
	waitForLog(t, records, "shutdown grace expired, cancelling the work")
	if released := waitForLog(t, records, "work finished"); released["outcome"] != "released" {
		t.Errorf("logged %v, want the delivery in flight released", released)
	}
	waitForLog(t, records, "worker pool stopped")

	mu.Lock()
	got := slices.Clone(received)
	mu.Unlock()
	want := []receivedWebhook{{eventID: answered.String()}, {eventID: inFlight.String(), aborted: true}}
	if !slices.Equal(got, want) {
		t.Errorf("the receiver got %+v, want %+v", got, want)
	}
	states := map[uuid.UUID]leaseState{
		answered: {state: "delivered", attempts: 1},
		inFlight: {state: "queued", attempts: 0},
	}
	for eventID, want := range states {
		st := s.leaseState(t, "SELECT state, attempts, locked_by IS NOT NULL FROM webhook_deliveries WHERE event_id = ?",
			db.ID(eventID))
		if st != want {
			t.Errorf("delivery of event %s is %+v after shutdown, want %+v", eventID, st, want)
		}
	}
	if locked, _, err := s.relay.Once(logging.CtxWithLogger(t.Context(), testutil.Logger(t))); err != nil || !locked {
		t.Errorf("another relay's Once() = %t, %v after shutdown, want it to take the relay lock", locked, err)
	}
	if stdout.Len() > 0 {
		t.Errorf("wrote to stdout: %q", stdout.String())
	}
}
