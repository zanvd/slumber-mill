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
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
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

// logWatch keeps the JSON log records written to it, so a test can wait for a record whatever was logged around it,
// as with loops that run side by side.
type logWatch struct {
	mu      sync.Mutex
	records []map[string]any
	ended   bool
	// changed is closed, and replaced, whenever a record is added or the log ends.
	changed chan struct{}
}

// watchLog returns a writer for JSON log lines, which the caller closes, and the watch that keeps them.
func watchLog(t *testing.T) (io.WriteCloser, *logWatch) {
	t.Helper()
	r, w := io.Pipe()
	lw := &logWatch{changed: make(chan struct{})}
	go func() {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			var rec map[string]any
			if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
				t.Errorf("stderr line isn't JSON: %q", sc.Text())
				continue
			}
			lw.mu.Lock()
			lw.records = append(lw.records, rec)
			close(lw.changed)
			lw.changed = make(chan struct{})
			lw.mu.Unlock()
		}
		lw.mu.Lock()
		lw.ended = true
		close(lw.changed)
		lw.mu.Unlock()
	}()
	return w, lw
}

// wait returns the first record with msg and the attributes in attrs, given as name and value pairs, and its index.
func (lw *logWatch) wait(t *testing.T, msg string, attrs ...any) (int, map[string]any) {
	t.Helper()
	timeout := time.NewTimer(waitTimeout)
	defer timeout.Stop()
	for {
		lw.mu.Lock()
		for i, rec := range lw.records {
			if rec["msg"] == msg && hasAttrs(rec, attrs) {
				lw.mu.Unlock()
				return i, rec
			}
		}
		ended, changed := lw.ended, lw.changed
		lw.mu.Unlock()
		if ended {
			t.Fatalf("the log ended without %q %v", msg, attrs)
		}
		select {
		case <-changed:
		case <-timeout.C:
			t.Fatalf("no %q %v logged within %v", msg, attrs, waitTimeout)
		}
	}
}

func hasAttrs(rec map[string]any, attrs []any) bool {
	for i := 0; i+1 < len(attrs); i += 2 {
		name, _ := attrs[i].(string)
		if rec[name] != attrs[i+1] {
			return false
		}
	}
	return true
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

// cancelOnLog writes log lines to w, and calls cancel once it has written a record with msg,
// so the command is stopped at that point of its startup.
type cancelOnLog struct {
	w      io.Writer
	msg    string
	cancel context.CancelFunc
}

func (c cancelOnLog) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	var rec struct {
		Msg string `json:"msg"`
	}
	if json.Unmarshal(p, &rec) == nil && rec.Msg == c.msg {
		c.cancel()
	}
	return n, err
}

// TestStopDuringStartup signals the long-running commands before and while they start up. A stop isn't a crash,
// so each one exits 0, and the API stops without serving; a real startup error still exits 1.
func TestStopDuringStartup(t *testing.T) {
	dsn := testutil.NewDatabase(t)
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	apiEnv := func(addr string) map[string]string {
		return map[string]string{envDSN: dsn, envHTTPAddr: addr, envDrainDelay: "1s", envShutdownGrace: "1s"}
	}
	webhookEnv := map[string]string{envDSN: dsn, envSecretKeys: newKeyRing()}

	tests := []struct {
		name string
		args []string
		env  map[string]string
		// stopBefore cancels the context before the command starts, and stopOn once it logs that message.
		// With neither, nothing stops the command.
		stopBefore  bool
		stopOn      string
		wantCode    int
		wantLogs    []string
		notWantLogs []string
		wantError   []string
	}{
		{name: "api stopped before startup", args: []string{"api"}, env: apiEnv("127.0.0.1:0"), stopBefore: true,
			wantLogs: []string{"stopped before serving HTTP"}, notWantLogs: []string{"serving HTTP", "command failed"}},
		{name: "api stopped during startup", args: []string{"api"}, env: apiEnv("127.0.0.1:0"),
			stopOn:   "swept temp files",
			wantLogs: []string{"stopped before serving HTTP"}, notWantLogs: []string{"serving HTTP", "command failed"}},
		// The host name is resolved on the signal's context, which can fail the listen.
		{name: "api on a host name stopped before startup", args: []string{"api"}, env: apiEnv("localhost:0"),
			stopBefore: true,
			wantLogs:   []string{"stopped before serving HTTP"}, notWantLogs: []string{"serving HTTP", "command failed"}},
		{name: "api with its port taken", args: []string{"api"}, env: apiEnv(taken.Addr().String()),
			wantCode: exitError, wantLogs: []string{"command failed"}, notWantLogs: []string{"serving HTTP"},
			wantError: []string{"error listening on " + taken.Addr().String(), "address already in use"}},
		{name: "webhook pool stopped before startup", args: []string{"worker", "-pool", "webhook"}, env: webhookEnv,
			stopBefore: true, wantLogs: []string{"worker pool stopped"}, notWantLogs: []string{"command failed"}},
		{name: "webhook pool stopped during startup", args: []string{"worker", "-pool", "webhook"}, env: webhookEnv,
			stopOn: "swept temp files", wantLogs: []string{"worker pool stopped"}, notWantLogs: []string{"command failed"}},
		{name: "encode pool stopped during startup", args: []string{"worker", "-pool", "encode"},
			env: map[string]string{envDSN: dsn}, stopOn: "found ffmpeg",
			wantLogs: []string{"swept temp files", "worker pool stopped"}, notWantLogs: []string{"command failed"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := maps.Clone(tt.env)
			env[envDataDir] = t.TempDir()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.stopBefore {
				cancel()
			}
			var stdout, stderr bytes.Buffer
			var logs io.Writer = &stderr
			if tt.stopOn != "" {
				logs = cancelOnLog{w: &stderr, msg: tt.stopOn, cancel: cancel}
			}
			done := make(chan int, 1)
			go func() { done <- run(ctx, tt.args, envFrom(env), &stdout, logs) }()
			code := waitFor(t, done, "exit")

			if code != tt.wantCode {
				t.Errorf("exit code %d, want %d; stderr:\n%s", code, tt.wantCode, stderr.String())
			}
			records := map[string]map[string]any{}
			for line := range strings.Lines(stderr.String()) {
				var rec map[string]any
				if err := json.Unmarshal([]byte(line), &rec); err != nil {
					t.Errorf("stderr line isn't JSON: %q", line)
					continue
				}
				if msg, _ := rec["msg"].(string); records[msg] == nil {
					records[msg] = rec
				}
			}
			for _, msg := range tt.wantLogs {
				if records[msg] == nil {
					t.Errorf("no %q logged; stderr:\n%s", msg, stderr.String())
				}
			}
			for _, msg := range tt.notWantLogs {
				if records[msg] != nil {
					t.Errorf("logged %v, want no %q", records[msg], msg)
				}
			}
			for _, want := range tt.wantError {
				if got, _ := records["command failed"]["error"].(string); !strings.Contains(got, want) {
					t.Errorf("logged the error %q, want it to contain %q", got, want)
				}
			}
			if stdout.Len() > 0 {
				t.Errorf("wrote to stdout: %q", stdout.String())
			}
		})
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

// testEncodeLease differs from webhookLease, so the tests can tell which lease a process used.
const testEncodeLease = 90 * time.Second

// checkHeld checks that the row query selects, as locked_by and the microseconds left on its lease, is held by
// the worker that logged started, on a lease claimed or renewed for lease moments ago.
func (s *testStack) checkHeld(t *testing.T, started map[string]any, lease time.Duration, query string, args ...any) {
	t.Helper()
	const slack = 10 * time.Second
	var lockedBy string
	var micros int64
	if err := s.pool.QueryRowContext(t.Context(), query, args...).Scan(&lockedBy, &micros); err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
	worker, _ := started["worker"].(string)
	left := time.Duration(micros) * time.Microsecond
	if worker == "" || lockedBy != worker {
		t.Errorf("held row is locked by %q, want the pool's worker %q", lockedBy, worker)
	}
	if left <= lease-slack || left > lease {
		t.Errorf("held row's lease has %v left, want just under the pool's lease of %v", left, lease)
	}
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
	return waitForWithin(t, ch, waitTimeout, what)
}

func waitForWithin[T any](t *testing.T, ch <-chan T, timeout time.Duration, what string) T {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	select {
	case v := <-ch:
		return v
	case <-deadline.C:
		t.Fatalf("%s didn't happen within %v", what, timeout)
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
		envEncodeLease:       testEncodeLease.String(),
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
	checkSwept(t, waitForLog(t, records, "swept temp files"), "1m30s", stale, fresh)
	started := waitForLog(t, records, "worker pool started")
	if started["pool"] != "encode" || started["concurrency"] != float64(1) || started["lease"] != "1m30s" ||
		started["grace"] != "1s" {
		t.Errorf("logged %v, want the encode pool with concurrency 1, lease 1m30s and grace 1s", started)
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
	s.checkHeld(t, started, testEncodeLease,
		"SELECT locked_by, TIMESTAMPDIFF(MICROSECOND, NOW(6), lease_expires_at) FROM assets WHERE public_id = ?",
		db.ID(inFlight.ID))
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

// TestEncodeWorkerRecoversACrash starts the encode pool where a worker died mid-encode: its asset is still processing
// on an expired lease, and its temp outputs are older than the lease. The new pool sweeps those files,
// reaps the asset and encodes it as attempt 2.
func TestEncodeWorkerRecoversACrash(t *testing.T) {
	dsn := testutil.NewDatabase(t)
	dataDir := t.TempDir()
	s := newTestStack(t, dsn, dataDir)
	tenantID, _ := s.createTenant(t, map[string]string{envDSN: dsn, envSecretKeys: newKeyRing()}, "-slug", "crash-test")
	crashed := s.upload(t, tenantID, "Rain", testutil.DualMonoPink(t))
	s.exec(t, "UPDATE assets SET status = 'processing', attempts = 1, locked_by = 'dead:1', "+
		"lease_expires_at = NOW(6) - INTERVAL 1 SECOND WHERE public_id = ?", db.ID(crashed.ID))
	left := []string{
		plantTempFile(t, dataDir, "part-1", time.Now().Add(-2*time.Minute)),
		plantTempFile(t, dataDir, "part-2", time.Now().Add(-2*time.Minute)),
	}

	env := envFrom(map[string]string{envDSN: dsn, envDataDir: dataDir, envEncodeLease: testEncodeLease.String()})
	stderr, records := logRecords(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var stdout bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"worker", "-pool", "encode"}, env, &stdout, stderr)
		stderr.Close()
	}()

	if swept := waitForLog(t, records, "swept temp files"); swept["removed"] != float64(len(left)) {
		t.Errorf("logged %v, want the %d files the dead worker left removed", swept, len(left))
	}
	for _, path := range left {
		if exists(t, path) {
			t.Errorf("%s survived the startup sweep", path)
		}
	}
	if reaped := waitForLog(t, records, "reaped assets whose lease expired"); reaped["requeued"] != float64(1) {
		t.Errorf("logged %v, want the asset requeued", reaped)
	}
	if finished := waitForLog(t, records, "work finished"); finished["outcome"] != "done" ||
		finished["attempt"] != float64(2) {
		t.Errorf("logged %v, want the asset done on attempt 2", finished)
	}
	got, err := s.assets.Get(t.Context(), tenantID, crashed.ID.String())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Status != asset.StatusReady || len(got.Renditions) != 2 {
		t.Errorf("asset is %s with %d renditions, want ready with 2", got.Status, len(got.Renditions))
	}
	cancel()
	if code := waitFor(t, done, "exit"); code != exitOK {
		t.Errorf("exit code %d, want 0", code)
	}
}

// receivedWebhook is a webhook as the test receiver got it.
type receivedWebhook struct {
	eventID   string
	verifyErr error
	// aborted means the client went away before the receiver answered.
	aborted bool
}

// retentionRows are a relayed event and a delivered delivery, which finished at the same time.
type retentionRows struct {
	eventID, deliveryEventID uuid.UUID
}

func (s *testStack) addFinished(t *testing.T, tenantID uint64, daysAgo int) retentionRows {
	t.Helper()
	rows := retentionRows{eventID: s.addEvent(t, tenantID, "Relayed "+strconv.Itoa(daysAgo)), deliveryEventID: uuid.NewV7()}
	s.exec(t, "UPDATE asset_events SET relayed_at = NOW(6) - INTERVAL ? DAY WHERE event_id = ?",
		daysAgo, db.ID(rows.eventID))
	s.exec(t, "INSERT INTO webhook_deliveries (event_id, tenant_id, body, state, attempts, run_after, updated_at) "+
		"VALUES (?, ?, '{}', 'delivered', 1, NOW(6) - INTERVAL ? DAY, NOW(6) - INTERVAL ? DAY)",
		db.ID(rows.deliveryEventID), tenantID, daysAgo, daysAgo)
	return rows
}

func (s *testStack) remaining(t *testing.T, rows retentionRows) (event, delivery bool) {
	t.Helper()
	count := func(query string, id uuid.UUID) bool {
		var n int
		if err := s.pool.QueryRowContext(t.Context(), query, db.ID(id)).Scan(&n); err != nil {
			t.Fatalf("error running %q: %v", query, err)
		}
		return n > 0
	}
	return count("SELECT COUNT(*) FROM asset_events WHERE event_id = ?", rows.eventID),
		count("SELECT COUNT(*) FROM webhook_deliveries WHERE event_id = ?", rows.deliveryEventID)
}

// TestWebhookWorker runs the webhook pool against a receiver that answers the first delivery and holds the second.
// At startup the pool sweeps stale temp files, takes the relay lock and deletes what is past retention;
// it relays both events and delivers the first, signed; and when it is cancelled with the second in flight,
// it gives up the relay lock at once, releases that delivery once the grace runs out, and exits 0.
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
	// Retention is 30 days.
	expired, kept := s.addFinished(t, tenantID, 31), s.addFinished(t, tenantID, 29)

	env := envFrom(map[string]string{
		envDSN:                 dsn,
		envDataDir:             dataDir,
		envSecretKeys:          keys,
		envWebhookAllowPrivate: "true",
		envWebhookConcurrency:  "1",
		envEncodeLease:         testEncodeLease.String(),
		envShutdownGrace:       grace.String(),
	})
	stderr, logs := watchLog(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var stdout bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"worker", "-pool", "webhook"}, env, &stdout, stderr)
		stderr.Close()
	}()

	sweptAt, swept := logs.wait(t, "swept temp files")
	checkSwept(t, swept, "1m30s", stale, fresh)
	startedAt, started := logs.wait(t, "worker pool started")
	if started["pool"] != "webhook" || started["concurrency"] != float64(1) || started["lease"] != "1m0s" ||
		started["grace"] != "1s" {
		t.Errorf("logged %v, want the webhook pool with concurrency 1, lease 1m0s and grace 1s", started)
	}
	if sweptAt > startedAt {
		t.Error("the pool started before the temp sweep")
	}
	if _, locked := logs.wait(t, "took the relay lock"); locked["pool"] != "webhook" {
		t.Errorf("logged %v, want the relay's records to name the pool", locked)
	}
	for _, msg := range []string{
		"deleted relayed asset events past retention", "deleted delivered webhook deliveries past retention",
	} {
		if _, rec := logs.wait(t, msg); rec["deleted"] != float64(1) || rec["age"] != "720h0m0s" || rec["pool"] != "webhook" {
			t.Errorf("logged %v, want the webhook pool to have deleted 1 row past 720h0m0s", rec)
		}
	}
	logs.wait(t, "work finished", "outcome", "done")

	waitFor(t, holding, "the second delivery")
	s.checkHeld(t, started, webhookLease,
		"SELECT locked_by, TIMESTAMPDIFF(MICROSECOND, NOW(6), lease_expires_at) FROM webhook_deliveries "+
			"WHERE event_id = ?", db.ID(inFlight))
	cancelled := time.Now()
	cancel()
	exitsWithinGrace(t, done, cancelled, grace)
	unlockedAt, _ := logs.wait(t, "released the relay lock")
	expiredAt, _ := logs.wait(t, "shutdown grace expired, cancelling the work")
	releasedAt, _ := logs.wait(t, "work finished", "outcome", "released")
	stoppedAt, _ := logs.wait(t, "worker pool stopped")
	// The relay stops at once, while the delivery in flight has the grace.
	if !(unlockedAt < expiredAt && expiredAt < releasedAt && releasedAt < stoppedAt) {
		t.Errorf("logged the relay lock released at %d, the grace expired at %d, the delivery released at %d and "+
			"the pool stopped at %d, want them in that order", unlockedAt, expiredAt, releasedAt, stoppedAt)
	}
	if event, delivery := s.remaining(t, expired); event || delivery {
		t.Errorf("rows finished 31 days ago remain: event %t, delivery %t; want both deleted", event, delivery)
	}
	if event, delivery := s.remaining(t, kept); !event || !delivery {
		t.Errorf("rows finished 29 days ago remain: event %t, delivery %t; want both kept", event, delivery)
	}

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
