//go:build integration

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/tenant"
	"github.com/zanvd/slumber-mill/src/testutil"
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

	env := envFrom(map[string]string{
		envDSN:           dsn,
		envHTTPAddr:      "127.0.0.1:0",
		envDataDir:       t.TempDir(),
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
