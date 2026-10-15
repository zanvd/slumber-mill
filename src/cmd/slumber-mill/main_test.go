package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/tenant"
)

// None of these cases reach a database: each one stops at its arguments, its configuration or its DSN.
func TestRun(t *testing.T) {
	const badDSN = "no-slash"
	validKeys := "1:" + base64.StdEncoding.EncodeToString(make([]byte, 32))
	shortKey := base64.StdEncoding.EncodeToString([]byte("sixteen-byte-key"))
	// The API and the webhook pool sweep it before they open the database.
	dataDir := t.TempDir()

	tests := []struct {
		name string
		args []string
		env  map[string]string
		// stopped runs the command on a context cancelled already, as when a signal arrives during startup.
		stopped       bool
		wantCode      int
		wantStderr    []string
		notWantStderr []string
	}{
		{name: "no command", args: nil, wantCode: exitUsage, wantStderr: []string{"no command given", "usage:"}},
		{name: "help", args: []string{"help"}, wantCode: exitOK, wantStderr: []string{"usage:"}},
		{name: "help flag", args: []string{"-h"}, wantCode: exitOK, wantStderr: []string{"usage:"}},
		{name: "unknown command", args: []string{"serve"}, wantCode: exitUsage,
			wantStderr: []string{`unknown command "serve"`, "usage:"}},

		{name: "migrate with an argument", args: []string{"migrate", "now"}, wantCode: exitUsage,
			wantStderr: []string{`migrate: unexpected argument "now"`}},
		{name: "migrate with a flag", args: []string{"migrate", "-force"}, wantCode: exitUsage,
			wantStderr: []string{"migrate: flag provided but not defined: -force"}},
		{name: "migrate help", args: []string{"migrate", "-h"}, wantCode: exitOK, wantStderr: []string{"usage:"}},
		{name: "migrate with a bad DSN", args: []string{"migrate"}, env: map[string]string{envDSN: badDSN},
			wantCode: exitError, wantStderr: []string{"error parsing DSN"}},

		{name: "api with an argument", args: []string{"api", "now"}, wantCode: exitUsage,
			wantStderr: []string{`api: unexpected argument "now"`}},
		{name: "api with bad configuration", args: []string{"api"},
			env:      map[string]string{envShutdownGrace: "soon", envMaxUploadsPerTenant: "many"},
			wantCode: exitError,
			wantStderr: []string{`SLUMBER_MILL_SHUTDOWN_GRACE must be a positive duration, such as 30s, got \"soon\"`,
				`SLUMBER_MILL_MAX_UPLOADS_PER_TENANT must be a positive integer, got \"many\"`}},
		{name: "api with a bad DSN", args: []string{"api"}, env: map[string]string{envDSN: badDSN, envDataDir: dataDir},
			wantCode: exitError, wantStderr: []string{"error parsing DSN"}},

		{name: "worker without a pool", args: []string{"worker"}, wantCode: exitUsage,
			wantStderr: []string{"worker: -pool is required"}},
		{name: "worker with an unknown pool", args: []string{"worker", "-pool", "audio"}, wantCode: exitUsage,
			wantStderr: []string{`worker: unknown pool "audio", want encode or webhook`}},
		{name: "worker with an argument", args: []string{"worker", "-pool", "encode", "now"}, wantCode: exitUsage,
			wantStderr: []string{`worker: unexpected argument "now"`}},
		{name: "worker help", args: []string{"worker", "-h"}, wantCode: exitOK, wantStderr: []string{"usage:"}},
		{name: "encode worker with bad configuration", args: []string{"worker", "-pool", "encode"},
			env:      map[string]string{envEncodeLease: "soon", envEncodeConcurrency: "0"},
			wantCode: exitError,
			wantStderr: []string{`SLUMBER_MILL_ENCODE_LEASE must be a duration of at least 1m0s, such as 2m0s, got \"soon\"`,
				`SLUMBER_MILL_ENCODE_CONCURRENCY must be a positive integer, got \"0\"`}},
		{name: "webhook worker with bad configuration", args: []string{"worker", "-pool", "webhook"},
			env:      map[string]string{envWebhookConcurrency: "lots"},
			wantCode: exitError,
			wantStderr: []string{`SLUMBER_MILL_WEBHOOK_CONCURRENCY must be a positive integer, got \"lots\"`,
				"SLUMBER_MILL_SECRET_KEYS is required"}},
		{name: "webhook worker with a malformed key ring", args: []string{"worker", "-pool", "webhook"},
			env:           map[string]string{envSecretKeys: "1:secret-material!"},
			wantCode:      exitError,
			wantStderr:    []string{"error loading SLUMBER_MILL_SECRET_KEYS: error parsing key ring entry 1"},
			notWantStderr: []string{"secret-material"}},
		{name: "webhook worker with a bad DSN", args: []string{"worker", "-pool", "webhook"},
			env:      map[string]string{envSecretKeys: validKeys, envDSN: badDSN, envDataDir: dataDir},
			wantCode: exitError, wantStderr: []string{"error parsing DSN"}, notWantStderr: []string{validKeys[2:]}},
		{name: "api with an encode lease below the minimum", args: []string{"api"},
			env:      map[string]string{envEncodeLease: "59s", envDataDir: dataDir},
			wantCode: exitError,
			wantStderr: []string{
				`SLUMBER_MILL_ENCODE_LEASE must be a duration of at least 1m0s, such as 2m0s, got \"59s\"`}},
		{name: "encode worker with an encode lease below the minimum", args: []string{"worker", "-pool", "encode"},
			env:      map[string]string{envEncodeLease: "2ns", envDataDir: dataDir},
			wantCode: exitError,
			wantStderr: []string{
				`SLUMBER_MILL_ENCODE_LEASE must be a duration of at least 1m0s, such as 2m0s, got \"2ns\"`},
			notWantStderr: []string{"ffmpeg", "swept"}},
		{name: "webhook worker with an encode lease below the minimum", args: []string{"worker", "-pool", "webhook"},
			env:      map[string]string{envSecretKeys: validKeys, envEncodeLease: "59s", envDataDir: dataDir},
			wantCode: exitError,
			wantStderr: []string{
				`SLUMBER_MILL_ENCODE_LEASE must be a duration of at least 1m0s, such as 2m0s, got \"59s\"`},
			notWantStderr: []string{"swept"}},
		// Whether or not ffmpeg is installed, the check fails on the cancelled context; a bad DSN would fail
		// the command if startup went on.
		{name: "encode worker stopped during the ffmpeg check", args: []string{"worker", "-pool", "encode"},
			env: map[string]string{envDSN: badDSN, envDataDir: dataDir}, stopped: true, wantCode: exitOK,
			notWantStderr: []string{"command failed", "ffmpeg", "DSN"}},

		{name: "tenant without a subcommand", args: []string{"tenant"}, wantCode: exitUsage,
			wantStderr: []string{"tenant: the only subcommand is create"}},
		{name: "tenant with an unknown subcommand", args: []string{"tenant", "delete"}, wantCode: exitUsage,
			wantStderr: []string{"tenant: the only subcommand is create"}},
		{name: "tenant create without a slug", args: []string{"tenant", "create"}, wantCode: exitUsage,
			wantStderr: []string{"tenant create: -slug is required"}},
		{name: "tenant create with a bad target", args: []string{"tenant", "create", "-slug", "demo", "-target-lufs", "loud"},
			wantCode: exitUsage, wantStderr: []string{`tenant create: invalid value "loud" for flag -target-lufs`}},
		{name: "tenant create with an argument", args: []string{"tenant", "create", "-slug", "demo", "extra"},
			wantCode: exitUsage, wantStderr: []string{`tenant create: unexpected argument "extra"`}},
		{name: "tenant create without a key ring", args: []string{"tenant", "create", "-slug", "demo"},
			wantCode: exitError, wantStderr: []string{"SLUMBER_MILL_SECRET_KEYS is required"}},
		{name: "tenant create with a malformed key ring", args: []string{"tenant", "create", "-slug", "demo"},
			env:           map[string]string{envSecretKeys: "1:secret-material!"},
			wantCode:      exitError,
			wantStderr:    []string{"error loading SLUMBER_MILL_SECRET_KEYS: error parsing key ring entry 1"},
			notWantStderr: []string{"secret-material"}},
		{name: "tenant create with a short key", args: []string{"tenant", "create", "-slug", "demo"},
			env:           map[string]string{envSecretKeys: "1:" + shortKey},
			wantCode:      exitError,
			wantStderr:    []string{"error loading SLUMBER_MILL_SECRET_KEYS", "the key must be 32 bytes"},
			notWantStderr: []string{shortKey}},
		{name: "tenant create with a bad DSN", args: []string{"tenant", "create", "-slug", "demo"},
			env:      map[string]string{envSecretKeys: validKeys, envDSN: badDSN},
			wantCode: exitError, wantStderr: []string{"error parsing DSN"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.stopped {
				cancel()
			}
			var stdout, stderr bytes.Buffer
			code := run(ctx, tt.args, envFrom(tt.env), &stdout, &stderr)
			if code != tt.wantCode {
				t.Errorf("run(%q) = %d, want %d; stderr:\n%s", tt.args, code, tt.wantCode, stderr.String())
			}
			if stdout.Len() > 0 {
				t.Errorf("run(%q) wrote to stdout: %q", tt.args, stdout.String())
			}
			for _, want := range tt.wantStderr {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("run(%q) stderr = %q, want it to contain %q", tt.args, stderr.String(), want)
				}
			}
			for _, notWant := range tt.notWantStderr {
				if strings.Contains(stderr.String(), notWant) {
					t.Errorf("run(%q) stderr = %q, must not contain %q", tt.args, stderr.String(), notWant)
				}
			}
			if code == exitError {
				for line := range strings.Lines(stderr.String()) {
					if !json.Valid([]byte(line)) {
						t.Errorf("run(%q) wrote a stderr line that isn't JSON: %q", tt.args, line)
					}
				}
			}
		})
	}
}

// TestEncodeWorkerChecksFFmpegFirst runs the encode pool with nothing on PATH: it stops with a clear error
// before it sweeps temp files or reads its DSN.
func TestEncodeWorkerChecksFFmpegFirst(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	dataDir := t.TempDir()
	stale := plantTempFile(t, dataDir, "part-stale", time.Now().Add(-24*time.Hour))
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), []string{"worker", "-pool", "encode"},
		envFrom(map[string]string{envDSN: "no-slash", envDataDir: dataDir}), &stdout, &stderr)

	if code != exitError {
		t.Errorf("exit code %d, want %d; stderr:\n%s", code, exitError, stderr.String())
	}
	if want := "the encode pool needs ffmpeg and ffprobe: ffmpeg is not installed"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
	if strings.Contains(stderr.String(), "DSN") || strings.Contains(stderr.String(), "swept") {
		t.Errorf("stderr = %q, want it to stop before the sweep and the database", stderr.String())
	}
	if !exists(t, stale) {
		t.Error("the stale temp file was swept, want the ffmpeg check first")
	}
	if stdout.Len() > 0 {
		t.Errorf("wrote to stdout: %q", stdout.String())
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("broken pipe")
}

func TestPrintCredentials(t *testing.T) {
	tests := []struct {
		name  string
		creds tenant.Credentials
		want  string
	}{
		{
			name:  "API key only",
			creds: tenant.Credentials{APIKey: secret.NewValue([]byte("sm_key"))},
			want:  "api_key: sm_key\n",
		},
		{
			name: "with a webhook secret",
			creds: tenant.Credentials{
				APIKey:        secret.NewValue([]byte("sm_key")),
				WebhookSecret: secret.NewValue([]byte("whsec_secret")),
			},
			want: "api_key: sm_key\nwebhook_secret: whsec_secret\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b bytes.Buffer
			if err := printCredentials(&b, tt.creds); err != nil {
				t.Fatalf("printCredentials() error = %v", err)
			}
			if b.String() != tt.want {
				t.Errorf("printCredentials() wrote %q, want %q", b.String(), tt.want)
			}
		})
	}

	t.Run("write fails", func(t *testing.T) {
		creds := tenant.Credentials{APIKey: secret.NewValue([]byte("sm_key"))}
		if err := printCredentials(failingWriter{}, creds); err == nil {
			t.Error("printCredentials() error = nil, want the write's error")
		}
	})
}
