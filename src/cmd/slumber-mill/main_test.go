package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/tenant"
)

// None of these cases reach a database: each one stops at its arguments, its configuration or its DSN.
func TestRun(t *testing.T) {
	const badDSN = "no-slash"
	validKeys := "1:" + base64.StdEncoding.EncodeToString(make([]byte, 32))
	shortKey := base64.StdEncoding.EncodeToString([]byte("sixteen-byte-key"))

	tests := []struct {
		name          string
		args          []string
		env           map[string]string
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
		{name: "api with a bad DSN", args: []string{"api"}, env: map[string]string{envDSN: badDSN},
			wantCode: exitError, wantStderr: []string{"error parsing DSN"}},

		{name: "worker without a pool", args: []string{"worker"}, wantCode: exitUsage,
			wantStderr: []string{"worker: -pool is required"}},
		{name: "worker with an unknown pool", args: []string{"worker", "-pool", "audio"}, wantCode: exitUsage,
			wantStderr: []string{`worker: unknown pool "audio", want encode or webhook`}},
		{name: "worker with an argument", args: []string{"worker", "-pool", "encode", "now"}, wantCode: exitUsage,
			wantStderr: []string{`worker: unexpected argument "now"`}},
		{name: "encode worker", args: []string{"worker", "-pool", "encode"}, wantCode: exitError,
			wantStderr: []string{"worker -pool encode: not implemented yet"}},
		{name: "webhook worker", args: []string{"worker", "-pool", "webhook"}, wantCode: exitError,
			wantStderr: []string{"worker -pool webhook: not implemented yet"}},

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
			var stdout, stderr bytes.Buffer
			code := run(t.Context(), tt.args, envFrom(tt.env), &stdout, &stderr)
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
