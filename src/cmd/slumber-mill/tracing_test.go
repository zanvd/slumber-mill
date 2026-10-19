package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/tracing"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestSetupTracingLogsStartupErrors checks that what the SDK reports while it builds the provider, here a sampler
// it reads from the environment itself, reaches the JSON log rather than plain text on stderr.
// It sets OpenTelemetry's global error handler, as the api does, so it doesn't run in parallel.
func TestSetupTracingLogsStartupErrors(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", "not_a_sampler")
	var logs lockedBuffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	tp, err := setupTracing(t.Context(), log, tracing.Config{
		// Nothing is exported: no span is sampled, and the provider is shut down without one.
		Endpoint:    "http://127.0.0.1:1",
		Headers:     map[string]secret.Value{"authorization": secret.NewValue([]byte("test-only-otlp-key"))},
		ServiceName: "setup-test",
	})
	if err != nil {
		t.Fatalf("setupTracing() error = %v", err)
	}
	if err := tp.Shutdown(t.Context()); err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}

	var warned bool
	for line := range strings.Lines(logs.String()) {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Errorf("log line isn't JSON: %q", line)
			continue
		}
		if rec["msg"] == "tracing error" && rec["level"] == "WARN" &&
			strings.Contains(strings.ToLower(rec["error"].(string)), "not_a_sampler") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("no tracing error warning for the bad sampler in the log:\n%s", logs.String())
	}
}
