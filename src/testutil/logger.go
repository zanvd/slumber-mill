package testutil

import (
	"log/slog"
	"testing"
)

// Logger returns a logger that writes to tb's output, so logs show with the test that made them.
func Logger(tb testing.TB) *slog.Logger {
	return slog.New(slog.NewTextHandler(tb.Output(), &slog.HandlerOptions{Level: slog.LevelDebug}))
}
