package logging

import (
	"context"
	"log/slog"
	"testing"
)

func TestLoggerFromContext(t *testing.T) {
	stored := slog.New(slog.DiscardHandler)

	tests := []struct {
		name string
		ctx  context.Context
		want *slog.Logger
	}{
		{"stored logger", CtxWithLogger(t.Context(), stored), stored},
		{"no logger", t.Context(), slog.Default()},
		{"nil logger", CtxWithLogger(t.Context(), nil), slog.Default()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := LoggerFromContext(tc.ctx); got != tc.want {
				t.Errorf("LoggerFromContext() = %p, want %p", got, tc.want)
			}
		})
	}
}
