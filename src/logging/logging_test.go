package logging

import (
	"context"
	"log/slog"
	"testing"
)

func TestLoggerFromContext(t *testing.T) {
	stored := slog.New(slog.DiscardHandler)
	fallback := slog.New(slog.DiscardHandler)

	tests := []struct {
		name         string
		ctx          context.Context
		want         *slog.Logger
		wantFallback *slog.Logger
	}{
		{"stored logger", CtxWithLogger(t.Context(), stored), stored, stored},
		{"no logger", t.Context(), slog.Default(), fallback},
		{"nil logger", CtxWithLogger(t.Context(), nil), slog.Default(), fallback},
		{"nil context", nil, slog.Default(), fallback},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := LoggerFromContext(tc.ctx); got != tc.want {
				t.Errorf("LoggerFromContext() = %p, want %p", got, tc.want)
			}
			if got := LoggerFromContextOr(tc.ctx, fallback); got != tc.wantFallback {
				t.Errorf("LoggerFromContextOr() = %p, want %p", got, tc.wantFallback)
			}
		})
	}
}
