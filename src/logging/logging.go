package logging

import (
	"context"
	"log/slog"
)

type ctxKey struct{}

func CtxWithLogger(ctx context.Context, log *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, log)
}

// LoggerFromContext returns the logger stored by CtxWithLogger, or slog.Default() if there is none.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	return LoggerFromContextOr(ctx, slog.Default())
}

// LoggerFromContextOr returns the logger stored by CtxWithLogger, or fallback if there is none.
func LoggerFromContextOr(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	if ctx == nil {
		return fallback
	}
	if log, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok && log != nil {
		return log
	}
	return fallback
}
