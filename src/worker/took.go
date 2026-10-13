package worker

import (
	"context"
	"log/slog"
	"time"
)

// tookHandler adds the time since start to every record, so each of the work's log lines says how long it has run.
type tookHandler struct {
	slog.Handler
	start time.Time
}

func (h tookHandler) Handle(ctx context.Context, r slog.Record) error {
	r = r.Clone()
	r.AddAttrs(slog.Duration("took", time.Since(h.start)))
	return h.Handler.Handle(ctx, r)
}

func (h tookHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return tookHandler{Handler: h.Handler.WithAttrs(attrs), start: h.start}
}

func (h tookHandler) WithGroup(name string) slog.Handler {
	return tookHandler{Handler: h.Handler.WithGroup(name), start: h.start}
}
