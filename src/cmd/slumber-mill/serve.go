package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

type httpServer interface {
	Serve(l net.Listener) error
	Shutdown(ctx context.Context) error
	Close() error
}

type drainer interface {
	BeginShutdown()
}

// serve runs srv on ln until ctx is done. Readiness then fails while srv keeps serving for drainDelay,
// so a load balancer can take the node out before the listener closes.
// Requests still running after grace are cut off, and that is still a clean exit.
func serve(ctx context.Context, log *slog.Logger, srv httpServer, ln net.Listener, d drainer,
	drainDelay, grace time.Duration) error {
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	select {
	case err := <-served:
		return fmt.Errorf("error serving HTTP: %w", err)
	case <-ctx.Done():
	}
	d.BeginShutdown()
	log.InfoContext(ctx, "draining: readiness fails, still serving", "drain_delay", drainDelay.String())

	drain := time.NewTimer(drainDelay)
	defer drain.Stop()
	select {
	case err := <-served:
		return fmt.Errorf("error serving HTTP: %w", err)
	case <-drain.C:
	}

	log.InfoContext(ctx, "shutting down HTTP server", "grace", grace.String())
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), grace)
	defer cancel()
	err := srv.Shutdown(shutdownCtx)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		log.WarnContext(ctx, "shutdown grace ran out, closing open connections", "grace", grace.String())
		if err := srv.Close(); err != nil {
			log.WarnContext(ctx, "error closing HTTP server", "error", err)
		}
	case err != nil:
		return fmt.Errorf("error shutting down HTTP server: %w", err)
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("error serving HTTP: %w", err)
	}
	log.InfoContext(ctx, "HTTP server stopped")
	return nil
}
