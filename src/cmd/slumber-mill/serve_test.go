package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// fakeServer behaves like http.Server: Shutdown and Close make Serve return ErrServerClosed at once,
// and Shutdown then waits for open requests, which a hanging server never finishes.
type fakeServer struct {
	hang        bool
	serveErr    error
	shutdownErr error

	mu         sync.Mutex
	stopped    chan struct{}
	stopOnce   sync.Once
	shutdownAt time.Time
	closedAt   time.Time
}

func newFakeServer() *fakeServer {
	return &fakeServer{stopped: make(chan struct{})}
}

func (f *fakeServer) Serve(net.Listener) error {
	if f.serveErr != nil {
		return f.serveErr
	}
	<-f.stopped
	return http.ErrServerClosed
}

func (f *fakeServer) Shutdown(ctx context.Context) error {
	f.mu.Lock()
	f.shutdownAt = time.Now()
	f.mu.Unlock()
	f.stopOnce.Do(func() { close(f.stopped) })
	if f.shutdownErr != nil {
		return f.shutdownErr
	}
	if f.hang {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (f *fakeServer) Close() error {
	f.mu.Lock()
	f.closedAt = time.Now()
	f.mu.Unlock()
	f.stopOnce.Do(func() { close(f.stopped) })
	return nil
}

func (f *fakeServer) times() (shutdownAt, closedAt time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shutdownAt, f.closedAt
}

type fakeDrainer struct {
	mu      sync.Mutex
	drainAt time.Time
}

func (d *fakeDrainer) BeginShutdown() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.drainAt = time.Now()
}

func (d *fakeDrainer) at() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.drainAt
}

func TestServe(t *testing.T) {
	const (
		drainDelay = 10 * time.Second
		grace      = 30 * time.Second
	)
	tests := []struct {
		name         string
		hang         bool
		shutdownErr  error
		wantErr      bool
		wantClosedAt time.Duration // after the signal; zero means Close isn't called
	}{
		{name: "requests finish within the grace"},
		{name: "grace runs out", hang: true, wantClosedAt: drainDelay + grace},
		{name: "shutdown fails", shutdownErr: errors.New("listener close failed"), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				srv := newFakeServer()
				srv.hang, srv.shutdownErr = tt.hang, tt.shutdownErr
				d := &fakeDrainer{}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- serve(ctx, discardLogger(), srv, nil, d, drainDelay, grace) }()

				time.Sleep(time.Minute)
				synctest.Wait()
				// Errorf rather than Fatalf until serve returns, so its goroutine isn't left blocked in the bubble.
				if !d.at().IsZero() {
					t.Error("readiness failed before the signal")
				}
				signalled := time.Now()
				cancel()
				synctest.Wait()
				switch got := d.at(); {
				case got.IsZero():
					t.Error("readiness didn't begin failing after the signal")
				case !got.Equal(signalled):
					t.Errorf("readiness began failing %v after the signal, want at once", got.Sub(signalled))
				}

				time.Sleep(drainDelay - time.Nanosecond)
				synctest.Wait()
				if shutdownAt, _ := srv.times(); !shutdownAt.IsZero() {
					t.Errorf("Shutdown called %v after the signal, before the drain delay of %v",
						shutdownAt.Sub(signalled), drainDelay)
				}

				err := <-done
				if tt.wantErr != (err != nil) {
					t.Errorf("serve() error = %v, want error: %t", err, tt.wantErr)
				}
				shutdownAt, closedAt := srv.times()
				switch got := shutdownAt.Sub(signalled); {
				case shutdownAt.IsZero():
					t.Errorf("Shutdown not called, want it called %v after the signal", drainDelay)
				case got != drainDelay:
					t.Errorf("Shutdown called %v after the signal, want %v", got, drainDelay)
				}
				switch {
				case tt.wantClosedAt == 0 && !closedAt.IsZero():
					t.Errorf("Close called %v after the signal, want it not called", closedAt.Sub(signalled))
				case tt.wantClosedAt != 0 && closedAt.IsZero():
					t.Errorf("Close not called, want it called %v after the signal", tt.wantClosedAt)
				case tt.wantClosedAt != 0 && closedAt.Sub(signalled) != tt.wantClosedAt:
					t.Errorf("Close called %v after the signal, want %v", closedAt.Sub(signalled), tt.wantClosedAt)
				}
			})
		})
	}
}

func TestServeFailsBeforeSignal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := newFakeServer()
		srv.serveErr = errors.New("accept failed")
		d := &fakeDrainer{}
		err := serve(t.Context(), discardLogger(), srv, nil, d, time.Second, time.Second)
		if !errors.Is(err, srv.serveErr) {
			t.Errorf("serve() error = %v, want %v", err, srv.serveErr)
		}
		if !d.at().IsZero() {
			t.Error("readiness began failing, want it untouched")
		}
	})
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}
