package webhook

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zanvd/slumber-mill/src/worker"
)

// TestDelivererTraceReadTimesOut stalls the read of the trace context: Trace gives up after traceReadTimeout,
// logs it and returns no span, so the attempt runs untraced.
func TestDelivererTraceReadTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.repo.stallTraceRead = true
		d, rec := tracedDeliverer(t, f)
		var logs logBuffer
		start := time.Now()

		_, span := d.Trace(logs.ctx(t.Context()), claimed)

		if took := time.Since(start); took != traceReadTimeout {
			t.Errorf("Trace() returned after %v, want %v", took, traceReadTimeout)
		}
		if span != nil {
			t.Errorf("Trace() returned span %v, want none", span)
		}
		if n := len(rec.Started()); n != 0 {
			t.Errorf("recorded %d spans, want none", n)
		}
		if got := logs.messages(t); !slices.Equal(got, []string{"WARN error reading the delivery's trace context"}) ||
			!strings.Contains(logs.String(), "deadline exceeded") {
			t.Errorf("logs = %s, want a warning that the read timed out", logs.String())
		}
	})
}

// TestWebhookPoolStopsDespiteAStalledTraceRead stops the webhook pool while a slot waits on a stalled read of
// the trace context it just claimed: the read gives up after traceReadTimeout, the delivery is released unposted,
// and Run returns then, well inside the shutdown grace.
func TestWebhookPoolStopsDespiteAStalledTraceRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.repo.due = new(lease(claimed))
		f.repo.stallTraceRead = true
		d, _ := tracedDeliverer(t, f)
		pool, err := worker.NewPool(d, worker.Config{
			WorkerID: "webhook-1", Pool: "webhook", Concurrency: 1, Lease: time.Minute, PollInterval: time.Second,
			Grace: 30 * time.Second, Log: slog.New(slog.DiscardHandler),
		})
		if err != nil {
			t.Fatalf("NewPool() error = %v", err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		start := time.Now()
		stopped := make(chan time.Time, 1)
		go func() {
			pool.Run(ctx)
			stopped <- time.Now()
		}()
		synctest.Sleep(time.Second)
		cancel()

		at := <-stopped
		if took := at.Sub(start); took != traceReadTimeout {
			t.Errorf("Run returned %v after the claim, want %v: once the trace read gave up", took, traceReadTimeout)
		}
		f.repo.mu.Lock()
		defer f.repo.mu.Unlock()
		if want := []lease{lease(claimed), lease(claimed)}; !slices.Equal(f.repo.leases, want) ||
			f.repo.completed != 0 || len(f.repo.failures) != 0 || len(f.client.posts) != 0 {
			t.Errorf("leases %+v, %d completions, %d failures, %d posts; want the trace read and a release only",
				f.repo.leases, f.repo.completed, len(f.repo.failures), len(f.client.posts))
		}
	})
}
