package asset

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
	"uuid"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/tracing"
	"github.com/zanvd/slumber-mill/src/worker"
)

// stallingRepo is a lease repository whose read of a trace context answers only when its context ends, as on a
// stalled MySQL connection. It hands out due to one claim and records the calls, safely for a running pool.
type stallingRepo struct {
	mu    sync.Mutex
	due   *lease
	calls []string
}

func (r *stallingRepo) record(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name)
}

func (r *stallingRepo) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func (r *stallingRepo) claim(context.Context, string, time.Duration) (lease, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.due == nil {
		return lease{}, false, nil
	}
	l := *r.due
	r.due = nil
	r.calls = append(r.calls, "claim")
	return l, true, nil
}

func (r *stallingRepo) leasedTrace(ctx context.Context, _ lease) (uuid.UUID, string, error) {
	r.record("leasedTrace")
	<-ctx.Done()
	return uuid.UUID{}, "", ctx.Err()
}

func (r *stallingRepo) extend(context.Context, lease, time.Duration) error {
	r.record("extend")
	return nil
}

func (r *stallingRepo) leasedAsset(context.Context, lease) (uuid.UUID, float64, error) {
	r.record("leasedAsset")
	return uuid.NewV7(), -20, nil
}

func (r *stallingRepo) finish(context.Context, lease, processed) error {
	r.record("finish")
	return nil
}

func (r *stallingRepo) fail(context.Context, lease, failure) (bool, error) {
	r.record("fail")
	return false, nil
}

func (r *stallingRepo) release(context.Context, lease) error {
	r.record("release")
	return nil
}

func (r *stallingRepo) reap(context.Context) (int, int, error) {
	return 0, 0, nil
}

func stallingProcessor(t *testing.T, repo *stallingRepo) (*Processor, *tracetest.SpanRecorder) {
	t.Helper()
	f := newProcessorFixture(t)
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(tracing.Sampler()), sdktrace.WithSpanProcessor(rec))
	p, err := NewProcessor(repo, f.files, f.tc, ProcessorConfig{WorkerID: "encode-1", Lease: processorLease, Tracer: tp})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	return p, rec
}

// TestProcessorTraceReadTimesOut stalls the read of the trace context: Trace gives up after traceReadTimeout,
// logs it and returns no span, so the attempt runs untraced.
func TestProcessorTraceReadTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, rec := stallingProcessor(t, &stallingRepo{})
		var logs bytes.Buffer
		ctx := logging.CtxWithLogger(t.Context(), slog.New(slog.NewJSONHandler(&logs, nil)))
		start := time.Now()

		_, span := p.Trace(ctx, claimed)

		if took := time.Since(start); took != traceReadTimeout {
			t.Errorf("Trace() returned after %v, want %v", took, traceReadTimeout)
		}
		if span != nil {
			t.Errorf("Trace() returned span %v, want none", span)
		}
		if n := len(rec.Started()); n != 0 {
			t.Errorf("recorded %d spans, want none", n)
		}
		if !strings.Contains(logs.String(), `"level":"WARN","msg":"error reading the asset's trace context"`) ||
			!strings.Contains(logs.String(), "deadline exceeded") {
			t.Errorf("logs = %s, want a warning that the read timed out", &logs)
		}
	})
}

// TestEncodePoolStopsDespiteAStalledTraceRead stops the encode pool while a slot waits on a stalled read of
// the trace context it just claimed: the read gives up after traceReadTimeout, the lease is released unstarted,
// and Run returns then, well inside the shutdown grace.
func TestEncodePoolStopsDespiteAStalledTraceRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		repo := &stallingRepo{due: new(lease(claimed))}
		p, _ := stallingProcessor(t, repo)
		pool, err := worker.NewPool(p, worker.Config{
			WorkerID: "encode-1", Pool: "encode", Concurrency: 1, Lease: processorLease, PollInterval: time.Second,
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
		if want := []string{"claim", "leasedTrace", "release"}; !slices.Equal(repo.recorded(), want) {
			t.Errorf("calls = %v, want %v: the lease released unstarted", repo.recorded(), want)
		}
	})
}
