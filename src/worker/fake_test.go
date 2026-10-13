package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// call is one call to a fakeSource, with the state of its context at the time.
type call struct {
	lease    Lease
	at       time.Time
	cause    error
	ctxErr   error
	deadline time.Time
}

// fakeSource hands out the leases in due, one per Claim, and records every call.
// The hooks and errors are set before the pool starts.
type fakeSource struct {
	process    func(ctx context.Context, l Lease) error
	extend     func(ctx context.Context, beat int) error
	onClaim    func()
	claimErr   error
	failErr    error
	releaseErr error
	reapErr    error

	mu        sync.Mutex
	due       []Lease
	claims    []time.Time
	processed []call
	beats     []call
	fails     []call
	releases  []call
	reaps     []time.Time
}

func newFakeSource(due ...Lease) *fakeSource {
	return &fakeSource{due: due}
}

func (f *fakeSource) Claim(ctx context.Context) (Lease, bool, error) {
	if f.onClaim != nil {
		f.onClaim()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims = append(f.claims, time.Now())
	if f.claimErr != nil {
		return Lease{}, false, f.claimErr
	}
	if len(f.due) == 0 {
		return Lease{}, false, nil
	}
	l := f.due[0]
	f.due = f.due[1:]
	return l, true, nil
}

func (f *fakeSource) Extend(ctx context.Context, l Lease) error {
	n := f.record(&f.beats, ctx, l, nil)
	if f.extend == nil {
		return nil
	}
	return f.extend(ctx, n)
}

func (f *fakeSource) Process(ctx context.Context, l Lease) error {
	f.record(&f.processed, ctx, l, nil)
	if f.process == nil {
		return nil
	}
	return f.process(ctx, l)
}

func (f *fakeSource) Fail(ctx context.Context, l Lease, cause error) error {
	f.record(&f.fails, ctx, l, cause)
	return f.failErr
}

func (f *fakeSource) Release(ctx context.Context, l Lease) error {
	f.record(&f.releases, ctx, l, nil)
	return f.releaseErr
}

func (f *fakeSource) Reap(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reaps = append(f.reaps, time.Now())
	return f.reapErr
}

func (f *fakeSource) record(calls *[]call, ctx context.Context, l Lease, cause error) int {
	deadline, _ := ctx.Deadline()
	f.mu.Lock()
	defer f.mu.Unlock()
	*calls = append(*calls, call{lease: l, at: time.Now(), cause: cause, ctxErr: ctx.Err(), deadline: deadline})
	return len(*calls)
}

type fakeCalls struct {
	due                               int
	claims, reaps                     []time.Time
	processed, beats, fails, releases []call
}

func (f *fakeSource) calls() fakeCalls {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fakeCalls{
		due:       len(f.due),
		claims:    slices.Clone(f.claims),
		reaps:     slices.Clone(f.reaps),
		processed: slices.Clone(f.processed),
		beats:     slices.Clone(f.beats),
		fails:     slices.Clone(f.fails),
		releases:  slices.Clone(f.releases),
	}
}

// busy works for d on the bubble's clock, or until ctx ends.
func busy(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func callTimes(calls []call) []time.Time {
	times := make([]time.Time, len(calls))
	for i, c := range calls {
		times[i] = c.at
	}
	return times
}

// offsets returns each time as the duration since start.
func offsets(start time.Time, times []time.Time) []time.Duration {
	d := make([]time.Duration, len(times))
	for i, at := range times {
		d[i] = at.Sub(start)
	}
	return d
}

func ticks(every time.Duration, n int) []time.Duration {
	d := make([]time.Duration, n)
	for i := range d {
		d[i] = time.Duration(i+1) * every
	}
	return d
}

// logBuffer collects JSON log lines from every goroutine of a pool.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// find returns the records with message msg.
func (b *logBuffer) find(t *testing.T, msg string) []map[string]any {
	t.Helper()
	var found []map[string]any
	sc := bufio.NewScanner(strings.NewReader(b.String()))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("log line %q isn't JSON: %v", sc.Text(), err)
		}
		if rec["msg"] == msg {
			found = append(found, rec)
		}
	}
	return found
}

// testConfig returns a pool configuration with a 10 s heartbeat that logs to the returned buffer,
// which is printed if the test fails.
func testConfig(t *testing.T) (Config, *logBuffer) {
	logs := &logBuffer{}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("pool logs:\n%s", logs.String())
		}
	})
	return Config{
		WorkerID:     "worker-1",
		Pool:         "test",
		Concurrency:  1,
		Lease:        30 * time.Second,
		PollInterval: time.Second,
		Grace:        20 * time.Second,
		ReapInterval: time.Minute,
		Log:          slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}, logs
}

// running is a Pool.Run in progress inside the test's synctest bubble.
type running struct {
	t       *testing.T
	cancel  context.CancelFunc
	stopped chan struct{}
}

func startPool(t *testing.T, src Source, cfg Config) *running {
	t.Helper()
	p, err := NewPool(src, cfg)
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	r := &running{t: t, cancel: cancel, stopped: make(chan struct{})}
	go func() {
		defer close(r.stopped)
		p.Run(ctx)
	}()
	t.Cleanup(r.stop)
	return r
}

// stop cancels the pool's context and waits for Run to return.
func (r *running) stop() {
	r.cancel()
	r.wait()
}

func (r *running) returned() bool {
	select {
	case <-r.stopped:
		return true
	default:
		return false
	}
}

// wait waits for Run to return, and fails the test if any of the pool's goroutines outlived it.
func (r *running) wait() {
	r.t.Helper()
	<-r.stopped
	synctest.Wait()
	if left := poolGoroutines(); len(left) > 0 {
		r.t.Errorf("%d pool goroutines outlived Run:\n%s", len(left), strings.Join(left, "\n\n"))
	}
}

func poolGoroutines() []string {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	var found []string
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		if strings.Contains(g, "/worker.(*Pool).") {
			found = append(found, g)
		}
	}
	return found
}
