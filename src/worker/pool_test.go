package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zanvd/slumber-mill/src/logging"
)

var (
	errBoom = errors.New("boom")
	lease   = Lease{ID: 7, TenantID: 3, Attempt: 2}
)

func TestNewPool(t *testing.T) {
	valid, _ := testConfig(t)
	tests := []struct {
		name string
		src  source
		edit func(*Config)
	}{
		{"no source", nil, func(*Config) {}},
		{"no logger", newFakeSource(), func(c *Config) { c.Log = nil }},
		{"zero concurrency", newFakeSource(), func(c *Config) { c.Concurrency = 0 }},
		{"zero lease", newFakeSource(), func(c *Config) { c.Lease = 0 }},
		{"lease too short to beat", newFakeSource(), func(c *Config) { c.Lease = 2 }},
		{"negative lease", newFakeSource(), func(c *Config) { c.Lease = -time.Second }},
		{"zero poll interval", newFakeSource(), func(c *Config) { c.PollInterval = 0 }},
		{"negative grace", newFakeSource(), func(c *Config) { c.Grace = -time.Second }},
		{"negative reap interval", newFakeSource(), func(c *Config) { c.ReapInterval = -time.Second }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.edit(&cfg)
			if p, err := NewPool(tc.src, cfg); err == nil {
				t.Errorf("NewPool() = %+v, want an error", p)
			}
		})
	}

	t.Run("defaults", func(t *testing.T) {
		cfg := valid
		cfg.ReapInterval = 0
		cfg.Grace = 0
		p, err := NewPool(newFakeSource(), cfg)
		if err != nil {
			t.Fatalf("NewPool() error = %v", err)
		}
		if p.heartbeat != 10*time.Second || p.cfg.ReapInterval != 10*time.Second {
			t.Errorf("heartbeat, reap interval = %v, %v, want a third of the 30s lease", p.heartbeat, p.cfg.ReapInterval)
		}
	})
}

func TestHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		cfg, logs := testConfig(t)
		src := newFakeSource(lease)
		src.process = func(ctx context.Context, _ Lease) error { return busy(ctx, 95*time.Second) }
		r := startPool(t, src, cfg)
		synctest.Sleep(2 * time.Minute)
		r.stop()

		calls := src.calls()
		if got, want := offsets(start, callTimes(calls.beats)), ticks(10*time.Second, 9); !slices.Equal(got, want) {
			t.Errorf("beats at %v, want %v", got, want)
		}
		for _, b := range calls.beats {
			if b.lease != lease || b.ctxErr != nil || !b.deadline.Equal(b.at.Add(10*time.Second)) {
				t.Errorf("beat %+v, want lease %+v on a live context with a 10s timeout", b, lease)
			}
		}
		if len(calls.fails) != 0 || len(calls.releases) != 0 {
			t.Errorf("fails, releases = %d, %d, want none", len(calls.fails), len(calls.releases))
		}
		if got := logs.find(t, "work finished"); len(got) != 1 || got[0]["outcome"] != "done" {
			t.Errorf("outcome records = %v, want one done", got)
		}
	})
}

func TestClaimIsLogged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, logs := testConfig(t)
		src := newFakeSource(lease)
		r := startPool(t, src, cfg)
		synctest.Sleep(time.Minute)
		r.stop()

		got := logs.find(t, "work claimed")
		if len(got) != 1 {
			t.Fatalf("claim records = %v, want one", got)
		}
		rec := got[0]
		if rec["level"] != "INFO" || rec["worker"] != "worker-1" || rec["pool"] != "test" ||
			rec["id"] != float64(lease.ID) || rec["tenant_id"] != float64(lease.TenantID) ||
			rec["attempt"] != float64(lease.Attempt) {
			t.Errorf("claim record = %v, want INFO for worker-1 in pool test with lease %+v", rec, lease)
		}
	})
}

func TestHeartbeatLostLeaseCancelsTheWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, logs := testConfig(t)
		src := newFakeSource(lease)
		src.extend = func(_ context.Context, beat int) error {
			if beat == 2 {
				return fmt.Errorf("error extending: %w", ErrLeaseLost)
			}
			return nil
		}
		started := make(chan context.Context, 1)
		src.process = func(ctx context.Context, _ Lease) error {
			started <- ctx
			<-ctx.Done()
			return ctx.Err()
		}
		r := startPool(t, src, cfg)
		work := <-started

		synctest.Sleep(20*time.Second - time.Nanosecond)
		if err := work.Err(); err != nil {
			t.Fatalf("work context ended before the lease was lost: %v", err)
		}
		synctest.Sleep(time.Nanosecond)
		if cause := context.Cause(work); !errors.Is(cause, ErrLeaseLost) {
			t.Fatalf("work context cause = %v, want ErrLeaseLost", cause)
		}
		synctest.Sleep(time.Minute)
		r.stop()

		calls := src.calls()
		if len(calls.beats) != 2 {
			t.Errorf("%d beats, want 2 and none after the lease was lost", len(calls.beats))
		}
		if len(calls.fails) != 0 || len(calls.releases) != 0 {
			t.Errorf("fails, releases = %d, %d, want the result discarded", len(calls.fails), len(calls.releases))
		}
		if got := logs.find(t, "work finished"); len(got) != 1 || got[0]["outcome"] != "discarded" {
			t.Errorf("outcome records = %v, want one discarded", got)
		}
	})
}

func TestFailedHeartbeatKeepsTheWork(t *testing.T) {
	tests := []struct {
		name   string
		extend func(ctx context.Context) error
	}{
		{"error", func(context.Context) error { return errBoom }},
		{"timeout", func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				cfg, logs := testConfig(t)
				src := newFakeSource(lease)
				src.extend = func(ctx context.Context, beat int) error {
					if beat == 1 {
						return tc.extend(ctx)
					}
					return nil
				}
				src.process = func(ctx context.Context, _ Lease) error { return busy(ctx, 45*time.Second) }
				r := startPool(t, src, cfg)
				synctest.Sleep(time.Minute)
				r.stop()

				calls := src.calls()
				if got, want := offsets(start, callTimes(calls.beats)), ticks(10*time.Second, 4); !slices.Equal(got, want) {
					t.Errorf("beats at %v, want %v", got, want)
				}
				if got := logs.find(t, "heartbeat failed"); len(got) != 1 {
					t.Errorf("heartbeat failures logged = %v, want one", got)
				}
				if got := logs.find(t, "work finished"); len(got) != 1 || got[0]["outcome"] != "done" {
					t.Errorf("outcome records = %v, want one done", got)
				}
			})
		})
	}
}

func TestHeartbeatStopsBeforeTheOutcome(t *testing.T) {
	tests := []struct {
		name  string
		drain bool
	}{
		{"failed", false},
		{"released while draining", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				cfg, _ := testConfig(t)
				src := newFakeSource(lease)
				src.extend = func(ctx context.Context, _ int) error {
					return busy(context.WithoutCancel(ctx), 3*time.Second)
				}
				src.process = func(ctx context.Context, _ Lease) error {
					if err := busy(ctx, 11*time.Second); err != nil {
						return err
					}
					return errBoom
				}
				r := startPool(t, src, cfg)
				if tc.drain {
					synctest.Sleep(5 * time.Second)
					r.cancel()
				}
				synctest.Sleep(time.Minute)
				r.stop()

				calls := src.calls()
				writes, other := calls.fails, calls.releases
				if tc.drain {
					writes, other = calls.releases, calls.fails
				}
				if len(writes) != 1 || len(other) != 0 || writes[0].at.Sub(start) != 13*time.Second {
					t.Errorf("outcome written at %v, other outcome at %v, want one at 13s, when the beat in flight ended",
						offsets(start, callTimes(writes)), offsets(start, callTimes(other)))
				}
				if got, want := offsets(start, callTimes(calls.beats)), ticks(10*time.Second, 1); !slices.Equal(got, want) {
					t.Errorf("beats at %v, want %v and none once Process returned", got, want)
				}
			})
		})
	}
}

func TestOutcomes(t *testing.T) {
	ret := func(err error) func(context.Context) error {
		return func(context.Context) error { return err }
	}
	panics := func(context.Context) error { panic("boom") }
	ownTimeout := func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		<-ctx.Done()
		return fmt.Errorf("error encoding: %w", ctx.Err())
	}
	lost := fmt.Errorf("error completing: %w", ErrLeaseLost)

	tests := []struct {
		name      string
		drain     bool
		process   func(context.Context) error
		want      outcome
		wantCause string
		permanent bool
	}{
		{"success", false, ret(nil), outcomeDone, "", false},
		{"error", false, ret(errBoom), outcomeFail, "boom", false},
		{"permanent error", false, ret(Permanent(errBoom)), outcomeFail, "boom", true},
		{"own timeout", false, ownTimeout, outcomeFail, "error encoding: context deadline exceeded", false},
		{"lease lost", false, ret(lost), outcomeDiscard, "", false},
		{"panic", false, panics, outcomeFail, "panic: boom", false},
		{"success while draining", true, ret(nil), outcomeDone, "", false},
		{"error while draining", true, ret(errBoom), outcomeRelease, "", false},
		{"permanent error while draining", true, ret(Permanent(errBoom)), outcomeRelease, "", false},
		{"lease lost while draining", true, ret(lost), outcomeDiscard, "", false},
		{"panic while draining", true, panics, outcomeRelease, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg, logs := testConfig(t)
				src := newFakeSource(lease)
				proceed := make(chan struct{})
				var workErr error
				src.process = func(ctx context.Context, _ Lease) error {
					<-proceed
					workErr = ctx.Err()
					return tc.process(ctx)
				}
				r := startPool(t, src, cfg)
				synctest.Wait()
				if tc.drain {
					r.cancel()
					synctest.Wait()
					close(proceed)
					r.wait()
				} else {
					close(proceed)
					synctest.Sleep(time.Minute)
					r.stop()
				}

				calls := src.calls()
				if workErr != nil {
					t.Errorf("work context ended before Process returned: %v", workErr)
				}
				var writes []call
				switch tc.want {
				case outcomeFail:
					writes = calls.fails
					if len(calls.releases) != 0 {
						t.Errorf("released %d times, want 0", len(calls.releases))
					}
				case outcomeRelease:
					writes = calls.releases
					if len(calls.fails) != 0 {
						t.Errorf("failed %d times, want 0", len(calls.fails))
					}
				default:
					if len(calls.fails) != 0 || len(calls.releases) != 0 {
						t.Errorf("fails, releases = %d, %d, want none", len(calls.fails), len(calls.releases))
					}
				}
				if writes != nil {
					if len(writes) != 1 {
						t.Fatalf("%d outcome writes, want 1", len(writes))
					}
					w := writes[0]
					if w.lease != lease || w.ctxErr != nil || !w.deadline.Equal(w.at.Add(15*time.Second)) {
						t.Errorf("outcome written as %+v, want lease %+v on a fresh 15s context", w, lease)
					}
					if tc.wantCause != "" && (w.cause == nil || w.cause.Error() != tc.wantCause) {
						t.Errorf("cause = %v, want %q", w.cause, tc.wantCause)
					}
					if IsPermanent(w.cause) != tc.permanent {
						t.Errorf("IsPermanent(cause) = %t, want %t", IsPermanent(w.cause), tc.permanent)
					}
				}
				if got := logs.find(t, "work finished"); len(got) != 1 || got[0]["outcome"] != string(tc.want) {
					t.Errorf("outcome records = %v, want one %s", got, tc.want)
				}
			})
		})
	}
}

func TestOutcomeWriteErrors(t *testing.T) {
	tests := []struct {
		name    string
		failErr error
		wantMsg string
	}{
		{"lease lost", fmt.Errorf("error failing: %w", ErrLeaseLost), "lease lost before the outcome was written"},
		{"database error", errBoom, "writing the outcome failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg, logs := testConfig(t)
				src := newFakeSource(lease, lease)
				src.failErr = tc.failErr
				src.process = func(context.Context, Lease) error { return errBoom }
				r := startPool(t, src, cfg)
				synctest.Sleep(time.Minute)
				r.stop()

				calls := src.calls()
				if len(calls.fails) != 2 || len(calls.releases) != 0 {
					t.Errorf("fails, releases = %d, %d, want both leases failed once each", len(calls.fails), len(calls.releases))
				}
				if got := logs.find(t, tc.wantMsg); len(got) != 2 || got[0]["outcome"] != "failed" {
					t.Errorf("records %q = %v, want two for the failed outcome", tc.wantMsg, got)
				}
			})
		})
	}
}

func TestFailedReleaseIsTheLastWrite(t *testing.T) {
	tests := []struct {
		name       string
		releaseErr error
		wantMsg    string
	}{
		{"lease lost", fmt.Errorf("error releasing: %w", ErrLeaseLost), "lease lost before the outcome was written"},
		{"database error", errBoom, "writing the outcome failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				cfg, logs := testConfig(t)
				src := newFakeSource(lease)
				src.releaseErr = tc.releaseErr
				src.process = func(ctx context.Context, _ Lease) error {
					if err := busy(ctx, 10*time.Second); err != nil {
						return err
					}
					return errBoom
				}
				r := startPool(t, src, cfg)
				synctest.Sleep(5 * time.Second)
				r.cancel()
				r.wait()
				if took := time.Since(start); took != 10*time.Second {
					t.Errorf("Run returned at %v, want 10s, right after the release", took)
				}

				check := func(when string) {
					t.Helper()
					calls := src.calls()
					if len(calls.releases) != 1 || len(calls.fails) != 0 {
						t.Errorf("%s: releases, fails = %d, %d, want the one release and nothing after it",
							when, len(calls.releases), len(calls.fails))
					}
				}
				check("when Run returned")
				synctest.Sleep(time.Minute)
				check("a minute later")
				if got := logs.find(t, tc.wantMsg); len(got) != 1 || got[0]["outcome"] != "released" {
					t.Errorf("records %q = %v, want one for the released outcome", tc.wantMsg, got)
				}
			})
		})
	}
}

func TestDecide(t *testing.T) {
	live := t.Context()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	lostWork, cancelLost := context.WithCancelCause(t.Context())
	cancelLost(fmt.Errorf("error extending: %w", ErrLeaseLost))
	graceWork, cancelGrace := context.WithCancelCause(t.Context())
	cancelGrace(errGraceExpired)
	lost := fmt.Errorf("error completing: %w", ErrLeaseLost)

	tests := []struct {
		name       string
		err        error
		work, pool context.Context
		want       outcome
	}{
		{"success", nil, live, live, outcomeDone},
		{"error", errBoom, live, live, outcomeFail},
		{"process lost the lease", lost, live, live, outcomeDiscard},
		{"heartbeat lost the lease", context.Canceled, lostWork, live, outcomeDiscard},
		{"lost lease while draining", lost, graceWork, cancelled, outcomeDiscard},
		{"completed as a heartbeat lost the lease", nil, lostWork, live, outcomeDone},
		{"success while draining", nil, live, cancelled, outcomeDone},
		{"grace expired", context.Canceled, graceWork, cancelled, outcomeRelease},
		{"error while draining", errBoom, live, cancelled, outcomeRelease},
		{"own deadline", context.DeadlineExceeded, live, live, outcomeFail},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := decide(tc.err, tc.work, tc.pool); got != tc.want {
				t.Errorf("decide() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestShutdownStopsClaiming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, _ := testConfig(t)
		cfg.Concurrency = 3
		src := newFakeSource()
		r := startPool(t, src, cfg)
		synctest.Sleep(10 * time.Second)
		claimed := len(src.calls().claims)

		stopping := time.Now()
		r.stop()
		if took := time.Since(stopping); took != 0 {
			t.Errorf("Run returned %v after cancel, want at once", took)
		}
		synctest.Sleep(time.Minute)
		if got := len(src.calls().claims); got != claimed {
			t.Errorf("%d claims after cancel, want none", got-claimed)
		}
	})
}

func TestShutdownReleasesAfterGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, logs := testConfig(t)
		cfg.Concurrency = 2
		src := newFakeSource(lease)
		started := make(chan context.Context, 1)
		src.process = func(ctx context.Context, _ Lease) error {
			started <- ctx
			<-ctx.Done()
			return ctx.Err()
		}
		r := startPool(t, src, cfg)
		work := <-started
		synctest.Sleep(5 * time.Second)

		stopping := time.Now()
		claimed := len(src.calls().claims)
		r.cancel()
		synctest.Sleep(cfg.Grace - time.Nanosecond)
		if err := work.Err(); err != nil {
			t.Fatalf("work context ended %v into the grace: %v", time.Since(stopping), err)
		}
		synctest.Sleep(time.Nanosecond)
		if cause := context.Cause(work); !errors.Is(cause, errGraceExpired) {
			t.Fatalf("work context cause = %v, want errGraceExpired", cause)
		}
		r.wait()
		if took := time.Since(stopping); took != cfg.Grace {
			t.Errorf("Run returned %v after cancel, want the %v grace", took, cfg.Grace)
		}

		calls := src.calls()
		if got := len(calls.claims); got != claimed {
			t.Errorf("%d claims during the grace, want none", got-claimed)
		}
		if got, want := offsets(stopping.Add(-5*time.Second), callTimes(calls.beats)), ticks(10*time.Second, 2); !slices.Equal(got, want) {
			t.Errorf("beats at %v, want %v: they go on during the grace", got, want)
		}
		if len(calls.fails) != 0 || len(calls.releases) != 1 {
			t.Fatalf("fails, releases = %d, %d, want one release", len(calls.fails), len(calls.releases))
		}
		if rel := calls.releases[0]; rel.lease != lease || rel.ctxErr != nil || !rel.deadline.Equal(rel.at.Add(15*time.Second)) {
			t.Errorf("released as %+v, want lease %+v on a fresh 15s context", rel, lease)
		}
		if got := logs.find(t, "shutdown grace expired, cancelling the work"); len(got) != 1 {
			t.Errorf("grace expiry records = %v, want one", got)
		}
	})
}

func TestShutdownWaitsForProcessPastTheGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, _ := testConfig(t)
		src := newFakeSource(lease)
		src.process = func(ctx context.Context, _ Lease) error {
			<-ctx.Done()
			_ = busy(context.WithoutCancel(ctx), time.Minute)
			return errBoom
		}
		r := startPool(t, src, cfg)
		synctest.Sleep(5 * time.Second)
		stopping := time.Now()
		r.cancel()

		synctest.Sleep(cfg.Grace + time.Minute - time.Second)
		if calls := src.calls(); len(calls.fails) != 0 || len(calls.releases) != 0 {
			t.Fatalf("fails, releases = %d, %d while Process ignores the cancellation, want none",
				len(calls.fails), len(calls.releases))
		}
		if r.returned() {
			t.Fatal("Run returned while Process was still running")
		}
		synctest.Sleep(time.Second)
		if !r.returned() {
			t.Errorf("Run hasn't returned %v after cancel, when Process did", time.Since(stopping))
		}
		r.wait()

		calls := src.calls()
		returnedAt := stopping.Add(cfg.Grace + time.Minute)
		if len(calls.fails) != 0 || len(calls.releases) != 1 || !calls.releases[0].at.Equal(returnedAt) {
			t.Errorf("fails, releases at %v, %v after cancel, want one release at %v, when Process returned",
				offsets(stopping, callTimes(calls.fails)), offsets(stopping, callTimes(calls.releases)), cfg.Grace+time.Minute)
		}
		for _, b := range calls.beats {
			if b.at.After(stopping.Add(cfg.Grace)) {
				t.Errorf("beat at %v after cancel, want none after the %v grace", b.at.Sub(stopping), cfg.Grace)
			}
		}
	})
}

func TestShutdownLetsWorkFinishWithinGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, logs := testConfig(t)
		src := newFakeSource(lease)
		src.process = func(ctx context.Context, _ Lease) error { return busy(ctx, 15*time.Second) }
		r := startPool(t, src, cfg)
		synctest.Sleep(5 * time.Second)
		stopping := time.Now()
		r.stop()

		if took := time.Since(stopping); took != 10*time.Second {
			t.Errorf("Run returned %v after cancel, want 10s, when the work finished", took)
		}
		calls := src.calls()
		if len(calls.fails) != 0 || len(calls.releases) != 0 {
			t.Errorf("fails, releases = %d, %d, want none", len(calls.fails), len(calls.releases))
		}
		if got := logs.find(t, "work finished"); len(got) != 1 || got[0]["outcome"] != "done" {
			t.Errorf("outcome records = %v, want one done", got)
		}
	})
}

func TestShutdownReleasesWorkClaimedAsItStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, _ := testConfig(t)
		src := newFakeSource(lease)
		ctx, cancel := context.WithCancel(t.Context())
		src.onClaim = cancel
		p, err := NewPool(src, cfg)
		if err != nil {
			t.Fatalf("NewPool() error = %v", err)
		}
		p.Run(ctx)
		synctest.Wait()
		if left := poolGoroutines(); len(left) > 0 {
			t.Errorf("%d pool goroutines outlived Run:\n%s", len(left), strings.Join(left, "\n\n"))
		}

		calls := src.calls()
		if len(calls.processed) != 0 {
			t.Errorf("processed %d leases after shutdown started, want none", len(calls.processed))
		}
		if len(calls.releases) != 1 || calls.releases[0].lease != lease || calls.releases[0].ctxErr != nil {
			t.Errorf("releases = %+v, want lease %+v released on a live context", calls.releases, lease)
		}
		if len(calls.claims) != 1 {
			t.Errorf("%d claims, want 1", len(calls.claims))
		}
	})
}

func TestPanicIsAFailedAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, logs := testConfig(t)
		first, second := Lease{ID: 1, TenantID: 3, Attempt: 1}, Lease{ID: 2, TenantID: 3, Attempt: 1}
		src := newFakeSource(first, second)
		src.process = func(_ context.Context, l Lease) error {
			if l == first {
				panic("index out of range")
			}
			return nil
		}
		r := startPool(t, src, cfg)
		synctest.Sleep(time.Minute)
		r.stop()

		calls := src.calls()
		if len(calls.processed) != 2 {
			t.Errorf("processed %d leases, want both: the slot survives the panic", len(calls.processed))
		}
		if len(calls.fails) != 1 || calls.fails[0].lease != first {
			t.Fatalf("fails = %+v, want the first lease failed", calls.fails)
		}
		if cause := calls.fails[0].cause; cause == nil || cause.Error() != "panic: index out of range" || IsPermanent(cause) {
			t.Errorf("cause = %v, want a retryable panic error", cause)
		}
		recs := logs.find(t, "process panicked")
		if len(recs) != 1 {
			t.Fatalf("panic records = %v, want one", recs)
		}
		if stack, _ := recs[0]["stack"].(string); !strings.Contains(stack, "worker.(*fakeSource).Process") {
			t.Errorf("logged stack %q doesn't reach the panicking Process", stack)
		}
		if recs[0]["id"] != 1.0 {
			t.Errorf("panic record %v isn't the first lease's", recs[0])
		}
	})
}

func TestPollJitter(t *testing.T) {
	tests := []struct {
		name     string
		claimErr error
	}{
		{"nothing due", nil},
		{"claim error", errBoom},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg, logs := testConfig(t)
				src := newFakeSource()
				src.claimErr = tc.claimErr
				startPool(t, src, cfg)
				synctest.Sleep(10 * time.Minute)

				claims := src.calls().claims
				if len(claims) < 400 {
					t.Fatalf("%d claims in 10 minutes, want at least 400 at 0.5–1.5s apart", len(claims))
				}
				for i := 1; i < len(claims); i++ {
					if gap := claims[i].Sub(claims[i-1]); gap < cfg.PollInterval/2 || gap >= 3*cfg.PollInterval/2 {
						t.Fatalf("claims %d and %d are %v apart, want within [0.5, 1.5) × %v", i-1, i, gap, cfg.PollInterval)
					}
				}
				if got := len(logs.find(t, "claim failed")); tc.claimErr != nil && got != len(claims) {
					t.Errorf("%d claim failures logged, want %d", got, len(claims))
				}
			})
		})
	}
}

func TestReaper(t *testing.T) {
	tests := []struct {
		name    string
		reapErr error
	}{
		{"reaps", nil},
		{"reap error", errBoom},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				cfg, logs := testConfig(t)
				cfg.ReapInterval = 10 * time.Second
				src := newFakeSource()
				src.reapErr = tc.reapErr
				r := startPool(t, src, cfg)
				synctest.Sleep(10 * time.Minute)
				r.stop()
				reaps := src.calls().reaps
				synctest.Sleep(10 * time.Minute)

				if len(reaps) < 40 || !reaps[0].Equal(start) {
					t.Fatalf("reaps at %v, want one at start and at least 40 in 10 minutes", offsets(start, reaps))
				}
				for i := 1; i < len(reaps); i++ {
					if gap := reaps[i].Sub(reaps[i-1]); gap < 5*time.Second || gap >= 15*time.Second {
						t.Fatalf("reaps %d and %d are %v apart, want within [5s, 15s)", i-1, i, gap)
					}
				}
				if got := len(src.calls().reaps); got != len(reaps) {
					t.Errorf("%d reaps after Run returned, want none", got-len(reaps))
				}
				if got := len(logs.find(t, "reap failed")); tc.reapErr != nil && got != len(reaps) {
					t.Errorf("%d reap failures logged, want %d", got, len(reaps))
				}
			})
		})
	}
}

func TestConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		cfg, _ := testConfig(t)
		cfg.Concurrency = 3
		src := newFakeSource(lease, lease, lease, lease)
		src.process = func(ctx context.Context, _ Lease) error { return busy(ctx, 10*time.Second) }
		startPool(t, src, cfg)
		synctest.Sleep(time.Minute)

		got := offsets(start, callTimes(src.calls().processed))
		if want := []time.Duration{0, 0, 0, 10 * time.Second}; !slices.Equal(got, want) {
			t.Errorf("work started at %v, want %v", got, want)
		}
	})
}

func TestWorkLogger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg, logs := testConfig(t)
		src := newFakeSource(lease)
		src.process = func(ctx context.Context, _ Lease) error {
			if err := busy(ctx, 5*time.Second); err != nil {
				return err
			}
			logging.LoggerFromContext(ctx).InfoContext(ctx, "measuring")
			if err := busy(ctx, 2*time.Second); err != nil {
				return err
			}
			return errBoom
		}
		r := startPool(t, src, cfg)
		synctest.Sleep(time.Minute)
		r.stop()

		want := map[string]any{"worker": "worker-1", "pool": "test", "id": 7.0, "tenant_id": 3.0, "attempt": 2.0}
		for msg, took := range map[string]time.Duration{"measuring": 5 * time.Second, "work finished": 7 * time.Second} {
			recs := logs.find(t, msg)
			if len(recs) != 1 {
				t.Fatalf("records %q = %v, want one", msg, recs)
			}
			for k, v := range want {
				if recs[0][k] != v {
					t.Errorf("record %q has %s = %v, want %v", msg, k, recs[0][k], v)
				}
			}
			if recs[0]["took"] != float64(took) {
				t.Errorf("record %q has took = %v, want %d", msg, recs[0]["took"], took)
			}
		}
	})
}

func TestRunUntilIdle(t *testing.T) {
	t.Run("processes everything due", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			cfg, _ := testConfig(t)
			cfg.Concurrency = 2
			src := newFakeSource(lease, lease, lease, lease, lease)
			src.process = func(ctx context.Context, _ Lease) error { return busy(ctx, 3*time.Second) }
			runUntilIdle(t, src, cfg, t.Context(), nil)

			if took := time.Since(start); took != 9*time.Second {
				t.Errorf("RunUntilIdle returned after %v, want 9s: five 3s jobs on two slots", took)
			}
			calls := src.calls()
			if len(calls.processed) != 5 || calls.due != 0 {
				t.Errorf("processed %d, %d still due, want all 5 processed", len(calls.processed), calls.due)
			}
			if len(calls.reaps) != 0 {
				t.Errorf("reaped %d times, want none", len(calls.reaps))
			}
		})
	})

	t.Run("nothing due", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			cfg, _ := testConfig(t)
			src := newFakeSource()
			runUntilIdle(t, src, cfg, t.Context(), nil)
			if took := time.Since(start); took != 0 {
				t.Errorf("RunUntilIdle returned after %v, want at once", took)
			}
			if got := len(src.calls().claims); got != 1 {
				t.Errorf("%d claims, want 1", got)
			}
		})
	})

	t.Run("claim error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg, _ := testConfig(t)
			src := newFakeSource()
			src.claimErr = errBoom
			runUntilIdle(t, src, cfg, t.Context(), errBoom)
		})
	})

	t.Run("cancelled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg, _ := testConfig(t)
			src := newFakeSource(lease)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			runUntilIdle(t, src, cfg, ctx, context.Canceled)
			if got := len(src.calls().claims); got != 0 {
				t.Errorf("%d claims, want none", got)
			}
		})
	})
}

func runUntilIdle(t *testing.T, src source, cfg Config, ctx context.Context, want error) {
	t.Helper()
	p, err := NewPool(src, cfg)
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	if err := p.RunUntilIdle(ctx); !errors.Is(err, want) {
		t.Errorf("RunUntilIdle() error = %v, want %v", err, want)
	}
	synctest.Wait()
	if left := poolGoroutines(); len(left) > 0 {
		t.Errorf("%d pool goroutines outlived RunUntilIdle:\n%s", len(left), strings.Join(left, "\n\n"))
	}
}
