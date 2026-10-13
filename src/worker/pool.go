package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"time"

	"github.com/zanvd/slumber-mill/src/logging"
)

const outcomeTimeout = 15 * time.Second

var errGraceExpired = errors.New("shutdown grace expired")

type Config struct {
	WorkerID     string
	Pool         string
	Concurrency  int
	Lease        time.Duration // as long as the source's claims last; heartbeats renew it every Lease/3
	PollInterval time.Duration // an idle slot waits 0.5–1.5× this before claiming again
	Grace        time.Duration // how long in-flight work may run once shutdown starts
	ReapInterval time.Duration // zero means Lease/3
	Log          *slog.Logger
}

// Pool runs Concurrency slots that claim and process work from one source, and a reaper.
type Pool struct {
	src       source
	cfg       Config
	heartbeat time.Duration
	log       *slog.Logger
}

// NewPool returns a Pool that drives src. Claim reports false when nothing is due;
// Process does the work and completes it, fenced; Fail retries with backoff or gives up;
// Release requeues the work without using an attempt; Reap requeues or fails work whose lease expired;
// and Extend, Process, Fail and Release return ErrLeaseLost once fenced out.
func NewPool(src source, cfg Config) (*Pool, error) {
	switch {
	case src == nil:
		return nil, errors.New("a source is required")
	case cfg.Log == nil:
		return nil, errors.New("a logger is required")
	case cfg.Concurrency < 1:
		return nil, fmt.Errorf("concurrency %d is below 1", cfg.Concurrency)
	case cfg.Lease/3 <= 0:
		return nil, fmt.Errorf("lease %v is too short", cfg.Lease)
	case cfg.PollInterval <= 0:
		return nil, fmt.Errorf("poll interval %v is not positive", cfg.PollInterval)
	case cfg.Grace < 0:
		return nil, fmt.Errorf("grace %v is negative", cfg.Grace)
	case cfg.ReapInterval < 0:
		return nil, fmt.Errorf("reap interval %v is negative", cfg.ReapInterval)
	}
	if cfg.ReapInterval == 0 {
		cfg.ReapInterval = cfg.Lease / 3
	}
	return &Pool{
		src:       src,
		cfg:       cfg,
		heartbeat: cfg.Lease / 3,
		log:       cfg.Log.With("worker", cfg.WorkerID, "pool", cfg.Pool),
	}, nil
}

// Run claims and processes work until ctx is cancelled. Then it stops claiming at once,
// gives in-flight work Grace to finish, cancels and releases what is left,
// and returns when every slot and the reaper have stopped.
func (p *Pool) Run(ctx context.Context) {
	ctx = logging.CtxWithLogger(ctx, p.log)
	var wg sync.WaitGroup
	for range p.cfg.Concurrency {
		wg.Go(func() { p.poll(ctx) })
	}
	wg.Go(func() { p.reap(ctx) })
	wg.Wait()
}

// RunUntilIdle processes work until no slot finds any due, and returns ctx's error or the slots' claim errors.
// It doesn't reap. It's for tests.
func (p *Pool) RunUntilIdle(ctx context.Context) error {
	ctx = logging.CtxWithLogger(ctx, p.log)
	var wg sync.WaitGroup
	errs := make([]error, p.cfg.Concurrency)
	for i := range p.cfg.Concurrency {
		wg.Go(func() { errs[i] = p.untilIdle(ctx) })
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Join(errs...)
}

func (p *Pool) poll(ctx context.Context) {
	for ctx.Err() == nil {
		l, ok, err := p.src.Claim(ctx)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				p.log.ErrorContext(ctx, "claim failed", "error", err)
			}
		case ok:
			p.handle(ctx, l)
			continue
		}
		sleep(ctx, jitter(p.cfg.PollInterval, rand.Int64N))
	}
}

func (p *Pool) untilIdle(ctx context.Context) error {
	for ctx.Err() == nil {
		l, ok, err := p.src.Claim(ctx)
		if err != nil {
			return fmt.Errorf("error claiming work: %w", err)
		}
		if !ok {
			return nil
		}
		p.handle(ctx, l)
	}
	return ctx.Err()
}

func (p *Pool) reap(ctx context.Context) {
	for ctx.Err() == nil {
		if err := p.src.Reap(ctx); err != nil && ctx.Err() == nil {
			p.log.ErrorContext(ctx, "reap failed", "error", err)
		}
		sleep(ctx, jitter(p.cfg.ReapInterval, rand.Int64N))
	}
}

// handle runs one claimed lease to its outcome. The work's context outlives ctx by up to Grace.
func (p *Pool) handle(ctx context.Context, l Lease) {
	log := slog.New(tookHandler{Handler: p.log.Handler(), start: time.Now()}).
		With("id", l.ID, "tenant_id", l.TenantID, "attempt", l.Attempt)
	work, cancel := context.WithCancelCause(logging.CtxWithLogger(context.WithoutCancel(ctx), log))
	defer cancel(nil)

	err := ctx.Err() // claimed as the pool stopped: release it unstarted
	if err == nil {
		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() { p.beat(work, cancel, l, done) })
		wg.Go(func() { p.watchGrace(ctx, work, cancel, done) })
		err = p.process(work, l)
		close(done)
		wg.Wait()
	}
	p.settle(work, l, decide(err, work, ctx), err)
}

func (p *Pool) process(ctx context.Context, l Lease) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
			logging.LoggerFromContext(ctx).ErrorContext(ctx, "process panicked", "error", err, "stack", string(debug.Stack()))
		}
	}()
	return p.src.Process(ctx, l)
}

func (p *Pool) beat(work context.Context, cancel context.CancelCauseFunc, l Lease, done <-chan struct{}) {
	log := logging.LoggerFromContext(work)
	t := time.NewTicker(p.heartbeat)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-work.Done():
			return
		case <-t.C:
		}
		ctx, cancelBeat := context.WithTimeout(work, p.heartbeat)
		err := p.src.Extend(ctx, l)
		cancelBeat()
		switch {
		case err == nil:
		case errors.Is(err, ErrLeaseLost):
			log.WarnContext(work, "lease lost, cancelling the work", "error", err)
			cancel(err)
			return
		case work.Err() == nil:
			log.WarnContext(work, "heartbeat failed", "error", err)
		}
	}
}

func (p *Pool) watchGrace(ctx, work context.Context, cancel context.CancelCauseFunc, done <-chan struct{}) {
	select {
	case <-done:
		return
	case <-ctx.Done():
	}
	t := time.NewTimer(p.cfg.Grace)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		logging.LoggerFromContext(work).WarnContext(work, "shutdown grace expired, cancelling the work")
		cancel(errGraceExpired)
	}
}

// settle writes the outcome on a fresh context, so neither shutdown nor a cancelled work context stops it.
func (p *Pool) settle(work context.Context, l Lease, o outcome, cause error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(work), outcomeTimeout)
	defer cancel()
	log := logging.LoggerFromContext(ctx)

	var err error
	switch o {
	case outcomeDone:
		log.InfoContext(ctx, "work finished", "outcome", string(o))
	case outcomeDiscard:
		log.WarnContext(ctx, "work finished", "outcome", string(o), "error", cause)
	case outcomeRelease:
		log.InfoContext(ctx, "work finished", "outcome", string(o), "error", cause)
		err = p.src.Release(ctx, l)
	case outcomeFail:
		log.WarnContext(ctx, "work finished", "outcome", string(o), "error", cause)
		err = p.src.Fail(ctx, l, cause)
	}
	switch {
	case err == nil:
	case errors.Is(err, ErrLeaseLost):
		log.WarnContext(ctx, "lease lost before the outcome was written", "outcome", string(o), "error", err)
	default:
		log.ErrorContext(ctx, "writing the outcome failed", "outcome", string(o), "error", err)
	}
}

type outcome string

const (
	outcomeDone    outcome = "done"
	outcomeDiscard outcome = "discarded"
	outcomeRelease outcome = "released"
	outcomeFail    outcome = "failed"
)

// decide applies the outcome order to what Process returned. A nil error means Process completed the work,
// fenced, so the lease was still held then, even if a heartbeat racing the completion saw it gone.
func decide(err error, work, pool context.Context) outcome {
	switch {
	case errors.Is(err, ErrLeaseLost), err != nil && errors.Is(context.Cause(work), ErrLeaseLost):
		return outcomeDiscard
	case err == nil:
		return outcomeDone
	case work.Err() != nil, pool.Err() != nil:
		return outcomeRelease
	default:
		return outcomeFail
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
