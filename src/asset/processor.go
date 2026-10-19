package asset

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"
	"uuid"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/zanvd/slumber-mill/src/audio"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/tracing"
	"github.com/zanvd/slumber-mill/src/worker"
)

const (
	processTimeout = 5 * time.Hour
	finishTimeout  = 15 * time.Second
	retryBase      = 30 * time.Second
	retryMax       = 10 * time.Minute

	attrAssetID = "slumber_mill.asset.id"
	attrAttempt = "slumber_mill.attempt"
)

type leaseRepo interface {
	claim(ctx context.Context, workerID string, d time.Duration) (lease, bool, error)
	extend(ctx context.Context, l lease, d time.Duration) error
	leasedTrace(ctx context.Context, l lease) (uuid.UUID, string, error)
	leasedAsset(ctx context.Context, l lease) (uuid.UUID, float64, error)
	finish(ctx context.Context, l lease, p processed) error
	fail(ctx context.Context, l lease, f failure) (bool, error)
	release(ctx context.Context, l lease) error
	reap(ctx context.Context) (requeued, failed int, err error)
}

type renditionStore interface {
	TempFile() (*os.File, error)
	SourcePath(tenantID uint64, assetID uuid.UUID) string
	RenditionPath(tenantID uint64, assetID uuid.UUID, profile string) (string, error)
	Open(path string) (io.ReadSeekCloser, error)
	Promote(tmp, final string) error
	Remove(path string) error
	Touch(path string) error
}

type transcoder interface {
	Process(ctx context.Context, src string, targetLUFS float64, outputs []audio.Output) (audio.Result, error)
}

type ProcessorConfig struct {
	WorkerID string
	// Lease is how long a claim or a heartbeat holds an asset: the encode pool's Lease.
	Lease time.Duration
	// Tracer continues the traces stored with assets. Nil traces nothing, and reads no trace context.
	Tracer trace.TracerProvider
}

// Processor is the encode pool's source of work. It renders a claimed asset's renditions,
// and completes or fails the asset fenced on its lease.
type Processor struct {
	repo     leaseRepo
	files    renditionStore
	tc       transcoder
	cfg      ProcessorConfig
	profiles []audio.Profile
	// tracer is nil when tracing is off.
	tracer trace.Tracer
}

func NewProcessor(r leaseRepo, files renditionStore, tc transcoder, cfg ProcessorConfig) (*Processor, error) {
	switch {
	case r == nil:
		return nil, errors.New("a repository is required")
	case files == nil:
		return nil, errors.New("a file store is required")
	case tc == nil:
		return nil, errors.New("a transcoder is required")
	case cfg.WorkerID == "":
		return nil, errors.New("a worker ID is required")
	case cfg.Lease/3 <= 0:
		return nil, fmt.Errorf("lease %v is too short", cfg.Lease)
	}
	p := &Processor{repo: r, files: files, tc: tc, cfg: cfg, profiles: audio.Profiles()}
	if cfg.Tracer != nil {
		p.tracer = cfg.Tracer.Tracer(tracerName)
	}
	return p, nil
}

// Trace continues the trace of the asset's upload, if a Checkly check sent it, in an asset.encode span for this
// attempt, which the pool ends with the attempt's outcome. Failing to read the trace context only loses the span.
func (p *Processor) Trace(ctx context.Context, l worker.Lease) (context.Context, trace.Span) {
	if p.tracer == nil {
		return ctx, nil
	}
	assetID, stored, err := p.repo.leasedTrace(ctx, lease(l))
	if err != nil {
		logging.LoggerFromContext(ctx).LogAttrs(ctx, slog.LevelWarn, "error reading the asset's trace context",
			slog.String("error", err.Error()))
		return ctx, nil
	}
	return p.tracer.Start(tracing.ContinueStored(ctx, stored), "asset.encode",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String(attrAssetID, assetID.String()), attribute.Int(attrAttempt, l.Attempt)))
}

func (p *Processor) Claim(ctx context.Context) (worker.Lease, bool, error) {
	l, ok, err := p.repo.claim(ctx, p.cfg.WorkerID, p.cfg.Lease)
	if err != nil {
		return worker.Lease{}, false, toWorker(err)
	}
	return worker.Lease(l), ok, nil
}

func (p *Processor) Extend(ctx context.Context, l worker.Lease) error {
	return toWorker(p.repo.extend(ctx, lease(l), p.cfg.Lease))
}

func (p *Processor) Process(ctx context.Context, l worker.Lease) error {
	held := lease(l)
	work, cancel := context.WithTimeout(ctx, processTimeout)
	defer cancel()

	assetID, targetLUFS, err := p.repo.leasedAsset(work, held)
	if err != nil {
		return fmt.Errorf("error loading the leased asset: %w", err)
	}
	result, err := p.render(work, held, assetID, targetLUFS)
	if err != nil {
		return err
	}

	// A heartbeat that finds the asset already finished cancels ctx, which could make the driver
	// report a committed completion as cancelled.
	finishCtx, cancelFinish := context.WithTimeout(context.WithoutCancel(ctx), finishTimeout)
	defer cancelFinish()
	return toWorker(p.repo.finish(finishCtx, held, result))
}

// render transcodes the source into one temp file per profile, then hashes and promotes each rendition.
// Temp files that weren't promoted are removed. Promoted ones stay, since any attempt writes the same bytes.
func (p *Processor) render(ctx context.Context, l lease, assetID uuid.UUID, targetLUFS float64) (processed, error) {
	outputs := make([]audio.Output, 0, len(p.profiles))
	promoted := 0
	var held heldFiles
	stopRefreshing := p.keepFresh(ctx, &held)
	defer func() {
		stopRefreshing()
		for _, o := range outputs[promoted:] {
			p.remove(ctx, o.Path)
		}
	}()

	for _, profile := range p.profiles {
		f, err := p.files.TempFile()
		if err != nil {
			return processed{}, fmt.Errorf("error creating a temp output: %w", err)
		}
		held.hold(f.Name())
		outputs = append(outputs, audio.Output{Profile: profile, Path: f.Name()})
		if err := f.Close(); err != nil {
			return processed{}, fmt.Errorf("error closing a temp output: %w", err)
		}
	}

	res, err := p.tc.Process(ctx, p.files.SourcePath(l.TenantID, assetID), targetLUFS, outputs)
	if err != nil {
		return processed{}, err
	}
	if err := matchOutputs(res.Renditions, outputs); err != nil {
		return processed{}, err
	}

	out := newProcessed(res)
	for _, r := range res.Renditions {
		sum, size, err := p.hash(r.Path)
		if err != nil {
			return processed{}, fmt.Errorf("error hashing rendition %s: %w", r.Profile.Name, err)
		}
		final, err := p.files.RenditionPath(l.TenantID, assetID, r.Profile.Name)
		if err != nil {
			return processed{}, fmt.Errorf("error locating rendition %s: %w", r.Profile.Name, err)
		}
		held.release(r.Path)
		if err := p.files.Promote(r.Path, final); err != nil {
			return processed{}, fmt.Errorf("error promoting rendition %s: %w", r.Profile.Name, err)
		}
		promoted++
		out.Renditions = append(out.Renditions, newRendition(r, sum, size))
	}
	return out, nil
}

// keepFresh touches the held files every lease/3 until the returned stop has returned. The startup sweep
// removes temp files older than the lease, and nothing writes the outputs through pass 1 or verification.
func (p *Processor) keepFresh(ctx context.Context, held *heldFiles) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		t := time.NewTicker(p.cfg.Lease / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				held.each(func(path string) {
					if err := p.files.Touch(path); err != nil {
						logging.LoggerFromContext(ctx).LogAttrs(ctx, slog.LevelWarn, "error refreshing temp output",
							slog.String("path", path), slog.String("error", err.Error()))
					}
				})
			}
		}
	})
	return func() {
		close(done)
		wg.Wait()
	}
}

// heldFiles are the temp outputs that keepFresh touches.
type heldFiles struct {
	mu    sync.Mutex
	paths []string
}

func (h *heldFiles) hold(path string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.paths = append(h.paths, path)
}

// release stops refreshing path, after any touch under way has finished.
func (h *heldFiles) release(path string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.paths = slices.DeleteFunc(h.paths, func(held string) bool { return held == path })
}

func (h *heldFiles) each(fn func(path string)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, path := range h.paths {
		fn(path)
	}
}

// matchOutputs checks that the transcoder rendered the outputs it was given, in order,
// so that only the processor's own temp files are promoted.
func matchOutputs(renditions []audio.Rendition, outputs []audio.Output) error {
	if len(renditions) != len(outputs) {
		return fmt.Errorf("transcoder returned %d renditions for %d outputs", len(renditions), len(outputs))
	}
	for i, r := range renditions {
		if r.Path != outputs[i].Path || r.Profile != outputs[i].Profile {
			return fmt.Errorf("transcoder returned rendition %d as %s at %s, want %s at %s",
				i, r.Profile.Name, r.Path, outputs[i].Profile.Name, outputs[i].Path)
		}
	}
	return nil
}

func (p *Processor) hash(path string) (sum [sha256.Size]byte, size uint64, err error) {
	f, err := p.files.Open(path)
	if err != nil {
		return sum, 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return sum, 0, err
	}
	h.Sum(sum[:0])
	return sum, uint64(n), nil
}

func newProcessed(res audio.Result) processed {
	var warnings []Warning
	for _, w := range res.Warnings {
		warnings = append(warnings, Warning(w))
	}
	return processed{
		SourceCodec:       res.Source.Codec,
		DurationMS:        uint64(res.Source.Duration.Milliseconds()),
		InputLUFS:         res.Input.LUFS,
		InputTruePeakDBTP: res.Input.TruePeakDBTP,
		InputLRALU:        res.Input.LRALU,
		InputMaxMomentary: res.Input.MaxMomentaryLUFS,
		Warnings:          warnings,
	}
}

func newRendition(r audio.Rendition, sum [sha256.Size]byte, size uint64) Rendition {
	return Rendition{
		Profile:       Profile(r.Profile.Name),
		Codec:         CodecAAC,
		BitrateKbps:   uint32(r.Profile.BitrateKbps),
		Channels:      uint8(r.Channels),
		Bytes:         size,
		SHA256:        sum,
		LUFS:          r.Loudness.LUFS,
		TruePeakDBTP:  r.Loudness.TruePeakDBTP,
		LRALU:         r.Loudness.LRALU,
		Normalization: Normalization(r.Normalization),
	}
}

// Fail fails a rejected asset with the rejection's tenant-safe message, and an asset whose error is
// worker.Permanent with the generic one. Any other error is retried after a backoff while attempts remain.
func (p *Processor) Fail(ctx context.Context, l worker.Lease, cause error) error {
	f := failure{Detail: cause.Error()}
	switch rejection, ok := errors.AsType[*audio.Rejection](cause); {
	case ok:
		f.Permanent, f.Reason = true, rejection.Message
	case worker.IsPermanent(cause):
		f.Permanent = true
	default:
		f.Delay = worker.Backoff(l.Attempt, retryBase, retryMax)
	}
	_, err := p.repo.fail(ctx, lease(l), f)
	return toWorker(err)
}

func (p *Processor) Release(ctx context.Context, l worker.Lease) error {
	return toWorker(p.repo.release(ctx, lease(l)))
}

func (p *Processor) Reap(ctx context.Context) error {
	requeued, failed, err := p.repo.reap(ctx)
	if err != nil {
		return err
	}
	if requeued > 0 || failed > 0 {
		logging.LoggerFromContext(ctx).WarnContext(ctx, "reaped assets whose lease expired",
			"requeued", requeued, "failed", failed)
	}
	return nil
}

func (p *Processor) remove(ctx context.Context, path string) {
	if err := p.files.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		logging.LoggerFromContext(ctx).LogAttrs(ctx, slog.LevelWarn, "error removing temp output",
			slog.String("path", path), slog.String("error", err.Error()))
	}
}

// toWorker passes on a repository error, matching worker.ErrLeaseLost if the lease was lost.
func toWorker(err error) error {
	if errors.Is(err, errLeaseLost) {
		return worker.LeaseLost(err)
	}
	return err
}
