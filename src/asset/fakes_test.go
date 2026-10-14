package asset

import (
	"context"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/audio"
	"github.com/zanvd/slumber-mill/src/logging"
)

// fakeNow stands in for MySQL's clock, so a test can tell a re-read asset from the one the service built.
var fakeNow = time.Date(2000, 1, 2, 3, 4, 5, 600000, time.UTC)

// calls records the calls the fakes receive, in order, so tests can check what ran before what.
type calls struct {
	list []string
}

func (c *calls) add(name string) {
	c.list = append(c.list, name)
}

// fakeRepo keeps assets in memory. An asset's internal ID is its index in assets plus one,
// and a key that is already in use in the tenant makes insertAsset fail as the unique index would.
// Like a database driver, every method fails once its context is done.
type fakeRepo struct {
	calls  *calls
	assets []Asset
	errs   map[string]error
	// beforeInsert runs inside insertAsset before it looks at its context,
	// as if another request got there first or the client hung up.
	beforeInsert func(f *fakeRepo)
	// errAfterInsert is returned by an insert that stored its row, as when the commit's reply is lost.
	errAfterInsert error
	insertCtx      context.Context
	listArgs       []listArgs
	// events is the outbox, oldest first; relayed holds the IDs markRelayed has marked.
	events      []Event
	relayed     map[uuid.UUID]bool
	eventLimits []int
	markedIDs   [][]uuid.UUID
}

func (f *fakeRepo) unrelayedEvents(ctx context.Context, limit int) ([]Event, error) {
	f.calls.add("unrelayedEvents")
	f.eventLimits = append(f.eventLimits, limit)
	if err := f.err(ctx, "unrelayedEvents"); err != nil {
		return nil, err
	}
	var events []Event
	for _, e := range f.events {
		if !f.relayed[e.ID] && len(events) < limit {
			events = append(events, e)
		}
	}
	return events, nil
}

func (f *fakeRepo) markRelayed(ctx context.Context, ids []uuid.UUID) error {
	f.calls.add("markRelayed")
	f.markedIDs = append(f.markedIDs, slices.Clone(ids))
	if err := f.err(ctx, "markRelayed"); err != nil {
		return err
	}
	if f.relayed == nil {
		f.relayed = make(map[uuid.UUID]bool)
	}
	for _, id := range ids {
		f.relayed[id] = true
	}
	return nil
}

func (f *fakeRepo) err(ctx context.Context, method string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.errs[method]
}

type listArgs struct {
	tenantID, before uint64
	limit            int
}

func (f *fakeRepo) insertAsset(ctx context.Context, a Asset) error {
	f.calls.add("insertAsset")
	f.insertCtx = ctx
	if f.beforeInsert != nil {
		f.beforeInsert(f)
	}
	if err := f.err(ctx, "insertAsset"); err != nil {
		return err
	}
	for _, existing := range f.assets {
		if existing.ID == a.ID || a.IdempotencyKey != "" &&
			existing.TenantID == a.TenantID && existing.IdempotencyKey == a.IdempotencyKey {
			return errDuplicate
		}
	}
	a.CreatedAt, a.UpdatedAt = fakeNow, fakeNow
	f.assets = append(f.assets, a)
	return f.errAfterInsert
}

func (f *fakeRepo) assetByID(ctx context.Context, tenantID uint64, id uuid.UUID) (Asset, error) {
	f.calls.add("assetByID")
	if err := f.err(ctx, "assetByID"); err != nil {
		return Asset{}, err
	}
	return f.find(func(a Asset) bool { return a.TenantID == tenantID && a.ID == id })
}

func (f *fakeRepo) assetByIdempotencyKey(ctx context.Context, tenantID uint64, key string) (Asset, error) {
	f.calls.add("assetByIdempotencyKey")
	if err := f.err(ctx, "assetByIdempotencyKey"); err != nil {
		return Asset{}, err
	}
	return f.find(func(a Asset) bool { return a.TenantID == tenantID && a.IdempotencyKey == key })
}

func (f *fakeRepo) find(match func(Asset) bool) (Asset, error) {
	i := slices.IndexFunc(f.assets, match)
	if i < 0 {
		return Asset{}, ErrNotFound
	}
	return f.assets[i], nil
}

func (f *fakeRepo) listAssets(ctx context.Context, tenantID, before uint64, limit int) ([]Asset, uint64, error) {
	f.calls.add("listAssets")
	f.listArgs = append(f.listArgs, listArgs{tenantID, before, limit})
	if err := f.err(ctx, "listAssets"); err != nil {
		return nil, 0, err
	}
	var page []Asset
	var last uint64
	for i := len(f.assets) - 1; i >= 0; i-- {
		id := uint64(i + 1)
		if f.assets[i].TenantID != tenantID || before > 0 && id >= before {
			continue
		}
		if len(page) == limit {
			return page, last, nil
		}
		page = append(page, f.assets[i])
		last = id
	}
	return page, 0, nil
}

func (f *fakeRepo) rendition(ctx context.Context, tenantID uint64, assetID uuid.UUID, profile Profile) (
	Rendition, error) {
	f.calls.add("rendition")
	if err := f.err(ctx, "rendition"); err != nil {
		return Rendition{}, err
	}
	a, err := f.find(func(a Asset) bool { return a.TenantID == tenantID && a.ID == assetID })
	if err != nil {
		return Rendition{}, err
	}
	i := slices.IndexFunc(a.Renditions, func(r Rendition) bool { return r.Profile == profile })
	if i < 0 {
		return Rendition{}, ErrNotFound
	}
	return a.Renditions[i], nil
}

// fakeFiles keeps files under a test's temp directory, in the same layout as storage.Local.
type fakeFiles struct {
	calls *calls
	root  string
	errs  map[string]error
	// errAfter lets a method succeed that many times before it fails with its error in errs.
	errAfter map[string]int
	made     map[string]int
}

// fail counts a call to method and returns the error it fails with, if any.
func (f *fakeFiles) fail(method string) error {
	if f.made == nil {
		f.made = make(map[string]int)
	}
	f.made[method]++
	if f.made[method] <= f.errAfter[method] {
		return nil
	}
	return f.errs[method]
}

func (f *fakeFiles) TempFile() (*os.File, error) {
	f.calls.add("TempFile")
	if err := f.fail("TempFile"); err != nil {
		return nil, err
	}
	dir := filepath.Join(f.root, "tmp")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	return os.CreateTemp(dir, "part-*")
}

func (f *fakeFiles) SourcePath(tenantID uint64, assetID uuid.UUID) string {
	return filepath.Join(f.root, "sources", strconv.FormatUint(tenantID, 10), assetID.String())
}

func (f *fakeFiles) RenditionPath(tenantID uint64, assetID uuid.UUID, profile string) (string, error) {
	return filepath.Join(f.root, "renditions", strconv.FormatUint(tenantID, 10), assetID.String(),
		profile+".m4a"), nil
}

func (f *fakeFiles) Promote(tmp, final string) error {
	f.calls.add("Promote")
	if err := f.fail("Promote"); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o750); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}

func (f *fakeFiles) Open(path string) (io.ReadSeekCloser, error) {
	f.calls.add("Open")
	if err := f.fail("Open"); err != nil {
		return nil, err
	}
	return os.Open(path)
}

// Remove records which directory under the root the file was in: Remove tmp, Remove sources.
func (f *fakeFiles) Remove(path string) error {
	rel, err := filepath.Rel(f.root, path)
	if err != nil {
		return err
	}
	dir, _, _ := strings.Cut(filepath.ToSlash(rel), "/")
	f.calls.add("Remove " + dir)
	if err := f.fail("Remove"); err != nil {
		return err
	}
	return os.Remove(path)
}

// filesUnder lists the regular files under root as slash-separated relative paths.
func filesUnder(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		files = append(files, filepath.ToSlash(rel))
		return err
	})
	if err != nil {
		t.Fatalf("error listing files: %v", err)
	}
	return files
}

// fakeLeaseRepo is the processor's repository. It records what each call got, and like a database driver,
// every method fails once its context is done.
type fakeLeaseRepo struct {
	calls *calls
	errs  map[string]error
	// due is the lease claim hands out; nil means nothing is due.
	due *lease
	// assetID and targetLUFS are what leasedAsset returns.
	assetID    uuid.UUID
	targetLUFS float64
	// requeued and failed are what reap returns.
	requeued, failed int

	workerIDs []string
	// durations are the lease durations claim and extend got.
	durations []time.Duration
	// leases are the leases every other method got.
	leases   []lease
	finished []processed
	failures []failure
	// finishAt is when finish was called, and finishDeadline and finishLog are what its context carried.
	finishAt, finishDeadline time.Time
	finishLog                *slog.Logger
}

func (f *fakeLeaseRepo) err(ctx context.Context, method string) error {
	f.calls.add(method)
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.errs[method]
}

func (f *fakeLeaseRepo) claim(ctx context.Context, workerID string, d time.Duration) (lease, bool, error) {
	f.workerIDs = append(f.workerIDs, workerID)
	f.durations = append(f.durations, d)
	if err := f.err(ctx, "claim"); err != nil {
		return lease{}, false, err
	}
	if f.due == nil {
		return lease{}, false, nil
	}
	return *f.due, true, nil
}

func (f *fakeLeaseRepo) extend(ctx context.Context, l lease, d time.Duration) error {
	f.leases = append(f.leases, l)
	f.durations = append(f.durations, d)
	return f.err(ctx, "extend")
}

func (f *fakeLeaseRepo) leasedAsset(ctx context.Context, l lease) (uuid.UUID, float64, error) {
	f.leases = append(f.leases, l)
	if err := f.err(ctx, "leasedAsset"); err != nil {
		return uuid.UUID{}, 0, err
	}
	return f.assetID, f.targetLUFS, nil
}

func (f *fakeLeaseRepo) finish(ctx context.Context, l lease, p processed) error {
	f.leases = append(f.leases, l)
	f.finished = append(f.finished, p)
	f.finishAt = time.Now()
	f.finishDeadline, _ = ctx.Deadline()
	f.finishLog = logging.LoggerFromContext(ctx)
	return f.err(ctx, "finish")
}

func (f *fakeLeaseRepo) fail(ctx context.Context, l lease, fl failure) (bool, error) {
	f.leases = append(f.leases, l)
	f.failures = append(f.failures, fl)
	if err := f.err(ctx, "fail"); err != nil {
		return false, err
	}
	return fl.Permanent, nil
}

func (f *fakeLeaseRepo) release(ctx context.Context, l lease) error {
	f.leases = append(f.leases, l)
	return f.err(ctx, "release")
}

func (f *fakeLeaseRepo) reap(ctx context.Context) (int, int, error) {
	if err := f.err(ctx, "reap"); err != nil {
		return 0, 0, err
	}
	return f.requeued, f.failed, nil
}

// fakeTranscoder records what it was asked for, and renders with render, or with fakeRender if render is nil.
type fakeTranscoder struct {
	calls  *calls
	render func(ctx context.Context, outputs []audio.Output) (audio.Result, error)

	src        string
	targetLUFS float64
	outputs    []audio.Output
	// deadline is the deadline of the context Process got.
	deadline time.Time
}

func (f *fakeTranscoder) Process(ctx context.Context, src string, targetLUFS float64, outputs []audio.Output) (
	audio.Result, error) {
	if f.calls != nil {
		f.calls.add("Process")
	}
	f.src, f.targetLUFS, f.outputs = src, targetLUFS, slices.Clone(outputs)
	f.deadline, _ = ctx.Deadline()
	if f.render != nil {
		return f.render(ctx, outputs)
	}
	return fakeRender(outputs)
}

// fakeRender writes fakeRendering to each output and returns a result for them, as audio would:
// stereo renders linear and mono dynamic, and each rendition's measurement differs from its loudness.
func fakeRender(outputs []audio.Output) (audio.Result, error) {
	res := audio.Result{
		Source: audio.Source{Codec: "flac", SampleRate: 44_100, Channels: 2, Duration: 600_500 * time.Millisecond},
		Input:  audio.Input{LUFS: -23.45, TruePeakDBTP: -0.25, LRALU: 7.5, MaxMomentaryLUFS: -10.5},
		Warnings: []audio.Warning{
			audio.WarningSuddenLoudEvent, audio.WarningDynamicNormalization, audio.WarningSourceMayClip,
		},
	}
	for _, o := range outputs {
		if err := os.WriteFile(o.Path, []byte(fakeRendering(o.Profile.Name)), 0o640); err != nil {
			return audio.Result{}, err
		}
		r := audio.Rendition{
			Profile: o.Profile, Path: o.Path, Channels: 2,
			Measured:  audio.Loudness{LUFS: -24.1, TruePeakDBTP: -3.2, LRALU: 6.5},
			Predicted: audio.Dynamic, Normalization: audio.Linear,
			Loudness: audio.Loudness{LUFS: -16.03, TruePeakDBTP: -1.52, LRALU: 4.1},
		}
		if o.Profile.MaxChannels == 1 {
			r.Channels, r.Predicted, r.Normalization = 1, audio.Linear, audio.Dynamic
			r.Loudness = audio.Loudness{LUFS: -15.76, TruePeakDBTP: -1.61, LRALU: 3.9}
		}
		res.Renditions = append(res.Renditions, r)
	}
	return res, nil
}

func fakeRendering(profile string) string {
	return profile + " rendition"
}
