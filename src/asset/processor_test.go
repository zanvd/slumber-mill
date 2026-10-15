package asset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/audio"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/worker"
)

// claimed is the lease the pool hands the processor in these tests, for processorLease.
var claimed = worker.Lease{ID: 11, TenantID: tenantID, Attempt: 2}

const processorLease = time.Minute

type processorFixture struct {
	p     *Processor
	repo  *fakeLeaseRepo
	files *fakeFiles
	tc    *fakeTranscoder
	calls *calls
	root  string
}

func newProcessorFixture(t *testing.T) *processorFixture {
	t.Helper()
	c := &calls{}
	root := t.TempDir()
	f := &processorFixture{
		repo:  &fakeLeaseRepo{calls: c, errs: map[string]error{}, assetID: uuid.NewV7(), targetLUFS: -18.5},
		files: &fakeFiles{calls: c, root: root, errs: map[string]error{}, errAfter: map[string]int{}},
		tc:    &fakeTranscoder{calls: c},
		calls: c,
		root:  root,
	}
	p, err := NewProcessor(f.repo, f.files, f.tc, ProcessorConfig{WorkerID: "encode-1", Lease: processorLease})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	f.p = p
	return f
}

// renditionFile is where the leased asset's rendition of profile is promoted to, relative to the root.
func (f *processorFixture) renditionFile(profile Profile) string {
	return "renditions/7/" + f.repo.assetID.String() + "/" + string(profile) + ".m4a"
}

func TestNewProcessor(t *testing.T) {
	repo, files, tr := &fakeLeaseRepo{}, &fakeFiles{}, &fakeTranscoder{}
	valid := ProcessorConfig{WorkerID: "encode-1", Lease: time.Minute}
	tests := []struct {
		name  string
		repo  leaseRepo
		files renditionStore
		tc    transcoder
		cfg   ProcessorConfig
	}{
		{"no repository", nil, files, tr, valid},
		{"no file store", repo, nil, tr, valid},
		{"no transcoder", repo, files, nil, valid},
		{"no worker ID", repo, files, tr, ProcessorConfig{Lease: time.Minute}},
		{"no lease", repo, files, tr, ProcessorConfig{WorkerID: "encode-1"}},
		{"negative lease", repo, files, tr, ProcessorConfig{WorkerID: "encode-1", Lease: -time.Second}},
		{"lease too short to refresh", repo, files, tr, ProcessorConfig{WorkerID: "encode-1", Lease: 2}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if p, err := NewProcessor(tc.repo, tc.files, tc.tc, tc.cfg); err == nil {
				t.Errorf("NewProcessor() = %+v, want an error", p)
			}
		})
	}
}

func TestProcessorClaim(t *testing.T) {
	due := lease{ID: 11, TenantID: tenantID, Attempt: 2}
	dbErr := fmt.Errorf("error claiming asset: %w: %v", errDatabase, errBoom)
	tests := []struct {
		name    string
		due     *lease
		err     error
		want    worker.Lease
		wantOK  bool
		wantErr error
	}{
		{"due", &due, nil, claimed, true, nil},
		{"nothing due", nil, nil, worker.Lease{}, false, nil},
		{"database error", &due, dbErr, worker.Lease{}, false, errDatabase},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newProcessorFixture(t)
			f.repo.due = tc.due
			f.repo.errs["claim"] = tc.err

			got, ok, err := f.p.Claim(t.Context())

			if got != tc.want || ok != tc.wantOK || !errors.Is(err, tc.wantErr) {
				t.Errorf("Claim() = %+v, %t, %v; want %+v, %t, %v", got, ok, err, tc.want, tc.wantOK, tc.wantErr)
			}
			if !slices.Equal(f.repo.workerIDs, []string{"encode-1"}) ||
				!slices.Equal(f.repo.durations, []time.Duration{processorLease}) {
				t.Errorf("claim() got worker IDs %v and leases %v, want encode-1 for 1m",
					f.repo.workerIDs, f.repo.durations)
			}
		})
	}
}

func TestProcessorExtendAndRelease(t *testing.T) {
	f := newProcessorFixture(t)

	if err := f.p.Extend(t.Context(), claimed); err != nil {
		t.Errorf("Extend() error = %v", err)
	}
	if err := f.p.Release(t.Context(), claimed); err != nil {
		t.Errorf("Release() error = %v", err)
	}

	if want := []string{"extend", "release"}; !slices.Equal(f.calls.list, want) {
		t.Errorf("calls = %v, want %v", f.calls.list, want)
	}
	if want := []lease{lease(claimed), lease(claimed)}; !slices.Equal(f.repo.leases, want) {
		t.Errorf("leases = %+v, want %+v", f.repo.leases, want)
	}
	if want := []time.Duration{processorLease}; !slices.Equal(f.repo.durations, want) {
		t.Errorf("extend() got %v, want %v", f.repo.durations, want)
	}
}

func TestProcessorProcess(t *testing.T) {
	f := newProcessorFixture(t)

	if err := f.p.Process(t.Context(), claimed); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	wantCalls := []string{"leasedAsset", "TempFile", "TempFile", "Process",
		"Open", "Promote", "Open", "Promote", "finish"}
	if !slices.Equal(f.calls.list, wantCalls) {
		t.Errorf("calls = %v, want %v", f.calls.list, wantCalls)
	}
	if want := []lease{lease(claimed), lease(claimed)}; !slices.Equal(f.repo.leases, want) {
		t.Errorf("leases = %+v, want %+v", f.repo.leases, want)
	}
	if src := f.files.SourcePath(tenantID, f.repo.assetID); f.tc.src != src || f.tc.targetLUFS != -18.5 {
		t.Errorf("transcoded %s to %v LUFS, want %s to -18.5 LUFS", f.tc.src, f.tc.targetLUFS, src)
	}
	profiles := audio.Profiles()
	if len(f.tc.outputs) != len(profiles) {
		t.Fatalf("outputs = %+v, want one per profile", f.tc.outputs)
	}
	for i, o := range f.tc.outputs {
		if o.Profile != profiles[i] || filepath.Dir(o.Path) != filepath.Join(f.root, "tmp") {
			t.Errorf("output %d = %+v, want %s in tmp", i, o, profiles[i].Name)
		}
	}

	wantFiles := []string{f.renditionFile(ProfileAAC128k), f.renditionFile(ProfileAAC64k)}
	if files := filesUnder(t, f.root); !slices.Equal(files, wantFiles) {
		t.Errorf("files = %v, want %v", files, wantFiles)
	}
	content := func(p Profile) []byte { return []byte(fakeRendering(string(p))) }
	for _, p := range []Profile{ProfileAAC128k, ProfileAAC64k} {
		got, err := os.ReadFile(filepath.Join(f.root, f.renditionFile(p)))
		if err != nil || !bytes.Equal(got, content(p)) {
			t.Errorf("%s = %q, %v; want %q", p, got, err, content(p))
		}
	}

	want := processed{
		SourceCodec:       "flac",
		DurationMS:        600_500,
		InputLUFS:         -23.45,
		InputTruePeakDBTP: -0.25,
		InputLRALU:        7.5,
		InputMaxMomentary: -10.5,
		Warnings:          []Warning{WarningSuddenLoudEvent, WarningDynamicNormalization, WarningSourceMayClip},
		Renditions: []Rendition{
			{
				Profile: ProfileAAC128k, Codec: CodecAAC, BitrateKbps: 128, Channels: 2,
				Bytes: uint64(len(content(ProfileAAC128k))), SHA256: sha256.Sum256(content(ProfileAAC128k)),
				LUFS: -16.03, TruePeakDBTP: -1.52, LRALU: 4.1, Normalization: NormalizationLinear,
			},
			{
				Profile: ProfileAAC64k, Codec: CodecAAC, BitrateKbps: 64, Channels: 1,
				Bytes: uint64(len(content(ProfileAAC64k))), SHA256: sha256.Sum256(content(ProfileAAC64k)),
				LUFS: -15.76, TruePeakDBTP: -1.61, LRALU: 3.9, Normalization: NormalizationDynamic,
			},
		},
	}
	if len(f.repo.finished) != 1 || !reflect.DeepEqual(f.repo.finished[0], want) {
		t.Errorf("finished with %+v\nwant  %+v", f.repo.finished, want)
	}
}

// TestProcessorProcessFailures fails each step of Process: the error comes back, temp outputs are removed,
// and renditions promoted before the failure stay.
func TestProcessorProcessFailures(t *testing.T) {
	rejection := &audio.Rejection{Message: audio.MessageSilent, Err: errors.New("integrated -inf LUFS")}
	writeThenFail := func(err error) func(context.Context, []audio.Output) (audio.Result, error) {
		return func(_ context.Context, outputs []audio.Output) (audio.Result, error) {
			if _, writeErr := fakeRender(outputs); writeErr != nil {
				return audio.Result{}, writeErr
			}
			return audio.Result{}, err
		}
	}
	tests := []struct {
		name  string
		setup func(f *processorFixture)
		// wantErr, when set, must match the error with errors.Is.
		wantErr error
		// promoted are the renditions promoted before the failure; others are files outside tmp that stay.
		promoted   []Profile
		others     []string
		wantFinish bool
	}{
		{
			name:    "asset unreadable",
			setup:   func(f *processorFixture) { f.repo.errs["leasedAsset"] = errBoom },
			wantErr: errBoom,
		},
		{
			name:    "no temp file",
			setup:   func(f *processorFixture) { f.files.errs["TempFile"] = errBoom },
			wantErr: errBoom,
		},
		{
			name: "no second temp file",
			setup: func(f *processorFixture) {
				f.files.errs["TempFile"], f.files.errAfter["TempFile"] = errBoom, 1
			},
			wantErr: errBoom,
		},
		{
			name:    "transcoder fails after writing its outputs",
			setup:   func(f *processorFixture) { f.tc.render = writeThenFail(errBoom) },
			wantErr: errBoom,
		},
		{
			name:    "transcoder rejects the source",
			setup:   func(f *processorFixture) { f.tc.render = writeThenFail(rejection) },
			wantErr: rejection,
		},
		{
			name: "transcoder returns a rendition short",
			setup: func(f *processorFixture) {
				f.tc.render = func(_ context.Context, outputs []audio.Output) (audio.Result, error) {
					return fakeRender(outputs[:1])
				}
			},
		},
		{
			name: "transcoder returns a file of its own",
			setup: func(f *processorFixture) {
				f.tc.render = func(_ context.Context, outputs []audio.Output) (audio.Result, error) {
					res, err := fakeRender(outputs)
					if err != nil {
						return res, err
					}
					res.Renditions[1].Path = filepath.Join(f.root, "elsewhere")
					return res, os.WriteFile(res.Renditions[1].Path, []byte("not ours"), 0o640)
				}
			},
			others: []string{"elsewhere"},
		},
		{
			name:     "hashing the second rendition fails",
			setup:    func(f *processorFixture) { f.files.errs["Open"], f.files.errAfter["Open"] = errBoom, 1 },
			wantErr:  errBoom,
			promoted: []Profile{ProfileAAC128k},
		},
		{
			name:    "promoting fails",
			setup:   func(f *processorFixture) { f.files.errs["Promote"] = errBoom },
			wantErr: errBoom,
		},
		{
			name:     "promoting the second rendition fails",
			setup:    func(f *processorFixture) { f.files.errs["Promote"], f.files.errAfter["Promote"] = errBoom, 1 },
			wantErr:  errBoom,
			promoted: []Profile{ProfileAAC128k},
		},
		{
			name:       "finish fails",
			setup:      func(f *processorFixture) { f.repo.errs["finish"] = errBoom },
			wantErr:    errBoom,
			promoted:   []Profile{ProfileAAC128k, ProfileAAC64k},
			wantFinish: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newProcessorFixture(t)
			tc.setup(f)

			err := f.p.Process(t.Context(), claimed)

			if err == nil || tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("Process() error = %v, want an error matching %v", err, tc.wantErr)
			}
			if errors.Is(err, worker.ErrLeaseLost) {
				t.Errorf("Process() error = %v, which isn't a lost lease", err)
			}
			wantFiles := slices.Clone(tc.others)
			for _, p := range tc.promoted {
				wantFiles = append(wantFiles, f.renditionFile(p))
			}
			slices.Sort(wantFiles)
			if files := filesUnder(t, f.root); !slices.Equal(files, wantFiles) {
				t.Errorf("files = %v, want %v", files, wantFiles)
			}
			if finished := slices.Contains(f.calls.list, "finish"); finished != tc.wantFinish {
				t.Errorf("finish called: %t, want %t", finished, tc.wantFinish)
			}
		})
	}
}

// TestProcessorContexts cancels the work's context as the transcoder finishes, as a lost heartbeat or the end of
// the shutdown grace would: the completion still runs, on its own 15 s deadline and with the work's logger.
func TestProcessorContexts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newProcessorFixture(t)
		log := slog.New(slog.DiscardHandler)
		work, cancel := context.WithCancel(logging.CtxWithLogger(t.Context(), log))
		defer cancel()
		f.tc.render = func(_ context.Context, outputs []audio.Output) (audio.Result, error) {
			synctest.Sleep(time.Hour)
			cancel()
			return fakeRender(outputs)
		}
		start := time.Now()

		if err := f.p.Process(work, claimed); err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if want := start.Add(4 * time.Hour); !f.tc.deadline.Equal(want) {
			t.Errorf("transcoder deadline = %v, want %v", f.tc.deadline, want)
		}
		if !f.repo.finishAt.Equal(start.Add(time.Hour)) {
			t.Errorf("finished at %v, want %v", f.repo.finishAt, start.Add(time.Hour))
		}
		if want := f.repo.finishAt.Add(15 * time.Second); !f.repo.finishDeadline.Equal(want) {
			t.Errorf("finish deadline = %v, want %v", f.repo.finishDeadline, want)
		}
		if f.repo.finishLog != log {
			t.Error("finish's context lost the work's logger")
		}
	})
}

// TestProcessorKeepsTempOutputsFresh has the transcoder hold the temp outputs for three leases before pass 2
// writes them and for three after: whenever the startup sweep looks, each was modified less than a lease ago.
func TestProcessorKeepsTempOutputsFresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newProcessorFixture(t)
		// New files get the real time, decades after the bubble's clock starts. Moving the clock there
		// makes a file that nothing refreshes age as the clock advances.
		info, err := os.Stat(f.root)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Until(info.ModTime()))
		checkFresh := func(outputs []audio.Output) {
			for _, o := range outputs {
				info, err := os.Stat(o.Path)
				if err != nil {
					t.Errorf("error reading %s: %v", o.Path, err)
					continue
				}
				if age := time.Since(info.ModTime()); age < 0 || age >= processorLease {
					t.Errorf("at %v, %s was modified %v ago, want less than a lease", time.Now(),
						filepath.Base(o.Path), age)
				}
			}
		}
		f.tc.render = func(_ context.Context, outputs []audio.Output) (audio.Result, error) {
			for range 6 {
				synctest.Sleep(processorLease / 2)
				checkFresh(outputs)
			}
			res, err := fakeRender(outputs)
			for range 6 {
				synctest.Sleep(processorLease / 2)
				checkFresh(outputs)
			}
			return res, err
		}

		if err := f.p.Process(t.Context(), claimed); err != nil {
			t.Fatalf("Process() error = %v", err)
		}
	})
}

// TestProcessorStopsRefreshing lets the processor refresh its temp outputs for a lease, then checks that nothing
// is touched once Process has returned, however it ended. A refresher left running would also deadlock the bubble.
func TestProcessorStopsRefreshing(t *testing.T) {
	tests := []struct {
		name                     string
		transcodeErr, promoteErr error
	}{
		{"after success", nil, nil},
		{"after a transcoder error", errBoom, nil},
		{"after promoting fails", nil, errBoom},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newProcessorFixture(t)
				f.files.errs["Promote"] = tc.promoteErr
				f.tc.render = func(_ context.Context, outputs []audio.Output) (audio.Result, error) {
					synctest.Sleep(processorLease)
					res, err := fakeRender(outputs)
					if tc.transcodeErr != nil {
						return audio.Result{}, tc.transcodeErr
					}
					return res, err
				}

				start := time.Now()
				err := f.p.Process(t.Context(), claimed)
				touched := f.files.touches()
				synctest.Sleep(3 * processorLease)

				for _, o := range f.tc.outputs {
					want := []time.Time{start.Add(processorLease / 3), start.Add(2 * processorLease / 3)}
					if got := f.files.touchTimes(o.Path); len(got) < 2 || !slices.EqualFunc(got[:2], want, time.Time.Equal) {
						t.Errorf("%s touched at %v, want its first two at %v", filepath.Base(o.Path), got, want)
					}
				}

				if wantErr := tc.transcodeErr != nil || tc.promoteErr != nil; (err != nil) != wantErr {
					t.Errorf("Process() error = %v, want an error: %t", err, wantErr)
				}
				if len(touched) == 0 {
					t.Error("nothing was touched while Process ran")
				}
				if after := f.files.touches(); len(after) != len(touched) {
					t.Errorf("touched %v after Process returned", after[len(touched):])
				}
			})
		})
	}
}

func TestProcessorIgnoresFailedTouches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newProcessorFixture(t)
		f.files.errs["Touch"] = errBoom
		f.tc.render = func(_ context.Context, outputs []audio.Output) (audio.Result, error) {
			synctest.Sleep(processorLease)
			return fakeRender(outputs)
		}
		var logs bytes.Buffer
		ctx := logging.CtxWithLogger(t.Context(), slog.New(slog.NewJSONHandler(&logs, nil)))

		if err := f.p.Process(ctx, claimed); err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if len(f.repo.finished) != 1 {
			t.Errorf("finished %d times, want once", len(f.repo.finished))
		}
		touched := f.files.touches()
		warnings := strings.Count(logs.String(), `"msg":"error refreshing temp output"`)
		if len(touched) == 0 || warnings != len(touched) || !strings.Contains(logs.String(), "boom") {
			t.Errorf("%d touches and logs %s; want a warning with the error for each touch", len(touched), logs.String())
		}
	})
}

// TestProcessorSkipsPromotedOutputs holds the hash of the second rendition for a lease, after the first has been
// promoted: only the second temp output is touched meanwhile.
func TestProcessorSkipsPromotedOutputs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newProcessorFixture(t)
		f.files.beforeOpen = func(path string) {
			if path == f.tc.outputs[1].Path {
				synctest.Sleep(processorLease)
			}
		}
		var logs bytes.Buffer
		ctx := logging.CtxWithLogger(t.Context(), slog.New(slog.NewJSONHandler(&logs, nil)))

		if err := f.p.Process(ctx, claimed); err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		second := f.tc.outputs[1].Path
		touched := f.files.touches()
		if len(touched) == 0 || slices.ContainsFunc(touched, func(path string) bool { return path != second }) {
			t.Errorf("touched %v, want only %s, which wasn't promoted yet", touched, second)
		}
		if logs.Len() > 0 {
			t.Errorf("logs = %s, want none", logs.String())
		}
	})
}

func TestProcessorFail(t *testing.T) {
	rejection := &audio.Rejection{
		Message: audio.MessageNotAudio,
		Err:     errors.New("exit status 1: /data/sources/7/x: Invalid data found when processing input"),
	}
	wrapped := fmt.Errorf("error probing: %w", rejection)
	panicked := fmt.Errorf("error processing: %w", worker.Permanent(errors.New("panic: /data/tmp/part-1")))
	tests := []struct {
		name  string
		cause error
		want  failure
	}{
		{"rejection", rejection, failure{Permanent: true, Reason: "not audio", Detail: rejection.Error()}},
		{"wrapped rejection", wrapped, failure{Permanent: true, Reason: "not audio", Detail: wrapped.Error()}},
		{"permanent", worker.Permanent(errBoom), failure{Permanent: true, Detail: "boom"}},
		{"wrapped permanent", panicked, failure{Permanent: true, Detail: panicked.Error()}},
		{"permanent rejection", worker.Permanent(rejection),
			failure{Permanent: true, Reason: "not audio", Detail: rejection.Error()}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newProcessorFixture(t)

			if err := f.p.Fail(t.Context(), claimed, tc.cause); err != nil {
				t.Fatalf("Fail() error = %v", err)
			}

			if want := []failure{tc.want}; !reflect.DeepEqual(f.repo.failures, want) {
				t.Errorf("failures = %+v, want %+v", f.repo.failures, want)
			}
			if want := []lease{lease(claimed)}; !slices.Equal(f.repo.leases, want) {
				t.Errorf("leases = %+v, want %+v", f.repo.leases, want)
			}
		})
	}
}

// TestProcessorFailRetries fails retryable errors many times on each attempt: every delay falls in [d/2, d],
// where d doubles from 30 s up to 10 min.
func TestProcessorFailRetries(t *testing.T) {
	tests := []struct {
		attempt int
		cause   error
		d       time.Duration
	}{
		{1, errors.New("error rendering: exit status 1: /data/tmp/part-1: Conversion failed!"), 30 * time.Second},
		{2, fmt.Errorf("error rendering: %w", context.DeadlineExceeded), time.Minute},
		{3, errBoom, 2 * time.Minute},
		{5, errBoom, 8 * time.Minute},
		{6, errBoom, 10 * time.Minute},
		{20, errBoom, 10 * time.Minute},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("attempt %d", tc.attempt), func(t *testing.T) {
			f := newProcessorFixture(t)
			l := worker.Lease{ID: 11, TenantID: tenantID, Attempt: tc.attempt}

			for range 100 {
				if err := f.p.Fail(t.Context(), l, tc.cause); err != nil {
					t.Fatalf("Fail() error = %v", err)
				}
			}

			for _, got := range f.repo.failures {
				if got.Permanent || got.Reason != "" || got.Detail != tc.cause.Error() ||
					got.Delay < tc.d/2 || got.Delay > tc.d {
					t.Fatalf("failure = %+v, want a retry after %v to %v with detail %q",
						got, tc.d/2, tc.d, tc.cause.Error())
				}
			}
		})
	}
}

func TestProcessorLostLease(t *testing.T) {
	ops := []struct {
		method string
		run    func(ctx context.Context, p *Processor) error
	}{
		{"extend", func(ctx context.Context, p *Processor) error { return p.Extend(ctx, claimed) }},
		{"finish", func(ctx context.Context, p *Processor) error { return p.Process(ctx, claimed) }},
		{"fail", func(ctx context.Context, p *Processor) error { return p.Fail(ctx, claimed, errBoom) }},
		{"release", func(ctx context.Context, p *Processor) error { return p.Release(ctx, claimed) }},
	}
	errs := []struct {
		name     string
		err      error
		wantLost bool
	}{
		{"lease lost", fmt.Errorf("error writing asset: %w", errLeaseLost), true},
		{"database error", fmt.Errorf("error writing asset: %w: %v", errDatabase, errBoom), false},
	}
	for _, op := range ops {
		for _, e := range errs {
			t.Run(op.method+"/"+e.name, func(t *testing.T) {
				f := newProcessorFixture(t)
				f.repo.errs[op.method] = e.err

				err := op.run(t.Context(), f.p)

				if err == nil || errors.Is(err, worker.ErrLeaseLost) != e.wantLost {
					t.Errorf("error = %v; matches worker.ErrLeaseLost: %t, want %t",
						err, errors.Is(err, worker.ErrLeaseLost), e.wantLost)
				}
				if err != nil && err.Error() != e.err.Error() {
					t.Errorf("error = %q, want the repository's %q", err, e.err)
				}
			})
		}
	}
}

func TestProcessorReap(t *testing.T) {
	tests := []struct {
		name             string
		requeued, failed int
		err              error
		wantLog          bool
	}{
		{"nothing expired", 0, 0, nil, false},
		{"requeued", 2, 0, nil, true},
		{"failed", 0, 1, nil, true},
		{"both", 3, 2, nil, true},
		{"database error", 0, 0, fmt.Errorf("error reaping assets: %w: %v", errDatabase, errBoom), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newProcessorFixture(t)
			f.repo.requeued, f.repo.failed, f.repo.errs["reap"] = tc.requeued, tc.failed, tc.err
			var logs bytes.Buffer
			ctx := logging.CtxWithLogger(t.Context(), slog.New(slog.NewJSONHandler(&logs, nil)))

			if err := f.p.Reap(ctx); !errors.Is(err, tc.err) {
				t.Errorf("Reap() error = %v, want %v", err, tc.err)
			}

			if !tc.wantLog {
				if logs.Len() > 0 {
					t.Errorf("logs = %s, want none", logs.String())
				}
				return
			}
			var entry struct {
				Level            string
				Requeued, Failed int
			}
			if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
				t.Fatalf("error decoding logs %s: %v", logs.String(), err)
			}
			if entry.Level != "WARN" || entry.Requeued != tc.requeued || entry.Failed != tc.failed {
				t.Errorf("logs = %s, want a warning with %d requeued and %d failed", logs.String(), tc.requeued, tc.failed)
			}
		})
	}
}
