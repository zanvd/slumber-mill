//go:build integration

package asset

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/audio"
	"github.com/zanvd/slumber-mill/src/testutil"
	"github.com/zanvd/slumber-mill/src/worker"
)

// runEncodePool runs an encode pool of one slot over the stack until nothing is due, with tc in place of ffmpeg.
func (s *stack) runEncodePool(t *testing.T, tc transcoder) {
	t.Helper()
	p, err := NewProcessor(s.repo, s.store, tc, ProcessorConfig{WorkerID: "encode-1", Lease: testLease})
	if err != nil {
		t.Fatalf("NewProcessor() error = %v", err)
	}
	pool, err := worker.NewPool(p, worker.Config{
		WorkerID: "encode-1", Pool: "encode", Concurrency: 1, Lease: testLease,
		PollInterval: time.Second, Log: testutil.Logger(t),
	})
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	if err := pool.RunUntilIdle(t.Context()); err != nil {
		t.Fatalf("RunUntilIdle() error = %v", err)
	}
}

// relFiles are paths under the stack's root, as filesUnder lists them.
func relFiles(tenantID uint64, assetID uuid.UUID, renditions ...Profile) []string {
	dir := strconv.FormatUint(tenantID, 10) + "/" + assetID.String()
	var files []string
	for _, p := range renditions {
		files = append(files, "renditions/"+dir+"/"+string(p)+".m4a")
	}
	return append(files, "sources/"+dir)
}

func TestProcessorReadiesAnUpload(t *testing.T) {
	s := newLeaseStack(t)
	tenant := s.tenant(t, "owner")
	publicID, id := s.queue(t, tenant)
	s.exec(t, "UPDATE assets SET target_lufs = -18.5 WHERE id = ?", id)
	tc := &fakeTranscoder{calls: &calls{}}

	s.runEncodePool(t, tc)

	if src := s.store.SourcePath(tenant, publicID); tc.src != src || tc.targetLUFS != -18.5 {
		t.Errorf("transcoded %s to %v LUFS, want %s to -18.5 LUFS", tc.src, tc.targetLUFS, src)
	}
	s.wantUnleased(t, id, StatusReady, 1)
	got, err := s.svc.Get(t.Context(), tenant, publicID.String())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	content := func(p Profile) []byte { return []byte(fakeRendering(string(p))) }
	want := Asset{
		ID: publicID, TenantID: tenant, Title: "Rain", Status: StatusReady,
		SourceSHA256: sha256.Sum256([]byte("audio")), SourceBytes: 5, SourceCodec: "flac",
		DurationMS: new(uint64(600_500)), TargetLUFS: -18.5, InputLUFS: new(-23.45), InputTruePeakDBTP: new(-0.25),
		InputLRALU: new(7.5), InputMaxMomentary: new(-10.5),
		Warnings: []Warning{WarningSuddenLoudEvent, WarningDynamicNormalization, WarningSourceMayClip},
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
		CreatedAt: got.CreatedAt, UpdatedAt: got.UpdatedAt,
	}
	got.Renditions = withoutCreatedAt(t, got.Renditions)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("asset = %+v\nwant  %+v", got, want)
	}

	wantFiles := relFiles(tenant, publicID, ProfileAAC128k, ProfileAAC64k)
	if files := filesUnder(t, s.root); !slices.Equal(files, wantFiles) {
		t.Errorf("files = %v, want %v", files, wantFiles)
	}
	for _, p := range []Profile{ProfileAAC128k, ProfileAAC64k} {
		path, err := s.store.RenditionPath(tenant, publicID, string(p))
		if err != nil {
			t.Fatalf("RenditionPath() error = %v", err)
		}
		if b, err := os.ReadFile(path); err != nil || string(b) != string(content(p)) {
			t.Errorf("%s holds %q, %v; want %q", path, b, err, content(p))
		}
	}
	s.wantEvents(t, EventReady, []uint64{id}, []uint64{tenant})
}

// TestProcessorFailsInThePool has the transcoder write its outputs and then fail.
func TestProcessorFailsInThePool(t *testing.T) {
	tests := []struct {
		name  string
		cause func(source string) error
		// wantError is the tenant-safe error of an asset that failed for good; empty means it waits for a retry.
		wantError string
	}{
		{
			name: "rejection fails at once",
			cause: func(source string) error {
				return &audio.Rejection{Message: audio.MessageNotAudio,
					Err: errors.New("exit status 1: " + source + ": Invalid data found when processing input")}
			},
			wantError: "not audio",
		},
		{
			name:      "permanent error fails at once",
			cause:     func(string) error { return worker.Permanent(errors.New("unsupported layout")) },
			wantError: failedMessage,
		},
		{
			name:  "other errors retry",
			cause: func(string) error { return errors.New("exit status 1: /data/tmp/part-1: Conversion failed!") },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newLeaseStack(t)
			tenant := s.tenant(t, "owner")
			publicID, id := s.queue(t, tenant)
			cause := tc.cause(s.store.SourcePath(tenant, publicID))
			render := func(_ context.Context, outputs []audio.Output) (audio.Result, error) {
				if _, err := fakeRender(outputs); err != nil {
					return audio.Result{}, err
				}
				return audio.Result{}, cause
			}
			tr := &fakeTranscoder{calls: &calls{}, render: render}

			s.runEncodePool(t, tr)

			if len(tr.calls.list) != 1 {
				t.Errorf("transcoder calls = %v, want 1", tr.calls.list)
			}
			if files, want := filesUnder(t, s.root), relFiles(tenant, publicID); !slices.Equal(files, want) {
				t.Errorf("files = %v, want %v", files, want)
			}
			if n := len(s.rows(t, "SELECT id FROM renditions WHERE asset_id = ?", id)); n != 0 {
				t.Errorf("%d renditions, want none", n)
			}
			s.wantNothingDue(t)

			if tc.wantError == "" {
				st := s.wantUnleased(t, id, StatusQueued, 1)
				if st.Error.Valid || st.LastError.String != cause.Error() {
					t.Errorf("error, last_error = %+v, %q; want NULL, %q", st.Error, st.LastError.String, cause)
				}
				if d := s.until(t, id, "run_after"); d <= 15*time.Second-slack || d > 30*time.Second {
					t.Errorf("due in %v, want 15s to 30s", d)
				}
				s.wantNoEvents(t)
				return
			}
			st := s.wantUnleased(t, id, StatusFailed, 1)
			if st.Error.String != tc.wantError || st.LastError.String != cause.Error() {
				t.Errorf("error, last_error = %q, %q; want %q, %q", st.Error.String, st.LastError.String,
					tc.wantError, cause)
			}
			s.wantEvents(t, EventFailed, []uint64{id}, []uint64{tenant})
		})
	}
}
