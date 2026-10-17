//go:build integration

package audio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/testutil"
)

const testTargetLUFS = -16.0

// tempOutputs mimics the empty temp files that storage hands the processor.
func tempOutputs(tb testing.TB) []Output {
	tb.Helper()
	dir := tb.TempDir()
	var outputs []Output
	for _, p := range Profiles() {
		path := filepath.Join(dir, "part-"+p.Name)
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			tb.Fatalf("error creating %s: %v", path, err)
		}
		outputs = append(outputs, Output{Profile: p, Path: path})
	}
	return outputs
}

func testContext(tb testing.TB) context.Context {
	return logging.CtxWithLogger(tb.Context(), testutil.Logger(tb))
}

func TestVersion(t *testing.T) {
	ffmpeg, ffprobe, err := New().Version(t.Context())
	if err != nil {
		t.Fatalf("Version() error = %v", err)
	}
	if !strings.HasPrefix(ffmpeg, "ffmpeg version ") || !strings.HasPrefix(ffprobe, "ffprobe version ") {
		t.Errorf("Version() = %q, %q, want ffmpeg's and ffprobe's version lines", ffmpeg, ffprobe)
	}
	t.Logf("%s; %s", ffmpeg, ffprobe)
}

func TestProcessRoundTrip(t *testing.T) {
	tests := []struct {
		name         string
		fixture      func(testing.TB) string
		channels     int
		duration     time.Duration
		wantModes    []Normalization // per profile, in Profiles() order
		wantWarnings []Warning
		// wantDownmixLU is how much quieter pass 1 measures the mono downmix than the stereo one, if not NaN.
		wantDownmixLU float64
	}{
		{
			name: "dual-mono pink", fixture: testutil.DualMonoPink, channels: 2, duration: 10 * time.Second,
			wantModes:     []Normalization{Linear, Dynamic},
			wantDownmixLU: 0,
		},
		{
			name: "uncorrelated pink", fixture: testutil.UncorrelatedPink, channels: 2, duration: 10 * time.Second,
			wantModes:     []Normalization{Linear, Dynamic},
			wantDownmixLU: 3,
		},
		{
			name: "uncorrelated white", fixture: testutil.UncorrelatedWhite, channels: 2, duration: 10 * time.Second,
			wantModes:     []Normalization{Linear, Linear},
			wantDownmixLU: 3,
		},
		{
			name: "mono pink", fixture: testutil.MonoPink, channels: 1, duration: 10 * time.Second,
			wantModes:     []Normalization{Dynamic, Dynamic},
			wantDownmixLU: math.NaN(),
		},
		{
			name: "5.1 pink", fixture: testutil.Surround, channels: 6, duration: 10 * time.Second,
			wantModes:     []Normalization{Dynamic, Dynamic},
			wantDownmixLU: math.NaN(),
		},
		{
			name: "thunder", fixture: testutil.Thunder, channels: 2, duration: time.Minute,
			wantModes:     []Normalization{Dynamic, Dynamic},
			wantWarnings:  []Warning{WarningSuddenLoudEvent, WarningDynamicNormalization},
			wantDownmixLU: math.NaN(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := New()
			ctx := testContext(t)
			outputs := tempOutputs(t)
			res, err := f.Process(ctx, tc.fixture(t), testTargetLUFS, outputs)
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			t.Logf("source %+v, input %+v, warnings %v", res.Source, res.Input, res.Warnings)

			if res.Source.Channels != tc.channels || res.Source.Duration != tc.duration ||
				res.Source.Codec != "pcm_s16le" || res.Source.SampleRate != 48000 {
				t.Errorf("Source = %+v, want pcm_s16le at 48000 Hz, %d channels, %v", res.Source, tc.channels, tc.duration)
			}
			if !slices.Equal(res.Warnings, tc.wantWarnings) {
				t.Errorf("Warnings = %v, want %v", res.Warnings, tc.wantWarnings)
			}
			if len(res.Renditions) != len(outputs) {
				t.Fatalf("got %d renditions, want %d", len(res.Renditions), len(outputs))
			}
			measured := map[int]Loudness{}
			for i, r := range res.Renditions {
				t.Logf("%s: %d channels, measured %+v, predicted %s, rendered %s, verified %+v, flattened by %.2f LU",
					r.Profile.Name, r.Channels, r.Measured, r.Predicted, r.Normalization, r.Loudness, flattening(r))
				// A loudest moment under the integrated loudness would come from another signal than the one measured.
				for _, m := range []Loudness{r.Measured, r.Loudness} {
					if m.MaxMomentaryLUFS < m.LUFS-0.1 {
						t.Errorf("%s has a max momentary loudness of %.1f LUFS under its %.2f LUFS integrated",
							r.Profile.Name, m.MaxMomentaryLUFS, m.LUFS)
					}
				}
				wantChannels := min(tc.channels, outputs[i].Profile.MaxChannels)
				if r.Profile != outputs[i].Profile || r.Path != outputs[i].Path || r.Channels != wantChannels {
					t.Errorf("rendition %d is %s at %s with %d channels, want %s at %s with %d",
						i, r.Profile.Name, r.Path, r.Channels, outputs[i].Profile.Name, outputs[i].Path, wantChannels)
				}
				if math.Abs(r.Loudness.LUFS-testTargetLUFS) > ToleranceLU || r.Loudness.TruePeakDBTP > CeilingDBTP {
					t.Errorf("%s verified at %.2f LUFS and %.2f dBTP, want %.1f ± %.0f LU and at most %.1f dBTP",
						r.Profile.Name, r.Loudness.LUFS, r.Loudness.TruePeakDBTP,
						testTargetLUFS, ToleranceLU, CeilingDBTP)
				}
				if r.Predicted != r.Normalization {
					t.Errorf("%s predicted %s, but pass 2 reported %s", r.Profile.Name, r.Predicted, r.Normalization)
				}
				if r.Normalization != tc.wantModes[i] {
					t.Errorf("%s normalization = %s, want %s", r.Profile.Name, r.Normalization, tc.wantModes[i])
				}
				encoded, err := f.probe(ctx, r.Path)
				if err != nil {
					t.Fatalf("error probing %s: %v", r.Profile.Name, err)
				}
				if encoded.Codec != "aac" || encoded.SampleRate != 48000 || encoded.Channels != wantChannels ||
					(encoded.Duration-tc.duration).Abs() > 100*time.Millisecond {
					t.Errorf("%s is %+v, want aac at 48000 Hz, %d channels, about %v",
						r.Profile.Name, encoded, wantChannels, tc.duration)
				}
				measured[r.Channels] = r.Measured
			}
			if !math.IsNaN(tc.wantDownmixLU) {
				drop := measured[2].LUFS - measured[1].LUFS
				if math.Abs(drop-tc.wantDownmixLU) > 0.5 {
					t.Errorf("the mono downmix measures %.2f LU quieter than stereo, want %.0f", drop, tc.wantDownmixLU)
				}
			}
		})
	}
}

// correctionLog is the warning a corrective re-encode logs.
type correctionLog struct {
	Level             string  `json:"level"`
	Msg               string  `json:"msg"`
	Profile           string  `json:"profile"`
	GainDB            float64 `json:"gain_db"`
	FirstLUFS         float64 `json:"first_lufs"`
	FirstTruePeakDBTP float64 `json:"first_true_peak_dbtp"`
	LUFS              float64 `json:"lufs"`
	TruePeakDBTP      float64 `json:"true_peak_dbtp"`
}

func TestProcessCorrectsBrightContent(t *testing.T) {
	var logs bytes.Buffer
	ctx := logging.CtxWithLogger(t.Context(), slog.New(slog.NewJSONHandler(io.MultiWriter(&logs, t.Output()), nil)))
	res, err := New().Process(ctx, testutil.BrightWhite(t), testTargetLUFS, tempOutputs(t))
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	for _, r := range res.Renditions {
		t.Logf("%s: rendered %s, verified %+v", r.Profile.Name, r.Normalization, r.Loudness)
		if math.Abs(r.Loudness.LUFS-testTargetLUFS) > ToleranceLU || r.Loudness.TruePeakDBTP > CeilingDBTP {
			t.Errorf("%s verified at %.2f LUFS and %.2f dBTP, want %.1f ± %.0f LU and at most %.1f dBTP",
				r.Profile.Name, r.Loudness.LUFS, r.Loudness.TruePeakDBTP, testTargetLUFS, ToleranceLU, CeilingDBTP)
		}
	}

	var corrections []correctionLog
	for line := range strings.Lines(logs.String()) {
		var l correctionLog
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			t.Fatalf("error parsing the log line %q: %v", line, err)
		}
		if l.Msg == "re-encoded a rendition with a corrective gain" {
			corrections = append(corrections, l)
		}
	}
	// The fixture is only useful while its first 64k encode misses the target.
	if len(corrections) != 1 || corrections[0].Profile != "aac_64k" || corrections[0].Level != "WARN" {
		t.Fatalf("corrective re-encodes = %+v, want one warning, for aac_64k", corrections)
	}
	c := corrections[0]
	if math.Abs(c.FirstLUFS-testTargetLUFS) <= ToleranceLU || math.Abs(c.GainDB) > maxCorrectionDB {
		t.Errorf("aac_64k first verified at %.2f LUFS and re-encoded with %+.2f dB, "+
			"want it outside the tolerance and a gain within %.1f dB", c.FirstLUFS, c.GainDB, maxCorrectionDB)
	}
	if r := res.Renditions[1]; r.Loudness.LUFS != c.LUFS || r.Loudness.TruePeakDBTP != c.TruePeakDBTP {
		t.Errorf("aac_64k stored %+v, want the re-encode's %.2f LUFS and %.2f dBTP", r.Loudness, c.LUFS, c.TruePeakDBTP)
	}
}

func TestProcessRejects(t *testing.T) {
	tests := []struct {
		name    string
		fixture func(testing.TB) string
		want    string
		// prefix accepts any message that starts with want.
		prefix bool
	}{
		{name: "text file", fixture: testutil.NotAudio, want: MessageNotAudio},
		{name: "0.3 s clip", fixture: func(tb testing.TB) string { return testutil.PinkClip(tb, 300*time.Millisecond) }, want: MessageTooShort},
		{name: "2 s clip", fixture: func(tb testing.TB) string { return testutil.PinkClip(tb, 2*time.Second) }, want: MessageTooShort},
		{name: "silence", fixture: testutil.Silence, want: MessageSilent},
		{name: "blue noise", fixture: testutil.BlueNoise, want: "aac_128k rendition out of spec", prefix: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := New().Process(testContext(t), tc.fixture(t), testTargetLUFS, tempOutputs(t))
			t.Logf("Process() error = %v", err)
			rej, ok := errors.AsType[*Rejection](err)
			if !ok {
				t.Fatalf("Process() error = %v, want a rejection", err)
			}
			matches := rej.Message == tc.want
			if tc.prefix {
				matches = strings.HasPrefix(rej.Message, tc.want)
			}
			if !matches {
				t.Errorf("rejection message = %q, want %q", rej.Message, tc.want)
			}
		})
	}
}

func TestProcessMissingSourceIsRetryable(t *testing.T) {
	src := filepath.Join(t.TempDir(), "missing.wav")
	_, err := New().Process(testContext(t), src, testTargetLUFS, tempOutputs(t))
	t.Logf("Process() error = %v", err)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Process() error = %v, want one matching fs.ErrNotExist", err)
	}
	if _, ok := errors.AsType[*Rejection](err); ok {
		t.Errorf("Process() error = %v, want it not to be a rejection", err)
	}
}

// recordingRunner runs commands for real and keeps what each one wrote to stderr.
type recordingRunner struct {
	runner
	calls []recordedCall
}

type recordedCall struct {
	args   []string
	stderr string
	err    error
}

func (r *recordingRunner) Run(ctx context.Context, name string, args []string, stdout, stderr io.Writer) error {
	var buf bytes.Buffer
	w := io.Writer(&buf)
	if stderr != nil {
		w = io.MultiWriter(stderr, &buf)
	}
	err := r.runner.Run(ctx, name, args, stdout, w)
	r.calls = append(r.calls, recordedCall{args: args, stderr: buf.String(), err: err})
	return err
}

func TestProcessFailsWhenFFmpegExitsAfterItsReport(t *testing.T) {
	rec := &recordingRunner{runner: execRunner{}}
	outputs := tempOutputs(t)
	outputs[1].Path = "/dev/full"
	_, err := (&FFmpeg{run: rec}).Process(testContext(t), testutil.UncorrelatedPink(t), testTargetLUFS, outputs)
	t.Logf("Process() error = %v", err)

	exitErr, ok := errors.AsType[*exitError](err)
	if !ok || exitErr.code == 0 {
		t.Fatalf("Process() error = %v, want ffmpeg's non-zero exit", err)
	}
	if _, ok := errors.AsType[*Rejection](err); ok {
		t.Errorf("Process() error = %v, want it not to be a rejection", err)
	}
	pass2 := rec.calls[len(rec.calls)-1]
	if !slices.Contains(pass2.args, "/dev/full") || !errors.Is(err, pass2.err) {
		t.Fatalf("the last command was %q, want pass 2 to have failed", pass2.args)
	}
	for _, instance := range []string{"[loudnorm@stereo @ ", "[loudnorm@mono @ "} {
		if !strings.Contains(pass2.stderr, instance) {
			t.Errorf("pass 2's stderr has no report from %s, want ffmpeg to have printed both", instance)
		}
	}
	if !strings.Contains(pass2.stderr, `"normalization_type" : "`) {
		t.Errorf("pass 2's stderr has no normalization_type, want a complete report:\n%s", pass2.stderr)
	}
}
