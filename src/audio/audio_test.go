package audio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/zanvd/slumber-mill/src/logging"
)

type fakeResponse struct {
	stdout, stderr string
	err            error
}

type fakeCall struct {
	name string
	args []string
}

// fakeRunner answers commands in order with its scripted responses, and records them.
type fakeRunner struct {
	responses []fakeResponse
	calls     []fakeCall
}

func (f *fakeRunner) Run(_ context.Context, name string, args []string, stdout, stderr io.Writer) error {
	f.calls = append(f.calls, fakeCall{name: name, args: args})
	if len(f.responses) == 0 {
		return fmt.Errorf("unexpected command %s %q", name, args)
	}
	r := f.responses[0]
	f.responses = f.responses[1:]
	if stdout != nil {
		io.WriteString(stdout, r.stdout)
	}
	if stderr != nil {
		io.WriteString(stderr, r.stderr)
	}
	return r.err
}

func probeJSON(channels int, duration string) string {
	return fmt.Sprintf(`{"programs": [], "streams": [{"codec_name": "pcm_s16le", "sample_rate": "48000", "channels": %d}],
		"format": {"duration": %q}}`, channels, duration)
}

func report(instance string, m Loudness, mode Normalization) string {
	return reportWithOffset(instance, m, mode, "0.00")
}

func reportWithOffset(instance string, m Loudness, mode Normalization, offset string) string {
	return fmt.Sprintf("[%s @ 0x5581] \n{\n\t\"input_i\" : \"%.2f\",\n\t\"input_tp\" : \"%.2f\",\n"+
		"\t\"input_lra\" : \"%.2f\",\n\t\"input_thresh\" : \"%.2f\",\n\t\"output_i\" : \"-16.00\",\n"+
		"\t\"normalization_type\" : \"%s\",\n\t\"target_offset\" : \"%s\"\n}\n",
		instance, m.LUFS, m.TruePeakDBTP, m.LRALU, m.ThresholdLUFS, mode, offset)
}

func frames(momentary ...float64) string {
	var b strings.Builder
	for i, m := range momentary {
		fmt.Fprintf(&b, "[ebur128@input @ 0x5582] t: %.1f  TARGET:-23 LUFS    M:%6.1f S:-120.7     I: -70.0 LUFS\n",
			float64(i+1)/10, m)
	}
	return b.String()
}

var (
	stereoMeasured = Loudness{LUFS: -17.69, TruePeakDBTP: -7.26, LRALU: 0.1, ThresholdLUFS: -27.69}
	monoMeasured   = Loudness{LUFS: -20.7, TruePeakDBTP: -6.59, LRALU: 0.1, ThresholdLUFS: -30.7}
	stereoEncoded  = Loudness{LUFS: -16.02, TruePeakDBTP: -5.41, LRALU: 0.3, ThresholdLUFS: -26.02}
	monoEncoded    = Loudness{LUFS: -15.85, TruePeakDBTP: -2.99, LRALU: 0.5, ThresholdLUFS: -25.85}
)

// stereoScript is a successful run on a stereo source: probe, pass 1, pass 2 and two verifications.
func stereoScript() []fakeResponse {
	return []fakeResponse{
		{stdout: probeJSON(2, "10.000000")},
		{stderr: frames(-120.7, -17.6, -17.8) + reportWithOffset("loudnorm@mono", monoMeasured, Dynamic, "0.07") +
			reportWithOffset("loudnorm@stereo", stereoMeasured, Dynamic, "-0.31")},
		{stderr: report("loudnorm@mono", monoEncoded, Dynamic) + report("loudnorm@stereo", stereoEncoded, Linear)},
		{stderr: report(verifyInstance, stereoEncoded, Dynamic)},
		{stderr: report(verifyInstance, monoEncoded, Dynamic)},
	}
}

var (
	monoShort     = Loudness{LUFS: -17.3, TruePeakDBTP: -6.79, LRALU: 0.4, ThresholdLUFS: -27.3}
	monoCorrected = Loudness{LUFS: -15.98, TruePeakDBTP: -5.47, LRALU: 0.4, ThresholdLUFS: -25.98}
)

// correctiveScript is stereoScript with a 64k rendition that verifies 1.3 LU short of the target,
// then its corrective re-encode and second verification.
func correctiveScript() []fakeResponse {
	s := stereoScript()
	s[4].stderr = report(verifyInstance, monoShort, Dynamic)
	return append(s,
		fakeResponse{stderr: report("loudnorm@mono", monoEncoded, Dynamic)},
		fakeResponse{stderr: report(verifyInstance, monoCorrected, Dynamic)},
	)
}

func outputs() []Output {
	return []Output{{Profile: Profiles()[0], Path: "/data/tmp/part-1"}, {Profile: Profiles()[1], Path: "/data/tmp/part-2"}}
}

func TestProcess(t *testing.T) {
	run := &fakeRunner{responses: stereoScript()}
	res, err := (&FFmpeg{run: run}).Process(t.Context(), "/data/sources/1/a", -16, outputs())
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	want := Result{
		Source:   Source{Codec: "pcm_s16le", SampleRate: 48000, Channels: 2, Duration: 10 * time.Second},
		Input:    Input{LUFS: -17.69, TruePeakDBTP: -7.26, LRALU: 0.1, MaxMomentaryLUFS: -17.6},
		Warnings: []Warning{WarningDynamicNormalization},
		Renditions: []Rendition{
			{
				Profile: Profiles()[0], Path: "/data/tmp/part-1", Channels: 2, Measured: stereoMeasured,
				Predicted: Linear, Normalization: Linear, Loudness: stereoEncoded,
			},
			{
				Profile: Profiles()[1], Path: "/data/tmp/part-2", Channels: 1, Measured: monoMeasured,
				Predicted: Dynamic, Normalization: Dynamic, Loudness: monoEncoded,
			},
		},
	}
	if res.Source != want.Source || res.Input != want.Input || !slices.Equal(res.Warnings, want.Warnings) ||
		!slices.Equal(res.Renditions, want.Renditions) {
		t.Errorf("Process() = %+v\nwant %+v", res, want)
	}
	if len(run.responses) != 0 {
		t.Errorf("Process() left %d commands unrun", len(run.responses))
	}
}

// The whitelists only apply to an input they precede.
const (
	whitelists = "-protocol_whitelist file -format_whitelist wav,mp3,flac,ogg,mov,aac,matroska,aiff"
	common     = "-nostdin -hide_banner -nostats -loglevel info " + whitelists
)

func TestProcessCommands(t *testing.T) {
	run := &fakeRunner{responses: stereoScript()}
	if _, err := (&FFmpeg{run: run}).Process(t.Context(), "/src.wav", -16, outputs()); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	want := []string{
		"ffprobe -v error " + whitelists +
			" -select_streams a:0 -show_entries stream=codec_name,sample_rate,channels:format=duration -of json /src.wav",
		"ffmpeg " + common + " -i /src.wav -filter_complex " +
			"[0:a:0]ebur128@input=framelog=info,asplit=2[p0][p1];" +
			"[p0]aformat=channel_layouts=stereo,loudnorm@stereo=I=-16:TP=-4.5:LRA=50:print_format=json[m0];" +
			"[p1]aformat=channel_layouts=mono,loudnorm@mono=I=-16:TP=-4.5:LRA=50:print_format=json[m1] " +
			"-map [m0] -f null - -map [m1] -f null -",
		"ffmpeg -y " + common + " -i /src.wav -filter_complex " +
			"[0:a:0]asplit=2[s0][s1];" +
			"[s0]aformat=channel_layouts=stereo,loudnorm@stereo=I=-16:TP=-4.5:LRA=50:measured_I=-17.69:measured_TP=-7.26:" +
			"measured_LRA=0.1:measured_thresh=-27.69:offset=-0.31:linear=true:print_format=json,aresample=48000[o0];" +
			"[s1]aformat=channel_layouts=mono,loudnorm@mono=I=-16:TP=-4.5:LRA=50:measured_I=-20.7:measured_TP=-6.59:" +
			"measured_LRA=0.1:measured_thresh=-30.7:offset=0.07:linear=true:print_format=json,aresample=48000[o1] " +
			"-map [o0] -map_metadata -1 -c:a aac -b:a 128k -cutoff 20000 -movflags +faststart -f mp4 /data/tmp/part-1 " +
			"-map [o1] -map_metadata -1 -c:a aac -b:a 64k -movflags +faststart -f mp4 /data/tmp/part-2",
		"ffmpeg " + common + " -i /data/tmp/part-1 -filter_complex " +
			"[0:a:0]loudnorm@verify=I=-16:TP=-4.5:LRA=50:print_format=json[v] -map [v] -f null -",
		"ffmpeg " + common + " -i /data/tmp/part-2 -filter_complex " +
			"[0:a:0]loudnorm@verify=I=-16:TP=-4.5:LRA=50:print_format=json[v] -map [v] -f null -",
	}
	var got []string
	for _, c := range run.calls {
		got = append(got, c.name+" "+strings.Join(c.args, " "))
	}
	if !slices.Equal(got, want) {
		t.Errorf("Process() ran\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestProcessLayouts(t *testing.T) {
	tests := []struct {
		name         string
		channels     int
		measure      []string // filter graph fragments pass 1 must hold
		render       []string // and pass 2
		measureNot   []string
		wantChannels []int
	}{
		{
			name: "mono source", channels: 1,
			measure:      []string{"asplit=1[p0];[p0]aformat=channel_layouts=mono,loudnorm@mono="},
			measureNot:   []string{"loudnorm@stereo", "loudnorm@source"},
			render:       []string{"asplit=1[s0];[s0]aformat=channel_layouts=mono,", ",aresample=48000,asplit=2[o0][o1]"},
			wantChannels: []int{1, 1},
		},
		{
			name: "5.1 source", channels: 6,
			measure: []string{
				"asplit=3[p0][p1][p2];",
				";[p2]loudnorm@source=I=-16:TP=-4.5:LRA=50:print_format=json[m2]",
				"-map [m2] -f null -",
			},
			render:       []string{"asplit=2[s0][s1];", ",aresample=48000[o0];", ",aresample=48000[o1]"},
			wantChannels: []int{2, 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pass1 := frames(-20.5)
			pass2 := ""
			if tc.channels == 1 {
				pass1 += report("loudnorm@mono", monoMeasured, Dynamic)
				pass2 = report("loudnorm@mono", monoEncoded, Dynamic)
			} else {
				pass1 += report("loudnorm@stereo", stereoMeasured, Dynamic) + report("loudnorm@mono", monoMeasured, Dynamic) +
					report("loudnorm@source", Loudness{LUFS: -17.49, TruePeakDBTP: -9.92, LRALU: 0.1, ThresholdLUFS: -27.49}, Dynamic)
				pass2 = report("loudnorm@stereo", stereoEncoded, Linear) + report("loudnorm@mono", monoEncoded, Dynamic)
			}
			run := &fakeRunner{responses: []fakeResponse{
				{stdout: probeJSON(tc.channels, "10.0")},
				{stderr: pass1},
				{stderr: pass2},
				{stderr: report(verifyInstance, stereoEncoded, Dynamic)},
				{stderr: report(verifyInstance, monoEncoded, Dynamic)},
			}}
			res, err := (&FFmpeg{run: run}).Process(t.Context(), "/src.wav", -16, outputs())
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			measure, render := strings.Join(run.calls[1].args, " "), strings.Join(run.calls[2].args, " ")
			for _, s := range tc.measure {
				if !strings.Contains(measure, s) {
					t.Errorf("pass 1 %q doesn't contain %q", measure, s)
				}
			}
			for _, s := range tc.measureNot {
				if strings.Contains(measure, s) {
					t.Errorf("pass 1 %q contains %q", measure, s)
				}
			}
			for _, s := range tc.render {
				if !strings.Contains(render, s) {
					t.Errorf("pass 2 %q doesn't contain %q", render, s)
				}
			}
			for i, r := range res.Renditions {
				if r.Channels != tc.wantChannels[i] {
					t.Errorf("%s has %d channels, want %d", r.Profile.Name, r.Channels, tc.wantChannels[i])
				}
			}
			if tc.channels == 6 && res.Input.LUFS != -17.49 {
				t.Errorf("Input = %+v, want the 5.1 source's own measurement", res.Input)
			}
		})
	}
}

func TestProcessCorrectsARendition(t *testing.T) {
	stereoLoud := Loudness{LUFS: -14.99, TruePeakDBTP: -3, LRALU: 0.3, ThresholdLUFS: -24.99}
	stereoCorrected := Loudness{LUFS: -16.01, TruePeakDBTP: -4.02, LRALU: 0.3, ThresholdLUFS: -26.01}
	tests := []struct {
		name string
		// failing is the index of the rendition that fails its first verification.
		failing int
		script  []fakeResponse
		retry   string
		want    Rendition
		log     string
	}{
		{
			name: "aac_64k short of the target", failing: 1, script: correctiveScript(),
			retry: "ffmpeg -y " + common + " -i /src.wav -filter_complex " +
				"[0:a:0]asplit=1[s0];" +
				"[s0]aformat=channel_layouts=mono,loudnorm@mono=I=-16:TP=-4.5:LRA=50:measured_I=-20.7:measured_TP=-6.59:" +
				"measured_LRA=0.1:measured_thresh=-30.7:offset=0.07:linear=true:print_format=json,aresample=48000," +
				"volume=1.3dB:precision=double[o0] " +
				"-map [o0] -map_metadata -1 -c:a aac -b:a 64k -movflags +faststart -f mp4 /data/tmp/part-2",
			want: Rendition{
				Profile: Profiles()[1], Path: "/data/tmp/part-2", Channels: 1, Measured: monoMeasured,
				Predicted: Dynamic, Normalization: Dynamic, Loudness: monoCorrected,
			},
			log: `"profile":"aac_64k","gain_db":1.3,"first_lufs":-17.3,"first_true_peak_dbtp":-6.79,` +
				`"lufs":-15.98,"true_peak_dbtp":-5.47`,
		},
		{
			name: "aac_128k over the target", failing: 0,
			script: func() []fakeResponse {
				s := stereoScript()
				s[3].stderr = report(verifyInstance, stereoLoud, Linear)
				return append(s,
					fakeResponse{stderr: report("loudnorm@stereo", stereoEncoded, Dynamic)},
					fakeResponse{stderr: report(verifyInstance, stereoCorrected, Linear)},
				)
			}(),
			retry: "ffmpeg -y " + common + " -i /src.wav -filter_complex " +
				"[0:a:0]asplit=1[s0];" +
				"[s0]aformat=channel_layouts=stereo,loudnorm@stereo=I=-16:TP=-4.5:LRA=50:measured_I=-17.69:measured_TP=-7.26:" +
				"measured_LRA=0.1:measured_thresh=-27.69:offset=-0.31:linear=true:print_format=json,aresample=48000," +
				"volume=-1.01dB:precision=double[o0] " +
				"-map [o0] -map_metadata -1 -c:a aac -b:a 128k -cutoff 20000 -movflags +faststart -f mp4 /data/tmp/part-1",
			want: Rendition{
				Profile: Profiles()[0], Path: "/data/tmp/part-1", Channels: 2, Measured: stereoMeasured,
				Predicted: Linear, Normalization: Dynamic, Loudness: stereoCorrected,
			},
			log: `"profile":"aac_128k","gain_db":-1.01,"first_lufs":-14.99,"first_true_peak_dbtp":-3,` +
				`"lufs":-16.01,"true_peak_dbtp":-4.02`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run := &fakeRunner{responses: tc.script}
			var logs bytes.Buffer
			ctx := logging.CtxWithLogger(t.Context(), slog.New(slog.NewJSONHandler(&logs, nil)))

			res, err := (&FFmpeg{run: run}).Process(ctx, "/src.wav", -16, outputs())
			if err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			if len(run.calls) != 7 {
				t.Fatalf("Process() ran %d commands, want 7: pass 2 and a verification again for one rendition", len(run.calls))
			}
			if got := run.calls[5].name + " " + strings.Join(run.calls[5].args, " "); got != tc.retry {
				t.Errorf("the corrective re-encode ran\n%s\nwant\n%s", got, tc.retry)
			}
			if first, again := run.calls[3+tc.failing].args, run.calls[6].args; !slices.Equal(again, first) {
				t.Errorf("the second verification ran %q, want %q", again, first)
			}
			if got := res.Renditions[tc.failing]; got != tc.want {
				t.Errorf("rendition %d = %+v\nwant %+v", tc.failing, got, tc.want)
			}
			kept := []Loudness{stereoEncoded, monoEncoded}
			if other := 1 - tc.failing; res.Renditions[other].Loudness != kept[other] {
				t.Errorf("rendition %d verified at %+v, want its first verification %+v",
					other, res.Renditions[other].Loudness, kept[other])
			}
			if !strings.Contains(logs.String(), `"level":"WARN","msg":"re-encoded a rendition with a corrective gain",`+tc.log) {
				t.Errorf("logs = %s, want a warning with %s", &logs, tc.log)
			}
			if n := strings.Count(logs.String(), "corrective gain"); n != 1 {
				t.Errorf("logs = %s, want one corrective re-encode, not %d", &logs, n)
			}
		})
	}
}

// TestProcessChecksExitStatusFirst runs each ffmpeg stage so that it prints complete reports
// and then exits non-zero, as an interrupted ffmpeg does.
func TestProcessChecksExitStatusFirst(t *testing.T) {
	tests := []struct {
		name  string
		stage int
	}{
		{"pass 1", 1},
		{"pass 2", 2},
		{"verification", 3},
		{"corrective re-encode", 5},
		{"verification after the re-encode", 6},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			script := correctiveScript()
			failed := &exitError{name: "ffmpeg", code: 255, status: "exit status 255", stderr: "Exiting normally, received signal 15."}
			script[tc.stage].err = failed
			run := &fakeRunner{responses: script}

			_, err := (&FFmpeg{run: run}).Process(t.Context(), "/src.wav", -16, outputs())
			if !errors.Is(err, failed) {
				t.Errorf("Process() error = %v, want the exit error", err)
			}
			if _, ok := errors.AsType[*Rejection](err); ok {
				t.Errorf("Process() error = %v, want it not to be a rejection", err)
			}
			if len(run.calls) != tc.stage+1 {
				t.Errorf("Process() ran %d commands, want it to stop after the failed one (%d)", len(run.calls), tc.stage+1)
			}
		})
	}
}

func TestProcessReturnsTheContextError(t *testing.T) {
	script := stereoScript()
	script[2].err = context.Canceled
	_, err := (&FFmpeg{run: &fakeRunner{responses: script}}).Process(t.Context(), "/src.wav", -16, outputs())
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Process() error = %v, want context.Canceled", err)
	}
}

func TestProcessRejectsFromReports(t *testing.T) {
	silent := Loudness{LUFS: math.Inf(-1), TruePeakDBTP: math.Inf(-1), ThresholdLUFS: -70}
	quiet := Loudness{LUFS: -19.5, TruePeakDBTP: -1.2, LRALU: 0.4, ThresholdLUFS: -29.5}
	tests := []struct {
		name     string
		script   func([]fakeResponse) []fakeResponse
		want     string
		wantRuns int
	}{
		{
			name: "silent",
			script: func(s []fakeResponse) []fakeResponse {
				s[1].stderr = frames(-120.7) + reportWithOffset("loudnorm@stereo", silent, Dynamic, "inf") +
					reportWithOffset("loudnorm@mono", silent, Dynamic, "inf")
				return s
			},
			want: MessageSilent, wantRuns: 2,
		},
		{
			name: "mono rendition out of spec",
			script: func(s []fakeResponse) []fakeResponse {
				s[4].stderr = report(verifyInstance, quiet, Dynamic)
				return s
			},
			want: "aac_64k rendition out of spec: -19.50 LUFS against a target of -16.0 ± 1 LU, " +
				"true peak -1.20 dBTP against a ceiling of -1.5 dBTP",
			wantRuns: 5,
		},
		{
			name: "retry skipped",
			script: func(s []fakeResponse) []fakeResponse {
				s[4].stderr = report(verifyInstance, Loudness{LUFS: -17.6, TruePeakDBTP: -6}, Dynamic)
				return s
			},
			want: "aac_64k rendition out of spec: -17.60 LUFS against a target of -16.0 ± 1 LU, " +
				"true peak -6.00 dBTP against a ceiling of -1.5 dBTP",
			wantRuns: 5,
		},
		{
			name: "retry fails again",
			script: func([]fakeResponse) []fakeResponse {
				s := correctiveScript()
				s[6].stderr = report(verifyInstance, Loudness{LUFS: -17.1, TruePeakDBTP: -5.6}, Dynamic)
				return s
			},
			want: "aac_64k rendition out of spec: -17.10 LUFS against a target of -16.0 ± 1 LU, " +
				"true peak -5.60 dBTP against a ceiling of -1.5 dBTP",
			wantRuns: 7,
		},
		{
			name: "both renditions out of spec",
			script: func(s []fakeResponse) []fakeResponse {
				s[3].stderr = report(verifyInstance, Loudness{LUFS: -14.99, TruePeakDBTP: -3}, Linear)
				s[4].stderr = report(verifyInstance, Loudness{LUFS: -16, TruePeakDBTP: -1.49}, Linear)
				return append(s,
					fakeResponse{stderr: report("loudnorm@stereo", stereoEncoded, Linear)},
					fakeResponse{stderr: report(verifyInstance, Loudness{LUFS: -17.01, TruePeakDBTP: -4.01}, Linear)},
					fakeResponse{stderr: report("loudnorm@mono", monoEncoded, Dynamic)},
					fakeResponse{stderr: report(verifyInstance, Loudness{LUFS: -16.51, TruePeakDBTP: -1.49}, Linear)},
				)
			},
			want: "aac_128k rendition out of spec: -17.01 LUFS against a target of -16.0 ± 1 LU, " +
				"true peak -4.01 dBTP against a ceiling of -1.5 dBTP; " +
				"aac_64k rendition out of spec: -16.51 LUFS against a target of -16.0 ± 1 LU, " +
				"true peak -1.49 dBTP against a ceiling of -1.5 dBTP",
			wantRuns: 9,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run := &fakeRunner{responses: tc.script(stereoScript())}
			_, err := (&FFmpeg{run: run}).Process(t.Context(), "/data/sources/1/a", -16, outputs())
			rej, ok := errors.AsType[*Rejection](err)
			if !ok {
				t.Fatalf("Process() error = %v, want a rejection", err)
			}
			if rej.Message != tc.want {
				t.Errorf("rejection message = %q\nwant %q", rej.Message, tc.want)
			}
			if strings.Contains(rej.Message, "/data") {
				t.Errorf("rejection message %q contains a path", rej.Message)
			}
			if len(run.calls) != tc.wantRuns {
				t.Errorf("Process() ran %d commands, want %d", len(run.calls), tc.wantRuns)
			}
		})
	}
}

func TestProcessRefusesUnusableOffsets(t *testing.T) {
	for _, offset := range []string{"inf", "-inf", "nan", "99.01", "", "loud"} {
		t.Run(offset, func(t *testing.T) {
			script := stereoScript()
			script[1].stderr = frames(-17.6) + reportWithOffset("loudnorm@stereo", stereoMeasured, Dynamic, offset) +
				report("loudnorm@mono", monoMeasured, Dynamic)
			run := &fakeRunner{responses: script}
			_, err := (&FFmpeg{run: run}).Process(t.Context(), "/src.wav", -16, outputs())
			if err == nil || !strings.Contains(err.Error(), "target_offset") {
				t.Errorf("Process() error = %v, want the offset refused", err)
			}
			if _, ok := errors.AsType[*Rejection](err); ok {
				t.Errorf("Process() error = %v, want it not to be a rejection", err)
			}
			if len(run.calls) != 2 {
				t.Errorf("Process() ran %d commands, want it to stop before pass 2", len(run.calls))
			}
		})
	}
	t.Run("loudnorm's limit", func(t *testing.T) {
		script := stereoScript()
		script[1].stderr = frames(-17.6) + reportWithOffset("loudnorm@stereo", stereoMeasured, Dynamic, "-99.00") +
			report("loudnorm@mono", monoMeasured, Dynamic)
		run := &fakeRunner{responses: script}
		if _, err := (&FFmpeg{run: run}).Process(t.Context(), "/src.wav", -16, outputs()); err != nil {
			t.Fatalf("Process() error = %v", err)
		}
		if render := strings.Join(run.calls[2].args, " "); !strings.Contains(render, ":offset=-99:") {
			t.Errorf("pass 2 %q doesn't pass offset=-99", render)
		}
	})
}

func TestProcessMissingReport(t *testing.T) {
	tests := []struct {
		name  string
		stage int
	}{
		{"pass 1", 1},
		{"pass 2", 2},
		{"verification", 3},
		{"corrective re-encode", 5},
		{"verification after the re-encode", 6},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			script := correctiveScript()
			script[tc.stage].stderr = strings.Replace(script[tc.stage].stderr, "loudnorm@", "Parsed_loudnorm_", 1)
			_, err := (&FFmpeg{run: &fakeRunner{responses: script}}).Process(t.Context(), "/src.wav", -16, outputs())
			if err == nil || !strings.Contains(err.Error(), "no report from loudnorm@") {
				t.Errorf("Process() error = %v, want a missing report", err)
			}
			if _, ok := errors.AsType[*Rejection](err); ok {
				t.Errorf("Process() error = %v, want it not to be a rejection", err)
			}
		})
	}
}

func TestProcessLogsAMispredictedMode(t *testing.T) {
	script := stereoScript()
	script[2].stderr = report("loudnorm@mono", monoEncoded, Dynamic) + report("loudnorm@stereo", stereoEncoded, Dynamic)
	var logs bytes.Buffer
	ctx := logging.CtxWithLogger(t.Context(), slog.New(slog.NewJSONHandler(&logs, nil)))

	res, err := (&FFmpeg{run: &fakeRunner{responses: script}}).Process(ctx, "/src.wav", -16, outputs())
	if err != nil {
		t.Fatalf("Process() error = %v, want a misprediction to be only logged", err)
	}
	if r := res.Renditions[0]; r.Predicted != Linear || r.Normalization != Dynamic {
		t.Errorf("aac_128k predicted %s and rendered %s, want linear and dynamic", r.Predicted, r.Normalization)
	}
	if !strings.Contains(logs.String(), `"level":"WARN","msg":"loudnorm's mode differs from the prediction",`+
		`"layout":"stereo","predicted":"linear","rendered":"dynamic"`) {
		t.Errorf("logs = %s, want a warning for the stereo layout", &logs)
	}
	if strings.Contains(logs.String(), `"layout":"mono"`) {
		t.Errorf("logs = %s, want no warning for the mono layout", &logs)
	}
}

func TestProcessRefusesBadArguments(t *testing.T) {
	tests := []struct {
		name    string
		target  float64
		outputs []Output
	}{
		{"no outputs", -16, nil},
		{"no path", -16, []Output{{Profile: Profiles()[0]}}},
		{"three channels", -16, []Output{{Profile: Profile{Name: "x", BitrateKbps: 64, MaxChannels: 3}, Path: "/x"}}},
		{"no bitrate", -16, []Output{{Profile: Profile{Name: "x", MaxChannels: 1}, Path: "/x"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run := &fakeRunner{}
			_, err := (&FFmpeg{run: run}).Process(t.Context(), "/src.wav", tc.target, tc.outputs)
			if err == nil {
				t.Fatal("Process() error = nil, want one")
			}
			if _, ok := errors.AsType[*Rejection](err); ok {
				t.Errorf("Process() error = %v, want it not to be a rejection", err)
			}
			if len(run.calls) != 0 {
				t.Errorf("Process() ran %d commands, want none", len(run.calls))
			}
		})
	}
}

func TestProcessRejectsOutOfRangeTargets(t *testing.T) {
	tests := []struct {
		name   string
		target float64
	}{
		{"just louder than -16", -15.9},
		{"louder than loudnorm's range", -4.9},
		{"quieter than loudnorm's range", -70.1},
		{"not a number", math.NaN()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run := &fakeRunner{}
			_, err := (&FFmpeg{run: run}).Process(t.Context(), "/src.wav", tc.target, outputs())
			rej, ok := errors.AsType[*Rejection](err)
			if !ok || rej.Message != MessageTargetOutOfRange {
				t.Errorf("Process() error = %v, want a rejection with %q", err, MessageTargetOutOfRange)
			}
			if len(run.calls) != 0 {
				t.Errorf("Process() ran %d commands, want none", len(run.calls))
			}
		})
	}
}

func TestValidateAcceptsLoudnormsLimits(t *testing.T) {
	tests := []struct {
		name   string
		target float64
	}{
		{"lowest target", -70},
		{"highest target", -16},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validate(tc.target, outputs()); err != nil {
				t.Errorf("validate(%v) error = %v, want it accepted", tc.target, err)
			}
		})
	}
}

func TestProbe(t *testing.T) {
	existing := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(existing, []byte("RIFF"), 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing")
	invalid := &exitError{name: "ffprobe", code: 1, status: "exit status 1", stderr: "Invalid data found when processing input"}

	stream := func(s string) fakeResponse {
		return fakeResponse{stdout: `{"streams": [` + s + `], "format": {"duration": "10.0"}}`}
	}
	tests := []struct {
		name     string
		src      string
		response fakeResponse
		want     Source
		// wantRejection is the tenant message of the rejection wanted; wantErr asks for an error that isn't one,
		// matching wantIs if it is set.
		wantRejection string
		wantErr       bool
		wantIs        error
	}{
		{
			name: "audio", src: existing, response: fakeResponse{stdout: probeJSON(2, "10.000000")},
			want: Source{Codec: "pcm_s16le", SampleRate: 48000, Channels: 2, Duration: 10 * time.Second},
		},
		{
			name: "exactly 3 s", src: existing, response: fakeResponse{stdout: probeJSON(1, "3.000000")},
			want: Source{Codec: "pcm_s16le", SampleRate: 48000, Channels: 1, Duration: 3 * time.Second},
		},
		{name: "just under 3 s", src: existing, response: fakeResponse{stdout: probeJSON(1, "2.999979")}, wantRejection: MessageTooShort},
		{name: "unparseable", src: existing, response: fakeResponse{err: invalid}, wantRejection: MessageNotAudio},
		{name: "missing", src: missing, response: fakeResponse{err: invalid}, wantErr: true, wantIs: fs.ErrNotExist},
		{
			name: "killed", src: existing, wantErr: true,
			response: fakeResponse{err: &exitError{name: "ffprobe", code: -1, status: "signal: killed"}},
		},
		{
			name: "other exit status", src: existing, wantErr: true,
			response: fakeResponse{err: &exitError{name: "ffprobe", code: 127, status: "exit status 127"}},
		},
		{name: "cancelled", src: existing, response: fakeResponse{err: context.Canceled}, wantErr: true, wantIs: context.Canceled},
		{name: "output not JSON", src: existing, response: fakeResponse{stdout: "{"}, wantRejection: MessageNotAudio},
		{name: "no audio stream", src: existing, response: stream(""), wantRejection: MessageNotAudio},
		{name: "no codec", src: existing, response: stream(`{"sample_rate": "48000", "channels": 2}`), wantRejection: MessageNotAudio},
		{name: "no sample rate", src: existing, response: stream(`{"codec_name": "mp3", "channels": 2}`), wantRejection: MessageNotAudio},
		{name: "no channels", src: existing, response: fakeResponse{stdout: probeJSON(0, "10.0")}, wantRejection: MessageNotAudio},
		{name: "no duration", src: existing, response: fakeResponse{stdout: probeJSON(2, "N/A")}, wantRejection: MessageNotAudio},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := (&FFmpeg{run: &fakeRunner{responses: []fakeResponse{tc.response}}}).probe(t.Context(), tc.src)
			rej, rejected := errors.AsType[*Rejection](err)
			switch {
			case tc.wantRejection != "":
				if !rejected || rej.Message != tc.wantRejection {
					t.Errorf("probe() error = %v, want the rejection %q", err, tc.wantRejection)
				}
			case tc.wantErr:
				if err == nil || rejected {
					t.Errorf("probe() error = %v, want an error that isn't a rejection", err)
				}
				if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
					t.Errorf("probe() error = %v, want %v", err, tc.wantIs)
				}
			default:
				if err != nil || got != tc.want {
					t.Errorf("probe() = %+v, %v, want %+v", got, err, tc.want)
				}
			}
		})
	}
}

func TestVersionLines(t *testing.T) {
	ffmpegOut := "ffmpeg version 6.1.1-3ubuntu5 Copyright (c) 2000-2023 the FFmpeg developers\nbuilt with gcc 13\n"
	ffprobeOut := "ffprobe version 6.1.1-3ubuntu5 Copyright (c) 2007-2023 the FFmpeg developers\n"
	notFound := &exec.Error{Name: "ffprobe", Err: exec.ErrNotFound}
	tests := []struct {
		name      string
		responses []fakeResponse
		wantErr   string
	}{
		{name: "both", responses: []fakeResponse{{stdout: ffmpegOut}, {stdout: ffprobeOut}}},
		{name: "ffmpeg missing", responses: []fakeResponse{{err: &exec.Error{Name: "ffmpeg", Err: exec.ErrNotFound}}}, wantErr: "ffmpeg is not installed"},
		{name: "ffprobe missing", responses: []fakeResponse{{stdout: ffmpegOut}, {err: notFound}}, wantErr: "ffprobe is not installed"},
		{name: "ffmpeg broken", responses: []fakeResponse{{err: &exitError{name: "ffmpeg", code: 127, status: "exit status 127"}}}, wantErr: "error running ffmpeg -version"},
		{name: "something else", responses: []fakeResponse{{stdout: "avconv version 12\n"}}, wantErr: `unexpected output "avconv version 12"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ffmpeg, ffprobe, err := (&FFmpeg{run: &fakeRunner{responses: tc.responses}}).Version(t.Context())
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("Version() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || ffmpeg != strings.Split(ffmpegOut, "\n")[0] || ffprobe != strings.TrimSuffix(ffprobeOut, "\n") {
				t.Errorf("Version() = %q, %q, %v, want both first lines", ffmpeg, ffprobe, err)
			}
		})
	}
}

func TestPredict(t *testing.T) {
	tests := []struct {
		name string
		m    Loudness
		want Normalization
	}{
		{"headroom to spare", Loudness{LUFS: -20, TruePeakDBTP: -10, LRALU: 5}, Linear},
		{"peak lands exactly on TP", Loudness{LUFS: -20, TruePeakDBTP: -8.5, LRALU: 5}, Linear},
		{"peak lands just above TP", Loudness{LUFS: -20, TruePeakDBTP: -8.49, LRALU: 5}, Dynamic},
		{"under the ceiling but above TP", Loudness{LUFS: -20, TruePeakDBTP: -6, LRALU: 5}, Dynamic},
		{"cut, not boosted", Loudness{LUFS: -10, TruePeakDBTP: 1.5, LRALU: 5}, Linear},
		{"LRA of 0", Loudness{LUFS: -20, TruePeakDBTP: -10, LRALU: 0}, Linear},
		{"LRA at loudnorm's maximum", Loudness{LUFS: -20, TruePeakDBTP: -10, LRALU: 50}, Linear},
		{"LRA above it", Loudness{LUFS: -20, TruePeakDBTP: -10, LRALU: 50.01}, Dynamic},
		{"thunder", Loudness{LUFS: -38.85, TruePeakDBTP: -1.26, LRALU: 0.1}, Dynamic},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := predict(tc.m, -16); got != tc.want {
				t.Errorf("predict(%+v) = %s, want %s", tc.m, got, tc.want)
			}
		})
	}
}

func TestMeasuredLRA(t *testing.T) {
	tests := []struct {
		name string
		lra  float64
		want float64
	}{
		{"0 is passed as 0.01", 0, 0.01},
		{"0.01 is kept", 0.01, 0.01},
		{"anything larger is kept", 0.1, 0.1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := measuredLRA(Loudness{LRALU: tc.lra}); got != tc.want {
				t.Errorf("measuredLRA(%v) = %v, want %v", tc.lra, got, tc.want)
			}
		})
	}
}

func TestWarnings(t *testing.T) {
	linear := []Rendition{{Normalization: Linear}, {Normalization: Linear}}
	tests := []struct {
		name       string
		in         Input
		renditions []Rendition
		want       []Warning
	}{
		{"none", Input{LUFS: -20, TruePeakDBTP: -3, MaxMomentaryLUFS: -15}, linear, nil},
		{"momentary exactly 10 LU above", Input{LUFS: -38.7, TruePeakDBTP: -3, MaxMomentaryLUFS: -28.7}, linear, nil},
		{"momentary just over 10 LU above", Input{LUFS: -38.7, TruePeakDBTP: -3, MaxMomentaryLUFS: -28.6}, linear,
			[]Warning{WarningSuddenLoudEvent}},
		{"one dynamic rendition", Input{LUFS: -20, TruePeakDBTP: -3, MaxMomentaryLUFS: -15},
			[]Rendition{{Normalization: Linear}, {Normalization: Dynamic}}, []Warning{WarningDynamicNormalization}},
		{"peak exactly -0.5", Input{LUFS: -20, TruePeakDBTP: -0.5, MaxMomentaryLUFS: -15}, linear, nil},
		{"peak above -0.5", Input{LUFS: -20, TruePeakDBTP: -0.49, MaxMomentaryLUFS: -15}, linear,
			[]Warning{WarningSourceMayClip}},
		{"all", Input{LUFS: -38.7, TruePeakDBTP: 0.2, MaxMomentaryLUFS: -24.3},
			[]Rendition{{Normalization: Dynamic}, {Normalization: Dynamic}},
			[]Warning{WarningSuddenLoudEvent, WarningDynamicNormalization, WarningSourceMayClip}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := warnings(tc.in, tc.renditions); !slices.Equal(got, tc.want) {
				t.Errorf("warnings() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name   string
		target float64
		m      Loudness
		ok     bool
	}{
		{"on target, peak on the ceiling", -16, Loudness{LUFS: -16, TruePeakDBTP: -1.5}, true},
		{"1 LU under", -16, Loudness{LUFS: -17, TruePeakDBTP: -3}, true},
		{"1 LU over", -16, Loudness{LUFS: -15, TruePeakDBTP: -3}, true},
		{"1 LU under a fractional target", -16.3, Loudness{LUFS: -17.3, TruePeakDBTP: -3}, true},
		{"1 LU over a fractional target", -16.3, Loudness{LUFS: -15.3, TruePeakDBTP: -3}, true},
		{"just over 1 LU under", -16, Loudness{LUFS: -17.01, TruePeakDBTP: -3}, false},
		{"just over 1 LU over", -16, Loudness{LUFS: -14.99, TruePeakDBTP: -3}, false},
		{"peak just above the ceiling", -16, Loudness{LUFS: -16, TruePeakDBTP: -1.49}, false},
		{"silent", -16, Loudness{LUFS: math.Inf(-1), TruePeakDBTP: math.Inf(-1)}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if msg := check("aac_64k", tc.m, tc.target); (msg == "") != tc.ok {
				t.Errorf("check(%+v, %v) = %q, want it to pass: %t", tc.m, tc.target, msg, tc.ok)
			}
		})
	}
}

func TestCorrection(t *testing.T) {
	tests := []struct {
		name   string
		target float64
		m      Loudness
		want   float64
		ok     bool
	}{
		{"short of the target", -16, Loudness{LUFS: -17.3, TruePeakDBTP: -6.79}, 1.3, true},
		{"over the target", -16, Loudness{LUFS: -14.99, TruePeakDBTP: -3}, -1.01, true},
		{"peak over the ceiling only", -16, Loudness{LUFS: -16, TruePeakDBTP: -1.49}, -0.51, true},
		{"the peak lowers the gain", -16, Loudness{LUFS: -17.3, TruePeakDBTP: -3}, 1, true},
		{"peak lands exactly 0.5 dB under the ceiling", -16, Loudness{LUFS: -17.3, TruePeakDBTP: -3.3}, 1.3, true},
		{"gain of exactly 1.5 dB", -16, Loudness{LUFS: -17.5, TruePeakDBTP: -6}, 1.5, true},
		{"cut of exactly 1.5 dB", -16, Loudness{LUFS: -14.5, TruePeakDBTP: -4}, -1.5, true},
		{"1.5 dB to a fractional target", -16.3, Loudness{LUFS: -17.8, TruePeakDBTP: -6}, 1.5, true},
		{"gain over 1.5 dB", -16, Loudness{LUFS: -17.51, TruePeakDBTP: -6}, 0, false},
		{"cut over 1.5 dB", -16, Loudness{LUFS: -14.49, TruePeakDBTP: -6}, 0, false},
		{"lowered to exactly 0.8 LU short", -16, Loudness{LUFS: -17.5, TruePeakDBTP: -2.7}, 0.7, true},
		{"lowered to more than 0.8 LU short", -16, Loudness{LUFS: -17.5, TruePeakDBTP: -2.69}, 0, false},
		{"peak cut to exactly 0.8 LU short", -16, Loudness{LUFS: -16, TruePeakDBTP: -1.2}, -0.8, true},
		{"peak cut to more than 0.8 LU short", -16, Loudness{LUFS: -16, TruePeakDBTP: -1.19}, 0, false},
		{"no gain", -16, Loudness{LUFS: -16, TruePeakDBTP: -3}, 0, false},
		{"the ceiling leaves no room", -16, Loudness{LUFS: -17.2, TruePeakDBTP: -2}, 0, false},
		{"blue noise", -16, Loudness{LUFS: -22, TruePeakDBTP: -8}, 0, false},
		{"silent", -16, Loudness{LUFS: math.Inf(-1), TruePeakDBTP: math.Inf(-1)}, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got, ok := correction(tc.m, tc.target); got != tc.want || ok != tc.ok {
				t.Errorf("correction(%+v, %v) = %v, %t, want %v, %t", tc.m, tc.target, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestRejection(t *testing.T) {
	inner := errors.New("ffprobe failed: /data/sources/1/a: Invalid data")
	err := fmt.Errorf("error probing: %w", reject(MessageNotAudio, inner))
	rej, ok := errors.AsType[*Rejection](err)
	if !ok || rej.Message != MessageNotAudio {
		t.Fatalf("errors.AsType() = %v, %t, want the rejection", rej, ok)
	}
	if !errors.Is(err, inner) || rej.Error() != "not audio: ffprobe failed: /data/sources/1/a: Invalid data" {
		t.Errorf("rejection error = %q, want it to wrap the internal error", rej.Error())
	}
	if got := reject(MessageTooShort, nil).Error(); got != MessageTooShort {
		t.Errorf("rejection without detail = %q, want %q", got, MessageTooShort)
	}
}

func TestPlanLayouts(t *testing.T) {
	stereo := Profile{Name: "s", BitrateKbps: 128, MaxChannels: 2}
	mono := Profile{Name: "m", BitrateKbps: 64, MaxChannels: 1}
	tests := []struct {
		name     string
		channels int
		profiles []Profile
		want     []layout
	}{
		{"stereo source", 2, []Profile{stereo, mono}, []layout{{"stereo", 2, []int{0}}, {"mono", 1, []int{1}}}},
		{"mono source", 1, []Profile{stereo, mono}, []layout{{"mono", 1, []int{0, 1}}}},
		{"5.1 source, repeated layout", 6, []Profile{mono, stereo, mono},
			[]layout{{"mono", 1, []int{0, 2}}, {"stereo", 2, []int{1}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var outs []Output
			for _, p := range tc.profiles {
				outs = append(outs, Output{Profile: p, Path: "/x"})
			}
			got := planLayouts(tc.channels, outs)
			if !slices.EqualFunc(got, tc.want, func(a, b layout) bool {
				return a.name == b.name && a.channels == b.channels && slices.Equal(a.outputs, b.outputs)
			}) {
				t.Errorf("planLayouts(%d) = %+v, want %+v", tc.channels, got, tc.want)
			}
		})
	}
}

func TestNum(t *testing.T) {
	tests := []struct {
		name string
		v    float64
		want string
	}{
		{"whole target", -16, "-16"},
		{"two decimals", -17.69, "-17.69"},
		{"one decimal", -0.1, "-0.1"},
		{"the smallest LRA", 0.01, "0.01"},
		{"positive", 50, "50"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := num(tc.v); got != tc.want {
				t.Errorf("num(%v) = %q, want %q", tc.v, got, tc.want)
			}
		})
	}
}
