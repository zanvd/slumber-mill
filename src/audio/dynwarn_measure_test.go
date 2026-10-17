//go:build measure

// Measurement harness for the dynamic_normalization warning: scaffolding, not for merge.
//
// TestMeasureDynamicWarning runs Process, the production pipeline, on each source at each target,
// and adds two ebur128 runs Process doesn't make: one over each layout's downmix of the source and one over
// each rendition, for their max momentary loudness. Each rendition is one JSON line in <out>/<batch>.jsonl.
// TestDynamicWarningReport reads every <out>/*.jsonl and prints the tables and the threshold analysis.
// In the dev service:
//
//	go test -tags measure -run TestMeasureDynamicWarning -count 1 -timeout 0 -parallel 6 -v ./audio/ -args \
//	  -dw-batch b1 -dw-sources pink-unc,thunder -dw-durations 10m -dw-targets -16,-23
//	go test -tags measure -run TestDynamicWarningReport -count 1 -v ./audio/
package audio

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/testutil"
)

var (
	dwBatch     = flag.String("dw-batch", "", "results go to <out>/<batch>.jsonl")
	dwOut       = flag.String("dw-out", "/out/dynwarn", "directory for results and the sources/ cache")
	dwSources   = flag.String("dw-sources", "", "comma-separated source kinds; see dwKinds")
	dwDurations = flag.String("dw-durations", "10m", "comma-separated durations; thunder ignores them")
	dwTargets   = flag.String("dw-targets", "-16,-23", "comma-separated targets in LUFS")
)

// dwEvents are the kinds the warning exists for; every other kind is steady content.
var dwEvents = map[string]bool{"thunder": true, "thunder-periodic": true, "bed-events": true}

type dwKind struct {
	inputs []string
	filter string
	extra  []string
	note   string
}

func dwNoise(color string, amplitude float64, seed int, d time.Duration) string {
	return fmt.Sprintf("anoisesrc=color=%s:amplitude=%s:seed=%d:sample_rate=48000:duration=%s",
		color, num(amplitude), seed, num(d.Seconds()))
}

// dwKinds builds each source kind at length d. The noise kinds use testutil's colours, amplitudes and seeds.
func dwKinds(d time.Duration) map[string]dwKind {
	secs := num(d.Seconds())
	minutes := int(d / time.Minute)
	var surround []string
	for seed := 31; seed <= 36; seed++ {
		surround = append(surround, dwNoise("pink", 0.3, seed, d))
	}
	// Ten 50 ms cracks, one a minute from 30 s, in the Thunder fixture's rain.
	const crackPeriod = 60 * 48000
	// Five 3 s rumbles of low-passed brown noise at irregular times in a quiet bed of low-passed pink noise.
	rumbles := []int{70, 190, 305, 420, 540}
	var rumbleGraph strings.Builder
	rumbleGraph.WriteString("[0:a][1:a]amerge=inputs=2,lowpass=f=6000[bed];" +
		"[2:a]lowpass=f=400,afade=t=in:d=0.15,afade=t=out:st=0.4:d=2.6:curve=exp,aformat=channel_layouts=stereo,")
	fmt.Fprintf(&rumbleGraph, "asplit=%d", len(rumbles))
	for i := range rumbles {
		fmt.Fprintf(&rumbleGraph, "[r%d]", i)
	}
	for i, s := range rumbles {
		fmt.Fprintf(&rumbleGraph, ";[r%d]adelay=%d:all=1[d%d]", i, s*1000, i)
	}
	rumbleGraph.WriteString(";[bed]")
	for i := range rumbles {
		fmt.Fprintf(&rumbleGraph, "[d%d]", i)
	}
	fmt.Fprintf(&rumbleGraph, "amix=inputs=%d:duration=first:normalize=0", len(rumbles)+1)
	// Syllables at 4.3 a second, phrases of 2.9 s with 1.2 s gaps, a longer gap every 13.7 s,
	// a phrase level swinging 0.4 to 1.0 over 29 s, and a room-tone floor.
	envelope := "0.002+(0.7+0.3*sin(2*PI*t/29))*pow(sin(PI*4.3*t),2)*lt(mod(t,4.1),2.9)*lt(mod(t,13.7),11.5)"

	return map[string]dwKind{
		"pink-unc": {
			inputs: []string{dwNoise("pink", 0.5, 1, d), dwNoise("pink", 0.5, 2, d)},
			filter: "[0:a][1:a]amerge=inputs=2", note: "UncorrelatedPink",
		},
		"pink-dm":   {inputs: []string{dwNoise("pink", 0.5, 1, d)}, extra: []string{"-ac", "2"}, note: "DualMonoPink"},
		"pink-mono": {inputs: []string{dwNoise("pink", 0.5, 1, d)}, note: "MonoPink"},
		"white-unc": {
			inputs: []string{dwNoise("white", 0.25, 3, d), dwNoise("white", 0.25, 4, d)},
			filter: "[0:a][1:a]amerge=inputs=2", note: "UncorrelatedWhite",
		},
		"bright-white": {
			inputs: []string{dwNoise("white", 0.25, 3, d), dwNoise("white", 0.25, 4, d)},
			filter: "[0:a][1:a]amerge=inputs=2,highshelf=g=3:f=12000", note: "BrightWhite",
		},
		"surround": {
			inputs: surround, filter: "[0:a][1:a][2:a][3:a][4:a][5:a]join=inputs=6:channel_layout=5.1",
			note: "Surround",
		},
		"speech": {
			inputs: []string{
				dwNoise("pink", 0.5, 41, d),
				fmt.Sprintf("aevalsrc=exprs='%s':sample_rate=48000:duration=%s", envelope, secs),
			},
			filter: "[0:a]highpass=f=100,lowpass=f=5000[n];[n][1:a]amultiply",
			extra:  []string{"-ac", "2"},
			note:   "dual-mono band-limited pink noise under a syllable, phrase and pause envelope",
		},
		"thunder-periodic": {
			inputs: []string{
				dwNoise("pink", 0.04, 21, d), dwNoise("pink", 0.04, 22, d),
				dwNoise("white", 0.95, 23, 50*time.Millisecond),
			},
			filter: fmt.Sprintf("[0:a][1:a]amerge=inputs=2[rain];"+
				"[2:a]afade=t=out:d=0.05:curve=exp,apad=whole_len=%d,aloop=loop=%d:size=%d,"+
				"aformat=channel_layouts=stereo,adelay=30000:all=1[crack];"+
				"[rain][crack]amix=inputs=2:duration=first:normalize=0",
				crackPeriod, max(minutes-1, 0), crackPeriod),
			note: "the Thunder fixture's rain and crack, with a crack every minute from 30 s",
		},
		"bed-events": {
			inputs: []string{
				dwNoise("pink", 0.03, 51, d), dwNoise("pink", 0.03, 52, d),
				dwNoise("brown", 0.9, 53, 3*time.Second),
			},
			filter: rumbleGraph.String(),
			note:   "a quiet stereo bed with 3 s rumbles at 70, 190, 305, 420 and 540 s",
		},
	}
}

type dwRow struct {
	Batch    string  `json:"batch"`
	FFmpeg   string  `json:"ffmpeg"`
	Source   string  `json:"source"`
	Event    bool    `json:"event"`
	Duration string  `json:"duration"`
	Channels int     `json:"channels"`
	Target   float64 `json:"target"`

	InputLUFS         float64  `json:"input_lufs"`
	InputTP           float64  `json:"input_tp"`
	InputLRA          float64  `json:"input_lra"`
	InputMaxMomentary float64  `json:"input_max_momentary"`
	SuddenLU          float64  `json:"sudden_lu"`
	Warnings          []string `json:"warnings"`
	Rejected          string   `json:"rejected,omitempty"`

	Profile   string  `json:"profile"`
	Layout    string  `json:"layout"`
	P1LUFS    float64 `json:"p1_lufs"`
	P1TP      float64 `json:"p1_tp"`
	P1LRA     float64 `json:"p1_lra"`
	P1MaxMom  float64 `json:"p1_max_momentary"`
	Predicted string  `json:"predicted"`
	Rendered  string  `json:"rendered"`

	OutLUFS   float64 `json:"out_lufs"`
	OutTP     float64 `json:"out_tp"`
	OutLRA    float64 `json:"out_lra"`
	OutMaxMom float64 `json:"out_max_momentary"`

	// MetricA is pass 1's LRA less the verified LRA, MetricB the limiter excess over the pre-encode TP,
	// and MetricC how much closer the loudest momentary sits to integrated after rendering than before.
	MetricA float64 `json:"metric_a"`
	MetricB float64 `json:"metric_b"`
	MetricC float64 `json:"metric_c"`

	Corrected bool    `json:"corrected"`
	GainDB    float64 `json:"gain_db,omitempty"`
	FirstLUFS float64 `json:"first_lufs,omitempty"`
	FirstTP   float64 `json:"first_tp,omitempty"`
	Seconds   float64 `json:"seconds"`
}

func TestMeasureDynamicWarning(t *testing.T) {
	if *dwBatch == "" || *dwSources == "" {
		t.Fatal("-dw-batch and -dw-sources are required")
	}
	targets, err := dwFloats(*dwTargets)
	if err != nil {
		t.Fatalf("-dw-targets: %v", err)
	}
	var durations []time.Duration
	for _, s := range strings.Split(*dwDurations, ",") {
		d, err := time.ParseDuration(s)
		if err != nil {
			t.Fatalf("-dw-durations: %v", err)
		}
		durations = append(durations, d)
	}
	version, _, err := New().Version(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s", version)
	cache := filepath.Join(*dwOut, "sources")
	if err := os.MkdirAll(cache, 0o755); err != nil {
		t.Fatal(err)
	}
	results := filepath.Join(*dwOut, *dwBatch+".jsonl")
	f, err := os.OpenFile(results, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var rows []dwRow
	emit := func(t *testing.T, r dwRow) {
		line, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if _, err := f.Write(append(line, '\n')); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
		t.Log(dwLine(r))
	}
	t.Cleanup(func() {
		f.Close()
		t.Logf("batch %s, %s:\n%s", *dwBatch, version, dwTable(rows))
	})

	for _, kind := range strings.Split(*dwSources, ",") {
		ds := durations
		if kind == "thunder" {
			ds = []time.Duration{0}
		}
		for _, d := range ds {
			label := d.String()
			if d == 0 {
				label = "60s"
			}
			t.Run(kind+"/"+label, func(t *testing.T) {
				t.Parallel()
				src := dwSource(t, cache, kind, d)
				for _, target := range targets {
					t.Run(num(target), func(t *testing.T) {
						t.Parallel()
						base := dwRow{
							Batch: *dwBatch, FFmpeg: version, Source: kind, Event: dwEvents[kind],
							Duration: label, Target: target,
						}
						dwRun(t, src, base, emit)
					})
				}
			})
		}
	}
}

// dwSource returns the path of the source, rendering it into cache unless it is there already.
func dwSource(t *testing.T, cache, kind string, d time.Duration) string {
	if kind == "thunder" {
		return testutil.Thunder(t)
	}
	if kind == "narration" {
		return dwNarration(t, cache, d)
	}
	k, ok := dwKinds(d)[kind]
	if !ok {
		t.Fatalf("unknown source kind %q", kind)
	}
	path := filepath.Join(cache, fmt.Sprintf("%s-%s.wav", kind, d))
	if info, err := os.Stat(path); err == nil && info.Size() > 0 {
		return path
	}
	args := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-y"}
	for _, in := range k.inputs {
		args = append(args, "-f", "lavfi", "-i", in)
	}
	if k.filter != "" {
		args = append(args, "-filter_complex", k.filter)
	}
	args = append(args, k.extra...)
	args = append(args, "-t", num(d.Seconds()), "-c:a", "pcm_s16le", "-f", "wav", path+".part")
	start := time.Now()
	if err := (execRunner{}).Run(t.Context(), "ffmpeg", args, nil, nil); err != nil {
		t.Fatalf("generating %s: %v", path, err)
	}
	if err := os.Rename(path+".part", path); err != nil {
		t.Fatal(err)
	}
	t.Logf("generated %s (%s) in %.0f s: ffmpeg %s", path, k.note, time.Since(start).Seconds(), strings.Join(args, " "))
	return path
}

const dwNarrationText = "The evening settles slowly over the valley, and the last light fades from the tops of the pines. " +
	"Down by the river, the water moves without hurry, carrying small leaves past the stones. " +
	"A lantern glows in the window of the old mill, where the wheel turns once, and then again, " +
	"as if it, too, were growing sleepy. Breathe in, and let the air fill you gently. " +
	"Breathe out, and let the day go. There is nothing left to do tonight. " +
	"The path is quiet, the birds are tucked into their branches, " +
	"and the stars are coming out one by one, small and patient, keeping watch while you rest.\n"

// dwNarration is flite's slt voice reading a fixed paragraph, resampled to 48 kHz, dual-mono and looped to d,
// as feat/audio-loudness-fixes's harness built it.
func dwNarration(t *testing.T, cache string, d time.Duration) string {
	path := filepath.Join(cache, fmt.Sprintf("narration-%s.wav", d))
	if info, err := os.Stat(path); err == nil && info.Size() > 0 {
		return path
	}
	text := filepath.Join(cache, "narration.txt")
	if err := os.WriteFile(text, []byte(dwNarrationText), 0o644); err != nil {
		t.Fatal(err)
	}
	speech := filepath.Join(t.TempDir(), "speech.wav")
	quiet := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-y"}
	for _, args := range [][]string{
		{"-f", "lavfi", "-i", "flite=textfile=" + text + ":voice=slt", "-af", "aresample=48000", "-ac", "2",
			"-c:a", "pcm_s16le", "-f", "wav", speech},
		{"-stream_loop", "-1", "-i", speech, "-t", num(d.Seconds()), "-c:a", "pcm_s16le", "-f", "wav", path + ".part"},
	} {
		if err := (execRunner{}).Run(t.Context(), "ffmpeg", append(slices.Clone(quiet), args...), nil, nil); err != nil {
			t.Fatalf("generating %s: %v", path, err)
		}
	}
	if err := os.Rename(path+".part", path); err != nil {
		t.Fatal(err)
	}
	return path
}

// dwRun runs Process's steps as Process does, but keeps the renditions of an asset Process would reject
// and records why it would.
func dwRun(t *testing.T, src string, base dwRow, emit func(*testing.T, dwRow)) {
	ctx := logging.CtxWithLogger(t.Context(), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	dir := t.TempDir()
	var outputs []Output
	for _, p := range Profiles() {
		outputs = append(outputs, Output{Profile: p, Path: filepath.Join(dir, p.Name+".m4a")})
	}
	start := time.Now()
	f := New()
	if err := validate(base.Target, outputs); err != nil {
		t.Fatal(err)
	}
	source, err := f.probe(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	layouts := planLayouts(source.Channels, outputs)
	input, measured, offsets, err := f.measure(ctx, src, source, base.Target, layouts)
	if err != nil {
		t.Fatal(err)
	}
	predicted := make([]Normalization, len(layouts))
	for i, m := range measured {
		predicted[i] = predict(m, base.Target)
	}
	rendered, err := f.render(ctx, src, base.Target, layouts, measured, offsets, 0, outputs)
	if err != nil {
		t.Fatal(err)
	}
	renditions := make([]Rendition, len(outputs))
	layoutOf := make([]int, len(outputs))
	for i, l := range layouts {
		for _, o := range l.outputs {
			renditions[o] = Rendition{
				Profile: outputs[o].Profile, Path: outputs[o].Path, Channels: l.channels,
				Measured: measured[i], Predicted: predicted[i], Normalization: rendered[i],
			}
			layoutOf[o] = i
		}
	}
	for i := range renditions {
		if renditions[i].Loudness, err = f.verify(ctx, renditions[i].Path, base.Target); err != nil {
			t.Fatal(err)
		}
	}
	first := make([]Loudness, len(renditions))
	gains := make([]float64, len(renditions))
	corrected := make([]bool, len(renditions))
	var failures []string
	for i := range renditions {
		r := &renditions[i]
		first[i] = r.Loudness
		if check(r.Profile.Name, r.Loudness, base.Target) != "" {
			gains[i], corrected[i] = correction(r.Loudness, base.Target)
			if err := f.correct(ctx, src, base.Target, layouts[layoutOf[i]], offsets[layoutOf[i]], r); err != nil {
				t.Fatal(err)
			}
		}
		if msg := check(r.Profile.Name, r.Loudness, base.Target); msg != "" {
			failures = append(failures, msg)
		}
	}
	secs := time.Since(start).Seconds()

	var warns []string
	for _, w := range warnings(input, renditions) {
		warns = append(warns, string(w))
	}
	downmixMax := map[int]float64{}
	for _, r := range renditions {
		if _, ok := downmixMax[r.Channels]; ok {
			continue
		}
		m, err := dwMaxMomentary(ctx, src, "aformat=channel_layouts="+dwLayout(r.Channels)+",")
		if err != nil {
			t.Fatal(err)
		}
		downmixMax[r.Channels] = m
	}
	for i, r := range renditions {
		outMax, err := dwMaxMomentary(ctx, r.Path, "")
		if err != nil {
			t.Fatal(err)
		}
		row := base
		row.Channels = source.Channels
		row.InputLUFS, row.InputTP, row.InputLRA = input.LUFS, input.TruePeakDBTP, input.LRALU
		row.InputMaxMomentary = input.MaxMomentaryLUFS
		row.SuddenLU = input.MaxMomentaryLUFS - input.LUFS
		row.Warnings = warns
		row.Rejected = strings.Join(failures, "; ")
		row.Profile, row.Layout = r.Profile.Name, dwLayout(r.Channels)
		row.P1LUFS, row.P1TP, row.P1LRA = r.Measured.LUFS, r.Measured.TruePeakDBTP, r.Measured.LRALU
		row.P1MaxMom = downmixMax[r.Channels]
		row.Predicted, row.Rendered = string(r.Predicted), string(r.Normalization)
		row.OutLUFS, row.OutTP, row.OutLRA = r.Loudness.LUFS, r.Loudness.TruePeakDBTP, r.Loudness.LRALU
		row.OutMaxMom = outMax
		row.MetricA = r.Measured.LRALU - r.Loudness.LRALU
		row.MetricB = r.Measured.TruePeakDBTP + (base.Target - r.Measured.LUFS) - preEncodeTruePeakDBTP
		row.MetricC = (row.P1MaxMom - row.P1LUFS) - (row.OutMaxMom - row.OutLUFS)
		row.Corrected, row.GainDB = corrected[i], gains[i]
		row.FirstLUFS, row.FirstTP = first[i].LUFS, first[i].TruePeakDBTP
		row.Seconds = secs
		emit(t, row)
	}
}

func dwLayout(channels int) string {
	if channels == 1 {
		return "mono"
	}
	return "stereo"
}

// dwMaxMomentary runs ebur128 over path after the filters, which end in a comma if there are any.
func dwMaxMomentary(ctx context.Context, path, filters string) (float64, error) {
	const instance = "ebur128@dw"
	args := append(ffmpegArgs(path), "-filter_complex", "[0:a:0]"+filters+instance+"=framelog=info[e]",
		"-map", "[e]", "-f", "null", "-")
	log := newFFmpegLog(instance)
	err := (execRunner{}).Run(ctx, "ffmpeg", args, nil, log)
	log.flush()
	if err != nil {
		return 0, fmt.Errorf("ebur128 over %s: %w", path, err)
	}
	return log.maxMomentary()
}

func dwFloats(s string) ([]float64, error) {
	var out []float64
	for _, f := range strings.Split(s, ",") {
		v, err := strconv.ParseFloat(strings.TrimSpace(f), 64)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func dwLine(r dwRow) string {
	corr := "-"
	if r.Corrected {
		corr = fmt.Sprintf("%+.2f", r.GainDB)
	}
	if r.Rejected != "" {
		corr += " REJ"
	}
	return fmt.Sprintf("%-16s %5s %4s %-8s %-3s/%-3s B %+6.2f | LRA %5.2f>%5.2f A %+6.2f | C %+6.2f | "+
		"%6.2f LUFS %6.2f dBTP corr %5s | sudden %5.2f %s",
		r.Source, r.Duration, num(r.Target), r.Profile, r.Predicted[:3], r.Rendered[:3], r.MetricB,
		r.P1LRA, r.OutLRA, r.MetricA, r.MetricC, r.OutLUFS, r.OutTP, corr, r.SuddenLU, strings.Join(r.Warnings, ","))
}

func dwSortRows(rows []dwRow) {
	order := []string{"pink-unc", "pink-dm", "pink-mono", "white-unc", "bright-white", "surround", "speech", "narration",
		"thunder", "thunder-periodic", "bed-events"}
	slices.SortStableFunc(rows, func(a, b dwRow) int {
		return cmp.Or(
			cmp.Compare(slices.Index(order, a.Source), slices.Index(order, b.Source)),
			cmp.Compare(dwDurationOrder(a.Duration), dwDurationOrder(b.Duration)),
			cmp.Compare(b.Target, a.Target),
			cmp.Compare(a.Profile, b.Profile),
		)
	})
}

func dwDurationOrder(s string) time.Duration {
	d, _ := time.ParseDuration(s)
	return d
}

func dwTable(rows []dwRow) string {
	rows = slices.Clone(rows)
	dwSortRows(rows)
	var b strings.Builder
	for _, r := range rows {
		b.WriteString(dwLine(r) + "\n")
	}
	return b.String()
}

type dwAsset struct {
	source, duration          string
	target                    float64
	event                     bool
	a, rise, b, c             float64
	dynamic, sudden, rejected bool
	suddenLU                  float64
}

func TestDynamicWarningReport(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(*dwOut, "*.jsonl"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no results in %s: %v", *dwOut, err)
	}
	latest := map[string]dwRow{}
	var keys []string
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		s := bufio.NewScanner(f)
		s.Buffer(nil, 1<<20)
		for s.Scan() {
			var r dwRow
			if err := json.Unmarshal(s.Bytes(), &r); err != nil {
				t.Fatalf("%s: %v", p, err)
			}
			k := fmt.Sprintf("%s|%s|%v|%s", r.Source, r.Duration, r.Target, r.Profile)
			if _, ok := latest[k]; !ok {
				keys = append(keys, k)
			}
			latest[k] = r
		}
		f.Close()
	}
	var rows []dwRow
	for _, k := range keys {
		rows = append(rows, latest[k])
	}
	if len(rows) > 0 {
		t.Logf("%s", rows[0].FFmpeg)
	}
	t.Logf("renditions:\n%s", dwTable(rows))

	assets := map[string]*dwAsset{}
	var order []string
	for _, r := range rows {
		k := fmt.Sprintf("%s|%s|%v", r.Source, r.Duration, r.Target)
		a, ok := assets[k]
		if !ok {
			a = &dwAsset{
				source: r.Source, duration: r.Duration, target: r.Target, event: r.Event,
				a: math.Inf(-1), rise: math.Inf(-1), b: math.Inf(-1), c: math.Inf(-1),
				sudden: slices.Contains(r.Warnings, string(WarningSuddenLoudEvent)), suddenLU: r.SuddenLU,
			}
			assets[k] = a
			order = append(order, k)
		}
		a.a, a.b, a.c = max(a.a, r.MetricA), max(a.b, r.MetricB), max(a.c, r.MetricC)
		a.rise = max(a.rise, -r.MetricA)
		a.dynamic = a.dynamic || r.Rendered == string(Dynamic)
		a.rejected = a.rejected || r.Rejected != ""
	}
	var b strings.Builder
	b.WriteString("assets (the largest metric over the two renditions):\n")
	list := make([]*dwAsset, 0, len(order))
	for _, k := range order {
		list = append(list, assets[k])
	}
	for _, a := range list {
		fmt.Fprintf(&b, "%-16s %5s %4s event=%-5v dynamic=%-5v A %+6.2f B %+6.2f C %+6.2f sudden %5.2f (%v) rejected=%v\n",
			a.source, a.duration, num(a.target), a.event, a.dynamic, a.a, a.b, a.c, a.suddenLU, a.sudden, a.rejected)
	}
	t.Log(b.String())

	b.Reset()
	b.WriteString("separation (event min against steady max; a positive gap separates):\n")
	groups := []struct {
		name string
		keep func(*dwAsset) bool
	}{
		{"all", func(*dwAsset) bool { return true }},
		{"-16", func(a *dwAsset) bool { return a.target == -16 }},
		{"-23", func(a *dwAsset) bool { return a.target == -23 }},
	}
	metrics := []struct {
		name string
		get  func(*dwAsset) float64
	}{
		{"A range squash (LU)", func(a *dwAsset) float64 { return a.a }},
		{"-A LRA rise (LU)", func(a *dwAsset) float64 { return a.rise }},
		{"B limiter excess (dB)", func(a *dwAsset) float64 { return a.b }},
		{"C event squash (LU)", func(a *dwAsset) float64 { return a.c }},
		{"sudden max M - I (LU)", func(a *dwAsset) float64 { return a.suddenLU }},
	}
	for _, g := range groups {
		for _, m := range metrics {
			eventMin, steadyMax := math.Inf(1), math.Inf(-1)
			var eventArg, steadyArg string
			for _, a := range list {
				if !g.keep(a) {
					continue
				}
				v := m.get(a)
				name := fmt.Sprintf("%s/%s@%s", a.source, a.duration, num(a.target))
				if a.event && v < eventMin {
					eventMin, eventArg = v, name
				}
				if !a.event && v > steadyMax {
					steadyMax, steadyArg = v, name
				}
			}
			gap := eventMin - steadyMax
			fmt.Fprintf(&b, "%-4s %-22s event min %+7.2f (%s), steady max %+7.2f (%s), gap %+6.2f, midpoint %+6.2f\n",
				g.name, m.name, eventMin, eventArg, steadyMax, steadyArg, gap, (eventMin+steadyMax)/2)
		}
	}
	t.Log(b.String())
}
