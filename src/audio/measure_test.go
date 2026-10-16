//go:build measure

// Measurement harness for the loudness investigation on feat/audio-loudness-fixes: scaffolding, not for merge.
//
// It runs the production commands: measureArgs for pass 1, a copy of renderArgs that can add a pre-encode
// gain and encoder options per output (TestMeasureMirrorsRenderArgs pins it to renderArgs), verifyArgs,
// and check for the verdict. Sources use testutil's noise seeds and amplitudes, and are cached.
//
//	variants: one profile's layout, measured once and rendered once, with one encoded output per variant.
//	pipe:     every profile as production renders it, with optional encoder overrides per profile
//	          and an optional single corrective re-encode of a rendition with volume=<delta>dB.
//
// Each rendition is one JSON line in <out>/results/<name>.jsonl. In the dev service:
//
//	go test -tags measure -run TestMeasure -count 1 -timeout 0 -v ./audio/ -args \
//	  -experiment variants -sources white-unc,narration -variants base/cutoff=18000/pns=0/gain=-0.5
//	go test -tags measure -run TestMeasure -count 1 -timeout 0 -v ./audio/ -args \
//	  -experiment pipe -sources pink-unc -duration 60m -retry fail
package audio

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	measureExperiment = flag.String("experiment", "", "variants or pipe")
	measureName       = flag.String("name", "", "results file <out>/results/<name>.jsonl; defaults to the experiment")
	measureOut        = flag.String("out", "/out/audio-b", "directory for results/, the sources/ cache and kept renditions/")
	measureSources    = flag.String("sources", "white-unc,pink-unc,pink-dm,white-dm,narration",
		"comma-separated <kind>[@<seed>], kind one of white-unc, pink-unc, pink-dm, white-dm, narration")
	measureDuration = flag.Duration("duration", 10*time.Minute, "length of each source")
	measureTargets  = flag.String("targets", "-16", "comma-separated integrated targets in LUFS")
	measureProfile  = flag.String("profile", "aac_64k", "variants: the profile whose layout and encoder are varied")
	measureVariants = flag.String("variants", "base",
		"variants: slash-separated variants, each base or comma-separated cutoff=<Hz|none>, pns=<0|1>, gain=<dB>, "+
			"opt.<encoder option>=<value>")
	measureOpts = flag.String("opts", "",
		"pipe: slash-separated <profile>:<variant> overrides, such as aac_64k:cutoff=18000,pns=0")
	measureRetry = flag.String("retry", "off",
		"pipe: off; fail, to re-encode once a rendition that fails the check; all, to plan one for every rendition; "+
			"force, to re-encode every rendition even where the plan says skip")
	measureRetryTPMargin   = flag.Float64("retry-tp-margin", 0.3, "pipe: dB below the ceiling a corrective gain aims the true peak at, at most")
	measureRetryLUFSMargin = flag.Float64("retry-lufs-margin", 0.3, "pipe: LU inside the tolerance the expected loudness must land")
	measureRetryMaxGain    = flag.Float64("retry-max-gain", 2, "pipe: the largest corrective gain in dB, either way")
	measureJobs            = flag.Int("jobs", 4, "verifications run at once")
	measureKeep            = flag.Bool("keep", false, "keep the encoded renditions under <out>/renditions/<run>")
)

const measureNarration = "The evening settles slowly over the valley, and the last light fades from the tops of the pines. " +
	"Down by the river, the water moves without hurry, carrying small leaves past the stones. " +
	"A lantern glows in the window of the old mill, where the wheel turns once, and then again, " +
	"as if it, too, were growing sleepy. Breathe in, and let the air fill you gently. " +
	"Breathe out, and let the day go. There is nothing left to do tonight. " +
	"The path is quiet, the birds are tucked into their branches, " +
	"and the stars are coming out one by one, small and patient, keeping watch while you rest.\n"

// measureNoise mirrors testutil's fixtures: the colour, amplitude and first seed of each kind.
var measureNoise = map[string]struct {
	color     string
	amplitude float64
	seed      int
	dualMono  bool
}{
	"white-unc": {"white", 0.25, 3, false},
	"pink-unc":  {"pink", 0.5, 1, false},
	"pink-dm":   {"pink", 0.5, 1, true},
	"white-dm":  {"white", 0.25, 3, true},
}

type measureSource struct {
	Spec      string     `json:"spec"`
	Kind      string     `json:"kind"`
	Seeds     []int      `json:"seeds,omitempty"`
	DurationS float64    `json:"duration_s"`
	Channels  int        `json:"channels"`
	Path      string     `json:"path"`
	Commands  [][]string `json:"commands"`
	Note      string     `json:"note,omitempty"`
}

type measureVariant struct {
	Name     string
	CutoffHz *int
	GainDB   float64
	EncOpts  []string
}

func (v measureVariant) apply(p Profile) Profile {
	if v.CutoffHz != nil {
		p.CutoffHz = *v.CutoffHz
	}
	return p
}

type measureOutput struct {
	Output
	GainDB  float64
	EncOpts []string
}

type measurePass1 struct {
	Command          []string      `json:"command"`
	LUFS             float64       `json:"lufs"`
	TruePeakDBTP     float64       `json:"true_peak_dbtp"`
	LRALU            float64       `json:"lra_lu"`
	ThresholdLUFS    float64       `json:"threshold_lufs"`
	TargetOffsetLU   float64       `json:"target_offset_lu"`
	MaxMomentaryLUFS float64       `json:"max_momentary_lufs"`
	Predicted        Normalization `json:"predicted"`
	Seconds          float64       `json:"seconds"`
}

func (p measurePass1) loudness() Loudness {
	return Loudness{LUFS: p.LUFS, TruePeakDBTP: p.TruePeakDBTP, LRALU: p.LRALU, ThresholdLUFS: p.ThresholdLUFS}
}

type measurePass2 struct {
	Command       []string          `json:"command"`
	Report        map[string]string `json:"report"`
	Normalization Normalization     `json:"normalization"`
	Seconds       float64           `json:"seconds"`
}

type measureVerify struct {
	Command      []string `json:"command"`
	LUFS         float64  `json:"lufs"`
	TruePeakDBTP float64  `json:"true_peak_dbtp"`
	LRALU        float64  `json:"lra_lu"`
	ErrorLU      float64  `json:"error_lu"`
	HeadroomDB   float64  `json:"headroom_db"`
	InSpec       bool     `json:"in_spec"`
	Check        string   `json:"check,omitempty"`
	Bytes        int64    `json:"bytes"`
	Seconds      float64  `json:"seconds"`
}

type measureRetryPlan struct {
	Decision       string  `json:"decision"`
	FromLUFS       float64 `json:"from_lufs"`
	FromTPDBTP     float64 `json:"from_tp_dbtp"`
	DeltaDB        float64 `json:"delta_db"`
	ExpectedLUFS   float64 `json:"expected_lufs"`
	ExpectedTPDBTP float64 `json:"expected_tp_dbtp"`
	TPMarginDB     float64 `json:"tp_margin_db"`
	LUFSMarginLU   float64 `json:"lufs_margin_lu"`
	MaxGainDB      float64 `json:"max_gain_db"`
}

type measureRow struct {
	Experiment      string            `json:"experiment"`
	Name            string            `json:"name"`
	Run             string            `json:"run"`
	FFmpeg          string            `json:"ffmpeg"`
	Source          measureSource     `json:"source"`
	TargetLUFS      float64           `json:"target_lufs"`
	CeilingDBTP     float64           `json:"ceiling_dbtp"`
	PreEncodeTPDBTP float64           `json:"pre_encode_tp_dbtp"`
	Profile         string            `json:"profile"`
	Layout          string            `json:"layout"`
	Channels        int               `json:"channels"`
	BitrateKbps     int               `json:"bitrate_kbps"`
	CutoffHz        int               `json:"cutoff_hz"`
	Variant         string            `json:"variant"`
	GainDB          float64           `json:"gain_db"`
	EncoderOptions  []string          `json:"encoder_options"`
	Attempt         int               `json:"attempt"`
	Pass1           measurePass1      `json:"pass1"`
	Pass2           measurePass2      `json:"pass2"`
	Verify          measureVerify     `json:"verify"`
	Retry           *measureRetryPlan `json:"retry,omitempty"`
}

// measureRenderArgs is renderArgs with a volume filter after aresample for outputs with a gain,
// and extra encoder options after -cutoff.
func measureRenderArgs(src string, targetLUFS float64, layouts []layout, measured []Loudness, offsets []float64,
	outs []measureOutput,
) []string {
	var graph strings.Builder
	fmt.Fprintf(&graph, "[0:a:0]asplit=%d", len(layouts))
	for i := range layouts {
		fmt.Fprintf(&graph, "[s%d]", i)
	}
	var gains []string
	for i, l := range layouts {
		m := measured[i]
		fmt.Fprintf(&graph, ";[s%d]aformat=channel_layouts=%s,%s=%s:measured_I=%s:measured_TP=%s:measured_LRA=%s"+
			":measured_thresh=%s:offset=%s:linear=true:print_format=json,aresample=48000",
			i, l.name, loudnormInstance(l), loudnormTargets(targetLUFS),
			num(m.LUFS), num(m.TruePeakDBTP), num(measuredLRA(m)), num(m.ThresholdLUFS), num(offsets[i]))
		if len(l.outputs) > 1 {
			fmt.Fprintf(&graph, ",asplit=%d", len(l.outputs))
		}
		for _, o := range l.outputs {
			if outs[o].GainDB == 0 {
				fmt.Fprintf(&graph, "[o%d]", o)
				continue
			}
			fmt.Fprintf(&graph, "[g%d]", o)
			gains = append(gains, fmt.Sprintf(";[g%d]volume=volume=%sdB:precision=double[o%d]", o, num(outs[o].GainDB), o))
		}
	}
	graph.WriteString(strings.Join(gains, ""))
	args := append([]string{"-y"}, ffmpegArgs(src)...)
	args = append(args, "-filter_complex", graph.String())
	for i, o := range outs {
		args = append(args, "-map", fmt.Sprintf("[o%d]", i), "-map_metadata", "-1",
			"-c:a", "aac", "-b:a", fmt.Sprintf("%dk", o.Profile.BitrateKbps))
		if o.Profile.CutoffHz > 0 {
			args = append(args, "-cutoff", strconv.Itoa(o.Profile.CutoffHz))
		}
		args = append(args, o.EncOpts...)
		args = append(args, "-movflags", "+faststart", "-f", "mp4", o.Path)
	}
	return args
}

func TestMeasureMirrorsRenderArgs(t *testing.T) {
	var outs []Output
	var mouts []measureOutput
	for _, p := range Profiles() {
		o := Output{Profile: p, Path: "/tmp/" + p.Name}
		outs = append(outs, o)
		mouts = append(mouts, measureOutput{Output: o})
	}
	for _, channels := range []int{1, 2, 6} {
		layouts := planLayouts(channels, outs)
		measured := make([]Loudness, len(layouts))
		offsets := make([]float64, len(layouts))
		for i := range layouts {
			measured[i] = Loudness{LUFS: -20.25 - float64(i), TruePeakDBTP: -3.5, LRALU: 0, ThresholdLUFS: -30.25}
			offsets[i] = 0.07 * float64(i+1)
		}
		want := renderArgs("src.wav", -16, layouts, measured, offsets, outs)
		got := measureRenderArgs("src.wav", -16, layouts, measured, offsets, mouts)
		if !slices.Equal(got, want) {
			t.Errorf("%d channels: measureRenderArgs() =\n%q\nwant renderArgs()\n%q", channels, got, want)
		}
	}
}

func TestMeasure(t *testing.T) {
	experiment := *measureExperiment
	if experiment != "variants" && experiment != "pipe" {
		t.Fatalf("-experiment is %q, want variants or pipe", experiment)
	}
	switch *measureRetry {
	case "off", "fail", "all", "force":
	default:
		t.Fatalf("-retry is %q, want off, fail, all or force", *measureRetry)
	}
	name := *measureName
	if name == "" {
		name = experiment
	}
	targets, err := measureFloats(*measureTargets)
	if err != nil {
		t.Fatalf("-targets: %v", err)
	}
	ctx := t.Context()
	version, _, err := New().Version(ctx)
	if err != nil {
		t.Fatalf("Version() error = %v", err)
	}
	run := time.Now().UTC().Format("20060102T150405Z")
	sources := filepath.Join(*measureOut, "sources")
	if err := os.MkdirAll(sources, 0o755); err != nil {
		t.Fatal(err)
	}
	emit, summary := measureResults(t, filepath.Join(*measureOut, "results", name+".jsonl"))
	t.Logf("run %s, %s, results in %s", run, version, filepath.Join(*measureOut, "results", name+".jsonl"))

	base := measureRow{
		Experiment: experiment, Name: name, Run: run, FFmpeg: version,
		CeilingDBTP: CeilingDBTP, PreEncodeTPDBTP: preEncodeTruePeakDBTP,
	}
	for _, spec := range strings.Split(*measureSources, ",") {
		t.Run(spec, func(t *testing.T) {
			src := measurePrepareSource(t, ctx, sources, spec, *measureDuration)
			for _, target := range targets {
				t.Run(num(target), func(t *testing.T) {
					dir := t.TempDir()
					if *measureKeep {
						dir = filepath.Join(*measureOut, "renditions", run, measureFileName(spec), num(target))
						if err := os.MkdirAll(dir, 0o755); err != nil {
							t.Fatal(err)
						}
					}
					row := base
					row.Source, row.TargetLUFS = src, target
					if experiment == "variants" {
						measureVariantsRun(t, ctx, row, dir, emit)
					} else {
						measurePipeRun(t, ctx, row, dir, emit)
					}
				})
			}
		})
	}
	t.Logf("summary of run %s:\n%s", run, strings.Join(*summary, "\n"))
}

func measureVariantsRun(t *testing.T, ctx context.Context, row measureRow, dir string, emit func(*testing.T, measureRow)) {
	i := slices.IndexFunc(Profiles(), func(p Profile) bool { return p.Name == *measureProfile })
	if i < 0 {
		t.Fatalf("-profile %q is not one of Profiles()", *measureProfile)
	}
	profile := Profiles()[i]
	var variants []measureVariant
	for _, s := range strings.Split(*measureVariants, "/") {
		v, err := measureParseVariant(s)
		if err != nil {
			t.Fatalf("-variants: %v", err)
		}
		variants = append(variants, v)
	}
	outs := make([]measureOutput, len(variants))
	plain := make([]Output, len(variants))
	for i, v := range variants {
		outs[i] = measureOutput{
			Output:  Output{Profile: v.apply(profile), Path: filepath.Join(dir, fmt.Sprintf("v%02d.m4a", i))},
			GainDB:  v.GainDB,
			EncOpts: v.EncOpts,
		}
		plain[i] = outs[i].Output
	}
	layouts := planLayouts(row.Source.Channels, plain)
	p1, err := measurePass(ctx, row.Source.Path, row.TargetLUFS, layouts, false)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := measureRender(ctx, row.Source.Path, row.TargetLUFS, layouts, p1, outs)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := measureVerifyAll(ctx, outs, row.TargetLUFS)
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range variants {
		r := row
		measureFill(&r, layouts[0], outs[i], v.Name, 1, p1[0], p2[0], verified[i])
		emit(t, r)
	}
}

func measurePipeRun(t *testing.T, ctx context.Context, row measureRow, dir string, emit func(*testing.T, measureRow)) {
	overrides := map[string]measureVariant{}
	if *measureOpts != "" {
		for _, s := range strings.Split(*measureOpts, "/") {
			profile, spec, ok := strings.Cut(s, ":")
			v, err := measureParseVariant(spec)
			if !ok || err != nil {
				t.Fatalf("-opts: %q is not <profile>:<variant>: %v", s, err)
			}
			overrides[profile] = v
		}
	}
	profiles := Profiles()
	outs := make([]measureOutput, len(profiles))
	plain := make([]Output, len(profiles))
	names := make([]string, len(profiles))
	for i, p := range profiles {
		v, ok := overrides[p.Name]
		if !ok {
			v = measureVariant{Name: "base"}
		}
		outs[i] = measureOutput{
			Output:  Output{Profile: v.apply(p), Path: filepath.Join(dir, p.Name+".m4a")},
			GainDB:  v.GainDB,
			EncOpts: v.EncOpts,
		}
		plain[i], names[i] = outs[i].Output, v.Name
	}
	layouts := planLayouts(row.Source.Channels, plain)
	layoutOf := make([]int, len(outs))
	for li, l := range layouts {
		for _, o := range l.outputs {
			layoutOf[o] = li
		}
	}
	native := slices.IndexFunc(layouts, func(l layout) bool { return l.channels == row.Source.Channels })
	p1, err := measurePass(ctx, row.Source.Path, row.TargetLUFS, layouts, native < 0)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := measureRender(ctx, row.Source.Path, row.TargetLUFS, layouts, p1, outs)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := measureVerifyAll(ctx, outs, row.TargetLUFS)
	if err != nil {
		t.Fatal(err)
	}
	for i := range outs {
		li := layoutOf[i]
		r := row
		measureFill(&r, layouts[li], outs[i], names[i], 1, p1[li], p2[li], verified[i])
		if *measureRetry == "off" {
			emit(t, r)
			continue
		}
		plan := measurePlanRetry(verified[i], row.TargetLUFS)
		r.Retry = &plan
		emit(t, r)
		if plan.Decision != "re-encode" && *measureRetry != "force" {
			continue
		}
		if plan.DeltaDB == 0 {
			continue
		}
		l := layouts[li]
		one := []layout{{name: l.name, channels: l.channels, outputs: []int{0}}}
		o := outs[i]
		o.Path = strings.TrimSuffix(o.Path, ".m4a") + "-retry.m4a"
		o.GainDB += plan.DeltaDB
		rp2, err := measureRender(ctx, row.Source.Path, row.TargetLUFS, one, p1[li:li+1], []measureOutput{o})
		if err != nil {
			t.Fatal(err)
		}
		rv, err := measureVerifyAll(ctx, []measureOutput{o}, row.TargetLUFS)
		if err != nil {
			t.Fatal(err)
		}
		r2 := row
		measureFill(&r2, l, o, names[i], 2, p1[li], rp2[0], rv[0])
		r2.Retry = &plan
		emit(t, r2)
	}
}

// measurePlanRetry aims a corrective gain at the target, lowered where it would put the true peak
// closer than the margin to the ceiling, and skips gains beyond the bound or that would leave the
// loudness outside the tolerance less the margin.
func measurePlanRetry(v measureVerify, targetLUFS float64) measureRetryPlan {
	p := measureRetryPlan{
		FromLUFS: v.LUFS, FromTPDBTP: v.TruePeakDBTP,
		TPMarginDB: *measureRetryTPMargin, LUFSMarginLU: *measureRetryLUFSMargin, MaxGainDB: *measureRetryMaxGain,
	}
	if *measureRetry == "fail" && v.InSpec {
		p.Decision = "in spec"
		return p
	}
	delta := targetLUFS - v.LUFS
	if v.TruePeakDBTP+delta > CeilingDBTP-p.TPMarginDB {
		delta = CeilingDBTP - p.TPMarginDB - v.TruePeakDBTP
	}
	delta = math.Round(delta*100) / 100
	if math.Abs(delta) > p.MaxGainDB {
		delta = math.Copysign(p.MaxGainDB, delta)
		p.Decision = "skip: gain beyond the bound"
	}
	p.DeltaDB = delta
	p.ExpectedLUFS, p.ExpectedTPDBTP = v.LUFS+delta, v.TruePeakDBTP+delta
	switch {
	case p.Decision != "":
	case math.Abs(p.ExpectedLUFS-targetLUFS) > ToleranceLU-p.LUFSMarginLU:
		p.Decision = "skip: the ceiling leaves the loudness off target"
	case delta == 0:
		p.Decision = "skip: no gain"
	default:
		p.Decision = "re-encode"
	}
	return p
}

func measureFill(r *measureRow, l layout, o measureOutput, variant string, attempt int,
	p1 measurePass1, p2 measurePass2, v measureVerify,
) {
	r.Profile, r.Layout, r.Channels = o.Profile.Name, l.name, l.channels
	r.BitrateKbps, r.CutoffHz = o.Profile.BitrateKbps, o.Profile.CutoffHz
	r.Variant, r.GainDB, r.EncoderOptions, r.Attempt = variant, o.GainDB, o.EncOpts, attempt
	r.Pass1, r.Pass2, r.Verify = p1, p2, v
}

func measureParseVariant(s string) (measureVariant, error) {
	v := measureVariant{Name: s}
	if s == "" || s == "base" {
		v.Name = "base"
		return v, nil
	}
	for _, kv := range strings.Split(s, ",") {
		k, val, ok := strings.Cut(kv, "=")
		if !ok {
			return v, fmt.Errorf("%q in %q is not key=value", kv, s)
		}
		switch {
		case k == "cutoff":
			c := 0
			if val != "none" {
				var err error
				if c, err = strconv.Atoi(val); err != nil || c <= 0 {
					return v, fmt.Errorf("cutoff %q is not a positive number of Hz or none", val)
				}
			}
			v.CutoffHz = &c
		case k == "pns":
			v.EncOpts = append(v.EncOpts, "-aac_pns", val)
		case k == "gain":
			g, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return v, fmt.Errorf("gain %q: %w", val, err)
			}
			v.GainDB = g
		case strings.HasPrefix(k, "opt.") && len(k) > len("opt."):
			v.EncOpts = append(v.EncOpts, "-"+strings.TrimPrefix(k, "opt."), val)
		default:
			return v, fmt.Errorf("unknown key %q in %q", k, s)
		}
	}
	return v, nil
}

func measureRun(ctx context.Context, args []string, ebur128 string) (*ffmpegLog, float64, error) {
	log := newFFmpegLog(ebur128)
	start := time.Now()
	err := execRunner{}.Run(ctx, "ffmpeg", args, nil, log)
	log.flush()
	return log, time.Since(start).Seconds(), err
}

func measurePass(ctx context.Context, src string, targetLUFS float64, layouts []layout, withSource bool,
) ([]measurePass1, error) {
	args := measureArgs(src, targetLUFS, layouts, withSource)
	log, secs, err := measureRun(ctx, args, inputInstance)
	if err != nil {
		return nil, fmt.Errorf("pass 1: %w", err)
	}
	maxMomentary, err := log.maxMomentary()
	if err != nil {
		return nil, err
	}
	out := make([]measurePass1, len(layouts))
	for i, l := range layouts {
		m, report, err := log.loudnorm(loudnormInstance(l))
		if err != nil {
			return nil, err
		}
		offset, err := parseLevel(report.TargetOffset)
		if err != nil {
			return nil, fmt.Errorf("pass 1 %s target_offset: %w", l.name, err)
		}
		out[i] = measurePass1{
			Command: args, LUFS: m.LUFS, TruePeakDBTP: m.TruePeakDBTP, LRALU: m.LRALU, ThresholdLUFS: m.ThresholdLUFS,
			TargetOffsetLU: offset, MaxMomentaryLUFS: maxMomentary, Predicted: predict(m, targetLUFS), Seconds: secs,
		}
	}
	return out, nil
}

func measureRender(ctx context.Context, src string, targetLUFS float64, layouts []layout, p1 []measurePass1,
	outs []measureOutput,
) ([]measurePass2, error) {
	measured := make([]Loudness, len(layouts))
	offsets := make([]float64, len(layouts))
	for i := range layouts {
		measured[i], offsets[i] = p1[i].loudness(), p1[i].TargetOffsetLU
	}
	args := measureRenderArgs(src, targetLUFS, layouts, measured, offsets, outs)
	log, secs, err := measureRun(ctx, args, "")
	if err != nil {
		return nil, fmt.Errorf("pass 2: %w", err)
	}
	out := make([]measurePass2, len(layouts))
	for i, l := range layouts {
		block, ok := log.reports[loudnormInstance(l)]
		if !ok {
			return nil, fmt.Errorf("pass 2: no report from %s", loudnormInstance(l))
		}
		var report map[string]string
		if err := json.Unmarshal([]byte(block), &report); err != nil {
			return nil, fmt.Errorf("pass 2 report from %s: %w", loudnormInstance(l), err)
		}
		out[i] = measurePass2{
			Command: args, Report: report, Normalization: Normalization(report["normalization_type"]), Seconds: secs,
		}
	}
	return out, nil
}

func measureVerifyAll(ctx context.Context, outs []measureOutput, targetLUFS float64) ([]measureVerify, error) {
	res := make([]measureVerify, len(outs))
	errs := make([]error, len(outs))
	sem := make(chan struct{}, max(*measureJobs, 1))
	var wg sync.WaitGroup
	for i, o := range outs {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			args := verifyArgs(o.Path, targetLUFS)
			log, secs, err := measureRun(ctx, args, "")
			if err != nil {
				errs[i] = fmt.Errorf("verifying %s: %w", o.Path, err)
				return
			}
			l, _, err := log.loudnorm(verifyInstance)
			if err != nil {
				errs[i] = err
				return
			}
			info, err := os.Stat(o.Path)
			if err != nil {
				errs[i] = err
				return
			}
			msg := check(o.Profile.Name, l, targetLUFS)
			res[i] = measureVerify{
				Command: args, LUFS: l.LUFS, TruePeakDBTP: l.TruePeakDBTP, LRALU: l.LRALU,
				ErrorLU: l.LUFS - targetLUFS, HeadroomDB: CeilingDBTP - l.TruePeakDBTP,
				InSpec: msg == "", Check: msg, Bytes: info.Size(), Seconds: secs,
			}
		})
	}
	wg.Wait()
	return res, errors.Join(errs...)
}

// measurePrepareSource renders a source into dir unless it is already there.
func measurePrepareSource(t *testing.T, ctx context.Context, dir, spec string, d time.Duration) measureSource {
	kind, seedText, hasSeed := strings.Cut(spec, "@")
	src := measureSource{Spec: spec, Kind: kind, DurationS: d.Seconds(), Channels: 2}
	secs := strconv.FormatFloat(d.Seconds(), 'f', -1, 64)
	src.Path = filepath.Join(dir, fmt.Sprintf("%s-%ss.wav", measureFileName(spec), secs))
	if kind == "narration" {
		if hasSeed {
			t.Fatalf("source %q: narration takes no seed", spec)
		}
		text := filepath.Join(dir, "narration.txt")
		if err := os.WriteFile(text, []byte(measureNarration), 0o644); err != nil {
			t.Fatal(err)
		}
		speech := filepath.Join(dir, "narration-speech.wav")
		src.Commands = [][]string{
			{"-f", "lavfi", "-i", "flite=textfile=" + text + ":voice=slt", "-af", "aresample=48000", "-ac", "2",
				"-c:a", "pcm_s16le", "-f", "wav", speech},
			{"-stream_loop", "-1", "-i", speech, "-t", secs, "-c:a", "pcm_s16le", "-f", "wav", src.Path},
		}
		src.Note = "flite's slt voice (16 kHz) reading a fixed paragraph, resampled to 48 kHz, dual-mono, looped"
	} else {
		n, ok := measureNoise[kind]
		if !ok {
			t.Fatalf("source %q: unknown kind %q", spec, kind)
		}
		seed := n.seed
		if hasSeed {
			var err error
			if seed, err = strconv.Atoi(seedText); err != nil {
				t.Fatalf("source %q: seed: %v", spec, err)
			}
		}
		noise := func(seed int) string {
			return fmt.Sprintf("anoisesrc=color=%s:amplitude=%s:seed=%d:sample_rate=48000:duration=%s",
				n.color, strconv.FormatFloat(n.amplitude, 'f', -1, 64), seed, secs)
		}
		if n.dualMono {
			src.Seeds = []int{seed}
			src.Commands = [][]string{{"-f", "lavfi", "-i", noise(seed), "-ac", "2",
				"-c:a", "pcm_s16le", "-f", "wav", src.Path}}
		} else {
			src.Seeds = []int{seed, seed + 1}
			src.Commands = [][]string{{"-f", "lavfi", "-i", noise(seed), "-f", "lavfi", "-i", noise(seed + 1),
				"-filter_complex", "[0:a][1:a]amerge=inputs=2", "-c:a", "pcm_s16le", "-f", "wav", src.Path}}
		}
	}
	for i := range src.Commands {
		src.Commands[i] = append([]string{"ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-y"},
			src.Commands[i]...)
	}
	for _, cmd := range src.Commands {
		out := cmd[len(cmd)-1]
		if info, err := os.Stat(out); err == nil && info.Size() > 0 {
			continue
		}
		part := slices.Clone(cmd)
		part[len(part)-1] = out + ".part"
		start := time.Now()
		if err := (execRunner{}).Run(ctx, part[0], part[1:], nil, nil); err != nil {
			t.Fatalf("generating %s: %v", out, err)
		}
		if err := os.Rename(out+".part", out); err != nil {
			t.Fatal(err)
		}
		t.Logf("generated %s in %.0f s", out, time.Since(start).Seconds())
	}
	return src
}

func measureFileName(spec string) string { return strings.ReplaceAll(spec, "@", "-s") }

func measureFloats(s string) ([]float64, error) {
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

// measureResults appends rows to path as JSON lines and keeps a one-line summary of each.
func measureResults(t *testing.T, path string) (emit func(*testing.T, measureRow), summary *[]string) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	var mu sync.Mutex
	var lines []string
	return func(t *testing.T, r measureRow) {
		line, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("encoding a result row: %v", err)
		}
		verdict := "in spec"
		if !r.Verify.InSpec {
			verdict = "OUT OF SPEC"
		}
		if r.Retry != nil && r.Attempt == 1 {
			verdict += fmt.Sprintf(" [%s %+.2f dB]", r.Retry.Decision, r.Retry.DeltaDB)
		}
		s := fmt.Sprintf("%-14s %6s %-8s %-24s gain %+5.2f a%d %7.2f LUFS %6.2f dBTP LRA %5.2f %-7s %6.0f kB %s",
			r.Source.Spec, num(r.TargetLUFS), r.Profile, r.Variant, r.GainDB, r.Attempt,
			r.Verify.LUFS, r.Verify.TruePeakDBTP, r.Verify.LRALU, r.Pass2.Normalization,
			float64(r.Verify.Bytes)/1000, verdict)
		mu.Lock()
		defer mu.Unlock()
		if _, err := f.Write(append(line, '\n')); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
		lines = append(lines, s)
		t.Log(s)
	}, &lines
}
