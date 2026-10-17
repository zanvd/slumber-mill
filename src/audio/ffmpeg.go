package audio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/zanvd/slumber-mill/src/logging"
)

const (
	inputInstance           = "ebur128@input"
	sourceInstance          = "loudnorm@source"
	verifyInstance          = "loudnorm@verify"
	verifyMomentaryInstance = "ebur128@verify"
	// ffprobeInvalid is ffprobe's exit status when it can't read the input.
	ffprobeInvalid = 1
	// maxOffsetLU is the largest gain offset loudnorm accepts.
	maxOffsetLU = 99.0
	// allowedDemuxers are the containers an input may come in. Playlists, such as hls and concat,
	// would open further files or URLs, so they aren't among them.
	allowedDemuxers = "wav,mp3,flac,ogg,mov,aac,matroska,aiff"
)

// FFmpeg runs ffprobe and ffmpeg from PATH.
type FFmpeg struct {
	run runner
}

func New() *FFmpeg { return &FFmpeg{run: execRunner{}} }

// Process probes src, measures it, renders outputs normalised to targetLUFS and verifies each one,
// giving one that fails a corrective re-encode if a small gain can bring it within spec.
// A *Rejection means retrying can't help. A missing source isn't one: its error matches fs.ErrNotExist.
func (f *FFmpeg) Process(ctx context.Context, src string, targetLUFS float64, outputs []Output) (Result, error) {
	if err := validate(targetLUFS, outputs); err != nil {
		return Result{}, err
	}
	source, err := f.probe(ctx, src)
	if err != nil {
		return Result{}, err
	}
	layouts := planLayouts(source.Channels, outputs)
	input, measured, offsets, err := f.measure(ctx, src, source, targetLUFS, layouts)
	if err != nil {
		return Result{}, err
	}
	predicted := make([]Normalization, len(layouts))
	for i, m := range measured {
		predicted[i] = predict(m, targetLUFS)
	}
	rendered, err := f.render(ctx, src, targetLUFS, layouts, measured, offsets, 0, outputs)
	if err != nil {
		return Result{}, err
	}
	log := logging.LoggerFromContext(ctx)
	for i, l := range layouts {
		if predicted[i] != rendered[i] {
			log.WarnContext(ctx, "loudnorm's mode differs from the prediction",
				"layout", l.name, "predicted", predicted[i], "rendered", rendered[i],
				"measured_lufs", measured[i].LUFS, "measured_true_peak_dbtp", measured[i].TruePeakDBTP,
				"measured_lra_lu", measured[i].LRALU)
		}
	}

	renditions := make([]Rendition, len(outputs))
	layoutOf := make([]int, len(outputs))
	for i, l := range layouts {
		for _, o := range l.outputs {
			renditions[o] = Rendition{
				Profile:       outputs[o].Profile,
				Path:          outputs[o].Path,
				Channels:      l.channels,
				Measured:      measured[i],
				Predicted:     predicted[i],
				Normalization: rendered[i],
			}
			layoutOf[o] = i
		}
	}
	for i := range renditions {
		r := &renditions[i]
		if r.Loudness, err = f.verify(ctx, r.Path, targetLUFS); err != nil {
			return Result{}, fmt.Errorf("error verifying %s: %w", r.Profile.Name, err)
		}
	}
	var failures []string
	for i := range renditions {
		r := &renditions[i]
		if check(r.Profile.Name, r.Loudness, targetLUFS) != "" {
			if err := f.correct(ctx, src, targetLUFS, layouts[layoutOf[i]], offsets[layoutOf[i]], r); err != nil {
				return Result{}, err
			}
		}
		if msg := check(r.Profile.Name, r.Loudness, targetLUFS); msg != "" {
			failures = append(failures, msg)
		}
	}
	if len(failures) > 0 {
		return Result{}, reject(strings.Join(failures, "; "), nil)
	}

	return Result{
		Source:     source,
		Input:      input,
		Warnings:   warnings(input, renditions),
		Renditions: renditions,
	}, nil
}

// Version runs ffmpeg -version and ffprobe -version and returns the first line of each, for logging.
func (f *FFmpeg) Version(ctx context.Context) (ffmpeg, ffprobe string, err error) {
	if ffmpeg, err = f.version(ctx, "ffmpeg"); err != nil {
		return "", "", err
	}
	if ffprobe, err = f.version(ctx, "ffprobe"); err != nil {
		return "", "", err
	}
	return ffmpeg, ffprobe, nil
}

func (f *FFmpeg) version(ctx context.Context, name string) (string, error) {
	var stdout bytes.Buffer
	if err := f.run.Run(ctx, name, []string{"-version"}, &stdout, nil); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return "", fmt.Errorf("%s is not installed: %w", name, err)
		}
		return "", fmt.Errorf("error running %s -version: %w", name, err)
	}
	line, _, _ := strings.Cut(stdout.String(), "\n")
	if !strings.HasPrefix(line, name+" version ") {
		return "", fmt.Errorf("error running %s -version: unexpected output %q", name, line)
	}
	return line, nil
}

type probeOutput struct {
	Streams []struct {
		CodecName  string `json:"codec_name"`
		SampleRate string `json:"sample_rate"`
		Channels   int    `json:"channels"`
	} `json:"streams"`
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

func (f *FFmpeg) probe(ctx context.Context, src string) (Source, error) {
	var stdout bytes.Buffer
	if err := f.run.Run(ctx, "ffprobe", probeArgs(src), &stdout, nil); err != nil {
		if exitErr, ok := errors.AsType[*exitError](err); !ok || exitErr.code != ffprobeInvalid {
			return Source{}, fmt.Errorf("error probing the source: %w", err)
		}
		// ffprobe fails the same way for a file it can't read and one it can't parse.
		file, openErr := os.Open(src)
		if openErr != nil {
			return Source{}, fmt.Errorf("error opening the source: %w", openErr)
		}
		file.Close()
		return Source{}, reject(MessageNotAudio, err)
	}

	var out probeOutput
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		return Source{}, reject(MessageNotAudio, fmt.Errorf("error parsing ffprobe's output: %w", err))
	}
	if len(out.Streams) == 0 {
		return Source{}, reject(MessageNotAudio, errors.New("no audio stream"))
	}
	stream := out.Streams[0]
	sampleRate, err := strconv.Atoi(stream.SampleRate)
	if stream.CodecName == "" || err != nil || sampleRate <= 0 || stream.Channels <= 0 {
		return Source{}, reject(MessageNotAudio, fmt.Errorf("unusable audio stream: codec %q, sample rate %q, %d channels",
			stream.CodecName, stream.SampleRate, stream.Channels))
	}
	duration, err := time.ParseDuration(out.Format.Duration + "s")
	if err != nil || duration <= 0 {
		return Source{}, reject(MessageNotAudio, fmt.Errorf("unusable duration %q", out.Format.Duration))
	}
	if duration < MinDuration {
		return Source{}, reject(MessageTooShort, fmt.Errorf("duration %v is under %v", duration, MinDuration))
	}
	return Source{Codec: stream.CodecName, SampleRate: sampleRate, Channels: stream.Channels, Duration: duration}, nil
}

// measure runs pass 1: ebur128 on the source as it is and on each layout's downmix, for their momentary loudness,
// and loudnorm on each layout's downmix and on the source as it is, unless one of the layouts already is.
// It returns each layout's measurement and the target offset its loudnorm reported, for pass 2.
func (f *FFmpeg) measure(ctx context.Context, src string, source Source, targetLUFS float64, layouts []layout,
) (Input, []Loudness, []float64, error) {
	native := slices.IndexFunc(layouts, func(l layout) bool { return l.channels == source.Channels })
	log := newFFmpegLog()
	err := f.run.Run(ctx, "ffmpeg", measureArgs(src, targetLUFS, layouts, native < 0), nil, log)
	log.flush()
	if err != nil {
		return Input{}, nil, nil, fmt.Errorf("error measuring the source: %w", err)
	}
	measured := make([]Loudness, len(layouts))
	offsets := make([]float64, len(layouts))
	for i, l := range layouts {
		m, report, err := log.loudnorm(loudnormInstance(l))
		if err != nil {
			return Input{}, nil, nil, err
		}
		if m.LUFS < silentLUFS {
			return Input{}, nil, nil, reject(MessageSilent, fmt.Errorf("the %s downmix measures %v LUFS", l.name, m.LUFS))
		}
		// Silence reports an offset of inf, so this comes after the check for it.
		offset, err := parseLevel(report.TargetOffset)
		if err != nil || math.Abs(offset) > maxOffsetLU {
			return Input{}, nil, nil, fmt.Errorf("error reading the report from %s: target_offset %q is unusable",
				loudnormInstance(l), report.TargetOffset)
		}
		if m.MaxMomentaryLUFS, err = log.maxMomentary(ebur128Instance(l)); err != nil {
			return Input{}, nil, nil, err
		}
		measured[i], offsets[i] = m, offset
	}
	var in Loudness
	if native >= 0 {
		in = measured[native]
	} else if in, _, err = log.loudnorm(sourceInstance); err != nil {
		return Input{}, nil, nil, err
	}
	maxMomentary, err := log.maxMomentary(inputInstance)
	if err != nil {
		return Input{}, nil, nil, err
	}
	return Input{LUFS: in.LUFS, TruePeakDBTP: in.TruePeakDBTP, LRALU: in.LRALU, MaxMomentaryLUFS: maxMomentary},
		measured, offsets, nil
}

// render runs pass 2 and returns the mode each layout's loudnorm reported.
func (f *FFmpeg) render(ctx context.Context, src string, targetLUFS float64, layouts []layout,
	measured []Loudness, offsets []float64, gainDB float64, outputs []Output,
) ([]Normalization, error) {
	log := newFFmpegLog()
	err := f.run.Run(ctx, "ffmpeg", renderArgs(src, targetLUFS, layouts, measured, offsets, gainDB, outputs), nil, log)
	log.flush()
	if err != nil {
		return nil, fmt.Errorf("error rendering: %w", err)
	}
	modes := make([]Normalization, len(layouts))
	for i, l := range layouts {
		_, report, err := log.loudnorm(loudnormInstance(l))
		if err != nil {
			return nil, err
		}
		switch mode := Normalization(report.NormalizationType); mode {
		case Linear, Dynamic:
			modes[i] = mode
		default:
			return nil, fmt.Errorf("error reading the report from %s: normalization_type %q", loudnormInstance(l), mode)
		}
	}
	return modes, nil
}

// correct gives a rendition that failed verification one corrective re-encode, if correction finds a gain for it.
func (f *FFmpeg) correct(ctx context.Context, src string, targetLUFS float64, l layout, offset float64, r *Rendition,
) error {
	gain, ok := correction(r.Loudness, targetLUFS)
	if !ok {
		return nil
	}
	only := layout{name: l.name, channels: l.channels, outputs: []int{0}}
	modes, err := f.render(ctx, src, targetLUFS, []layout{only}, []Loudness{r.Measured}, []float64{offset}, gain,
		[]Output{{Profile: r.Profile, Path: r.Path}})
	if err != nil {
		return fmt.Errorf("error re-encoding %s: %w", r.Profile.Name, err)
	}
	first := r.Loudness
	if r.Loudness, err = f.verify(ctx, r.Path, targetLUFS); err != nil {
		return fmt.Errorf("error verifying %s after re-encoding it: %w", r.Profile.Name, err)
	}
	r.Normalization = modes[0]
	logging.LoggerFromContext(ctx).WarnContext(ctx, "re-encoded a rendition with a corrective gain",
		"profile", r.Profile.Name, "gain_db", gain,
		"first_lufs", first.LUFS, "first_true_peak_dbtp", first.TruePeakDBTP,
		"lufs", r.Loudness.LUFS, "true_peak_dbtp", r.Loudness.TruePeakDBTP)
	return nil
}

// verify measures an encoded rendition as it is.
func (f *FFmpeg) verify(ctx context.Context, path string, targetLUFS float64) (Loudness, error) {
	log := newFFmpegLog()
	err := f.run.Run(ctx, "ffmpeg", verifyArgs(path, targetLUFS), nil, log)
	log.flush()
	if err != nil {
		return Loudness{}, err
	}
	m, _, err := log.loudnorm(verifyInstance)
	if err != nil {
		return Loudness{}, err
	}
	if m.MaxMomentaryLUFS, err = log.maxMomentary(verifyMomentaryInstance); err != nil {
		return Loudness{}, err
	}
	return m, nil
}

func loudnormInstance(l layout) string { return "loudnorm@" + l.name }

func ebur128Instance(l layout) string { return "ebur128@" + l.name }

// inputOptions let ffmpeg and ffprobe open an input only as a local file in one of the allowed containers.
// They apply to the input that follows them; after it they would be ignored.
func inputOptions() []string {
	return []string{"-protocol_whitelist", "file", "-format_whitelist", allowedDemuxers}
}

func probeArgs(src string) []string {
	args := append([]string{"-v", "error"}, inputOptions()...)
	return append(args,
		"-select_streams", "a:0",
		"-show_entries", "stream=codec_name,sample_rate,channels:format=duration",
		"-of", "json",
		src)
}

func ffmpegArgs(src string) []string {
	args := append([]string{"-nostdin", "-hide_banner", "-nostats", "-loglevel", "info"}, inputOptions()...)
	return append(args, "-i", src)
}

func loudnormTargets(targetLUFS float64) string {
	return "I=" + num(targetLUFS) + ":TP=" + num(preEncodeTruePeakDBTP) + ":LRA=" + num(loudnormLRA)
}

func measureArgs(src string, targetLUFS float64, layouts []layout, measureSource bool) []string {
	branches := len(layouts)
	if measureSource {
		branches++
	}
	var graph strings.Builder
	fmt.Fprintf(&graph, "[0:a:0]%s=framelog=info,asplit=%d", inputInstance, branches)
	for i := range branches {
		fmt.Fprintf(&graph, "[p%d]", i)
	}
	for i, l := range layouts {
		fmt.Fprintf(&graph, ";[p%d]aformat=channel_layouts=%s,%s=framelog=info,%s=%s:print_format=json[m%d]",
			i, l.name, ebur128Instance(l), loudnormInstance(l), loudnormTargets(targetLUFS), i)
	}
	if measureSource {
		fmt.Fprintf(&graph, ";[p%d]%s=%s:print_format=json[m%d]",
			len(layouts), sourceInstance, loudnormTargets(targetLUFS), len(layouts))
	}
	args := append(ffmpegArgs(src), "-filter_complex", graph.String())
	for i := range branches {
		args = append(args, "-map", fmt.Sprintf("[m%d]", i), "-f", "null", "-")
	}
	return args
}

// renderArgs builds pass 2. A gainDB other than 0 follows the resampler in every layout.
func renderArgs(src string, targetLUFS float64, layouts []layout, measured []Loudness, offsets []float64,
	gainDB float64, outputs []Output,
) []string {
	var graph strings.Builder
	fmt.Fprintf(&graph, "[0:a:0]asplit=%d", len(layouts))
	for i := range layouts {
		fmt.Fprintf(&graph, "[s%d]", i)
	}
	for i, l := range layouts {
		m := measured[i]
		fmt.Fprintf(&graph, ";[s%d]aformat=channel_layouts=%s,%s=%s:measured_I=%s:measured_TP=%s:measured_LRA=%s"+
			":measured_thresh=%s:offset=%s:linear=true:print_format=json,aresample=48000",
			i, l.name, loudnormInstance(l), loudnormTargets(targetLUFS),
			num(m.LUFS), num(m.TruePeakDBTP), num(measuredLRA(m)), num(m.ThresholdLUFS), num(offsets[i]))
		if gainDB != 0 {
			fmt.Fprintf(&graph, ",volume=%sdB:precision=double", num(gainDB))
		}
		if len(l.outputs) > 1 {
			fmt.Fprintf(&graph, ",asplit=%d", len(l.outputs))
		}
		for _, o := range l.outputs {
			fmt.Fprintf(&graph, "[o%d]", o)
		}
	}
	args := append([]string{"-y"}, ffmpegArgs(src)...)
	args = append(args, "-filter_complex", graph.String())
	for i, o := range outputs {
		args = append(args, "-map", fmt.Sprintf("[o%d]", i), "-map_metadata", "-1",
			"-c:a", "aac", "-b:a", fmt.Sprintf("%dk", o.Profile.BitrateKbps))
		if o.Profile.CutoffHz > 0 {
			args = append(args, "-cutoff", strconv.Itoa(o.Profile.CutoffHz))
		}
		args = append(args, "-movflags", "+faststart", "-f", "mp4", o.Path)
	}
	return args
}

func verifyArgs(path string, targetLUFS float64) []string {
	graph := fmt.Sprintf("[0:a:0]%s=framelog=info,%s=%s:print_format=json[v]",
		verifyMomentaryInstance, verifyInstance, loudnormTargets(targetLUFS))
	return append(ffmpegArgs(path), "-filter_complex", graph, "-map", "[v]", "-f", "null", "-")
}

// num formats a value so that ffmpeg parses back exactly the float64 the prediction used.
func num(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
