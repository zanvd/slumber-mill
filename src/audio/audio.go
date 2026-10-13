// Package audio wraps ffprobe and ffmpeg: it probes a source, measures it per output channel layout,
// renders the loudness-normalised renditions in one pass and verifies each encoded file.
package audio

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/zanvd/slumber-mill/src/logging"
)

const (
	// CeilingDBTP is the published true-peak ceiling that every rendition is verified against.
	CeilingDBTP = -1.5
	// ToleranceLU is how far a rendition's integrated loudness may land from the target.
	ToleranceLU = 1.0
	// MinDuration is the shortest source accepted; very short clips measure as silent.
	MinDuration = 3 * time.Second

	// preEncodeTruePeakDBTP is the TP loudnorm aims for, leaving headroom for the peak the encoder adds.
	preEncodeTruePeakDBTP = -4.0
	// loudnormLRA is the largest LRA loudnorm accepts, so it never blocks linear mode.
	loudnormLRA = 50.0
	// minMeasuredLRA stands in for a measured LRA of 0, which loudnorm reads as unmeasured.
	minMeasuredLRA = 0.01

	minTargetLUFS = -70.0
	maxTargetLUFS = -5.0
	silentLUFS    = -70.0

	suddenLoudEventLU = 10.0
	mayClipDBTP       = -0.5
)

// Tenant-safe rejection messages.
const (
	MessageNotAudio = "not audio"
	MessageTooShort = "too short"
	MessageSilent   = "silent"
)

// Rejection is a permanent failure caused by the source. Message is safe to show the tenant;
// Error adds internal detail, such as ffmpeg's stderr and file paths.
type Rejection struct {
	Message string
	Err     error
}

func (r *Rejection) Error() string {
	if r.Err == nil {
		return r.Message
	}
	return r.Message + ": " + r.Err.Error()
}

func (r *Rejection) Unwrap() error { return r.Err }

func reject(message string, err error) error { return &Rejection{Message: message, Err: err} }

// Profile is how one rendition is encoded.
type Profile struct {
	Name        string
	BitrateKbps int
	// MaxChannels caps the source's channel count: 1 renders mono, 2 keeps stereo sources stereo.
	MaxChannels int
	// CutoffHz is the encoder's low-pass; 0 keeps its default.
	CutoffHz int
}

// Profiles returns the renditions every asset gets.
func Profiles() []Profile {
	return []Profile{
		{Name: "aac_128k", BitrateKbps: 128, MaxChannels: 2, CutoffHz: 20000},
		{Name: "aac_64k", BitrateKbps: 64, MaxChannels: 1},
	}
}

// Output asks for a profile to be rendered to Path, which ffmpeg overwrites.
type Output struct {
	Profile Profile
	Path    string
}

type Normalization string

const (
	Linear  Normalization = "linear"
	Dynamic Normalization = "dynamic"
)

type Warning string

const (
	WarningSuddenLoudEvent      Warning = "sudden_loud_event"
	WarningDynamicNormalization Warning = "dynamic_normalization"
	WarningSourceMayClip        Warning = "source_may_clip"
)

// Source is what ffprobe reports about the first audio stream.
type Source struct {
	Codec      string
	SampleRate int
	Channels   int
	Duration   time.Duration
}

// Loudness is a loudnorm measurement.
type Loudness struct {
	LUFS          float64
	TruePeakDBTP  float64
	LRALU         float64
	ThresholdLUFS float64
}

// Input is the source measured in its own channel layout.
type Input struct {
	LUFS             float64
	TruePeakDBTP     float64
	LRALU            float64
	MaxMomentaryLUFS float64
}

type Rendition struct {
	Profile  Profile
	Path     string
	Channels int
	// Measured is pass 1's measurement of the source downmixed to this rendition's layout.
	Measured Loudness
	// Predicted is the mode Measured predicts, and Normalization the one pass 2 reported.
	Predicted     Normalization
	Normalization Normalization
	// Loudness is the encoded file's, which passed verification.
	Loudness Loudness
}

type Result struct {
	Source     Source
	Input      Input
	Warnings   []Warning
	Renditions []Rendition
}

// FFmpeg runs ffprobe and ffmpeg from PATH.
type FFmpeg struct {
	run runner
}

func New() *FFmpeg { return &FFmpeg{run: execRunner{}} }

// Process probes src, measures it, renders outputs normalised to targetLUFS and verifies each one.
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
	rendered, err := f.render(ctx, src, targetLUFS, layouts, measured, offsets, outputs)
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
		}
	}
	var failures []string
	for i := range renditions {
		r := &renditions[i]
		if r.Loudness, err = f.verify(ctx, r.Path, targetLUFS); err != nil {
			return Result{}, fmt.Errorf("error verifying %s: %w", r.Profile.Name, err)
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

func validate(targetLUFS float64, outputs []Output) error {
	if math.IsNaN(targetLUFS) || targetLUFS < minTargetLUFS || targetLUFS > maxTargetLUFS {
		return fmt.Errorf("target of %v LUFS is outside loudnorm's range of %v to %v LUFS",
			targetLUFS, minTargetLUFS, maxTargetLUFS)
	}
	if len(outputs) == 0 {
		return errors.New("no outputs to render")
	}
	for _, o := range outputs {
		if o.Path == "" {
			return fmt.Errorf("output %s has no path", o.Profile.Name)
		}
		if o.Profile.MaxChannels != 1 && o.Profile.MaxChannels != 2 {
			return fmt.Errorf("output %s has %d channels, want 1 or 2", o.Profile.Name, o.Profile.MaxChannels)
		}
		if o.Profile.BitrateKbps <= 0 || o.Profile.CutoffHz < 0 {
			return fmt.Errorf("output %s has a bitrate of %d kbps and a cutoff of %d Hz",
				o.Profile.Name, o.Profile.BitrateKbps, o.Profile.CutoffHz)
		}
	}
	return nil
}

// layout is one distinct output channel layout and the outputs rendered from it.
type layout struct {
	name     string
	channels int
	outputs  []int
}

func planLayouts(sourceChannels int, outputs []Output) []layout {
	var layouts []layout
	index := map[int]int{}
	for i, o := range outputs {
		channels := min(sourceChannels, o.Profile.MaxChannels)
		j, ok := index[channels]
		if !ok {
			j = len(layouts)
			index[channels] = j
			name := "stereo"
			if channels == 1 {
				name = "mono"
			}
			layouts = append(layouts, layout{name: name, channels: channels})
		}
		layouts[j].outputs = append(layouts[j].outputs, i)
	}
	return layouts
}

// predict reports the mode loudnorm will pick for a layout measured as m:
// linear only if the gain keeps the true peak within TP and the measured LRA is within LRA.
func predict(m Loudness, targetLUFS float64) Normalization {
	if m.TruePeakDBTP+(targetLUFS-m.LUFS) <= preEncodeTruePeakDBTP && measuredLRA(m) <= loudnormLRA {
		return Linear
	}
	return Dynamic
}

func measuredLRA(m Loudness) float64 { return max(m.LRALU, minMeasuredLRA) }

// check verifies an encoded rendition's loudness, and if it is out of spec returns a tenant-safe message
// with the numbers.
func check(profile string, l Loudness, targetLUFS float64) string {
	if hundredths(math.Abs(l.LUFS-targetLUFS)) <= hundredths(ToleranceLU) && l.TruePeakDBTP <= CeilingDBTP {
		return ""
	}
	return fmt.Sprintf("%s rendition out of spec: %.2f LUFS against a target of %.1f ± %.0f LU, "+
		"true peak %.2f dBTP against a ceiling of %.1f dBTP",
		profile, l.LUFS, targetLUFS, ToleranceLU, l.TruePeakDBTP, CeilingDBTP)
}

func warnings(in Input, renditions []Rendition) []Warning {
	var w []Warning
	if hundredths(in.MaxMomentaryLUFS-in.LUFS) > hundredths(suddenLoudEventLU) {
		w = append(w, WarningSuddenLoudEvent)
	}
	for _, r := range renditions {
		if r.Normalization == Dynamic {
			w = append(w, WarningDynamicNormalization)
			break
		}
	}
	if in.TruePeakDBTP > mayClipDBTP {
		w = append(w, WarningSourceMayClip)
	}
	return w
}

// hundredths rounds a level to the precision ffmpeg prints, so differences compare without float noise.
func hundredths(v float64) float64 { return math.Round(v * 100) }
