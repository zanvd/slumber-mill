package audio

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ffmpegLog reads ffmpeg's stderr as it streams and keeps what named filter instances report:
// each loudnorm's JSON block, and the highest momentary loudness the ebur128 instance logs.
// Callers check ffmpeg's exit status before reading any of it: an interrupted ffmpeg still prints reports.
type ffmpegLog struct {
	lines   lineWriter
	ebur128 string

	collecting string
	block      []string
	reports    map[string]string

	momentary        bool
	maxMomentaryLUFS float64
}

// newFFmpegLog returns a log that also follows the ebur128 instance with the given name, if it isn't empty.
func newFFmpegLog(ebur128 string) *ffmpegLog {
	l := &ffmpegLog{ebur128: ebur128, reports: map[string]string{}}
	l.lines.fn = l.line
	return l
}

func (l *ffmpegLog) Write(p []byte) (int, error) { return l.lines.Write(p) }

// flush ends the stream; call it once the command has returned.
func (l *ffmpegLog) flush() { l.lines.flush() }

func (l *ffmpegLog) line(s string) {
	if name, rest, ok := cutInstance(s); ok {
		// loudnorm's report starts on the line after its prefix; any other prefixed line ends a block.
		l.collecting = ""
		rest = strings.TrimSpace(rest)
		switch {
		case strings.HasPrefix(name, "loudnorm@") && rest == "":
			l.collecting, l.block = name, l.block[:0]
		case l.ebur128 != "" && name == l.ebur128 && strings.HasPrefix(rest, "t:"):
			l.frame(rest)
		}
		return
	}
	if l.collecting == "" {
		return
	}
	l.block = append(l.block, s)
	if strings.TrimSpace(s) == "}" {
		l.reports[l.collecting] = strings.Join(l.block, "\n")
		l.collecting = ""
	}
}

// cutInstance splits a line that starts with an ffmpeg log prefix, "[name @ 0x...] rest".
func cutInstance(s string) (name, rest string, ok bool) {
	if !strings.HasPrefix(s, "[") {
		return "", "", false
	}
	inside, rest, ok := strings.Cut(s[1:], "]")
	if !ok {
		return "", "", false
	}
	name, addr, ok := strings.Cut(inside, " @ ")
	if !ok || !strings.HasPrefix(addr, "0x") {
		return "", "", false
	}
	return name, rest, true
}

// frame reads the momentary loudness from one of ebur128's frame lines, "t: 0.1 TARGET:-23 LUFS M:-120.7 S:...".
func (l *ffmpegLog) frame(rest string) {
	_, after, ok := strings.Cut(rest, " M:")
	if !ok {
		return
	}
	value, _, _ := strings.Cut(strings.TrimSpace(after), " ")
	m, err := parseLevel(value)
	if err != nil {
		return
	}
	if !l.momentary || m > l.maxMomentaryLUFS {
		l.momentary, l.maxMomentaryLUFS = true, m
	}
}

func (l *ffmpegLog) maxMomentary() (float64, error) {
	if !l.momentary {
		return 0, fmt.Errorf("error reading ffmpeg's output: no momentary loudness from %s", l.ebur128)
	}
	return l.maxMomentaryLUFS, nil
}

type loudnormReport struct {
	InputI            string `json:"input_i"`
	InputTP           string `json:"input_tp"`
	InputLRA          string `json:"input_lra"`
	InputThresh       string `json:"input_thresh"`
	NormalizationType string `json:"normalization_type"`
	TargetOffset      string `json:"target_offset"`
}

// loudnorm parses the report of the loudnorm instance with the given name.
func (l *ffmpegLog) loudnorm(name string) (Loudness, loudnormReport, error) {
	block, ok := l.reports[name]
	if !ok {
		return Loudness{}, loudnormReport{}, fmt.Errorf("error reading ffmpeg's output: no report from %s", name)
	}
	var r loudnormReport
	if err := json.Unmarshal([]byte(block), &r); err != nil {
		return Loudness{}, loudnormReport{}, fmt.Errorf("error parsing the report from %s: %w", name, err)
	}
	var m Loudness
	var errs []error
	for _, f := range []struct {
		key   string
		value string
		dst   *float64
	}{
		{"input_i", r.InputI, &m.LUFS},
		{"input_tp", r.InputTP, &m.TruePeakDBTP},
		{"input_lra", r.InputLRA, &m.LRALU},
		{"input_thresh", r.InputThresh, &m.ThresholdLUFS},
	} {
		v, err := parseLevel(f.value)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", f.key, err))
		}
		*f.dst = v
	}
	if err := errors.Join(errs...); err != nil {
		return Loudness{}, loudnormReport{}, fmt.Errorf("error parsing the report from %s: %w", name, err)
	}
	return m, r, nil
}

// parseLevel parses a level as ffmpeg prints it, including -inf and inf.
func parseLevel(s string) (float64, error) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(v) {
		return 0, fmt.Errorf("not a number: %q", s)
	}
	return v, nil
}
