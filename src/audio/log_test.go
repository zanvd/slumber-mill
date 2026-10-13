package audio

import (
	"math"
	"strings"
	"testing"
)

// pass1Stderr is trimmed from a real pass 1 on the uncorrelated pink fixture, without the trailing spaces
// ffmpeg prints after a prefix that starts a multi-line message (withTrailingSpaces puts them back).
// loudnorm prints its reports while the graph is freed, so mono comes before stereo.
const pass1Stderr = `[aist#0:0/pcm_s16le @ 0x55b9c1bcae40] Guessed Channel Layout: stereo
Input #0, wav, from 'up.wav':
  Duration: 00:00:10.00, bitrate: 1536 kb/s
  Stream #0:0: Audio: pcm_s16le ([1][0][0][0] / 0x0001), 48000 Hz, 2 channels, s16, 1536 kb/s
[ebur128@input @ 0x55b9c1b6c680] t: 0.0999948  TARGET:-23 LUFS    M:-120.7 S:-120.7     I: -70.0 LUFS       LRA:   0.0 LU
[ebur128@input @ 0x55b9c1b6c680] t: 0.399995   TARGET:-23 LUFS    M: -17.6 S:-120.7     I: -17.6 LUFS       LRA:   0.0 LU
[ebur128@input @ 0x55b9c1b6c680] t: 0.499995   TARGET:-23 LUFS    M: -17.8 S:-120.7     I: -17.7 LUFS       LRA:   0.0 LU
[out#0/null @ 0x55b9c1b9c880] video:0kB audio:7500kB subtitle:0kB other streams:0kB global headers:0kB muxing overhead: unknown
size=N/A time=00:00:07.10 bitrate=N/A speed=12.7x
[ebur128@input @ 0x55b9c1b6c680] Summary:

  Integrated loudness:
    I:         -17.7 LUFS
    Threshold: -27.7 LUFS
[loudnorm@mono @ 0x55b9c1ba24c0]
{
	"input_i" : "-20.70",
	"input_tp" : "-6.59",
	"input_lra" : "0.10",
	"input_thresh" : "-30.70",
	"output_i" : "-16.07",
	"output_tp" : "-4.00",
	"output_lra" : "0.10",
	"output_thresh" : "-26.07",
	"normalization_type" : "dynamic",
	"target_offset" : "0.07"
}
[loudnorm@stereo @ 0x55b9c1b6d0c0]
{
	"input_i" : "-17.69",
	"input_tp" : "-7.26",
	"input_lra" : "0.10",
	"input_thresh" : "-27.69",
	"output_i" : "-16.00",
	"output_tp" : "-5.57",
	"output_lra" : "0.10",
	"output_thresh" : "-26.00",
	"normalization_type" : "linear",
	"target_offset" : "-0.00"
}
`

func parseLog(ebur128, stderr string, chunk int) *ffmpegLog {
	l := newFFmpegLog(ebur128)
	for len(stderr) > 0 {
		n := min(chunk, len(stderr))
		l.Write([]byte(stderr[:n]))
		stderr = stderr[n:]
	}
	l.flush()
	return l
}

func withTrailingSpaces(stderr string) string { return strings.ReplaceAll(stderr, "]\n", "] \n") }

func TestFFmpegLogReadsReportsByName(t *testing.T) {
	chunks := []struct {
		name string
		size int
	}{
		{"a byte at a time", 1},
		{"7 bytes at a time", 7},
		{"all at once", len(pass1Stderr)},
	}
	reports := []struct {
		instance string
		m        Loudness
		mode     string
	}{
		{"loudnorm@stereo", Loudness{LUFS: -17.69, TruePeakDBTP: -7.26, LRALU: 0.1, ThresholdLUFS: -27.69}, "linear"},
		{"loudnorm@mono", Loudness{LUFS: -20.7, TruePeakDBTP: -6.59, LRALU: 0.1, ThresholdLUFS: -30.7}, "dynamic"},
	}
	for _, chunk := range chunks {
		t.Run(chunk.name, func(t *testing.T) {
			l := parseLog(inputInstance, withTrailingSpaces(pass1Stderr), chunk.size)
			for _, want := range reports {
				t.Run(want.instance, func(t *testing.T) {
					m, report, err := l.loudnorm(want.instance)
					if err != nil {
						t.Fatalf("loudnorm(%q) error = %v", want.instance, err)
					}
					if m != want.m || report.NormalizationType != want.mode {
						t.Errorf("loudnorm(%q) = %+v, %q, want %+v, %q",
							want.instance, m, report.NormalizationType, want.m, want.mode)
					}
				})
			}
			if got, err := l.maxMomentary(); err != nil || got != -17.6 {
				t.Errorf("maxMomentary() = %v, %v, want -17.6", got, err)
			}
		})
	}
}

func TestFFmpegLogReadsSilence(t *testing.T) {
	const stderr = `[loudnorm@stereo @ 0x55a06b1efe80]
{
	"input_i" : "-inf",
	"input_tp" : "-inf",
	"input_lra" : "0.00",
	"input_thresh" : "-70.00",
	"normalization_type" : "dynamic",
	"target_offset" : "inf"
}
`
	m, _, err := parseLog("", withTrailingSpaces(stderr), 64).loudnorm("loudnorm@stereo")
	if err != nil {
		t.Fatalf("loudnorm() error = %v", err)
	}
	if !math.IsInf(m.LUFS, -1) || !math.IsInf(m.TruePeakDBTP, -1) || m.LRALU != 0 || m.ThresholdLUFS != -70 {
		t.Errorf("loudnorm() = %+v, want -inf LUFS and dBTP, 0 LU and -70 LUFS", m)
	}
}

func TestFFmpegLogRefusesBadReports(t *testing.T) {
	report := func(body string) string {
		return "[loudnorm@stereo @ 0x1] \n{\n" + body + "\n}\n"
	}
	tests := []struct {
		name   string
		stderr string
	}{
		{"no report", "[loudnorm@mono @ 0x1] \n{\n\t\"input_i\" : \"-20.00\"\n}\n"},
		{"another filter's prefix", "[Parsed_loudnorm_0 @ 0x1] \n{\n\t\"input_i\" : \"-20.00\"\n}\n"},
		{"not JSON", report(`"input_i" : -20.00,,`)},
		{"missing field", report(`"input_i" : "-20.00", "input_tp" : "-1.00", "input_lra" : "1.00"`)},
		{"not a number", report(`"input_i" : "loud", "input_tp" : "-1.00", "input_lra" : "1.00", "input_thresh" : "-30.00"`)},
		{"nan", report(`"input_i" : "nan", "input_tp" : "-1.00", "input_lra" : "1.00", "input_thresh" : "-30.00"`)},
		{"cut off by another line", "[loudnorm@stereo @ 0x1] \n{\n\t\"input_i\" : \"-20.00\",\n" +
			"[out#0/null @ 0x2] error\n\t\"input_tp\" : \"-1.00\"\n}\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if m, _, err := parseLog("", tc.stderr, 5).loudnorm("loudnorm@stereo"); err == nil {
				t.Errorf("loudnorm() = %+v, want an error", m)
			}
		})
	}
}

func TestFFmpegLogMomentary(t *testing.T) {
	tests := []struct {
		name    string
		ebur128 string
		stderr  string
		want    float64
		wantErr bool
	}{
		{"highest", inputInstance, pass1Stderr, -17.6, false},
		{"silence", inputInstance, "[ebur128@input @ 0x1] t: 0.1 TARGET:-23 LUFS M:-120.7 S:-120.7\n", -120.7, false},
		{"another instance", "ebur128@other", pass1Stderr, 0, true},
		{"no frames", inputInstance, "[ebur128@input @ 0x1] Summary:\n", 0, true},
		{"unreadable frames", inputInstance, "[ebur128@input @ 0x1] t: 0.1 M:loud\n[ebur128@input @ 0x1] t: 0.2\n", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLog(tc.ebur128, tc.stderr, 3).maxMomentary()
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("maxMomentary() = %v, %v, want %v and an error: %t", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestCutInstance(t *testing.T) {
	tests := []struct {
		name         string
		line         string
		wantInstance string
		wantRest     string
		wantOK       bool
	}{
		{"report header", "[loudnorm@mono @ 0x55b9c1ba24c0] ", "loudnorm@mono", " ", true},
		{"frame line", "[ebur128@input @ 0x1] t: 0.1", "ebur128@input", " t: 0.1", true},
		{"another component", "[aist#0:0/pcm_s16le @ 0x2] Guessed Channel Layout: stereo",
			"aist#0:0/pcm_s16le", " Guessed Channel Layout: stereo", true},
		{"no address", "[loudnorm@mono]", "", "", false},
		{"address not hex", "[loudnorm@mono @ nowhere] x", "", "", false},
		{"unclosed", "[loudnorm@mono @ 0x1", "", "", false},
		{"report body", `	"input_i" : "-20.70",`, "", "", false},
		{"empty", "", "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			instance, rest, ok := cutInstance(tc.line)
			if instance != tc.wantInstance || rest != tc.wantRest || ok != tc.wantOK {
				t.Errorf("cutInstance(%q) = %q, %q, %t, want %q, %q, %t",
					tc.line, instance, rest, ok, tc.wantInstance, tc.wantRest, tc.wantOK)
			}
		})
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		want    float64
		wantErr bool
	}{
		{"minus infinity", "-inf", math.Inf(-1), false},
		{"infinity", "inf", math.Inf(1), false},
		{"level", "-20.70", -20.7, false},
		{"zero", "0.00", 0, false},
		{"nan", "nan", 0, true},
		{"empty", "", 0, true},
		{"text", "loud", 0, true},
		{"out of range", strings.Repeat("9", 400), 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLevel(tc.s)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("parseLevel(%q) = %v, %v, want %v and an error: %t", tc.s, got, err, tc.want, tc.wantErr)
			}
		})
	}
}
