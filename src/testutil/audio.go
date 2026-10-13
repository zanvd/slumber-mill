package testutil

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Audio fixtures are 48 kHz pcm_s16le WAV files, generated with ffmpeg from fixed noise seeds
// into a directory removed when tb ends. Use two seeds and amerge for real stereo:
// anoisesrc gives one channel, and upmixing it gives dual-mono.

const noiseDuration = 10 * time.Second

// DualMonoPink is pink noise with the same signal in both channels, like most narration.
func DualMonoPink(tb testing.TB) string {
	tb.Helper()
	return audioFixture(tb, "dual-mono-pink.wav", []string{noise("pink", 0.5, 1, noiseDuration)}, "", "-ac", "2")
}

// UncorrelatedPink is stereo pink noise with a different seed per channel, like a field recording.
func UncorrelatedPink(tb testing.TB) string {
	tb.Helper()
	return audioFixture(tb, "uncorrelated-pink.wav",
		[]string{noise("pink", 0.5, 1, noiseDuration), noise("pink", 0.5, 2, noiseDuration)},
		"[0:a][1:a]amerge=inputs=2")
}

// UncorrelatedWhite is stereo white noise with a different seed per channel. Its LRA measures 0.
func UncorrelatedWhite(tb testing.TB) string {
	tb.Helper()
	return audioFixture(tb, "uncorrelated-white.wav",
		[]string{noise("white", 0.25, 3, noiseDuration), noise("white", 0.25, 4, noiseDuration)},
		"[0:a][1:a]amerge=inputs=2")
}

// MonoPink is one channel of pink noise.
func MonoPink(tb testing.TB) string {
	tb.Helper()
	return audioFixture(tb, "mono-pink.wav", []string{noise("pink", 0.5, 1, noiseDuration)}, "")
}

// Surround is 5.1 pink noise with a different seed per channel.
func Surround(tb testing.TB) string {
	tb.Helper()
	var inputs []string
	for seed := 31; seed <= 36; seed++ {
		inputs = append(inputs, noise("pink", 0.3, seed, noiseDuration))
	}
	return audioFixture(tb, "surround.wav", inputs, "[0:a][1:a][2:a][3:a][4:a][5:a]join=inputs=6:channel_layout=5.1")
}

// Thunder is 60 s of quiet stereo rain with a 50 ms crack at the midpoint,
// which makes loudnorm fall back to dynamic mode.
func Thunder(tb testing.TB) string {
	tb.Helper()
	const bed = 60 * time.Second
	return audioFixture(tb, "thunder.wav",
		[]string{
			noise("pink", 0.04, 21, bed),
			noise("pink", 0.04, 22, bed),
			noise("white", 0.95, 23, 50*time.Millisecond),
		},
		fmt.Sprintf("[0:a][1:a]amerge=inputs=2[rain];"+
			"[2:a]afade=t=out:d=0.05:curve=exp,aformat=channel_layouts=stereo,adelay=%d:all=1[crack];"+
			"[rain][crack]amix=inputs=2:duration=first:normalize=0", (bed/2).Milliseconds()))
}

// PinkClip is mono pink noise lasting d.
func PinkClip(tb testing.TB, d time.Duration) string {
	tb.Helper()
	return audioFixture(tb, "pink-clip.wav", []string{noise("pink", 0.5, 1, d)}, "")
}

// Silence is 10 s of digital silence in stereo.
func Silence(tb testing.TB) string {
	tb.Helper()
	return audioFixture(tb, "silence.wav", []string{"anullsrc=channel_layout=stereo:sample_rate=48000"}, "",
		"-t", seconds(noiseDuration))
}

// NotAudio is a text file named like a WAV file.
func NotAudio(tb testing.TB) string {
	tb.Helper()
	return writeFixture(tb, filepath.Join(tb.TempDir(), "text.wav"), "this is not audio\n")
}

// HLSPlaylist writes an HLS playlist to dir, named .m3u8 so ffmpeg detects it, whose one segment is segment:
// a path relative to dir, or a URL.
func HLSPlaylist(tb testing.TB, dir, segment string) string {
	tb.Helper()
	return writeFixture(tb, filepath.Join(dir, "playlist.m3u8"),
		"#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXTINF:10,\n"+segment+"\n#EXT-X-ENDLIST\n")
}

// ConcatList writes an ffconcat list to dir that plays file, a path relative to dir.
func ConcatList(tb testing.TB, dir, file string) string {
	tb.Helper()
	return writeFixture(tb, filepath.Join(dir, "list.ffconcat"), "ffconcat version 1.0\nfile '"+file+"'\n")
}

func writeFixture(tb testing.TB, path, content string) string {
	tb.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		tb.Fatalf("error writing %s: %v", path, err)
	}
	return path
}

func noise(color string, amplitude float64, seed int, d time.Duration) string {
	return fmt.Sprintf("anoisesrc=color=%s:amplitude=%s:seed=%d:sample_rate=48000:duration=%s",
		color, strconv.FormatFloat(amplitude, 'f', -1, 64), seed, seconds(d))
}

func seconds(d time.Duration) string { return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) }

// audioFixture renders the lavfi inputs, joined by filter if it isn't empty, to a WAV file named name.
func audioFixture(tb testing.TB, name string, inputs []string, filter string, extra ...string) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), name)
	args := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-y"}
	for _, in := range inputs {
		args = append(args, "-f", "lavfi", "-i", in)
	}
	if filter != "" {
		args = append(args, "-filter_complex", filter)
	}
	args = append(args, extra...)
	args = append(args, "-c:a", "pcm_s16le", path)
	out, err := exec.CommandContext(tb.Context(), "ffmpeg", args...).CombinedOutput()
	if err != nil {
		tb.Fatalf("error generating the audio fixture %s; audio tests need ffmpeg: %v\n%s", name, err, out)
	}
	return path
}
