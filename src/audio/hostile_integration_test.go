//go:build integration

package audio

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/zanvd/slumber-mill/src/testutil"
)

// watchOpens returns a function that reports whether path was opened or read since it was last called.
func watchOpens(t *testing.T, path string) func(testing.TB) bool {
	t.Helper()
	fd, err := syscall.InotifyInit1(syscall.IN_NONBLOCK | syscall.IN_CLOEXEC)
	if err != nil {
		t.Fatalf("error starting inotify: %v", err)
	}
	t.Cleanup(func() { syscall.Close(fd) })
	if _, err := syscall.InotifyAddWatch(fd, path, syscall.IN_OPEN|syscall.IN_ACCESS); err != nil {
		t.Fatalf("error watching %s: %v", path, err)
	}
	return func(tb testing.TB) bool {
		tb.Helper()
		buf := make([]byte, 4096)
		n, err := syscall.Read(fd, buf)
		if errors.Is(err, syscall.EAGAIN) {
			return false
		}
		if err != nil {
			tb.Fatalf("error reading inotify events: %v", err)
		}
		return n > 0
	}
}

type command struct {
	step string
	name string
	args []string
}

// commands are the ffprobe and ffmpeg runs that read an input, as Process builds them.
func commands(t *testing.T, src string) []command {
	outputs := tempOutputs(t)
	layouts := planLayouts(2, outputs)
	measured := []Loudness{stereoMeasured, monoMeasured}
	return []command{
		{"probe", "ffprobe", probeArgs(src)},
		{"measure", "ffmpeg", measureArgs(src, testTargetLUFS, layouts, false)},
		{"render", "ffmpeg", renderArgs(src, testTargetLUFS, layouts, measured, []float64{0, 0}, outputs)},
		{"verify", "ffmpeg", verifyArgs(src, testTargetLUFS)},
	}
}

// probeUnrestricted runs ffprobe on src without the whitelists and returns the first codec it lists.
func probeUnrestricted(t *testing.T, src string) string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "ffprobe", "-v", "error",
		"-select_streams", "a:0", "-show_entries", "stream=codec_name", "-of", "csv=p=0", src).Output()
	if err != nil {
		t.Fatalf("error probing %s without the whitelists: %v", src, err)
	}
	codec, _, _ := strings.Cut(strings.TrimSpace(string(out)), "\n")
	return codec
}

func TestProcessRefusesPlaylists(t *testing.T) {
	tests := []struct {
		name     string
		playlist func(tb testing.TB, dir, file string) string
	}{
		{"hls", testutil.HLSPlaylist},
		{"concat", testutil.ConcatList},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target := testutil.UncorrelatedPink(t)
			playlist := tc.playlist(t, filepath.Dir(target), filepath.Base(target))
			opened := watchOpens(t, target)

			// Unrestricted, ffprobe follows the playlist and reports the target's audio as the upload's.
			codec := probeUnrestricted(t, playlist)
			if read := opened(t); codec != "pcm_s16le" || !read {
				t.Fatalf("without the whitelists, ffprobe found %q and opened the target: %t; want pcm_s16le and true",
					codec, read)
			}

			_, err := New().Process(testContext(t), playlist, testTargetLUFS, tempOutputs(t))
			t.Logf("Process() error = %v", err)
			if rej, ok := errors.AsType[*Rejection](err); !ok || rej.Message != MessageNotAudio {
				t.Errorf("Process() error = %v, want the rejection %q", err, MessageNotAudio)
			}
			if opened(t) {
				t.Errorf("Process() opened %s through the playlist", target)
			}

			for _, c := range commands(t, playlist) {
				t.Run(c.step, func(t *testing.T) {
					err := execRunner{}.Run(t.Context(), c.name, c.args, nil, nil)
					if _, ok := errors.AsType[*exitError](err); !ok {
						t.Errorf("%s error = %v, want it to refuse the playlist", c.name, err)
					}
					if opened(t) {
						t.Errorf("%s opened %s through the playlist", c.name, target)
					}
				})
			}
		})
	}
}

func TestProcessRefusesURLs(t *testing.T) {
	wav, err := os.ReadFile(testutil.UncorrelatedPink(t))
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "audio/wav")
		w.Write(wav)
	}))
	defer srv.Close()
	url := srv.URL + "/upload.wav"

	// Unrestricted, ffprobe fetches the URL.
	if codec := probeUnrestricted(t, url); codec != "pcm_s16le" || requests.Load() == 0 {
		t.Fatalf("without the whitelists, ffprobe found %q after %d requests; want pcm_s16le and some",
			codec, requests.Load())
	}
	requests.Store(0)

	for _, c := range commands(t, url) {
		t.Run(c.step, func(t *testing.T) {
			err := execRunner{}.Run(t.Context(), c.name, c.args, nil, nil)
			if _, ok := errors.AsType[*exitError](err); !ok {
				t.Errorf("%s error = %v, want it to refuse the URL", c.name, err)
			}
			if n := requests.Swap(0); n != 0 {
				t.Errorf("%s made %d requests, want none", c.name, n)
			}
		})
	}

	t.Run("playlist", func(t *testing.T) {
		playlist := testutil.HLSPlaylist(t, t.TempDir(), url)
		_, err := New().Process(testContext(t), playlist, testTargetLUFS, tempOutputs(t))
		t.Logf("Process() error = %v", err)
		if rej, ok := errors.AsType[*Rejection](err); !ok || rej.Message != MessageNotAudio {
			t.Errorf("Process() error = %v, want the rejection %q", err, MessageNotAudio)
		}
		if n := requests.Load(); n != 0 {
			t.Errorf("Process() made %d requests, want none", n)
		}
	})
}

// TestProcessIgnoresExternalTracks guards the mov demuxer's default: it reads a track that a "dref" alias puts
// in another file only when enable_drefs is set. The whitelists don't stop it, since that file is a local one,
// and the option can't be pinned off, because ffmpeg fails other inputs, such as WAV, that don't know it.
func TestProcessIgnoresExternalTracks(t *testing.T) {
	target := testutil.UncorrelatedPink(t)
	m4a := testutil.ExternalTrackM4A(t, filepath.Dir(target), filepath.Base(target))
	opened := watchOpens(t, target)

	// With external tracks enabled, ffprobe opens the target, whitelists and all.
	enabled := append([]string{"-enable_drefs", "1"}, probeArgs(m4a)...)
	err := execRunner{}.Run(t.Context(), "ffprobe", enabled, nil, nil)
	if read := opened(t); !read {
		t.Fatalf("with -enable_drefs 1, ffprobe (error %v) didn't open %s; want the fixture to point at it", err, target)
	}

	for _, c := range commands(t, m4a) {
		t.Run(c.step, func(t *testing.T) {
			var stderr bytes.Buffer
			err := execRunner{}.Run(t.Context(), c.name, c.args, nil, &stderr)
			t.Logf("%s error = %v", c.name, err)
			if opened(t) {
				t.Errorf("%s opened %s, the track's external file", c.name, target)
			}
			// ffprobe runs at -v error, which hides this warning.
			if c.name == "ffmpeg" && !strings.Contains(stderr.String(), "Skipped opening external track") {
				t.Errorf("ffmpeg didn't log that it skipped the external track:\n%s", &stderr)
			}
		})
	}

	t.Run("process", func(t *testing.T) {
		_, err := New().Process(testContext(t), m4a, testTargetLUFS, tempOutputs(t))
		t.Logf("Process() error = %v", err)
		// The track has no samples it may read, so it measures as silence.
		if rej, ok := errors.AsType[*Rejection](err); !ok || rej.Message != MessageSilent {
			t.Errorf("Process() error = %v, want the rejection %q", err, MessageSilent)
		}
		if opened(t) {
			t.Errorf("Process() opened %s, the track's external file", target)
		}
	})
}
