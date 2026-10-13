//go:build integration

package audio

import (
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

// watchOpens reports, each time it is called, whether path was opened or read since the last call.
func watchOpens(t *testing.T, path string) func() bool {
	t.Helper()
	fd, err := syscall.InotifyInit1(syscall.IN_NONBLOCK | syscall.IN_CLOEXEC)
	if err != nil {
		t.Fatalf("error starting inotify: %v", err)
	}
	t.Cleanup(func() { syscall.Close(fd) })
	if _, err := syscall.InotifyAddWatch(fd, path, syscall.IN_OPEN|syscall.IN_ACCESS); err != nil {
		t.Fatalf("error watching %s: %v", path, err)
	}
	return func() bool {
		buf := make([]byte, 4096)
		n, err := syscall.Read(fd, buf)
		if errors.Is(err, syscall.EAGAIN) {
			return false
		}
		if err != nil {
			t.Fatalf("error reading inotify events: %v", err)
		}
		return n > 0
	}
}

type command struct {
	name string
	args []string
}

// commands are the ffprobe and ffmpeg runs that read an input, as Process builds them.
func commands(t *testing.T, src string) map[string]command {
	outputs := tempOutputs(t)
	layouts := planLayouts(2, outputs)
	measured := []Loudness{stereoMeasured, monoMeasured}
	return map[string]command{
		"probe":   {"ffprobe", probeArgs(src)},
		"measure": {"ffmpeg", measureArgs(src, testTargetLUFS, layouts, false)},
		"render":  {"ffmpeg", renderArgs(src, testTargetLUFS, layouts, measured, []float64{0, 0}, outputs)},
		"verify":  {"ffmpeg", verifyArgs(src, testTargetLUFS)},
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
			if read := opened(); codec != "pcm_s16le" || !read {
				t.Fatalf("without the whitelists, ffprobe found %q and opened the target: %t; want pcm_s16le and true",
					codec, read)
			}

			_, err := New().Process(testContext(t), playlist, testTargetLUFS, tempOutputs(t))
			t.Logf("Process() error = %v", err)
			if rej, ok := errors.AsType[*Rejection](err); !ok || rej.Message != MessageNotAudio {
				t.Errorf("Process() error = %v, want the rejection %q", err, MessageNotAudio)
			}
			if opened() {
				t.Errorf("Process() opened %s through the playlist", target)
			}

			for name, c := range commands(t, playlist) {
				err := execRunner{}.Run(t.Context(), c.name, c.args, nil, nil)
				if _, ok := errors.AsType[*exitError](err); !ok {
					t.Errorf("%s: %s error = %v, want it to refuse the playlist", name, c.name, err)
				}
				if opened() {
					t.Errorf("%s: %s opened %s through the playlist", name, c.name, target)
				}
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

	for name, c := range commands(t, url) {
		err := execRunner{}.Run(t.Context(), c.name, c.args, nil, nil)
		if _, ok := errors.AsType[*exitError](err); !ok {
			t.Errorf("%s: %s error = %v, want it to refuse the URL", name, c.name, err)
		}
	}

	playlist := testutil.HLSPlaylist(t, t.TempDir(), url)
	_, err = New().Process(testContext(t), playlist, testTargetLUFS, tempOutputs(t))
	t.Logf("Process() error = %v", err)
	if rej, ok := errors.AsType[*Rejection](err); !ok || rej.Message != MessageNotAudio {
		t.Errorf("Process() error = %v, want the rejection %q", err, MessageNotAudio)
	}

	if n := requests.Load(); n != 0 {
		t.Errorf("the server got %d requests, want none", n)
	}
}
