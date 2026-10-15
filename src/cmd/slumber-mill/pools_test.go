package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// plantTempFile writes a temp file into dataDir as storage names them, last modified at modTime.
func plantTempFile(t *testing.T, dataDir, name string, modTime time.Time) string {
	t.Helper()
	dir := filepath.Join(dataDir, "tmp")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, time.Time{}, modTime); err != nil {
		t.Fatal(err)
	}
	return path
}

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return err == nil
}

// recorder notes the steps that fakes take, in order, and when they took them.
type recorder struct {
	mu    sync.Mutex
	start time.Time
	steps []step
}

type step struct {
	name string
	at   time.Duration // since the recorder was made
}

func newRecorder() *recorder {
	return &recorder{start: time.Now()}
}

func (r *recorder) add(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.steps = append(r.steps, step{name: name, at: time.Since(r.start)})
}

func (r *recorder) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var names []string
	for _, s := range r.steps {
		names = append(names, s.name)
	}
	return names
}

// find returns the index and the step of the only step called name, or -1 if there isn't exactly one.
func (r *recorder) find(name string) (int, step) {
	r.mu.Lock()
	defer r.mu.Unlock()
	found, at := -1, step{}
	for i, s := range r.steps {
		if s.name != name {
			continue
		}
		if found >= 0 {
			return -1, step{}
		}
		found, at = i, s
	}
	return found, at
}

type fakeVersioner struct {
	steps           *recorder
	ffmpeg, ffprobe string
	err             error
}

func (f fakeVersioner) Version(context.Context) (string, string, error) {
	f.steps.add("version")
	return f.ffmpeg, f.ffprobe, f.err
}

type fakeSweeper struct {
	steps   *recorder
	removed int
	err     error
}

func (f fakeSweeper) SweepTemp(olderThan time.Duration) (int, error) {
	f.steps.add("sweep older than " + olderThan.String())
	return f.removed, f.err
}

// logLines decodes the JSON log lines in b.
func logLines(t *testing.T, b *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for line := range strings.Lines(b.String()) {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line isn't JSON: %q", line)
		}
		lines = append(lines, rec)
	}
	return lines
}

func TestSweepTemp(t *testing.T) {
	tests := []struct {
		name      string
		removed   int
		err       error
		wantLevel string
		wantMsg   string
	}{
		{name: "swept", removed: 3, wantLevel: "INFO", wantMsg: "swept temp files"},
		{name: "fails part way", removed: 1, err: errors.New("permission denied"), wantLevel: "WARN",
			wantMsg: "error sweeping temp files"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			steps := newRecorder()
			var b bytes.Buffer
			sweepTemp(t.Context(), slog.New(slog.NewJSONHandler(&b, nil)),
				fakeSweeper{steps: steps, removed: tt.removed, err: tt.err}, 2*time.Minute)

			if got, want := steps.names(), []string{"sweep older than 2m0s"}; !slices.Equal(got, want) {
				t.Errorf("steps = %q, want %q", got, want)
			}
			lines := logLines(t, &b)
			if len(lines) != 1 {
				t.Fatalf("logged %d lines, want 1: %v", len(lines), lines)
			}
			got := lines[0]
			if got["level"] != tt.wantLevel || got["msg"] != tt.wantMsg || got["removed"] != float64(tt.removed) ||
				got["older_than"] != "2m0s" {
				t.Errorf("logged %v, want %s %q with removed %d and older_than 2m0s", got, tt.wantLevel, tt.wantMsg,
					tt.removed)
			}
			if tt.err != nil && got["error"] != tt.err.Error() {
				t.Errorf("logged error %v, want %q", got["error"], tt.err)
			}
		})
	}
}

func TestStartEncode(t *testing.T) {
	notFound := errors.New(`ffmpeg is not installed: exec: "ffmpeg": executable file not found in $PATH`)
	tests := []struct {
		name      string
		err       error
		wantSteps []string
	}{
		{name: "ffmpeg runs", wantSteps: []string{"version", "sweep older than 2m0s"}},
		{name: "ffmpeg missing", err: notFound, wantSteps: []string{"version"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			steps := newRecorder()
			var b bytes.Buffer
			ff := fakeVersioner{steps: steps, ffmpeg: "ffmpeg version 6.1.1-3ubuntu5", ffprobe: "ffprobe version 6.1.1-3ubuntu5",
				err: tt.err}
			err := startEncode(t.Context(), slog.New(slog.NewJSONHandler(&b, nil)), ff, fakeSweeper{steps: steps},
				2*time.Minute)

			if got := steps.names(); !slices.Equal(got, tt.wantSteps) {
				t.Errorf("steps = %q, want %q", got, tt.wantSteps)
			}
			lines := logLines(t, &b)
			if tt.err != nil {
				if !errors.Is(err, tt.err) || !strings.HasPrefix(err.Error(), "the encode pool needs ffmpeg and ffprobe: ") {
					t.Errorf("startEncode() error = %v, want it to say the encode pool needs ffmpeg and wrap %v", err, tt.err)
				}
				if len(lines) != 0 {
					t.Errorf("logged %v, want nothing", lines)
				}
				return
			}
			if err != nil {
				t.Fatalf("startEncode() error = %v", err)
			}
			if len(lines) != 2 || lines[0]["msg"] != "found ffmpeg" || lines[0]["ffmpeg"] != ff.ffmpeg ||
				lines[0]["ffprobe"] != ff.ffprobe || lines[1]["msg"] != "swept temp files" {
				t.Errorf("logged %v, want the ffmpeg and ffprobe builds, then the sweep", lines)
			}
		})
	}
}

// fakeLoop stands in for the relay or a worker pool: Run blocks until ctx is done, then takes stopping to return,
// as a pool does while work in flight uses the grace.
type fakeLoop struct {
	name     string
	stopping time.Duration
	steps    *recorder
}

func (f fakeLoop) Run(ctx context.Context) {
	f.steps.add(f.name + " started")
	<-ctx.Done()
	time.Sleep(f.stopping)
	f.steps.add(f.name + " stopped")
}

type fakeClient struct {
	steps *recorder
}

func (c fakeClient) CloseIdleConnections() {
	c.steps.add("idle connections closed")
}

func TestRunWebhook(t *testing.T) {
	const (
		running = time.Hour
		grace   = 30 * time.Second
		// How long Relay.Run may take to give up its lock.
		release = 15 * time.Second
	)
	tests := []struct {
		name                  string
		relayStops, poolStops time.Duration // after the signal
	}{
		{name: "deliveries use the grace", relayStops: 0, poolStops: grace},
		{name: "releasing the relay lock takes longest", relayStops: release, poolStops: 0},
		{name: "both stop at once", relayStops: 0, poolStops: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				steps := newRecorder()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan struct{})
				go func() {
					defer close(done)
					runWebhook(ctx, fakeLoop{name: "relay", stopping: tt.relayStops, steps: steps},
						fakeLoop{name: "deliveries", stopping: tt.poolStops, steps: steps}, fakeClient{steps: steps})
					steps.add("returned")
				}()

				time.Sleep(running)
				synctest.Wait()
				if got := steps.names(); len(got) != 2 || !slices.Contains(got, "relay started") ||
					!slices.Contains(got, "deliveries started") {
					t.Errorf("steps before the signal = %q, want the relay and the deliveries started", got)
				}
				cancel()
				<-done

				want := map[string]time.Duration{
					"relay stopped":           running + tt.relayStops,
					"deliveries stopped":      running + tt.poolStops,
					"idle connections closed": running + tt.poolStops,
					"returned":                running + max(tt.relayStops, tt.poolStops),
				}
				for name, at := range want {
					if i, s := steps.find(name); i < 0 || s.at != at {
						t.Errorf("%q at %v (index %d), want it once at %v; steps %q", name, s.at, i, at, steps.names())
					}
				}
				deliveriesStopped, _ := steps.find("deliveries stopped")
				closed, _ := steps.find("idle connections closed")
				returned, _ := steps.find("returned")
				if closed < deliveriesStopped {
					t.Errorf("idle connections closed before the deliveries stopped; steps %q", steps.names())
				}
				if returned != len(steps.names())-1 {
					t.Errorf("runWebhook returned before every loop stopped; steps %q", steps.names())
				}
			})
		})
	}
}

func TestWorkerID(t *testing.T) {
	host, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := workerID(), host+":"+strconv.Itoa(os.Getpid()); got != want {
		t.Errorf("workerID() = %q, want %q", got, want)
	}
}
