package audio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const helperEnv = "SLUMBER_MILL_AUDIO_HELPER"

// TestHelperProcess isn't a test: the runner tests start the test binary again to run it as their child.
func TestHelperProcess(t *testing.T) {
	mode := os.Getenv(helperEnv)
	if mode == "" {
		return
	}
	switch mode {
	case "ok":
		fmt.Fprint(os.Stdout, "out")
		fmt.Fprint(os.Stderr, "first\nsecond\n")
		os.Exit(0)
	case "fail":
		fmt.Fprint(os.Stderr, "one\ntwo\nthree\n\nfour\r\n  \nfive")
		os.Exit(3)
	case "block":
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGUSR1)
		fmt.Fprintln(os.Stderr, "ready")
		<-stop
		os.Exit(0)
	case "pgid":
		fmt.Fprintf(os.Stdout, "%d %d", os.Getpid(), syscall.Getpgrp())
		os.Exit(0)
	}
	os.Exit(2)
}

func runHelper(t *testing.T, ctx context.Context, mode string, stdout, stderr io.Writer) error {
	t.Helper()
	t.Setenv(helperEnv, mode)
	return execRunner{}.Run(ctx, os.Args[0], []string{"-test.run=^TestHelperProcess$"}, stdout, stderr)
}

func TestExecRunnerSucceeds(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := runHelper(t, t.Context(), "ok", &stdout, &stderr); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if stdout.String() != "out" || stderr.String() != "first\nsecond\n" {
		t.Errorf("Run() wrote stdout %q and stderr %q, want %q and %q", &stdout, &stderr, "out", "first\nsecond\n")
	}
}

func TestExecRunnerReturnsTheTailOfStderr(t *testing.T) {
	var stderr bytes.Buffer
	err := runHelper(t, t.Context(), "fail", nil, &stderr)

	exitErr, ok := errors.AsType[*exitError](err)
	if !ok {
		t.Fatalf("Run() error = %v, want an *exitError", err)
	}
	if exitErr.code != 3 || exitErr.stderr != "three; four; five" {
		t.Errorf("Run() error has status %d and stderr %q, want 3 and %q", exitErr.code, exitErr.stderr, "three; four; five")
	}
	if want := "exit status 3: three; four; five"; !strings.HasSuffix(err.Error(), want) {
		t.Errorf("Run() error = %q, want it to end with %q", err, want)
	}
	if !strings.HasPrefix(stderr.String(), "one\ntwo\n") {
		t.Errorf("Run() passed stderr %q on, want all of it", &stderr)
	}
}

func TestExecRunnerReturnsTheContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// Cancel once the child has started, so it is killed mid-run rather than never started.
	stderr := &lineWriter{fn: func(line string) {
		if line == "ready" {
			cancel()
		}
	}}
	err := runHelper(t, ctx, "block", nil, stderr)
	if err != context.Canceled {
		t.Errorf("Run() error = %v, want context.Canceled itself", err)
	}
}

func TestExecRunnerStartsItsOwnProcessGroup(t *testing.T) {
	var stdout bytes.Buffer
	if err := runHelper(t, t.Context(), "pgid", &stdout, nil); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	fields := strings.Fields(stdout.String())
	if len(fields) != 2 {
		t.Fatalf("the child printed %q, want its pid and process group", &stdout)
	}
	pid, _ := strconv.Atoi(fields[0])
	pgid, _ := strconv.Atoi(fields[1])
	if pgid != pid || pgid == syscall.Getpgrp() {
		t.Errorf("the child %d ran in process group %d, want its own (the test's is %d)", pid, pgid, syscall.Getpgrp())
	}
}

func TestExecRunnerMissingCommand(t *testing.T) {
	err := execRunner{}.Run(t.Context(), "slumber-mill-no-such-command", nil, nil, nil)
	if !errors.Is(err, exec.ErrNotFound) {
		t.Errorf("Run() error = %v, want exec.ErrNotFound", err)
	}
	if _, ok := errors.AsType[*exitError](err); ok {
		t.Errorf("Run() error = %v, want it not to be an *exitError", err)
	}
}

func TestNewCommand(t *testing.T) {
	cmd := newCommand(t.Context(), "ffmpeg", []string{"-version"})
	if cmd.WaitDelay != 5*time.Second {
		t.Errorf("WaitDelay = %v, want 5s", cmd.WaitDelay)
	}
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Errorf("SysProcAttr = %+v, want Setpgid", cmd.SysProcAttr)
	}
}

func TestLineWriter(t *testing.T) {
	long := strings.Repeat("x", maxLineBytes+10)
	tests := []struct {
		name   string
		writes []string
		want   []string
	}{
		{"whole lines", []string{"a\nb\n"}, []string{"a", "b"}},
		{"split across writes", []string{"ab", "c\nd", "e\n"}, []string{"abc", "de"}},
		{"carriage returns", []string{"a\r\nb\r\n"}, []string{"a", "b"}},
		{"blank lines", []string{"\n\na\n"}, []string{"", "", "a"}},
		{"last line without newline", []string{"a\nb"}, []string{"a", "b"}},
		{"long line", []string{long[:100], long[100:] + "\nb\n"}, []string{long[:maxLineBytes], "b"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			w := &lineWriter{fn: func(line string) { got = append(got, line) }}
			for _, s := range tc.writes {
				if n, err := w.Write([]byte(s)); n != len(s) || err != nil {
					t.Fatalf("Write() = %d, %v, want %d, nil", n, err, len(s))
				}
			}
			w.flush()
			if !slices.Equal(got, tc.want) {
				t.Errorf("lines = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTailLines(t *testing.T) {
	tail := &tailLines{max: 3}
	for _, line := range []string{"one", "two", "", "three", "four", "   "} {
		tail.add(line)
	}
	if got := tail.String(); got != "two; three; four" {
		t.Errorf("tail = %q, want %q", got, "two; three; four")
	}
}
