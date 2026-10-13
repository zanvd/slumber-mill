package audio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

const (
	waitDelay       = 5 * time.Second
	stderrTailLines = 3
	maxLineBytes    = 64 << 10
)

// runner runs a command to completion. stdout and stderr may be nil.
// A command that exits with a non-zero status returns an *exitError,
// and one whose context ends returns ctx.Err().
type runner interface {
	Run(ctx context.Context, name string, args []string, stdout, stderr io.Writer) error
}

// exitError reports a command that ran and failed. code is -1 when a signal ended it.
type exitError struct {
	name   string
	code   int
	status string
	stderr string
}

func (e *exitError) Error() string {
	if e.stderr == "" {
		return fmt.Sprintf("%s failed: %s", e.name, e.status)
	}
	return fmt.Sprintf("%s failed: %s: %s", e.name, e.status, e.stderr)
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, name string, args []string, stdout, stderr io.Writer) error {
	tail := &tailLines{max: stderrTailLines}
	tailWriter := &lineWriter{fn: tail.add}
	cmd := newCommand(ctx, name, args)
	cmd.Stdout = stdout
	cmd.Stderr = tailWriter
	if stderr != nil {
		cmd.Stderr = io.MultiWriter(stderr, tailWriter)
	}
	err := cmd.Run()
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		tailWriter.flush()
		return &exitError{name: name, code: exitErr.ExitCode(), status: exitErr.String(), stderr: tail.String()}
	}
	return fmt.Errorf("error running %s: %w", name, err)
}

func newCommand(ctx context.Context, name string, args []string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = waitDelay
	// A process group of its own, so a terminal's Ctrl-C reaches only the worker, which drains first.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}

// lineWriter calls fn with each line written to it, without its line ending.
// It cuts lines longer than maxLineBytes short, and flush passes on a last line that has no newline.
type lineWriter struct {
	fn      func(line string)
	partial []byte
}

func (w *lineWriter) Write(p []byte) (int, error) {
	n := len(p)
	for {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			w.keep(p)
			return n, nil
		}
		w.keep(p[:i])
		w.emit()
		p = p[i+1:]
	}
}

func (w *lineWriter) keep(p []byte) {
	room := maxLineBytes - len(w.partial)
	w.partial = append(w.partial, p[:min(len(p), max(room, 0))]...)
}

func (w *lineWriter) emit() {
	line := strings.TrimSuffix(string(w.partial), "\r")
	w.partial = w.partial[:0]
	w.fn(line)
}

func (w *lineWriter) flush() {
	if len(w.partial) > 0 {
		w.emit()
	}
}

// tailLines keeps the last max lines that aren't blank.
type tailLines struct {
	max   int
	lines []string
}

func (t *tailLines) add(line string) {
	if strings.TrimSpace(line) == "" {
		return
	}
	if len(t.lines) == t.max {
		t.lines = append(t.lines[:0], t.lines[1:]...)
	}
	t.lines = append(t.lines, line)
}

func (t *tailLines) String() string {
	return strings.Join(t.lines, "; ")
}
