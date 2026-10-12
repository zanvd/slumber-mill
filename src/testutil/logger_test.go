package testutil

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

type outputRecorder struct {
	testing.TB
	out bytes.Buffer
}

func (r *outputRecorder) Output() io.Writer { return &r.out }

func TestLoggerWritesToTestOutput(t *testing.T) {
	rec := &outputRecorder{TB: t}
	Logger(rec).Debug("query", "rows", 1)
	if got := rec.out.String(); !strings.Contains(got, "level=DEBUG msg=query rows=1") {
		t.Errorf("test output = %q, want the debug record", got)
	}
}
