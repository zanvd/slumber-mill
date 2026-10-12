package testutil

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
)

// fatalRecorder stands in for a test so a helper's Fatal or Skip can be observed instead of ending the real test.
type fatalRecorder struct {
	testing.TB
	fatal   string
	skipped bool
}

func (r *fatalRecorder) Helper() {}

func (r *fatalRecorder) Fatal(args ...any) {
	r.fatal = fmt.Sprint(args...)
	runtime.Goexit()
}

func (r *fatalRecorder) Fatalf(format string, args ...any) {
	r.fatal = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func (r *fatalRecorder) FailNow() {
	r.fatal = "FailNow"
	runtime.Goexit()
}

func (r *fatalRecorder) Skip(...any)          { r.SkipNow() }
func (r *fatalRecorder) Skipf(string, ...any) { r.SkipNow() }

func (r *fatalRecorder) SkipNow() {
	r.skipped = true
	runtime.Goexit()
}

func TestNewDatabaseFailsWithoutMySQL(t *testing.T) {
	tests := []struct {
		name      string
		dsn       string
		wantFatal string
	}{
		{"unset", "", "SLUMBER_MILL_DSN is not set"},
		{"blank", "  ", "SLUMBER_MILL_DSN is not set"},
		{"malformed", "no slash", "error parsing SLUMBER_MILL_DSN"},
		{"strict parameter", "u:p@tcp(127.0.0.1:1)/?strict=true", "error parsing SLUMBER_MILL_DSN"},
		{"unreachable", "u:p@tcp(127.0.0.1:1)/?timeout=2s", "error connecting to MySQL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(dsnEnv, tc.dsn)
			rec := &fatalRecorder{TB: t}
			done := make(chan struct{})
			go func() {
				defer close(done)
				NewDatabase(rec)
			}()
			<-done

			if rec.skipped {
				t.Fatal("NewDatabase skipped the test, want a failure")
			}
			if !strings.Contains(rec.fatal, tc.wantFatal) {
				t.Errorf("NewDatabase failed with %q, want it to contain %q", rec.fatal, tc.wantFatal)
			}
		})
	}
}
