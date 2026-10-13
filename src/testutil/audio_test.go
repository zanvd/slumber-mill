package testutil

import (
	"strings"
	"testing"
)

func TestAudioFixtureFailsWithoutFFmpeg(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	rec := &fatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		DualMonoPink(rec)
	}()
	<-done

	if rec.skipped {
		t.Fatal("DualMonoPink skipped the test, want a failure")
	}
	if !strings.Contains(rec.fatal, "audio tests need ffmpeg") {
		t.Errorf("DualMonoPink failed with %q, want it to say ffmpeg is needed", rec.fatal)
	}
}
