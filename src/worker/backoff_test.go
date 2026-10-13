package worker

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

func lowest(int64) int64    { return 0 }
func highest(n int64) int64 { return n - 1 }

func TestBackoffBounds(t *testing.T) {
	const (
		encodeBase  = 30 * time.Second
		encodeMax   = 10 * time.Minute
		webhookBase = 30 * time.Second
		webhookMax  = 6 * time.Hour
	)
	tests := []struct {
		name      string
		attempt   int
		base, max time.Duration
		d         time.Duration
	}{
		{"encode attempt 1", 1, encodeBase, encodeMax, 30 * time.Second},
		{"encode attempt 2", 2, encodeBase, encodeMax, time.Minute},
		{"encode attempt 3", 3, encodeBase, encodeMax, 2 * time.Minute},
		{"encode attempt 5", 5, encodeBase, encodeMax, 8 * time.Minute},
		{"encode attempt 6 capped", 6, encodeBase, encodeMax, encodeMax},
		{"webhook attempt 1", 1, webhookBase, webhookMax, 30 * time.Second},
		{"webhook attempt 10", 10, webhookBase, webhookMax, 256 * time.Minute},
		{"webhook attempt 11 capped", 11, webhookBase, webhookMax, webhookMax},
		{"webhook attempt 12 capped", 12, webhookBase, webhookMax, webhookMax},
		{"attempt 0 as 1", 0, encodeBase, encodeMax, encodeBase},
		{"negative attempt as 1", -3, encodeBase, encodeMax, encodeBase},
		{"huge attempt capped", math.MaxInt, encodeBase, encodeMax, encodeMax},
		{"huge max doesn't overflow", 200, time.Second, math.MaxInt64, math.MaxInt64},
		{"base above max", 1, time.Hour, time.Minute, time.Minute},
		{"odd max", 3, 3 * time.Nanosecond, 11 * time.Nanosecond, 11 * time.Nanosecond},
		{"zero base", 5, 0, encodeMax, 0},
		{"negative base", 5, -time.Second, encodeMax, 0},
		{"zero max", 5, encodeBase, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := backoff(tc.attempt, tc.base, tc.max, lowest), tc.d/2; got != want {
				t.Errorf("lowest backoff = %v, want d/2 = %v", got, want)
			}
			if got := backoff(tc.attempt, tc.base, tc.max, highest); got != tc.d {
				t.Errorf("highest backoff = %v, want d = %v", got, tc.d)
			}
		})
	}
}

func TestBackoffStaysInRange(t *testing.T) {
	base, maxDelay := 30*time.Second, 6*time.Hour
	for attempt := 1; attempt <= 12; attempt++ {
		d := min(base<<(attempt-1), maxDelay)
		for range 1000 {
			if got := Backoff(attempt, base, maxDelay); got < d/2 || got > d {
				t.Fatalf("Backoff(%d) = %v, want within [%v, %v]", attempt, got, d/2, d)
			}
		}
	}
}

func TestJitter(t *testing.T) {
	const d = time.Second
	if got := jitter(d, lowest); got != d/2 {
		t.Errorf("lowest jitter = %v, want %v", got, d/2)
	}
	if got := jitter(d, highest); got != 3*d/2-1 {
		t.Errorf("highest jitter = %v, want %v", got, 3*d/2-1)
	}
	if got := jitter(0, highest); got != 0 {
		t.Errorf("jitter(0) = %v, want 0", got)
	}
	for range 1000 {
		if got := jitter(d, rand.Int64N); got < d/2 || got >= 3*d/2 {
			t.Fatalf("jitter(%v) = %v, want within [%v, %v)", d, got, d/2, 3*d/2)
		}
	}
}
