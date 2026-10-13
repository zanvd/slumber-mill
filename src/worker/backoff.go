package worker

import (
	"math/rand/v2"
	"time"
)

// Backoff is how long to wait after a failed attempt (counted from 1) before the next one:
// exponential with equal jitter, a random value in [d/2, d] where d = min(base·2^(attempt-1), maxDelay).
func Backoff(attempt int, base, maxDelay time.Duration) time.Duration {
	return backoff(attempt, base, maxDelay, rand.Int64N)
}

func backoff(attempt int, base, maxDelay time.Duration, randN func(int64) int64) time.Duration {
	d := min(base, maxDelay)
	if d <= 0 {
		return 0
	}
	for i := 1; i < attempt && d < maxDelay; i++ {
		if d > maxDelay/2 {
			d = maxDelay
		} else {
			d *= 2
		}
	}
	half := d / 2
	return half + time.Duration(randN(int64(d-half)+1))
}

// jitter returns a random duration in [d/2, 3d/2), so a fleet of workers doesn't act in lockstep.
func jitter(d time.Duration, randN func(int64) int64) time.Duration {
	if d <= 0 {
		return 0
	}
	return d/2 + time.Duration(randN(int64(d)))
}
