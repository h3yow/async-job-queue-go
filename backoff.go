package jobqueue

import "time"

// Backoff computes how long to wait before redelivering a job after its
// attempt-th delivery failed. attempt is 1-based.
type Backoff interface {
	Delay(attempt int) time.Duration
}

// ExponentialBackoff returns min(MaxDelay, BaseDelay*2^(attempt-1)).
// When Jitter is true, a deterministic-per-job small factor may be applied by
// the caller; this implementation itself returns the capped raw value to keep
// tests reproducible.
type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// Delay returns min(MaxDelay, BaseDelay*2^(attempt-1)), with a zero attempt
// (startup redelivery) treated as the first attempt.
func (b ExponentialBackoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.BaseDelay
	for i := 1; i < attempt; i++ {
		d *= 2
		if b.MaxDelay > 0 && d >= b.MaxDelay {
			return b.MaxDelay
		}
	}
	if b.MaxDelay > 0 && d > b.MaxDelay {
		d = b.MaxDelay
	}
	return d
}
