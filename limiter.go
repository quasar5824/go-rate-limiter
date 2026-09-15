package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Limiter implements a token bucket rate limiting algorithm.
type Limiter struct {
	rate       float64
	capacity    float64
	tokens     float64
	lastUpdate time.Time
	mu         sync.Mutex
}

// NewLimiter creates a new Limiter with a given rate (tokens per second) and bucket capacity.
func NewLimiter(rate float64, capacity float64) *Limiter {
	return &Limiter{
		rate:       rate,
		capacity:    capacity,
		tokens:     capacity,
		lastUpdate: time.Now(),
	}
}

// refill updates the token count based on the time elapsed since the last update.
// Must be called while holding the lock.
func (l *Limiter) refill() {
	now := time.Now()
	elapsed := now.Sub(l.lastUpdate).Seconds()
	l.lastUpdate = now

	l.tokens += elapsed * l.rate
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}
}

// SetLimit updates the rate and capacity of the limiter.
func (l *Limiter) SetLimit(rate float64, capacity float64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.rate = rate
	l.capacity = capacity
	if l.tokens > capacity {
		l.tokens = capacity
	}
}

// Allow checks if a request is allowed based on current token availability.
func (l *Limiter) Allow() bool {
	return l.AllowN(1.0)
}

// AllowN checks if a request requiring n tokens is allowed.
func (l *Limiter) AllowN(n float64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()

	if l.tokens >= n {
		l.tokens -= n
		return true
	}

	return false
}

// TryAllow is a convenience alias for AllowN, emphasizing the non-blocking attempt.
func (l *Limiter) TryAllow(n float64) bool {
	return l.AllowN(n)
}

// AllowWithDuration checks if n tokens are available. If they are, it consumes them and returns true, 0.
// If not, it returns false and the duration to wait until n tokens would be available, without consuming tokens.
func (l *Limiter) AllowWithDuration(n float64) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()

	if l.tokens >= n {
		l.tokens -= n
		return true, 0
	}

	if l.rate <= 0 {
		return false, time.Duration(1<<63 - 1)
	}

	waitDuration := time.Duration((n - l.tokens) / l.rate * float64(time.Second))
	return false, waitDuration
}

// BatchAllow checks multiple requests and consumes tokens for those that are allowed.
// It returns a slice of booleans corresponding to the input requirements.
func (l *Limiter) BatchAllow(requests []float64) []bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()

	results := make([]bool, len(requests))
	for i, n := range requests {
		if l.tokens >= n {
			l.tokens -= n
			results[i] = true
		} else {
			results[i] = false
		}
	}
	return results
}

// Available returns the number of tokens currently available in the bucket.
func (l *Limiter) Available() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()
	return l.tokens
}

// Peek returns the number of tokens available without triggering a refill.
// This is useful for inspecting the state as of the last operation.
func (l *Limiter) Peek() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tokens
}

// Reserve returns the duration to wait until n tokens become available.
// It consumes the tokens immediately (reserves them).
func (l *Limiter) Reserve(n float64) time.Duration {
	return l.ReserveN(n)
}

// ReserveN reserves n tokens and returns the duration to wait until they are available.
func (l *Limiter) ReserveN(n float64) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()

	if l.tokens >= n {
		l.tokens -= n
		return 0
	}

	tokensNeeded := n - l.tokens
	l.tokens = 0

	if l.rate <= 0 {
		return time.Duration(1<<63 - 1) // Max duration
	}

	waitDuration := time.Duration(tokensNeeded / l.rate * float64(time.Second))
	return waitDuration
}

// Wait blocks until a token is available.
func (l *Limiter) Wait() {
	l.WaitN(context.Background(), 1.0)
}

// WaitN blocks until n tokens are available or the context is canceled.
func (l *Limiter) WaitN(ctx context.Context, n float64) {
	waitDuration := l.ReserveN(n)
	if waitDuration <= 0 {
		return
	}

	select {
	case <-ctx.Done():
		// If the context is canceled, we ideally should return the reserved tokens,
		// but the token bucket algorithm's Reserve typically consumes them upfront
		// to guarantee the slot. Returning them would require more complex state tracking.
		return
	case <-time.After(waitDuration):
	}
}

// WaitUntil blocks until the specified time is reached or the context is canceled.
func (l *Limiter) WaitUntil(ctx context.Context, target time.Time) {
	now := time.Now()
	if target.Before(now) {
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-time.After(target.Sub(now)):
	}
}