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

// Reserve returns the duration to wait until n tokens become available.
// It consumes the tokens immediately (reserves them).
func (l *Limiter) Reserve(n float64) time.Duration {
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
	for {
		l.mu.Lock()
		l.refill()

		if l.tokens >= n {
			l.tokens -= n
			l.mu.Unlock()
			return
		}

		// Calculate time to wait for the remaining tokens
		tokensNeeded := n - l.tokens
		l.mu.Unlock()

		if l.rate <= 0 {
			// If rate is 0, we can never refill. Wait a bit and retry to see if rate changes
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
		}

		waitDuration := time.Duration(tokensNeeded / l.rate * float64(time.Second))
		select {
		case <-ctx.Done():
			return
		case <-time.After(waitDuration):
		}
	}
}