package ratelimit

import (
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

// Allow checks if a request is allowed based on current token availability.
func (l *Limiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.lastUpdate).Seconds()
	l.lastUpdate = now

	// Refill tokens based on elapsed time
	l.tokens += elapsed * l.rate
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}

	if l.tokens >= 1.0 {
		l.tokens -= 1.0
		return true
	}

	return false
}

// GetTokens returns the current number of tokens in the bucket.
func (l *Limiter) GetTokens() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tokens
}