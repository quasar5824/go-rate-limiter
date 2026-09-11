package ratelimit

import (
	"testing"
	"time"
)

func TestLimiter_Allow(t *testing.T) {
	// Rate of 10 tokens per second, capacity of 5
	l := NewLimiter(10, 5)

	// Use up initial capacity
	for i := 0; i < 5; i++ {
		if !l.Allow() {
			t.Errorf("Request %d should have been allowed", i+1)
		}
	}

	// 6th request should be denied immediately
	if l.Allow() {
		t.Error("Request 6 should have been denied")
	}

	// Wait for some tokens to refill (0.2s * 10 tokens/s = 2 tokens)
	time.Sleep(200 * time.Millisecond)

	if !l.Allow() {
		t.Error("Request after refill should be allowed")
	}
	if !l.Allow() {
		t.Error("Second request after refill should be allowed")
	}
	if l.Allow() {
		t.Error("Third request after refill should be denied")
	}
}
