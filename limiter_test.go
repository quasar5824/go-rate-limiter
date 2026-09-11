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

func TestLimiter_AllowN(t *testing.T) {
	l := NewLimiter(10, 5)

	// Request 3 tokens
	if !l.AllowN(3.0) {
		t.Error("Request for 3 tokens should have been allowed")
	}

	// Request 3 more tokens (only 2 left)
	if l.AllowN(3.0) {
		t.Error("Request for 3 tokens should have been denied")
	}

	// Request 2 tokens
	if !l.AllowN(2.0) {
		t.Error("Request for 2 tokens should have been allowed")
	}
}

func TestLimiter_Wait(t *testing.T) {
	l := NewLimiter(10, 1)

	// First request should be immediate
	start := time.Now()
	l.Wait()
	if time.Since(start) > 100*time.Millisecond {
		t.Errorf("First Wait took too long: %v", time.Since(start))
	}

	// Second request should wait ~100ms (1 token / 10 tps)
	start = time.Now()
	l.Wait()
	elapsed := time.Since(start)
	if elapsed < 80*time.Millisecond {
		t.Errorf("Wait returned too early: %v", elapsed)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("Wait took too long: %v", elapsed)
	}
}

func TestLimiter_WaitN(t *testing.T) {
	l := NewLimiter(10, 1)

	// Consume initial token
	l.Wait()

	// Request 2 tokens. Should wait ~200ms (2 tokens / 10 tps)
	start := time.Now()
	l.WaitN(2.0)
	elapsed := time.Since(start)
	if elapsed < 180*time.Millisecond {
		t.Errorf("WaitN returned too early: %v", elapsed)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("WaitN took too long: %v", elapsed)
	}
}