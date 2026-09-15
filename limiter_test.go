package ratelimit

import (
	"context"
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

func TestLimiter_AllowWithDuration(t *testing.T) {
	l := NewLimiter(10, 1)

	// Use initial token
	l.Allow()

	// Now bucket is empty. Request 1 token.
	allowed, wait := l.AllowWithDuration(1.0)
	if allowed {
		t.Error("Request should have been denied")
	}
	
	// Expect wait around 100ms (1 token / 10 tps)
	if wait < 80*time.Millisecond || wait > 120*time.Millisecond {
		t.Errorf("Unexpected wait duration: %v", wait)
	}

	// Ensure no tokens were consumed during the failed check
	time.Sleep(110 * time.Millisecond)
	if !l.Allow() {
		t.Error("Request should be allowed after waiting")
	}
}

func TestLimiter_BatchAllow(t *testing.T) {
	l := NewLimiter(10, 5)

	// Request 3, 1, 2 tokens. Total 6. Bucket has 5.
	// 3 should be allowed, 1 should be allowed (4 total), 2 should be denied.
	requests := []float64{3.0, 1.0, 2.0}
	expected := []bool{true, true, false}
	results := l.BatchAllow(requests)

	for i, res := range results {
		if res != expected[i] {
			t.Errorf("Request %d (%.1f tokens) expected %v, got %v", i, requests[i], expected[i], res)
		}
	}

	// Check remaining: 5 - 4 = 1
	if l.Available() != 1.0 {
		t.Errorf("Expected 1 token remaining, got %v", l.Available())
	}
}

func TestLimiter_Available(t *testing.T) {
	l := NewLimiter(10, 5)

	if l.Available() != 5.0 {
		t.Errorf("Expected 5 tokens, got %v", l.Available())
	}

	l.AllowN(2.0)
	if l.Available() != 3.0 {
		t.Errorf("Expected 3 tokens after consumption, got %v", l.Available())
	}

	// Wait for refill (0.1s * 10 = 1 token)
	time.Sleep(100 * time.Millisecond)
	avail := l.Available()
	if avail < 3.0 || avail > 4.1 {
		t.Errorf("Expected tokens to be around 4.0 after refill, got %v", avail)
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
	l.WaitN(context.Background(), 2.0)
	elapsed := time.Since(start)
	if elapsed < 180*time.Millisecond {
		t.Errorf("WaitN returned too early: %v", elapsed)
	}
	if elapsed > 300*time.Millisecond {
		t.Errorf("WaitN took too long: %v", elapsed)
	}
}

func TestLimiter_WaitN_Context(t *testing.T) {
	l := NewLimiter(10, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	l.WaitN(ctx, 1.0)
	elapsed := time.Since(start)

	if elapsed < 40*time.Millisecond || elapsed > 100*time.Millisecond {
		t.Errorf("WaitN should have returned due to context timeout around 50ms, took: %v", elapsed)
	}
}

func TestLimiter_Reserve(t *testing.T) {
	l := NewLimiter(10, 1)

	// Consume initial token
	l.Allow()

	// Reserve 1 token. Rate is 10/s, so 1 token = 100ms
	wait := l.Reserve(1.0)
	if wait < 80*time.Millisecond || wait > 120*time.Millisecond {
		t.Errorf("Reserve duration unexpected: %v", wait)
	}

	// Reserve 2 more tokens. Since the previous reserve emptied the bucket,
	// these 2 tokens should take 200ms from now
	wait2 := l.Reserve(2.0)
	if wait2 < 180*time.Millisecond || wait2 > 220*time.Millisecond {
		t.Errorf("Reserve duration unexpected: %v", wait2)
	}
}

func TestLimiter_SetLimit(t *testing.T) {
	l := NewLimiter(10, 1)
	
	// Use it up
	l.Allow()

	// Change rate to 100/s
	l.SetLimit(100, 1)

	// Reserve 1 token. Should be ~10ms now
	wait := l.Reserve(1.0)
	if wait < 0 || wait > 20*time.Millisecond {
		t.Errorf("Reserve duration after SetLimit unexpected: %v", wait)
	}
}

func TestLimiter_WaitUntil(t *testing.T) {
	l := NewLimiter(10, 1)
	ctx := context.Background()

	// Test waiting for a future time
	target := time.Now().Add(100 * time.Millisecond)
	start := time.Now()
	l.WaitUntil(ctx, target)
	elapsed := time.Since(start)

	if elapsed < 80*time.Millisecond {
		t.Errorf("WaitUntil returned too early: %v", elapsed)
	}

	// Test waiting for a past time
	start = time.Now()
	l.WaitUntil(ctx, time.Now().Add(-100*time.Millisecond))
	if time.Since(start) > 10*time.Millisecond {
		t.Error("WaitUntil should return immediately for past times")
	}

	// Test context cancellation
	ctxCancel, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start = time.Now()
	l.WaitUntil(ctxCancel, time.Now().Add(200*time.Millisecond))
	elapsed = time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("WaitUntil should have returned early due to context cancellation, took: %v", elapsed)
	}
}