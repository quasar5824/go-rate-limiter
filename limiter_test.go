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

func TestLimiter_NewBurstLimiter(t *testing.T) {
	l := NewBurstLimiter(10, 5)

	// Initial request should be denied immediately as bucket is empty
	if l.Allow() {
		t.Error("First request to NewBurstLimiter should be denied")
	}

	// Wait for 1 token (100ms)
	time.Sleep(110 * time.Millisecond)

	if !l.Allow() {
		t.Error("Request after refill should be allowed")
	}
}

func TestLimiter_NewLimiterWithTokens(t *testing.T) {
	// Rate 10, Capacity 5, Start with 2 tokens
	l := NewLimiterWithTokens(10, 5, 2)

	for i := 0; i < 2; i++ {
		if !l.Allow() {
			t.Errorf("Request %d should have been allowed", i+1)
		}
	}

	if l.Allow() {
		t.Error("Request 3 should have been denied")
	}

	// Test capping at capacity
	l2 := NewLimiterWithTokens(10, 5, 10)
	if l2.Available() != 5.0 {
		t.Errorf("Expected tokens to be capped at capacity (5), got %v", l2.Available())
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

func TestLimiter_Peek(t *testing.T) {
	l := NewLimiter(10, 5)

	// Initial capacity
	if l.Peek() != 5.0 {
		t.Errorf("Expected 5 tokens, got %v", l.Peek())
	}

	l.AllowN(2.0)
	if l.Peek() != 3.0 {
		t.Errorf("Expected 3 tokens after consumption, got %v", l.Peek())
	}

	// Wait for some refill
	time.Sleep(100 * time.Millisecond)

	// Peek should NOT refill
	peekVal := l.Peek()
	if peekVal != 3.0 {
		t.Errorf("Peek should not refill. Expected 3.0, got %v", peekVal)
	}

	// Available SHOULD refill
	availVal := l.Available()
	if availVal <= 3.0 {
		t.Errorf("Available should have triggered refill. Got %v", availVal)
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

func TestWeightedLimiter(t *testing.T) {
	l := NewLimiter(10, 5)
	weights := map[string]float64{
		"read":  1.0,
		"write": 5.0,
	}
	wl := NewWeightedLimiter(l, weights)

	// Should allow 5 reads
	for i := 0; i < 5; i++ {
		if !wl.AllowKey("read") {
			t.Errorf("Read %d should be allowed", i+1)
		}
	}

	// 6th read should be denied
	if wl.AllowKey("read") {
		t.Error("6th read should be denied")
	}

	// Reset bucket for write test
	l.SetLimit(10, 5)
	l.AllowN(-5.0) // Artificial refill or just use a new limiter
	l2 := NewLimiter(10, 5)
	wl2 := NewWeightedLimiter(l2, weights)

	// Should allow 1 write (5 tokens)
	if !wl2.AllowKey("write") {
		t.Error("First write should be allowed")
	}

	// Second write should be denied (0 tokens left)
	if wl2.AllowKey("write") {
		t.Error("Second write should be denied")
	}

	// Test default weight for unknown key
	if !wl2.AllowKey("unknown") {
		// Need to wait for refill first as bucket is empty
		time.Sleep(110 * time.Millisecond)
		if !wl2.AllowKey("unknown") {
			t.Error("Unknown key should default to weight 1.0 and be allowed after refill")
		}
	}
}

func TestAdaptiveLimiter(t *testing.T) {
	l := NewLimiter(10, 10)
	minRate, maxRate := 5.0, 20.0
	al := NewAdaptiveLimiter(l, minRate, maxRate)

	// Initial rate should be minRate
	if al.CurrentRate() != minRate {
		t.Errorf("Expected initial rate %v, got %v", minRate, al.CurrentRate())
	}

	// Test increasing rate within bounds
	al.AdjustRate(func(curr float64) float64 {
		return curr + 5.0
	})
	if al.CurrentRate() != 10.0 {
		t.Errorf("Expected rate 10.0, got %v", al.CurrentRate())
	}

	// Test capping at maxRate
	al.AdjustRate(func(curr float64) float64 {
		return curr + 50.0
	})
	if al.CurrentRate() != maxRate {
		t.Errorf("Expected rate capped at %v, got %v", maxRate, al.CurrentRate())
	}

	// Test capping at minRate
	al.AdjustRate(func(curr float64) float64 {
		return curr - 100.0
	})
	if al.CurrentRate() != minRate {
		t.Errorf("Expected rate capped at %v, got %v", minRate, al.CurrentRate())
	}

	// Test that SetLimit was actually called on the underlying limiter
	// After capping at maxRate (20.0), and then adjusting back to 15.0
	al.AdjustRate(func(curr float64) float64 {
		return 15.0
	})

	// To verify the underlying limiter is updated, we can use Reserve
	// Use up all tokens
	l.AllowN(100)
	// At 15 tokens/sec, 1 token should take ~66ms
	wait := l.Reserve(1.0)
	if wait < 50*time.Millisecond || wait > 80*time.Millisecond {
		t.Errorf("Underlying limiter rate not updated. Expected ~66ms, got %v", wait)
	}
}

func TestLeakyLimiter(t *testing.T) {
	// Rate 10/s, capacity for 2 seconds (20 tokens)
	l := NewLeakyLimiter(10, 20)

	// First request should be immediate
	if !l.Allow() {
		t.Error("First request should be allowed")
	}

	// Second request should be allowed, but it's scheduled 100ms later
	if !l.Allow() {
		t.Error("Second request should be allowed")
	}

	// Check that it is indeed scheduling: Reserve for 1 token should now be ~100ms
	wait := l.Reserve(1.0)
	if wait < 80*time.Millisecond || wait > 120*time.Millisecond {
		t.Errorf("Expected reserve to be ~100ms, got %v", wait)
	}

	// Test capacity limit
	l2 := NewLeakyLimiter(10, 1)
	// Fill the 1-second capacity (10 tokens)
	for i := 0; i < 10; i++ {
		l2.Allow()
	}

	// 11th request should exceed capacity (scheduled > 1s from now)
	if l2.Allow() {
		t.Error("Request 11 should have been denied due to capacity")
	}

	// Wait for some leak (500ms = 5 tokens capacity recovered)
	time.Sleep(500 * time.Millisecond)
	if !l2.Allow() {
		t.Error("Request after leak should be allowed")
	}
}

func TestSlidingWindowLimiter(t *testing.T) {
	// Limit: 5 requests per 500ms
	l := NewSlidingWindowLimiter(5, 500*time.Millisecond)

	// First 5 should be allowed
	for i := 0; i < 5; i++ {
		if !l.Allow() {
			t.Errorf("Request %d should have been allowed", i+1)
		}
	}

	// 6th should be denied
	if l.Allow() {
		t.Error("6th request should be denied")
	}

	// Wait for 300ms. 5 requests were made at t=0. 
	// At t=300ms, all 5 are still in the window [300ms-500ms, 300ms].
	time.Sleep(300 * time.Millisecond)
	if l.Allow() {
		t.Error("Request at 300ms should still be denied")
	}

	// Wait another 250ms (total 550ms). 
	// Window is [50ms, 550ms]. Original requests at t=0 are now gone.
	time.Sleep(250 * time.Millisecond)
	if !l.Allow() {
		t.Error("Request after window slide should be allowed")
	}
}

func TestSlidingWindowLimiter_ReserveFairness(t *testing.T) {
	// Limit: 2 requests per 500ms
	l := NewSlidingWindowLimiter(2, 500*time.Millisecond)

	// Fill window
	l.Allow()
	l.Allow()

	// Reserve 1st extra token. Should wait until t=0+500ms
	wait1 := l.Reserve(1.0)
	if wait1 < 400*time.Millisecond || wait1 > 600*time.Millisecond {
		t.Errorf("First reservation duration unexpected: %v", wait1)
	}

	// Reserve 2nd extra token. Should wait until t=1+500ms
	wait2 := l.Reserve(1.0)
	if wait2 <= wait1 {
		t.Errorf("Second reservation should be later than first. wait1: %v, wait2: %v", wait1, wait2)
	}
	if wait2 < 900*time.Millisecond || wait2 > 1100*time.Millisecond {
		t.Errorf("Second reservation duration unexpected: %v", wait2)
	}
}

func TestClusterLimiter(t *testing.T) {
	l1 := NewLimiter(10, 10)
	l2 := NewLimiter(5, 5)
	cl := NewClusterLimiter(l1, l2)

	// Should allow up to 5 requests (limited by l2)
	for i := 0; i < 5; i++ {
		if !cl.Allow() {
			t.Errorf("Request %d should have been allowed", i+1)
		}
	}

	// 6th should be denied by l2
	if cl.Allow() {
		t.Error("Request 6 should have been denied by l2")
	}

	// Test Available returns minimum
	if cl.Available() != 0 {
		t.Errorf("Expected 0 available, got %v", cl.Available())
	}

	// Test Reserve returns max wait
	// l2 needs 1 token, rate is 5/s -> 200ms
	wait := cl.Reserve(1.0)
	if wait < 150*time.Millisecond || wait > 250*time.Millisecond {
		t.Errorf("Unexpected cluster reserve wait: %v", wait)
	}
}

func TestKeyedLimiter(t *testing.T) {
	factory := func() Limiter {
		return NewLimiter(10, 5)
	}
	kl := NewKeyedLimiter(factory)

	userA := "user-a"
	userB := "user-b"

	// User A should be allowed 5 requests
	for i := 0; i < 5; i++ {
		if !kl.Allow(userA) {
			t.Errorf("User A request %d should be allowed", i+1)
		}
	}
	
	// User A 6th request denied
	if kl.Allow(userA) {
		t.Error("User A 6th request should be denied")
	}

	// User B should still be allowed 5 requests independently
	for i := 0; i < 5; i++ {
		if !kl.Allow(userB) {
			t.Errorf("User B request %d should be allowed", i+1)
		}
	}

	// Test AllowN for keyed limiter
	userC := "user-c"
	if !kl.AllowN(userC, 3.0) {
		t.Error("User C request for 3 tokens should be allowed")
	}
	if kl.AllowN(userC, 3.0) {
		t.Error("User C second request for 3 tokens should be denied (only 2 left)")
	}

	// Test Reserve for keyed limiter
	userD := "user-d"
	kl.AllowN(userD, 5.0)
	wait := kl.Reserve(userD, 1.0)
	if wait < 80*time.Millisecond || wait > 120*time.Millisecond {
		t.Errorf("User D reserve duration unexpected: %v", wait)
	}

	// Test Wait for keyed limiter
	userE := "user-e"
	kl.AllowN(userE, 5.0)
	start := time.Now()
	kl.Wait(context.Background(), userE, 1.0)
	elapsed := time.Since(start)
	if elapsed < 80*time.Millisecond {
		t.Errorf("User E Wait returned too early: %v", elapsed)
	}

	// Test Remove
	kl.Remove(userA)
	// After removal, userA should get a fresh limiter
	if !kl.Allow(userA) {
		t.Error("User A should be allowed after limiter removal")
	}
}

func TestPriorityLimiter(t *testing.T) {
	l := NewLimiter(10, 10)
	shares := map[int]float64{
		0: 0.5, // High priority: 5 tokens guaranteed
		1: 0.5, // Low priority: 5 tokens guaranteed
	}
	pl := NewPriorityLimiter(l, shares)

	// High priority should be allowed to use almost all tokens
	for i := 0; i < 9; i++ {
		if !pl.AllowPriority(0, 1.0) {
			t.Errorf("High priority request %d should be allowed", i+1)
		}
	}
	
	// Low priority should be blocked because only 1 token left < 5 tokens guaranteed for High priority
	if pl.AllowPriority(1, 1.0) {
		t.Error("Low priority request should be denied when tokens fall below higher priority floors")
	}

	// High priority should still be able to take the last token
	if !pl.AllowPriority(0, 1.0) {
		t.Error("High priority should be able to take the last token")
	}
}
