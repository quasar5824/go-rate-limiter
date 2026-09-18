package ratelimit

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Limiter defines the interface for a rate limiter.
type Limiter interface {
	Allow() bool
	AllowN(n float64) bool
	TryAllow(n float64) bool
	AllowWithDuration(n float64) (bool, time.Duration)
	BatchAllow(requests []float64) []bool
	Available() float64
	Peek() float64
	Reserve(n float64) time.Duration
	ReserveN(n float64) time.Duration
	Wait()
	WaitN(ctx context.Context, n float64)
	WaitUntil(ctx context.Context, target time.Time)
	SetLimit(rate, capacity float64)
}

// tokenBucket implements the Limiter interface using the token bucket algorithm.
type tokenBucket struct {
	rate       float64
	capacity    float64
	tokens     float64
	lastUpdate time.Time
	mu         sync.Mutex
}

// NewLimiter creates a new Limiter with a given rate (tokens per second) and bucket capacity.
// The bucket is initialized as full.
func NewLimiter(rate float64, capacity float64) Limiter {
	return NewLimiterWithTokens(rate, capacity, capacity)
}

// NewBurstLimiter creates a new Limiter with a given rate (tokens per second) and bucket capacity.
// The bucket is initialized as empty, meaning the first request will be subject to the rate.
func NewBurstLimiter(rate float64, capacity float64) Limiter {
	return NewLimiterWithTokens(rate, capacity, 0)
}

// NewLimiterWithTokens creates a new Limiter with a given rate, capacity, and initial token count.
// The initial tokens are capped at the capacity.
func NewLimiterWithTokens(rate float64, capacity float64, tokens float64) Limiter {
	if tokens > capacity {
		tokens = capacity
	}
	return &tokenBucket{
		rate:       rate,
		capacity:    capacity,
		tokens:     tokens,
		lastUpdate: time.Now(),
	}
}

// refill updates the token count based on the time elapsed since the last update.
// Must be called while holding the lock.
func (l *tokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(l.lastUpdate).Seconds()
	l.lastUpdate = now

	l.tokens += elapsed * l.rate
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}
}

// SetLimit updates the rate and capacity of the limiter.
func (l *tokenBucket) SetLimit(rate float64, capacity float64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.rate = rate
	l.capacity = capacity
	if l.tokens > capacity {
		l.tokens = capacity
	}
}

// Allow checks if a request is allowed based on current token availability.
func (l *tokenBucket) Allow() bool {
	return l.AllowN(1.0)
}

// AllowN checks if a request requiring n tokens is allowed.
func (l *tokenBucket) AllowN(n float64) bool {
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
func (l *tokenBucket) TryAllow(n float64) bool {
	return l.AllowN(n)
}

// AllowWithDuration checks if n tokens are available. If they are, it consumes them and returns true, 0.
// If not, it returns false and the duration to wait until n tokens would be available, without consuming tokens.
func (l *tokenBucket) AllowWithDuration(n float64) (bool, time.Duration) {
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
func (l *tokenBucket) BatchAllow(requests []float64) []bool {
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
func (l *tokenBucket) Available() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()
	return l.tokens
}

// Peek returns the number of tokens available without triggering a refill.
// This is useful for inspecting the state as of the last operation.
func (l *tokenBucket) Peek() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tokens
}

// Reserve returns the duration to wait until n tokens become available.
// It consumes the tokens immediately (reserves them).
func (l *tokenBucket) Reserve(n float64) time.Duration {
	return l.ReserveN(n)
}

// ReserveN reserves n tokens and returns the duration to wait until they are available.
func (l *tokenBucket) ReserveN(n float64) time.Duration {
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
func (l *tokenBucket) Wait() {
	l.WaitN(context.Background(), 1.0)
}

// WaitN blocks until n tokens are available or the context is canceled.
func (l *tokenBucket) WaitN(ctx context.Context, n float64) {
	waitDuration := l.ReserveN(n)
	if waitDuration <= 0 {
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-time.After(waitDuration):
	}
}

// WaitUntil blocks until the specified time is reached or the context is canceled.
func (l *tokenBucket) WaitUntil(ctx context.Context, target time.Time) {
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

// leakyBucket implements the Limiter interface using the leaky bucket algorithm.
// It ensures a constant output rate by scheduling requests at fixed intervals.
type leakyBucket struct {
	rate       float64
	capacity    float64
	nextFreeTime time.Time
	mu         sync.Mutex
}

// NewLeakyLimiter creates a new LeakyBucket limiter.
func NewLeakyLimiter(rate float64, capacity float64) Limiter {
	return &leakyBucket{
		rate:     rate,
		capacity: capacity,
		nextFreeTime: time.Now(),
	}
}

func (l *leakyBucket) SetLimit(rate, capacity float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rate = rate
	l.capacity = capacity
}

func (l *leakyBucket) Allow() bool {
	return l.AllowN(1.0)
}

func (l *leakyBucket) AllowN(n float64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	if l.nextFreeTime.Before(now) {
		l.nextFreeTime = now
	}

	waitDuration := time.Duration(n / l.rate * float64(time.Second))
	executionTime := l.nextFreeTime.Add(waitDuration)

	if executionTime.Sub(now).Seconds() > l.capacity/l.rate {
		return false
	}

	l.nextFreeTime = executionTime
	return true
}

func (l *leakyBucket) TryAllow(n float64) bool {
	return l.AllowN(n)
}

func (l *leakyBucket) AllowWithDuration(n float64) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	if l.nextFreeTime.Before(now) {
		l.nextFreeTime = now
	}

	waitDuration := time.Duration(n / l.rate * float64(time.Second))
	executionTime := l.nextFreeTime.Add(waitDuration)

	waitFromNow := executionTime.Sub(now)
	if waitFromNow.Seconds() > l.capacity/l.rate {
		return false, time.Duration(1<<63 - 1)
	}

	return false, waitFromNow
}

func (l *leakyBucket) BatchAllow(requests []float64) []bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	if l.nextFreeTime.Before(now) {
		l.nextFreeTime = now
	}

	results := make([]bool, len(requests))
	for i, n := range requests {
		waitDuration := time.Duration(n / l.rate * float64(time.Second))
		executionTime := l.nextFreeTime.Add(waitDuration)

		if executionTime.Sub(now).Seconds() > l.capacity/l.rate {
			results[i] = false
		} else {
			l.nextFreeTime = executionTime
			results[i] = true
		}
	}
	return results
}

func (l *leakyBucket) Available() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	now := time.Now()
	if l.nextFreeTime.Before(now) {
		return l.capacity
	}
	
	remainingCapacitySeconds := (l.capacity / l.rate) - l.nextFreeTime.Sub(now).Seconds()
	return remainingCapacitySeconds * l.rate
}

func (l *leakyBucket) Peek() float64 {
	return l.Available()
}

func (l *leakyBucket) Reserve(n float64) time.Duration {
	return l.ReserveN(n)
}

func (l *leakyBucket) ReserveN(n float64) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	if l.nextFreeTime.Before(now) {
		l.nextFreeTime = now
	}

	waitDuration := time.Duration(n / l.rate * float64(time.Second))
	executionTime := l.nextFreeTime.Add(waitDuration)
	l.nextFreeTime = executionTime

	return executionTime.Sub(now)
}

func (l *leakyBucket) Wait() {
	l.WaitN(context.Background(), 1.0)
}

func (l *leakyBucket) WaitN(ctx context.Context, n float64) {
	waitDuration := l.ReserveN(n)
	if waitDuration <= 0 {
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-time.After(waitDuration):
	}
}

func (l *leakyBucket) WaitUntil(ctx context.Context, target time.Time) {
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

// slidingWindow implements the Limiter interface using a sliding window log.
// It provides precise rate limiting by tracking individual request timestamps.
type slidingWindow struct {
	window   time.Duration
	capacity float64
	mu       sync.Mutex
	logs     []time.Time
}

// NewSlidingWindowLimiter creates a new SlidingWindowLimiter.
// rate is treated as total allowed requests per the window duration.
func NewSlidingWindowLimiter(rate float64, window time.Duration) Limiter {
	return &slidingWindow{
		window:   window,
		capacity: rate,
		logs:     make([]time.Time, 0),
	}
}

func (l *slidingWindow) SetLimit(rate, capacity float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.capacity = rate // In sliding window, capacity is the limit per window
}

func (l *slidingWindow) Allow() bool {
	return l.AllowN(1.0)
}

func (l *slidingWindow) AllowN(n float64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-l.window)

	// Clean up old logs using binary search for efficiency
	validIdx := sort.Search(len(l.logs), func(i int) bool {
		return l.logs[i].After(windowStart)
	})
	l.logs = l.logs[validIdx:]

	if float64(len(l.logs)) + n <= l.capacity {
		for i := 0; i < int(n); i++ {
			l.logs = append(l.logs, now)
		}
		return true
	}
	return false
}

func (l *slidingWindow) TryAllow(n float64) bool {
	return l.AllowN(n)
}

func (l *slidingWindow) AllowWithDuration(n float64) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-l.window)

	validIdx := sort.Search(len(l.logs), func(i int) bool {
		return l.logs[i].After(windowStart)
	})
	l.logs = l.logs[validIdx:]

	if float64(len(l.logs)) + n <= l.capacity {
		for i := 0; i < int(n); i++ {
			l.logs = append(l.logs, now)
		}
		return true, 0
	}

	if len(l.logs) == 0 {
		return false, time.Duration(1<<63 - 1)
	}

	// Wait until the oldest token falls out of the window
	waitDuration := l.logs[0].Add(l.window).Sub(now)
	return false, waitDuration
}

func (l *slidingWindow) BatchAllow(requests []float64) []bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-l.window)

	validIdx := sort.Search(len(l.logs), func(i int) bool {
		return l.logs[i].After(windowStart)
	})
	l.logs = l.logs[validIdx:]

	results := make([]bool, len(requests))
	for i, n := range requests {
		if float64(len(l.logs)) + n <= l.capacity {
			for j := 0; j < int(n); j++ {
				l.logs = append(l.logs, now)
			}
			results[i] = true
		} else {
			results[i] = false
		}
	}
	return results
}

func (l *slidingWindow) Available() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-l.window)
	
	validIdx := sort.Search(len(l.logs), func(i int) bool {
		return l.logs[i].After(windowStart)
	})
	
	return l.capacity - float64(len(l.logs)-validIdx)
}

func (l *slidingWindow) Peek() float64 {
	return l.Available()
}

func (l *slidingWindow) Reserve(n float64) time.Duration {
	return l.ReserveN(n)
}

func (l *slidingWindow) ReserveN(n float64) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-l.window)

	validIdx := sort.Search(len(l.logs), func(i int) bool {
		return l.logs[i].After(windowStart)
	})
	l.logs = l.logs[validIdx:]

	if float64(len(l.logs)) + n <= l.capacity {
		for i := 0; i < int(n); i++ {
			l.logs = append(l.logs, now)
		}
		return 0
	}

	// Reserve tokens by adding them to the log as if they happened now,
	// even if they exceed capacity. This ensures fairness.
	for i := 0; i < int(n); i++ {
		l.logs = append(l.logs, now)
	}

	// The wait duration is until the oldest tokens that make this request 'over capacity' expire.
	numToExpire := int(float64(len(l.logs)) - l.capacity)
	if numToExpire <= 0 {
		return 0
	}
	return l.logs[numToExpire-1].Add(l.window).Sub(now)
}

func (l *slidingWindow) Wait() {
	l.WaitN(context.Background(), 1.0)
}

func (l *slidingWindow) WaitN(ctx context.Context, n float64) {
	waitDuration := l.ReserveN(n)
	if waitDuration <= 0 {
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-time.After(waitDuration):
	}
}

func (l *slidingWindow) WaitUntil(ctx context.Context, target time.Time) {
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

// WeightedLimiter is a wrapper around Limiter that assigns weights to different operation keys.
type WeightedLimiter struct {
	limiter Limiter
	weights map[string]float64
	mu      sync.RWMutex
}

// NewWeightedLimiter creates a new WeightedLimiter.
func NewWeightedLimiter(l Limiter, weights map[string]float64) *WeightedLimiter {
	return &WeightedLimiter{
		limiter: l,
		weights: weights,
	}
}

// AllowKey checks if an operation with a specific key is allowed based on its weight.
func (wl *WeightedLimiter) AllowKey(key string) bool {
	wl.mu.RLock()
	weight, ok := wl.weights[key]
	wl.mu.RUnlock()

	if !ok {
		return wl.limiter.Allow() // Default to 1 token if key not found
	}
	return wl.limiter.AllowN(weight)
}

// SetWeight updates the weight for a specific operation key.
func (wl *WeightedLimiter) SetWeight(key string, weight float64) {
	wl.mu.Lock()
	defer wl.mu.Unlock()
	wl.weights[key] = weight
}

// AdaptiveLimiter adjusts the rate of an underlying limiter based on a feedback function.
type AdaptiveLimiter struct {
	limiter Limiter
	minRate float64
	maxRate float64
	curRate float64
	mu      sync.Mutex
}

// NewAdaptiveLimiter creates a new AdaptiveLimiter. It assumes the initial rate
// is equal to the minRate unless adjusted via AdjustRate.
func NewAdaptiveLimiter(l Limiter, minRate, maxRate float64) *AdaptiveLimiter {
	return &AdaptiveLimiter{
		limiter: l,
		minRate: minRate,
		maxRate: maxRate,
		curRate: minRate,
	}
}

// AdjustRate calls the provided feedback function to determine the new rate.
// The rate is clamped between minRate and maxRate.
func (al *AdaptiveLimiter) AdjustRate(feedback func(currentRate float64) float64) {
	al.mu.Lock()
	defer al.mu.Unlock()

	newRate := feedback(al.curRate)

	if newRate < al.minRate {
		newRate = al.minRate
	}
	if newRate > al.maxRate {
		newRate = al.maxRate
	}

	al.curRate = newRate
	
	// Since the Limiter interface uses SetLimit(rate, capacity), we need to
	// maintain the capacity. If the underlying limiter is a tokenBucket, we
	// can't easily get the current capacity from the interface.
	// However, usually adaptive limiting adjusts the rate while keeping the burst capacity constant.
	// We assume the capacity should be handled by the logic providing the Limiter instance
	// or we can use a fixed value. For the sake of the interface, we need a capacity.
	// In a real scenario, we might extend the Limiter interface or store the capacity in AdaptiveLimiter.
	// Here, we will use a reasonable default or allow the limiter to handle it if it's a tokenBucket.
	
	// Note: This implementation requires knowing the desired capacity. 
	// To make this robust, we'll assume a capacity of 1.0 for the update if unknown,
	// but a better way would be to let the feedback function return both or store it.
	// For now, we update the rate and assume a capacity of al.maxRate to allow bursts up to the max rate.
	al.limiter.SetLimit(al.curRate, al.maxRate)
}

// CurrentRate returns the currently configured rate of the adaptive limiter.
func (al *AdaptiveLimiter) CurrentRate() float64 {
	al.mu.Lock()
	defer al.mu.Unlock()
	return al.curRate
}
