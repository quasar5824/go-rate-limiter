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
	Capacity() float64
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
func (l *tokenBucket) SetLimit(rate, capacity float64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.rate = rate
	l.capacity = capacity
	if l.tokens > capacity {
		l.tokens = capacity
	}
}

// Capacity returns the maximum capacity of the bucket.
func (l *tokenBucket) Capacity() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.capacity
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

func (l *leakyBucket) Capacity() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.capacity
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

func (l *slidingWindow) Capacity() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.capacity
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
	limiter  Limiter
	minRate  float64
	maxRate  float64
	curRate  float64
	capacity float64
	mu       sync.Mutex
}

// NewAdaptiveLimiter creates a new AdaptiveLimiter. It assumes the initial rate
// is equal to the minRate unless adjusted via AdjustRate.
func NewAdaptiveLimiter(l Limiter, minRate, maxRate float64) *AdaptiveLimiter {
	return &AdaptiveLimiter{
		limiter:  l,
		minRate:  minRate,
		maxRate:  maxRate,
		curRate:  minRate,
		capacity: l.Capacity(),
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
	al.limiter.SetLimit(al.curRate, al.capacity)
}

// CurrentRate returns the currently configured rate of the adaptive limiter.
func (al *AdaptiveLimiter) CurrentRate() float64 {
	al.mu.Lock()
	defer al.mu.Unlock()
	return al.curRate
}

// ClusterLimiter aggregates multiple limiters. A request is allowed only if it
// is allowed by all underlying limiters.
type ClusterLimiter struct {
	limiters []Limiter
}

// NewClusterLimiter creates a new ClusterLimiter from a slice of limiters.
func NewClusterLimiter(limiters ...Limiter) Limiter {
	return &ClusterLimiter{
		limiters: limiters,
	}
}

func (cl *ClusterLimiter) Allow() bool {
	return cl.AllowN(1.0)
}

func (cl *ClusterLimiter) AllowN(n float64) bool {
	// Pre-flight check: ensure all limiters have enough tokens available
	for _, l := range cl.limiters {
		if l.Available() < n {
			return false
		}
	}

	// Consume from all. If any fails, we must attempt to return tokens to previous limiters.
	consumed := make([]Limiter, 0, len(cl.limiters))
	for _, l := range cl.limiters {
		if !l.AllowN(n) {
			// Rollback: return tokens to previously consumed limiters
			for _, prev := range consumed {
				prev.AllowN(-n)
			}
			return false
		}
		consumed = append(consumed, l)
	}
	return true
}

func (cl *ClusterLimiter) TryAllow(n float64) bool {
	return cl.AllowN(n)
}

func (cl *ClusterLimiter) AllowWithDuration(n float64) (bool, time.Duration) {
	var maxWait time.Duration
	for _, l := range cl.limiters {
		allowed, wait := l.AllowWithDuration(n)
		if !allowed {
			if wait > maxWait {
				maxWait = wait
			}
		}
	}

	if maxWait > 0 {
		return false, maxWait
	}

	// If all passed the AllowWithDuration check (meaning they are all available),
	// we attempt to consume from all.
	if cl.AllowN(n) {
		return true, 0
	}
	return false, time.Duration(1<<63 - 1)
}

func (cl *ClusterLimiter) BatchAllow(requests []float64) []bool {
	results := make([]bool, len(requests))
	for i, n := range requests {
		results[i] = cl.AllowN(n)
	}
	return results
}

func (cl *ClusterLimiter) Available() float64 {
	minAvail := 1e18
	for _, l := range cl.limiters {
		avail := l.Available()
		if avail < minAvail {
			minAvail = avail
		}
	}
	return minAvail
}

func (cl *ClusterLimiter) Peek() float64 {
	return cl.Available()
}

func (cl *ClusterLimiter) Reserve(n float64) time.Duration {
	return cl.ReserveN(n)
}

func (cl *ClusterLimiter) ReserveN(n float64) time.Duration {
	var maxWait time.Duration
	for _, l := range cl.limiters {
		wait := l.ReserveN(n)
		if wait > maxWait {
			maxWait = wait
		}
	}
	return maxWait
}

func (cl *ClusterLimiter) Wait() {
	cl.WaitN(context.Background(), 1.0)
}

func (cl *ClusterLimiter) WaitN(ctx context.Context, n float64) {
	waitDuration := cl.ReserveN(n)
	if waitDuration <= 0 {
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-time.After(waitDuration):
	}
}

func (cl *ClusterLimiter) WaitUntil(ctx context.Context, target time.Time) {
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

func (cl *ClusterLimiter) SetLimit(rate, capacity float64) {
	// Propagate limit changes to all underlying limiters that support it.
	for _, l := range cl.limiters {
		l.SetLimit(rate, capacity)
	}
}

func (cl *ClusterLimiter) Capacity() float64 {
	minCap := 1e18
	for _, l := range cl.limiters {
		cap := l.Capacity()
		if cap < minCap {
			minCap = cap
		}
	}
	return minCap
}

// KeyedLimiter manages multiple limiters identified by a string key.
type KeyedLimiter struct {
	factory func() Limiter
	limiters map[string]Limiter
	mu      sync.RWMutex
}

// NewKeyedLimiter creates a new KeyedLimiter that uses the provided factory to create limiters for new keys.
func NewKeyedLimiter(factory func() Limiter) *KeyedLimiter {
	return &KeyedLimiter{
		factory:  factory,
		limiters: make(map[string]Limiter),
	}
}

// getLimiter returns the limiter for the given key, creating one if it doesn't exist.
func (kl *KeyedLimiter) getLimiter(key string) Limiter {
	kl.mu.RLock()
	l, ok := kl.limiters[key]
	kl.mu.RUnlock()

	if ok {
		return l
	}

	kl.mu.Lock()
	defer kl.mu.Unlock()

	// Double-check after acquiring write lock
	if l, ok = kl.limiters[key]; ok {
		return l
	}

	l = kl.factory()
	kl.limiters[key] = l
	return l
}

// Allow checks if the request for the given key is allowed.
func (kl *KeyedLimiter) Allow(key string) bool {
	return kl.getLimiter(key).Allow()
}

// AllowN checks if the request for the given key requiring n tokens is allowed.
func (kl *KeyedLimiter) AllowN(key string, n float64) bool {
	return kl.getLimiter(key).AllowN(n)
}

// Reserve reserves tokens for the given key.
func (kl *KeyedLimiter) Reserve(key string, n float64) time.Duration {
	return kl.getLimiter(key).ReserveN(n)
}

// Wait blocks until tokens are available for the given key.
func (kl *KeyedLimiter) Wait(ctx context.Context, key string, n float64) {
	kl.getLimiter(key).WaitN(ctx, n)
}

// Remove deletes the limiter for the given key, freeing memory.
func (kl *KeyedLimiter) Remove(key string) {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	delete(kl.limiters, key)
}

// PriorityLimiter distributes tokens across different priority levels.
// Higher priority levels can consume tokens assigned to lower priorities.
type PriorityLimiter struct {
	limiter Limiter
	shares  map[int]float64 // priority -> % of total capacity/rate available
	mu      sync.RWMutex
}

// NewPriorityLimiter creates a PriorityLimiter. shares map should sum to 1.0.
func NewPriorityLimiter(l Limiter, shares map[int]float64) *PriorityLimiter {
	return &PriorityLimiter{
		limiter: l,
		shares:  shares,
	}
}

// SetShares updates the priority shares map.
func (pl *PriorityLimiter) SetShares(shares map[int]float64) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	pl.shares = shares
}

// AllowPriority checks if a request with a given priority is allowed.
// High priority requests (smaller int) are allowed if their share is available,
// or if they can 'borrow' from lower priorities.
func (pl *PriorityLimiter) AllowPriority(priority int, n float64) bool {
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	totalAvailable := pl.limiter.Available()
	
	share, ok := pl.shares[priority]
	if !ok {
		return pl.limiter.AllowN(n) // Fallback to default
	}

	// We allow if (totalAvailable) is enough for the request, 
	// but we only block low priority if available tokens fall below the sum of higher priority shares.
	var higherPriorityShare float64
	for p, s := range pl.shares {
		if p < priority {
			higherPriorityShare += s
		}
	}

	minFloor := pl.limiter.Capacity() * higherPriorityShare
	if totalAvailable-n < minFloor {
		return false
	}

	return pl.limiter.AllowN(n)
}
