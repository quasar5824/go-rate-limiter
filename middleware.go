package ratelimit

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
)

// RateLimitResponseHandler defines a function signature for customizing the response when a request is rate limited.
type RateLimitResponseHandler func(w http.ResponseWriter, r *http.Request)

// SkipCondition defines a function signature to determine if a request should bypass rate limiting.
type SkipCondition func(r *http.Request) bool

// CostExtractor defines a function signature to determine the token cost of a request.
type CostExtractor func(r *http.Request) float64

// DefaultRateLimitResponse is the default handler that returns a plain text 'Too Many Requests' message.
func DefaultRateLimitResponse(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
}

// sanitizeCost converts an invalid request cost into a safe value. Negative costs are
// treated as free requests, while non-finite values fall back to a single token.
func sanitizeCost(costVal float64) float64 {
	if math.IsNaN(costVal) || math.IsInf(costVal, 0) {
		return 1.0
	}
	if costVal <= 0 {
		return 0
	}
	return costVal
}

// clampedAvailable returns the limiter's available token count, ensuring that tiny
// negative floating-point results or non-finite values are never reported as an
// invalid remaining count.
func clampedAvailable(l Limiter) float64 {
	available := l.Available()
	if !math.IsFinite(available) || available <= 0 {
		return 0
	}
	return available
}

// RateLimitMiddleware returns an HTTP middleware that rate limits requests using the provided Limiter.
func RateLimitMiddleware(l Limiter) func(http.Handler) http.Handler {
	return RateLimitMiddlewareWithHandler(l, DefaultRateLimitResponse)
}

// RateLimitMiddlewareWithHandler is like RateLimitMiddleware, but allows providing a custom response handler.
func RateLimitMiddlewareWithHandler(l Limiter, handler RateLimitResponseHandler) func(http.Handler) http.Handler {
	return RateLimitMiddlewareWithCondition(l, nil, handler)
}

// RateLimitMiddlewareWithCondition allows providing a SkipCondition to bypass rate limiting for certain requests.
func RateLimitMiddlewareWithCondition(l Limiter, skip SkipCondition, handler RateLimitResponseHandler) func(http.Handler) http.Handler {
	if handler == nil {
		handler = DefaultRateLimitResponse
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if l == nil {
				next.ServeHTTP(w, r)
				return
			}
			if skip != nil && skip(r) {
				next.ServeHTTP(w, r)
				return
			}

			// Attempt to retrieve capacity if the limiter supports it
			if capLimiter, ok := l.(interface{ Capacity() float64 }); ok {
				w.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(capLimiter.Capacity(), 'f', 0, 64))
			}

			allowed := l.Allow()
			remaining := clampedAvailable(l)
			w.Header().Set("X-RateLimit-Remaining", strconv.FormatFloat(remaining, 'f', 0, 64))

			if !allowed {
				_, wait := l.AllowWithDuration(1.0)
				if wait > 0 {
					if seconds := wait.Seconds(); math.IsFinite(seconds) {
						w.Header().Set("Retry-After", fmt.Sprintf("%.3f", seconds))
					}
				}
				handler(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RateLimitMiddlewareWithCost allows providing a CostExtractor to determine how many tokens each request consumes.
func RateLimitMiddlewareWithCost(l Limiter, cost CostExtractor, handler RateLimitResponseHandler) func(http.Handler) http.Handler {
	if handler == nil {
		handler = DefaultRateLimitResponse
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if l == nil {
				next.ServeHTTP(w, r)
				return
			}

			costVal := sanitizeCost(1.0)
			if cost != nil {
				costVal = sanitizeCost(cost(r))
			}

			if capLimiter, ok := l.(interface{ Capacity() float64 }); ok {
				w.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(capLimiter.Capacity(), 'f', 0, 64))
			}

			// A zero-cost request is always allowed and does not consume tokens.
			allowed := true
			if costVal > 0 {
				allowed = l.AllowN(costVal)
			}
			remaining := clampedAvailable(l)
			w.Header().Set("X-RateLimit-Remaining", strconv.FormatFloat(remaining, 'f', 0, 64))

			if !allowed {
				if costVal > 0 {
					_, wait := l.AllowWithDuration(costVal)
					if wait > 0 {
						if seconds := wait.Seconds(); math.IsFinite(seconds) {
							w.Header().Set("Retry-After", fmt.Sprintf("%.3f", seconds))
						}
					}
				}
				handler(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// KeyedRateLimitMiddleware returns an HTTP middleware that rate limits requests per key extracted from the request.
func KeyedRateLimitMiddleware(kl *KeyedLimiter, keyExtractor func(*http.Request) string) func(http.Handler) http.Handler {
	return KeyedRateLimitMiddlewareWithHandler(kl, keyExtractor, DefaultRateLimitResponse)
}

// KeyedRateLimitMiddlewareWithHandler is like KeyedRateLimitMiddleware, but allows providing a custom response handler.
func KeyedRateLimitMiddlewareWithHandler(kl *KeyedLimiter, keyExtractor func(*http.Request) string, handler RateLimitResponseHandler) func(http.Handler) http.Handler {
	return KeyedRateLimitMiddlewareWithCost(kl, keyExtractor, func(r *http.Request) float64 { return 1.0 }, handler)
}

// KeyedRateLimitMiddlewareWithCost allows providing a CostExtractor for keyed rate limiting.
func KeyedRateLimitMiddlewareWithCost(kl *KeyedLimiter, keyExtractor func(*http.Request) string, cost CostExtractor, handler RateLimitResponseHandler) func(http.Handler) http.Handler {
	if handler == nil {
		handler = DefaultRateLimitResponse
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if kl == nil {
				next.ServeHTTP(w, r)
				return
			}

			key := ""
			if keyExtractor != nil {
				key = keyExtractor(r)
			}

			costVal := sanitizeCost(1.0)
			if cost != nil {
				costVal = sanitizeCost(cost(r))
			}

			// Ensure the per-key limiter exists so headers can be reported even for
			// zero-cost requests.
			lim := kl.getLimiter(key)

			// A zero-cost request is always allowed and does not consume tokens.
			allowed := true
			if costVal > 0 {
				allowed = kl.AllowN(key, costVal)
				lim = kl.getLimiter(key)
			}

			if lim != nil {
				if capLimiter, ok := lim.(interface{ Capacity() float64 }); ok {
					w.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(capLimiter.Capacity(), 'f', 0, 64))
				}
				remaining := clampedAvailable(lim)
				w.Header().Set("X-RateLimit-Remaining", strconv.FormatFloat(remaining, 'f', 0, 64))
			}

			if !allowed {
				if lim != nil && costVal > 0 {
					_, wait := lim.AllowWithDuration(costVal)
					if wait > 0 {
						if seconds := wait.Seconds(); math.IsFinite(seconds) {
							w.Header().Set("Retry-After", fmt.Sprintf("%.3f", seconds))
						}
					}
				}
				handler(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}