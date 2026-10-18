package ratelimit

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
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
			if skip != nil && skip(r) {
				next.ServeHTTP(w, r)
				return
			}

			// Attempt to retrieve capacity if the limiter supports it
			if capLimiter, ok := l.(interface{ Capacity() float64 }); ok {
				w.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(capLimiter.Capacity(), 'f', 0, 64))
			}

			available := l.Available()
			w.Header().Set("X-RateLimit-Remaining", strconv.FormatFloat(available, 'f', 0, 64))

			if !l.Allow() {
				_, wait := l.AllowWithDuration(1.0)
				if wait > 0 {
					// Use formatted float for Retry-After to provide sub-second precision if needed
					w.Header().Set("Retry-After", fmt.Sprintf("%.3f", wait.Seconds()))
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
			costVal := cost(r)

			if capLimiter, ok := l.(interface{ Capacity() float64 }); ok {
				w.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(capLimiter.Capacity(), 'f', 0, 64))
			}

			available := l.Available()
			w.Header().Set("X-RateLimit-Remaining", strconv.FormatFloat(available, 'f', 0, 64))

			if !l.AllowN(costVal) {
				_, wait := l.AllowWithDuration(costVal)
				if wait > 0 {
						w.Header().Set("Retry-After", fmt.Sprintf("%.3f", wait.Seconds()))
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
			key := keyExtractor(r)
			costVal := cost(r)
			
			lim := kl.getLimiter(key)
			if lim != nil {
				if capLimiter, ok := lim.(interface{ Capacity() float64 }); ok {
					w.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(capLimiter.Capacity(), 'f', 0, 64))
				}
				available := lim.Available()
				w.Header().Set("X-RateLimit-Remaining", strconv.FormatFloat(available, 'f', 0, 64))
			}

			if !kl.AllowN(key, costVal) {
				lim = kl.getLimiter(key)
				if lim != nil {
					_, wait := lim.AllowWithDuration(costVal)
					if wait > 0 {
							w.Header().Set("Retry-After", fmt.Sprintf("%.3f", wait.Seconds()))
					}
				}
				handler(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}