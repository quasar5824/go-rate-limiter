package ratelimit

import (
	"math"
	"net/http"
	"strconv"
	"time"
)

// RateLimitResponseHandler defines a function signature for customizing the response when a request is rate limited.
type RateLimitResponseHandler func(w http.ResponseWriter, r *http.Request)

// SkipCondition defines a function signature to determine if a request should bypass rate limiting.
type SkipCondition func(r *http.Request) bool

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
					seconds := int(math.Ceil(wait.Seconds()))
					w.Header().Set("Retry-After", strconv.Itoa(seconds))
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
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyExtractor(r)
			
			lim := kl.getLimiter(key)
			if lim != nil {
				if capLimiter, ok := lim.(interface{ Capacity() float64 }); ok {
					w.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(capLimiter.Capacity(), 'f', 0, 64))
				}
				available := lim.Available()
				w.Header().Set("X-RateLimit-Remaining", strconv.FormatFloat(available, 'f', 0, 64))
			}

			if !kl.Allow(key) {
				lim = kl.getLimiter(key)
				if lim != nil {
					_, wait := lim.AllowWithDuration(1.0)
					if wait > 0 {
						seconds := int(math.Ceil(wait.Seconds()))
						w.Header().Set("Retry-After", strconv.Itoa(seconds))
					}
				}
				handler(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}