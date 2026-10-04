package ratelimit

import (
	"fmt"
	"net/http"
	"strconv"
)

// RateLimitResponseHandler defines a function signature for customizing the response when a request is rate limited.
type RateLimitResponseHandler func(w http.ResponseWriter, r *http.Request)

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
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Try to set headers if the limiter supports Available()
			if w.Header() != nil {
				w.Header().Set("X-RateLimit-Remaining", strconv.FormatFloat(l.Available(), 'f', 0, 64))
			}

			if !l.Allow() {
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
			
			// For keyed limiters, we can't easily get the 'limit' because the factory creates them,
			// but we can provide the current availability for the specific key.
			if w.Header() != nil {
				// This is an approximation since Allow(key) will be called immediately after
				w.Header().Set("X-RateLimit-Remaining", strconv.FormatFloat(kl.Available(key), 'f', 0, 64))
			}

			if !kl.Allow(key) {
				handler(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
