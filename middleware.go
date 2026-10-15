package ratelimit

import (
	"net/http"
	"strconv"
	"time"
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
			available := l.Available()
			w.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(l.Peek()+available, 'f', 0, 64))
			w.Header().Set("X-RateLimit-Remaining", strconv.FormatFloat(available, 'f', 0, 64))

			if !l.Allow() {
				_, wait := l.AllowWithDuration(1.0)
				if wait > 0 {
					seconds := int(wait.Seconds())
					if float64(seconds) < wait.Seconds() {
						seconds++
					}
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
				available := lim.Available()
				w.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(lim.Peek()+available, 'f', 0, 64))
				w.Header().Set("X-RateLimit-Remaining", strconv.FormatFloat(available, 'f', 0, 64))
			}

			if !kl.Allow(key) {
				lim = kl.getLimiter(key)
				if lim != nil {
					_, wait := lim.AllowWithDuration(1.0)
					if wait > 0 {
						seconds := int(wait.Seconds())
						if float64(seconds) < wait.Seconds() {
							seconds++
						}
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
