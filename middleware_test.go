package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRateLimitMiddleware(t *testing.T) {
	l := NewLimiter(10, 1)
	handler := RateLimitMiddleware(l)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First request allowed
	req1 := httptest.NewRequest("GET", "/", nil)
	tr1 := httptest.NewRecorder()
	handler.ServeHTTP(tr1, req1)
	if tr1.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", tr1.Code)
	}

	// Second request denied (capacity 1)
	req2 := httptest.NewRequest("GET", "/", nil)
	tr2 := httptest.NewRecorder()
	handler.ServeHTTP(tr2, req2)
	if tr2.Code != http.StatusTooManyRequests {
		t.Errorf("Expected 429 Too Many Requests, got %d", tr2.Code)
	}
}

func TestKeyedRateLimitMiddleware(t *testing.T) {
	factory := func() Limiter {
		return NewLimiter(10, 1)
	}
	kl := NewKeyedLimiter(factory)
	
	extractor := func(r *http.Request) string {
		return r.Header.Get("X-API-Key")
	}

	handler := KeyedRateLimitMiddleware(kl, extractor)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// User A: First request allowed
	reqA1 := httptest.NewRequest("GET", "/", nil)
	reqA1.Header.Set("X-API-Key", "user-a")
	trA1 := httptest.NewRecorder()
	handler.ServeHTTP(trA1, reqA1)
	if trA1.Code != http.StatusOK {
		t.Errorf("User A first request expected 200, got %d", trA1.Code)
	}

	// User A: Second request denied
	reqA2 := httptest.NewRequest("GET", "/", nil)
	reqA2.Header.Set("X-API-Key", "user-a")
	trA2 := httptest.NewRecorder()
	handler.ServeHTTP(trA2, reqA2)
	if trA2.Code != http.StatusTooManyRequests {
		t.Errorf("User A second request expected 429, got %d", trA2.Code)
	}

	// User B: First request allowed independently
	reqB1 := httptest.NewRequest("GET", "/", nil)
	reqB1.Header.Set("X-API-Key", "user-b")
	trB1 := httptest.NewRecorder()
	handler.ServeHTTP(trB1, reqB1)
	if trB1.Code != http.StatusOK {
		t.Errorf("User B first request expected 200, got %d", trB1.Code)
	}
}
