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
	if tr1.Header().Get("X-RateLimit-Remaining") == "" {
		t.Error("Expected X-RateLimit-Remaining header to be present")
	}

	// Second request denied (capacity 1)
	req2 := httptest.NewRequest("GET", "/", nil)
	tr2 := httptest.NewRecorder()
	handler.ServeHTTP(tr2, req2)
	if tr2.Code != http.StatusTooManyRequests {
		t.Errorf("Expected 429 Too Many Requests, got %d", tr2.Code)
	}
	if tr2.Header().Get("X-RateLimit-Remaining") == "" {
		t.Error("Expected X-RateLimit-Remaining header to be present even on denied request")
	}
	if tr2.Header().Get("Retry-After") == "" {
		t.Error("Expected Retry-After header to be present on denied request")
	}
}

func TestRateLimitMiddleware_SkipCondition(t *testing.T) {
	l := NewLimiter(10, 0) // Deny all by default
	skip := func(r *http.Request) bool {
		return r.Header.Get("X-Skip-Limit") == "true"
	}

	handler := RateLimitMiddlewareWithCondition(l, skip, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Request without skip header should be denied
	req1 := httptest.NewRequest("GET", "/", nil)
	tr1 := httptest.NewRecorder()
	handler.ServeHTTP(tr1, req1)
	if tr1.Code != http.StatusTooManyRequests {
		t.Errorf("Expected 429 for non-skipped request, got %d", tr1.Code)
	}

	// Request with skip header should be allowed
	req2 := httptest.NewRequest("GET", "/", nil)
	req2.Header.Set("X-Skip-Limit", "true")
	tr2 := httptest.NewRecorder()
	handler.ServeHTTP(tr2, req2)
	if tr2.Code != http.StatusOK {
		t.Errorf("Expected 200 for skipped request, got %d", tr2.Code)
	}
}

func TestRateLimitMiddleware_CustomHandler(t *testing.T) {
	l := NewLimiter(10, 0)
	customHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom-Limit", "true")
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("Custom Error"))
	}

	handler := RateLimitMiddlewareWithHandler(l, customHandler)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	tr := httptest.NewRecorder()
	handler.ServeHTTP(tr, req)

	if tr.Code != http.StatusForbidden {
		t.Errorf("Expected 403 Forbidden, got %d", tr.Code)
	}
	if tr.Header().Get("X-Custom-Limit") != "true" {
		t.Error("Expected custom header X-Custom-Limit to be present")
	}
	if tr.Body.String() != "Custom Error" {
		t.Errorf("Expected 'Custom Error', got '%s'", tr.Body.String())
	}
}

func TestRateLimitMiddleware_WithCost(t *testing.T) {
	l := NewLimiter(10, 5)
	costExtractor := func(r *http.Request) float64 {
		if r.Method == "POST" {
			return 5.0
		}
		return 1.0
	}

	handler := RateLimitMiddlewareWithCost(l, costExtractor, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// POST should consume all 5 tokens
	reqPost := httptest.NewRequest("POST", "/", nil)
	trPost := httptest.NewRecorder()
	handler.ServeHTTP(trPost, reqPost)
	if trPost.Code != http.StatusOK {
		t.Errorf("Expected 200 OK for first POST, got %d", trPost.Code)
	}

	// Next GET should be denied as bucket is empty
	reqGet := httptest.NewRequest("GET", "/", nil)
	trGet := httptest.NewRecorder()
	handler.ServeHTTP(trGet, reqGet)
	if trGet.Code != http.StatusTooManyRequests {
		t.Errorf("Expected 429 for GET after heavy POST, got %d", trGet.Code)
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
	if trA1.Header().Get("X-RateLimit-Remaining") == "" {
		t.Error("Expected X-RateLimit-Remaining header for keyed limiter")
	}
	if trA1.Header().Get("X-RateLimit-Limit") == "" {
		t.Error("Expected X-RateLimit-Limit header for keyed limiter")
	}

	// User A: Second request denied
	reqA2 := httptest.NewRequest("GET", "/", nil)
	reqA2.Header.Set("X-API-Key", "user-a")
	trA2 := httptest.NewRecorder()
	handler.ServeHTTP(trA2, reqA2)
	if trA2.Code != http.StatusTooManyRequests {
		t.Errorf("User A second request expected 429, got %d", trA2.Code)
	}
	if trA2.Header().Get("Retry-After") == "" {
		t.Error("Expected Retry-After header for keyed limiter denied request")
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
