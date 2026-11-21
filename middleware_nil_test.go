package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRateLimitMiddleware_NilLimiter(t *testing.T) {
	handler := RateLimitMiddleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
}

func TestRateLimitMiddlewareWithCost_NilLimiter(t *testing.T) {
	handler := RateLimitMiddlewareWithCost(nil, func(r *http.Request) float64 {
		return 5
	}, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
}

func TestKeyedRateLimitMiddleware_NilLimiter(t *testing.T) {
	handler := KeyedRateLimitMiddleware(nil, func(r *http.Request) string {
		return "user"
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
}

func TestClampedAvailable_NilLimiter(t *testing.T) {
	if got := clampedAvailable(nil); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
}