package ratelimit

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestSanitizeCost_EdgeCases(t *testing.T) {
	cases := []struct {
		input float64
		want  float64
	}{
		{math.NaN(), 1},
		{math.Inf(1), 1},
		{math.Inf(-1), 1},
		{-1, 0},
		{-0.0, 0},
		{0, 0},
		{2.5, 2.5},
	}

	for _, tc := range cases {
		if got := sanitizeCost(tc.input); got != tc.want {
			t.Errorf("sanitizeCost(%v) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestRateLimitMiddleware_WithCost_NegativeZeroCost(t *testing.T) {
	l := NewLimiter(10, 3)
	costExtractor := func(r *http.Request) float64 {
		return -0.0
	}

	handler := RateLimitMiddlewareWithCost(l, costExtractor, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	tr := httptest.NewRecorder()
	handler.ServeHTTP(tr, req)

	if tr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", tr.Code)
	}
	if got := tr.Header().Get("X-RateLimit-Remaining"); got != "3" {
		t.Errorf("negative-zero cost should not consume tokens, got %s", got)
	}
}

func TestRateLimitMiddleware_WithCost_InfiniteCost(t *testing.T) {
	l := NewLimiter(10, 2)
	costExtractor := func(r *http.Request) float64 {
		return math.Inf(1)
	}

	handler := RateLimitMiddlewareWithCost(l, costExtractor, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	tr := httptest.NewRecorder()
	handler.ServeHTTP(tr, req)

	if tr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", tr.Code)
	}
	if got := tr.Header().Get("X-RateLimit-Remaining"); got != "1" {
		t.Errorf("infinite cost should fall back to one token, got %s", got)
	}
}

func TestClampedAvailable_NegativeZero(t *testing.T) {
	// clampedAvailable is package-private; exercise it through a real limiter by
	// ensuring a just-emptied bucket reports zero rather than negative zero.
	l := NewLimiter(10, 1)
	l.Allow()

	req := httptest.NewRequest("GET", "/", nil)
	_ = req
	handler := RateLimitMiddleware(l)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	tr := httptest.NewRecorder()
	handler.ServeHTTP(tr, req)

	if got := tr.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("expected remaining count 0, got %s", got)
	}
	// Keep strconv referenced for future header assertions.
	_ = strconv.Itoa
}