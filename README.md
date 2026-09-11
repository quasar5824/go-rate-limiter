# Go Rate Limiter

A lightweight, thread-safe implementation of the Token Bucket algorithm in Go.

## Installation

```bash
go get github.com/quasar5824/go-rate-limiter
```

## Usage

```go
import "github.com/quasar5824/go-rate-limiter"

// Create a limiter that allows 10 requests per second with a burst capacity of 5
limiter := ratelimit.NewLimiter(10, 5)

if limiter.Allow() {
    // Process request
} else {
    // Return 429 Too Many Requests
}
```