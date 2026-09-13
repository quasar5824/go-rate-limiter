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

## API Reference

- `NewLimiter(rate, capacity)`: Initializes a new limiter.
- `Allow()`: Checks if 1 token is available and consumes it.
- `AllowN(n)`: Checks if `n` tokens are available and consumes them.
- `Available()`: Returns the current number of available tokens without consuming them.
- `Reserve(n)`: Reserves `n` tokens and returns the duration to wait until they are available.
- `Wait()`: Blocks until 1 token is available.
- `WaitN(ctx, n)`: Blocks until `n` tokens are available or context is canceled.
- `SetLimit(rate, capacity)`: Dynamically updates the limiter configuration.