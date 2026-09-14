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

### Configuration
- `NewLimiter(rate, capacity)`: Initializes a new limiter.
- `SetLimit(rate, capacity)`: Dynamically updates the limiter configuration.

### Non-blocking Checks
- `Allow()`: Checks if 1 token is available and consumes it.
- `AllowN(n)`: Checks if `n` tokens are available and consumes them.
- `TryAllow(n)`: Alias for `AllowN(n)`, emphasizing a non-blocking attempt.
- `AllowWithDuration(n)`: Checks if `n` tokens are available. If not, returns the duration to wait until they become available without consuming them.
- `BatchAllow(requests)`: Takes a slice of token requirements and consumes tokens for as many as possible.
- `Available()`: Returns the current number of available tokens without consuming them.

### Blocking & Reservation
- `Reserve(n)`: Reserves `n` tokens immediately and returns the duration to wait until they are available.
- `ReserveN(n)`: Alias for `Reserve(n)`.
- `Wait()`: Blocks until 1 token is available.
- `WaitN(ctx, n)`: Blocks until `n` tokens are available or the provided context is canceled.