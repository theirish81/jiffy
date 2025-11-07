# Jiffy

Jiffy is a flexible and easy-to-use caching library for Go. It provides a generic cache interface and several implementations, including an in-memory cache, a Redis-backed cache, and a composite cache that can layer multiple caches.

## Features

- **Generic Cache Interface**: `Cache[T]` for type-safe caching.
- **In-Memory Cache**: `LocalCache` using the high-performance `ristretto` library.
- **Redis Cache**: `RedisCache` for distributed caching using `go-redis`.
- **Composite Cache**: `CompositeCache` to combine multiple caches into one, with a "spread" mechanism to keep caches in sync.

## Installation

```bash
go get github.com/theirish81/jiffy
```

## Usage

### Local Cache

The `LocalCache` is a simple, in-memory cache.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/theirish81/jiffy"
)

func main() {
	// Create a new local cache with a max of 10 items and a 2-second TTL
	cache, err := jiffy.NewLocalCache[string](10, 2*time.Second)
	if err != nil {
		panic(err)
	}
	defer cache.Close(context.Background())

	// Set a value
	cache.Set(context.Background(), "mykey", "myvalue")

	// Get a value
	value, err := cache.Get(context.Background(), "mykey")
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
}
```

### Redis Cache

The `RedisCache` uses a Redis server for storage.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/theirish81/jiffy"
)

func main() {
	// Create a new Redis cache
	cache, err := jiffy.NewRedisCache[string]("localhost:6379", "", 0, 5*time.Second)
	if err != nil {
		panic(err)
	}
	defer cache.Close(context.Background())

	// Set a value
	cache.Set(context.Background(), "mykey", "myvalue")

	// Get a value
	value, err := cache.Get(context.Background(), "mykey")
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
}
```

### Composite Cache

The `CompositeCache` layers multiple caches. When a value is retrieved from a "lower" cache, it is "spread" to the "higher" caches to keep them warm.

For example, you can have a `LocalCache` as your L1 cache and a `RedisCache` as your L2 cache. When you request an item, `jiffy` will first check the `LocalCache`. If it's not there, it will check the `RedisCache`. If the item is in the `RedisCache`, `jiffy` will return it and, in the background, also add it to the `LocalCache` for next time.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/theirish81/jiffy"
)

func main() {
	// Create the L1 and L2 caches
	local, _ := jiffy.NewLocalCache[string](10, 2*time.Second)
	redis, _ := jiffy.NewRedisCache[string]("localhost:6379", "", 0, 5*time.Second)

	// Create a composite cache
	cache := jiffy.NewCompositeCache[string]([]jiffy.Cache[string]{local, redis})
	defer cache.Close(context.Background())

	// Set a value
	cache.Set(context.Background(), "mykey", "myvalue")

	// Get a value
	value, err := cache.Get(context.Background(), "mykey")
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
}
```

#### Composite Cache Options

The `CompositeCache` can be configured with a few options:

- `WithSpreadBufferSize(size int)`: Sets the buffer size for the spread worker (default: 100).
- `WithSpreadTimeout(d time.Duration)`: Sets the timeout for the spread worker (default: 5 seconds).
- `WithLogger(l *slog.Logger)`: Sets the logger for the cache.

Example:

```go
import "log/slog"

// ...

cache := jiffy.NewCompositeCache[string](
    []jiffy.Cache[string]{local, redis},
    jiffy.WithSpreadBufferSize(200),
    jiffy.WithSpreadTimeout(10*time.Second),
    jiffy.WithLogger(slog.Default()),
)
```
