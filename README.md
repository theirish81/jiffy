# Jiffy

Jiffy is a flexible and easy-to-use caching library for Go. It provides a generic cache interface and several implementations, including an in-memory cache, a Redis-backed cache, and a composite cache that can layer multiple caches.

## Features

- **Generic Cache Interface**: `Cache[T]` for type-safe caching.
- **In-Memory Cache**: `LocalCache` using the high-performance `ristretto` library.
- **Redis Cache**: `RedisCache` for distributed caching using `go-redis`.
- **Composite Cache**: `CompositeCache` to combine multiple caches into one, with a "spread" mechanism to keep caches in sync.
- **Rate Limiter**: `Limiter`, a sliding window rate limiter that decides in memory and shares counts across replicas
  through Redis in the background.

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
- `WithMissCache(maxItems int, ttl time.Duration)`: Enables miss caching (default: off). See below.

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

#### Miss Caching

By default, a key that is in none of the caches is looked up in every cache, every time. When the lower caches are
remote (e.g. Redis) and most lookups are expected to miss, that means a network round trip per lookup.

With `WithMissCache`, a key that every cache reports as missing is remembered in memory for `ttl`, and further `Get`s
for it return `jiffy.ErrNotFound` straight away:

```go
cache := jiffy.NewCompositeCache[string](
    []jiffy.Cache[string]{local, redis},
    jiffy.WithMissCache(10_000, 2*time.Second),
)
```

- Only genuine misses are remembered: if any cache returns an error other than `ErrNotFound` (e.g. Redis is down),
  the miss is not cached.
- `Set` on the composite cache clears the remembered miss for that key immediately.
- Values written to a shared cache by another process (e.g. another replica writing to Redis) become visible once the
  miss expires, so `ttl` bounds how stale a "not found" can be.

#### Deleting

Every cache implements `Delete(ctx, key)`, and deleting a missing key is not an error. On a `CompositeCache`, the key
is deleted from every cache, from the lowest up, and any remembered miss is cleared. A copy to the upper caches
queued by an earlier `Get` may still put the value back until it expires.

#### Errors

`Get` returns `jiffy.ErrNotFound` when a key is not in the cache, so misses can be told apart from failures with
`errors.Is(err, jiffy.ErrNotFound)`. Operations on a closed `CompositeCache` return `jiffy.ErrClosed`.


### Rate Limiter

The `Limiter` is a sliding window rate limiter for setups with several replicas, such as an API gateway. It is built
to stop abuse cheaply, not to enforce a limit to the exact request.

- **Every decision is local.** A replica decides from what it knows of the global count, so Redis is never on the path
  of a request.
- **Counts are shared in the background.** A replica sends its hits to Redis in batches (after 10 hits or 1 second by
  default), and learns the global count from the reply.
- **Denials cost nothing.** A denied client is answered from memory, without touching Redis.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/theirish81/jiffy"
)

func main() {
	// Share one Redis client between caches and limiters
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()
	counter := jiffy.NewRedisCacheFromClient[string](client, time.Minute)

	// Allow up to 100 requests per key in any 1 minute window
	limiter, err := jiffy.NewLimiter(counter, 100, time.Minute)
	if err != nil {
		panic(err)
	}
	defer limiter.Close(context.Background())

	d, err := limiter.Allow(context.Background(), "api-key-42")
	if err != nil {
		panic(err)
	}
	if !d.Allowed {
		fmt.Println("too many requests, retry in", d.RetryAfter)
		return
	}
	fmt.Println("ok,", d.Remaining, "left")
}
```

`AllowN` counts several hits at once.

For a single process, or when each replica should limit on its own, use `NewLocalLimiter`. It needs no `Counter`,
and the options about sending hits (flush, fallback and key prefix) have no effect:

```go
limiter, err := jiffy.NewLocalLimiter(100, time.Minute)
```

#### How it limits

Counts are kept per window (aligned to the clock), and the limit applies to a sliding estimate:

```
estimate = previous window's count × (fraction of the previous window still in view) + current window's count
```

The previous window's weight decays over time, instead of resetting all at once at the window boundary.

#### Accuracy

The limit is approximate. A key can exceed it by the hits the replicas have counted but not yet sent to Redis, plus
those arriving during a round trip. Roughly up to 3 × the flush threshold per replica under steady load. A replica
that has never seen a key (for example after a restart) lets a few hits through before it learns the global count.
Hits a replica has not yet sent are lost if the key is evicted from memory, and hits in a send that fails are not
retried: the replica keeps counting them, but the other replicas never see them. These trade-offs are what keep
Redis off the request path. Lower `WithFlushThreshold` and `WithFlushInterval` for more accuracy at the cost of more Redis
traffic.

Denied requests are not counted by default, so a client that keeps sending while denied still gets roughly the
limit's throughput. With `WithCountDenied(true)` it stays denied until it slows down.

Windows are aligned to each replica's clock, so replicas should have reasonably synchronised clocks.

#### When Redis fails

When sending hits to Redis fails, the limiter leaves Redis alone for one flush interval, and each replica keeps
limiting on the hits it has seen, against the fallback limit set with `WithFallbackLimit`. Hits counted meanwhile are
sent once Redis recovers.

- By default, the fallback limit is the normal limit. Each replica then enforces it on its own share of the traffic,
  so the global limit loosens.
- `WithFallbackLimit(limit / replicas)` keeps the global limit roughly in place.
- `WithFallbackLimit(0)` denies everything, and `WithFallbackLimit(math.MaxInt64)` allows everything.

#### Limiter Options

- `WithFlushThreshold(hits int64)`: send hits after this many (default: 10).
- `WithFlushInterval(d time.Duration)`: send hits after this long (default: 1 second).
- `WithCountDenied(enabled bool)`: count denied requests (default: off).
- `WithFallbackLimit(limit int64)`: per-replica limit while Redis is failing (default: the normal limit).
- `WithLimiterMaxKeys(n int)`: keys tracked in memory per replica (default: 10,000).
- `WithFlushTimeout(d time.Duration)`: timeout of a single send to Redis (default: 2 seconds).
- `WithKeyPrefix(prefix string)`: prefix of the Redis keys (default: `jiffy:rl:`).
- `WithLimiterLogger(l *slog.Logger)`: logger (default: `slog.Default()`).

#### Counters

Every cache (`LocalCache` and `RedisCache`) also implements `Counter`, with the same behaviour:

- `Incr(ctx, key, n, ttl)` atomically adds `n` to the counter at `key` (a missing counter starts at 0), sets it to
  expire after `ttl`, and returns the new value. A non-positive `ttl` leaves the current expiry untouched.
- `Count(ctx, key)` returns the counter's value, or 0 if it does not exist.

Counters share the key space with cached values: on a `Cache[int64]`, `Get` reads a counter and `Incr` adds to a
value written by `Set`. Incrementing a key that holds anything other than an integer is an error. A `LocalCache` may
evict counters when full, like any other key.

```go
local, _ := jiffy.NewLocalCache[string](10_000, time.Minute)
hits, _ := local.Incr(ctx, "hits:/home", 1, time.Hour)
```

The limiter accepts any `Counter`: a `RedisCache` to share counts across replicas, or a `LocalCache` (or `nil`) to
limit within a single process.
