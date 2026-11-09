package jiffy

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by Get when the key is not in the cache.
var ErrNotFound = errors.New("object not found")

// ErrClosed is returned when operating on a cache that has been closed.
var ErrClosed = errors.New("cache is closed")

// Cache is a generic cache interface
type Cache[T any] interface {
	// Get returns the value for key, or ErrNotFound if the key is not in the cache. Any other error indicates a
	// failure of the cache itself.
	Get(ctx context.Context, key string) (T, error)
	Set(ctx context.Context, key string, value T) error
	// Delete removes key from the cache. Deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
	Close(ctx context.Context) error
}

// Counter is implemented by caches that keep atomic, expiring counters. A Limiter uses it to share hit counts
// between replicas. Every cache in this package implements it with the same behaviour.
type Counter interface {
	// Incr atomically adds n to the counter at key and returns the new value. A missing counter starts at 0. The key
	// is set to expire after ttl; if ttl is not positive, its expiry is left untouched.
	Incr(ctx context.Context, key string, n int64, ttl time.Duration) (int64, error)
	// Count returns the value of the counter at key, or 0 if it does not exist.
	Count(ctx context.Context, key string) (int64, error)
}

// LocalCache and RedisCache implement both Cache and Counter. CompositeCache implements Cache only: copying values up
// the layers, which is what it is for, would make counters stale.
var (
	_ Cache[any] = LocalCache[any]{}
	_ Counter    = LocalCache[any]{}
	_ Cache[any] = RedisCache[any]{}
	_ Counter    = RedisCache[any]{}
	_ Cache[any] = CompositeCache[any]{}
)
