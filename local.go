package jiffy

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// localCounterStripes is the number of locks that serialise counter updates. Keys are spread over them by hash.
const localCounterStripes = 64

// LocalCache is a simple in-memory cache. It also implements Counter.
//
// Like in Redis, values and counters share one key space: a counter is a key holding an int64, so on a
// LocalCache[int64], Get reads a counter and Incr adds to a value written by Set. Unlike Redis, a key may be evicted
// when the cache is full, counters included.
//
// The zero LocalCache holds nothing and ignores writes: Get returns ErrNotFound, Set, Delete and Close do nothing.
// Incr and Count must not be used on it.
type LocalCache[T any] struct {
	impl *ristretto.Cache[string, any]
	ttl  time.Duration
	// locks serialise Incr calls on the same key, as ristretto has no atomic increment.
	locks *[localCounterStripes]sync.Mutex
}

// NewLocalCache creates a new local cache.
// maxItems is the maximum number of items to store in the cache.
// ttl is the time-to-live for items in the cache.
func NewLocalCache[T any](maxItems int, ttl time.Duration) (LocalCache[T], error) {
	cx, err := ristretto.NewCache[string, any](&ristretto.Config[string, any]{
		NumCounters:        int64(maxItems * 10),
		MaxCost:            int64(maxItems),
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	return LocalCache[T]{
		impl:  cx,
		ttl:   ttl,
		locks: new([localCounterStripes]sync.Mutex),
	}, err
}

// Set sets the value for the given key.
func (c LocalCache[T]) Set(_ context.Context, key string, value T) error {
	c.impl.SetWithTTL(key, value, 1, c.ttl)
	c.impl.Wait()
	return nil
}

// Get gets the value for the given key.
func (c LocalCache[T]) Get(_ context.Context, key string) (T, error) {
	v, found := c.impl.Get(key)
	if !found {
		return *new(T), ErrNotFound
	}
	value, ok := v.(T)
	if !ok {
		return *new(T), fmt.Errorf("jiffy: value at key %q is a %T, not a %T", key, v, *new(T))
	}
	return value, nil
}

// Delete removes the value or counter at key. Deleting a missing key is not an error.
func (c LocalCache[T]) Delete(_ context.Context, key string) error {
	c.impl.Del(key)
	return nil
}

// Incr atomically adds n to the counter at key and returns the new value. A missing counter starts at 0. The key is
// set to expire after ttl; if ttl is not positive, its expiry is left untouched. Counters are independent of T.
func (c LocalCache[T]) Incr(_ context.Context, key string, n int64, ttl time.Duration) (int64, error) {
	lock := c.lock(key)
	lock.Lock()
	defer lock.Unlock()
	var current int64
	if v, found := c.impl.Get(key); found {
		var ok bool
		if current, ok = v.(int64); !ok {
			return 0, fmt.Errorf("jiffy: value at key %q is a %T, not a counter", key, v)
		}
		if ttl <= 0 {
			// Keep the current expiry, as Redis does. A remaining TTL of 0 means the key never expires.
			ttl, _ = c.impl.GetTTL(key)
		}
	}
	current += n
	c.impl.SetWithTTL(key, current, 1, ttl)
	c.impl.Wait()
	return current, nil
}

// Count returns the value of the counter at key, or 0 if it does not exist.
func (c LocalCache[T]) Count(_ context.Context, key string) (int64, error) {
	v, found := c.impl.Get(key)
	if !found {
		return 0, nil
	}
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("jiffy: value at key %q is a %T, not a counter", key, v)
	}
	return n, nil
}

// lock returns the lock guarding counter updates on key.
func (c LocalCache[T]) lock(key string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &c.locks[h.Sum32()%localCounterStripes]
}

// Close closes the cache.
func (c LocalCache[T]) Close(_ context.Context) error {
	c.impl.Close()
	return nil
}
