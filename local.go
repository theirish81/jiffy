package jiffy

import (
	"context"
	"errors"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// LocalCache is a simple in-memory cache.
type LocalCache[T any] struct {
	impl *ristretto.Cache[string, T]
	ttl  time.Duration
}

// NewLocalCache creates a new local cache.
// maxItems is the maximum number of items to store in the cache.
// ttl is the time-to-live for items in the cache.
func NewLocalCache[T any](maxItems int, ttl time.Duration) (LocalCache[T], error) {
	cx, err := ristretto.NewCache[string, T](&ristretto.Config[string, T]{
		NumCounters:        int64(maxItems * 10),
		MaxCost:            int64(maxItems),
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	return LocalCache[T]{
		impl: cx,
		ttl:  ttl,
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
	if v, found := c.impl.Get(key); found {
		return v, nil
	}
	return *new(T), errors.New("object not found")
}

// Close closes the cache.
func (c LocalCache[T]) Close(_ context.Context) error {
	c.impl.Close()
	return nil
}
