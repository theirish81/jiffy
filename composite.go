package jiffy

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// defaultSpreadTimeout is the default timeout for the spread worker.
const defaultSpreadTimeout = 5 * time.Second

// defaultSpreadBufferSize is the default buffer size for the spread worker.
const defaultSpreadBufferSize = 100

// CacheOptions are options for a cache.
type CacheOptions struct {
	spreadBufferSize int
	spreadTimeout    time.Duration
	logger           *slog.Logger
	missCacheSize    int
	missCacheTTL     time.Duration
}

// CacheOption is an option for a cache.
type CacheOption func(options *CacheOptions)

// WithSpreadBufferSize sets the buffer size for the spread worker.
func WithSpreadBufferSize(size int) CacheOption {
	return func(c *CacheOptions) {
		c.spreadBufferSize = size
	}
}

// WithSpreadTimeout sets the timeout for the spread worker.
func WithSpreadTimeout(d time.Duration) CacheOption {
	return func(c *CacheOptions) {
		c.spreadTimeout = d
	}
}

// WithLogger sets the logger for the cache
func WithLogger(l *slog.Logger) CacheOption {
	return func(c *CacheOptions) {
		c.logger = l
	}
}

// WithMissCache enables miss caching. When a key is not found in any of the caches, the miss is remembered locally
// for ttl, and subsequent Gets for that key return ErrNotFound without querying the caches. This is useful when
// lower caches are remote (e.g. Redis) and most lookups are expected to miss.
// maxItems is the maximum number of misses to remember. ttl is how long a miss is remembered.
// Miss caching is disabled by default, and is also disabled if maxItems or ttl are not positive.
//
// A Set on this CompositeCache clears the miss for its key immediately. Values written to the underlying caches by
// other means (e.g. another process writing to a shared Redis) become visible only once the miss expires.
func WithMissCache(maxItems int, ttl time.Duration) CacheOption {
	return func(c *CacheOptions) {
		c.missCacheSize = maxItems
		c.missCacheTTL = ttl
	}
}

// CompositeCache is a cache backed by a cascade of caches.
type CompositeCache[T any] struct {
	caches []Cache[T]
	opts   CacheOptions
	// spreads runs spread commands in the background. It is a pointer, so that copies of a CompositeCache value all
	// share the same closed state.
	spreads *backgroundQueue[spreadCommand[T]]
	// misses remembers keys not found in any cache. When miss caching is disabled, it is the zero LocalCache, which
	// holds nothing and ignores writes.
	misses LocalCache[miss]
}

// miss is the value stored in the miss cache: only the presence of a key matters.
type miss struct{}

// NewCompositeCache creates a new composite cache.
// caches is the list of caches to use.
// options optional settings
func NewCompositeCache[T any](caches []Cache[T], options ...CacheOption) CompositeCache[T] {
	cache := CompositeCache[T]{}
	cache.opts = CacheOptions{
		spreadTimeout:    defaultSpreadTimeout,
		spreadBufferSize: defaultSpreadBufferSize,
		logger:           slog.Default(),
	}
	for _, option := range options {
		option(&cache.opts)
	}
	cache.caches = caches
	if cache.opts.missCacheSize > 0 && cache.opts.missCacheTTL > 0 {
		misses, err := NewLocalCache[miss](cache.opts.missCacheSize, cache.opts.missCacheTTL)
		if err != nil {
			// Only reachable with an invalid config, which the guard above rules out.
			panic("jiffy: could not create miss cache: " + err.Error())
		}
		cache.misses = misses
	}
	cache.spreads = newBackgroundQueue(cache.opts.spreadBufferSize, 1, cache.opts.spreadTimeout,
		func(ctx context.Context, cmd spreadCommand[T]) {
			if err := cache.spread(ctx, cmd.key, cmd.value, cmd.idx); err != nil {
				cache.opts.logger.Error("could not spread cache", "err", err.Error())
			}
		})
	return cache
}

// spreadCommand is a command to spread a value to a cache.
type spreadCommand[T any] struct {
	key   string
	value T
	idx   int
}

// Get gets the value for the given key. The first cache to return a value is returned. If a cached value is found,
// the value is spread to all caches. If miss caching is enabled and the key is not found in any cache, the miss is
// remembered.
func (c CompositeCache[T]) Get(ctx context.Context, key string) (T, error) {
	if !c.spreads.enter() {
		return *new(T), ErrClosed
	}
	defer c.spreads.exit()
	if _, err := c.misses.Get(ctx, key); err == nil {
		c.opts.logger.Debug("miss cache hit")
		return *new(T), ErrNotFound
	}
	allNotFound := true
	for idx, cache := range c.caches {
		v, err := cache.Get(ctx, key)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				allNotFound = false
			}
			continue
		}
		c.opts.logger.Debug("cache hit", "index", idx)
		if idx > 0 {
			if c.spreads.tryPush(spreadCommand[T]{key: key, value: v, idx: idx}) {
				c.opts.logger.Debug("spread command sent", "index", idx)
			} else {
				c.opts.logger.Warn("spread command dropped. Channel full", "index", idx)
			}
		}
		return v, nil
	}
	// Only remember genuine misses: if a cache failed, the key may well exist there.
	if allNotFound {
		_ = c.misses.Set(ctx, key, miss{}) // LocalCache.Set never fails
	}
	return *new(T), ErrNotFound
}

// Set sets the value for the given key. The value is spread to all caches, and any remembered miss for the key is
// cleared.
func (c CompositeCache[T]) Set(ctx context.Context, key string, value T) error {
	if !c.spreads.enter() {
		return ErrClosed
	}
	defer c.spreads.exit()
	_ = c.misses.Delete(ctx, key) // LocalCache.Delete never fails
	return c.spread(ctx, key, value, len(c.caches))
}

// Delete removes key from every cache, and clears any remembered miss for it. Caches are cleared from the lowest
// up, so that a concurrent Get cannot copy the value back up from a cache not yet cleared. Every cache is attempted,
// and every error encountered is returned.
//
// A copy queued by an earlier Get, but not yet applied, can still put the value back into the upper caches until it
// expires.
func (c CompositeCache[T]) Delete(ctx context.Context, key string) error {
	if !c.spreads.enter() {
		return ErrClosed
	}
	defer c.spreads.exit()
	var errs []error
	for i := len(c.caches) - 1; i >= 0; i-- {
		if err := c.caches[i].Delete(ctx, key); err != nil {
			errs = append(errs, err)
		}
	}
	_ = c.misses.Delete(ctx, key) // LocalCache.Delete never fails
	return errors.Join(errs...)
}

// spread spreads the value to caches up to the given index.
func (c CompositeCache[T]) spread(ctx context.Context, key string, value T, idx int) error {
	for i := 0; i < idx; i++ {
		if err := c.caches[i].Set(ctx, key, value); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the cache. It waits for in-flight operations, drains pending spread commands, then closes all
// caches, returning every error encountered. Calling Close more than once is a no-op.
func (c CompositeCache[T]) Close(ctx context.Context) error {
	closed, err := c.spreads.close(ctx)
	if !closed {
		return nil
	}
	if err != nil {
		c.opts.logger.Warn("timed out waiting for spread worker to drain", "err", err)
	}

	_ = c.misses.Close(ctx) // LocalCache.Close never fails
	var errs []error
	for _, cache := range c.caches {
		if err := cache.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
