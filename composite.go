package jiffy

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
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

// compositeState holds the mutable lifecycle state of a CompositeCache. It is shared by pointer so that copies of
// a CompositeCache value all observe the same closed state.
type compositeState struct {
	// mu guards closed. Operations hold the read lock for their whole duration, so Close (which takes the write
	// lock) waits for in-flight operations to finish and no operation can send on the channel after it is closed.
	mu     sync.RWMutex
	closed bool
	// done is closed when the spread worker has drained the channel and exited.
	done chan struct{}
}

// CompositeCache is a cache backed by a cascade of caches.
type CompositeCache[T any] struct {
	caches []Cache[T]
	ch     chan spreadCommand[T]
	opts   CacheOptions
	state  *compositeState
	// misses remembers keys not found in any cache. nil when miss caching is disabled.
	misses *ristretto.Cache[string, struct{}]
}

// NewCompositeCache creates a new composite cache.
// caches is the list of caches to use.
// options optional settings
func NewCompositeCache[T any](caches []Cache[T], options ...CacheOption) CompositeCache[T] {
	cache := CompositeCache[T]{}
	cache.opts = CacheOptions{
		spreadTimeout:    defaultSpreadTimeout,
		spreadBufferSize: defaultSpreadBufferSize,
		logger: slog.New(
			slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			})),
	}
	for _, option := range options {
		option(&cache.opts)
	}
	cache.caches = caches
	if cache.opts.missCacheSize > 0 && cache.opts.missCacheTTL > 0 {
		misses, err := ristretto.NewCache[string, struct{}](&ristretto.Config[string, struct{}]{
			NumCounters:        int64(cache.opts.missCacheSize * 10),
			MaxCost:            int64(cache.opts.missCacheSize),
			BufferItems:        64,
			IgnoreInternalCost: true,
		})
		if err != nil {
			// Only reachable with an invalid config, which the guard above rules out.
			panic("jiffy: could not create miss cache: " + err.Error())
		}
		cache.misses = misses
	}
	cache.ch = make(chan spreadCommand[T], cache.opts.spreadBufferSize)
	cache.state = &compositeState{done: make(chan struct{})}
	cache.startSpreadWorker(cache.ch)
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
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	if c.state.closed {
		return *new(T), ErrClosed
	}
	if c.misses != nil {
		if _, found := c.misses.Get(key); found {
			c.opts.logger.Debug("miss cache hit")
			return *new(T), ErrNotFound
		}
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
			select {
			case c.ch <- spreadCommand[T]{key: key, value: v, idx: idx}:
				c.opts.logger.Debug("spread command sent", "index", idx)
			default:
				c.opts.logger.Warn("spread command dropped. Channel full", "index", idx)
			}
		}
		return v, nil
	}
	// Only remember genuine misses: if a cache failed, the key may well exist there.
	if c.misses != nil && allNotFound {
		c.misses.SetWithTTL(key, struct{}{}, 1, c.opts.missCacheTTL)
	}
	return *new(T), ErrNotFound
}

// Set sets the value for the given key. The value is spread to all caches, and any remembered miss for the key is
// cleared.
func (c CompositeCache[T]) Set(ctx context.Context, key string, value T) error {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	if c.state.closed {
		return ErrClosed
	}
	if c.misses != nil {
		c.misses.Del(key)
	}
	return c.spread(ctx, key, value, len(c.caches))
}

// startSpreadWorker starts a worker to spread cache updates. The worker exits once ch is closed and drained,
// signalling completion by closing state.done.
func (c CompositeCache[T]) startSpreadWorker(ch chan spreadCommand[T]) {
	go func() {
		defer close(c.state.done)
		for cmd := range ch {
			func(cmd spreadCommand[T]) {
				ctx, cancel := context.WithTimeout(context.Background(), c.opts.spreadTimeout)
				defer cancel()
				if err := c.spread(ctx, cmd.key, cmd.value, cmd.idx); err != nil {
					c.opts.logger.Error("could not spread cache", "err", err.Error())
				}
			}(cmd)
		}
	}()
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
	c.state.mu.Lock()
	if c.state.closed {
		c.state.mu.Unlock()
		return nil
	}
	c.state.closed = true
	close(c.ch)
	c.state.mu.Unlock()

	select {
	case <-c.state.done:
	case <-ctx.Done():
		c.opts.logger.Warn("timed out waiting for spread worker to drain", "err", ctx.Err())
	}

	if c.misses != nil {
		c.misses.Close()
	}
	var errs []error
	for _, cache := range c.caches {
		if err := cache.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
