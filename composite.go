package jiffy

import (
	"context"
	"errors"
	"log/slog"
	"os"
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

// CompositeCache is a cache backed by a cascade of caches.
type CompositeCache[T any] struct {
	caches []Cache[T]
	ch     chan spreadCommand[T]
	opts   CacheOptions
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
	cache.ch = make(chan spreadCommand[T], cache.opts.spreadBufferSize)
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
// the value is spread to all caches.
func (c CompositeCache[T]) Get(ctx context.Context, key string) (T, error) {
	for idx, cache := range c.caches {
		if v, err := cache.Get(ctx, key); err == nil {
			c.opts.logger.Debug("cache hit", "index", idx)
			if idx > 0 {
				select {
				case c.ch <- spreadCommand[T]{key: key, value: v, idx: idx}:
					c.opts.logger.Debug("spread command sent", "index", idx)
				default:
					c.opts.logger.Warn("spread command dropped. Channel full", "index", idx)
				}
				c.ch <- spreadCommand[T]{key: key, value: v, idx: idx}
			}

			return v, nil
		}
	}
	return *new(T), errors.New("object not found")
}

// Set sets the value for the given key. The value is spread to all caches.
func (c CompositeCache[T]) Set(ctx context.Context, key string, value T) error {
	return c.spread(ctx, key, value, len(c.caches))
}

// startSpreadWorker starts a worker to spread cache updates.
func (c CompositeCache[T]) startSpreadWorker(ch chan spreadCommand[T]) {
	go func() {
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

// Close closes the cache. It closes all caches and stops the spread worker.
func (c CompositeCache[T]) Close(ctx context.Context) error {
	close(c.ch)
	for _, cache := range c.caches {
		if err := cache.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
