package jiffy

import (
	"context"
	"errors"
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
	Close(ctx context.Context) error
}
