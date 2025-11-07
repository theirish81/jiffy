package jiffy

import (
	"context"
)

// Cache is a generic cache interface
type Cache[T any] interface {
	Get(ctx context.Context, key string) (T, error)
	Set(ctx context.Context, key string, value T) error
	Close(ctx context.Context) error
}
