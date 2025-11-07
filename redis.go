package jiffy

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache is a Redis backed cache.
type RedisCache[T any] struct {
	impl *redis.Client
	ttl  time.Duration
}

// NewRedisCache creates a new Redis cache.
// address is the Redis address.
// password is the Redis password.
// db is the Redis database.
// ttl is the time-to-live for items in the cache.
func NewRedisCache[T any](address string, password string, db int, ttl time.Duration) (RedisCache[T], error) {
	c := RedisCache[T]{
		impl: redis.NewClient(&redis.Options{
			Addr:     address,
			Password: password,
			DB:       db,
		}),
		ttl: ttl,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := c.impl.Ping(ctx)
	return c, s.Err()
}

// Set sets the value for the given key.
func (c RedisCache[T]) Set(ctx context.Context, key string, value T) error {
	cmd := c.impl.Set(ctx, key, &value, c.ttl)
	return cmd.Err()
}

// Get gets the value for the given key.
func (c RedisCache[T]) Get(ctx context.Context, key string) (T, error) {
	cmd := c.impl.Get(ctx, key)
	if cmd.Err() != nil {
		return *new(T), cmd.Err()
	}
	var value T
	err := cmd.Scan(&value)
	return value, err
}

// Close closes the cache.
func (c RedisCache[T]) Close(_ context.Context) error {
	return c.impl.Close()
}
