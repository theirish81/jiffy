package jiffy

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache is a Redis backed cache. It also implements Counter, so it can back a Limiter.
type RedisCache[T any] struct {
	impl redis.UniversalClient
	ttl  time.Duration
	// ownsClient is true when the cache created its client, and is therefore responsible for closing it.
	ownsClient bool
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
		ttl:        ttl,
		ownsClient: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := c.impl.Ping(ctx)
	return c, s.Err()
}

// NewRedisCacheFromClient creates a new Redis cache on top of an existing client, so that several caches (and
// limiters) can share one connection pool. The caller keeps ownership of the client: Close does not close it.
// ttl is the time-to-live for items in the cache.
func NewRedisCacheFromClient[T any](client redis.UniversalClient, ttl time.Duration) RedisCache[T] {
	return RedisCache[T]{impl: client, ttl: ttl}
}

// Set sets the value for the given key.
func (c RedisCache[T]) Set(ctx context.Context, key string, value T) error {
	cmd := c.impl.Set(ctx, key, &value, c.ttl)
	return cmd.Err()
}

// Get gets the value for the given key.
func (c RedisCache[T]) Get(ctx context.Context, key string) (T, error) {
	cmd := c.impl.Get(ctx, key)
	if err := cmd.Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return *new(T), ErrNotFound
		}
		return *new(T), err
	}
	var value T
	err := cmd.Scan(&value)
	return value, err
}

// Delete removes the value or counter at key. Deleting a missing key is not an error.
func (c RedisCache[T]) Delete(ctx context.Context, key string) error {
	return c.impl.Del(ctx, key).Err()
}

// Incr atomically adds n to the counter at key and returns the new value. The increment and the expiry are applied
// in a single MULTI/EXEC transaction, so a counter can never be left without an expiry. If ttl is not positive, the
// expiry is left untouched. Counters are independent of T: they are plain Redis integers.
func (c RedisCache[T]) Incr(ctx context.Context, key string, n int64, ttl time.Duration) (int64, error) {
	var incr *redis.IntCmd
	_, err := c.impl.TxPipelined(ctx, func(p redis.Pipeliner) error {
		incr = p.IncrBy(ctx, key, n)
		if ttl > 0 {
			// PEXPIRE, not EXPIRE, which only has second precision and rounds shorter TTLs up to 1s.
			p.PExpire(ctx, key, ttl)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return incr.Val(), nil
}

// Count returns the value of the counter at key, or 0 if it does not exist.
func (c RedisCache[T]) Count(ctx context.Context, key string) (int64, error) {
	v, err := c.impl.Get(ctx, key).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return v, err
}

// Close closes the cache. A client passed to NewRedisCacheFromClient is left open.
func (c RedisCache[T]) Close(_ context.Context) error {
	if !c.ownsClient {
		return nil
	}
	return c.impl.Close()
}
