package jiffy

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisCache_FromClientLeavesClientOpen(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	cache := NewRedisCacheFromClient[string](client, time.Minute)
	require.NoError(t, cache.Set(context.Background(), "k", "v"))
	v, err := cache.Get(context.Background(), "k")
	assert.NoError(t, err)
	assert.Equal(t, "v", v)

	assert.NoError(t, cache.Close(context.Background()))
	assert.NoError(t, client.Ping(context.Background()).Err(), "the caller's client stays open")
}
