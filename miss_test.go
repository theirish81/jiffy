package jiffy

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// getAndSettle performs a Get. The miss cache is a LocalCache, whose writes are applied before they return, so no
// extra settling is needed.
func getAndSettle(cache CompositeCache[string], key string) (string, error) {
	return cache.Get(context.Background(), key)
}

func TestMissCache_DisabledByDefault(t *testing.T) {
	l1, l2 := newFakeCache(), newFakeCache()
	cache := NewCompositeCache[string]([]Cache[string]{l1, l2}, quietLogger())
	defer cache.Close(context.Background())

	for i := 0; i < 3; i++ {
		_, err := getAndSettle(cache, "k")
		assert.ErrorIs(t, err, ErrNotFound)
	}
	assert.Equal(t, int32(3), l2.gets.Load(), "without miss caching every Get should reach the lower cache")
}

func TestMissCache_DisabledWithNonPositiveValues(t *testing.T) {
	for _, opt := range []CacheOption{WithMissCache(0, time.Second), WithMissCache(10, 0)} {
		cache := NewCompositeCache[string]([]Cache[string]{newFakeCache()}, quietLogger(), opt)
		assert.Nil(t, cache.misses)
		assert.Nil(t, cache.Close(context.Background()))
	}
}

func TestMissCache_RemembersMisses(t *testing.T) {
	l1, l2 := newFakeCache(), newFakeCache()
	cache := NewCompositeCache[string]([]Cache[string]{l1, l2}, quietLogger(), WithMissCache(10, time.Minute))
	defer cache.Close(context.Background())

	for i := 0; i < 3; i++ {
		_, err := getAndSettle(cache, "k")
		assert.ErrorIs(t, err, ErrNotFound)
	}
	assert.Equal(t, int32(1), l1.gets.Load())
	assert.Equal(t, int32(1), l2.gets.Load(), "only the first Get should reach the lower cache")
}

func TestMissCache_Expires(t *testing.T) {
	l1 := newFakeCache()
	cache := NewCompositeCache[string]([]Cache[string]{l1}, quietLogger(), WithMissCache(10, 100*time.Millisecond))
	defer cache.Close(context.Background())

	_, _ = getAndSettle(cache, "k")
	l1.data["k"] = "v" // written behind the composite cache's back
	_, err := getAndSettle(cache, "k")
	assert.ErrorIs(t, err, ErrNotFound, "miss should still be remembered")

	time.Sleep(200 * time.Millisecond)
	v, err := getAndSettle(cache, "k")
	assert.Nil(t, err)
	assert.Equal(t, "v", v)
}

func TestMissCache_SetClearsMiss(t *testing.T) {
	l1, l2 := newFakeCache(), newFakeCache()
	cache := NewCompositeCache[string]([]Cache[string]{l1, l2}, quietLogger(), WithMissCache(10, time.Minute))
	defer cache.Close(context.Background())

	_, err := getAndSettle(cache, "k")
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, cache.Set(context.Background(), "k", "v"))
	v, err := getAndSettle(cache, "k")
	assert.Nil(t, err)
	assert.Equal(t, "v", v)
}

func TestMissCache_DoesNotRememberFailures(t *testing.T) {
	l1, l2 := newFakeCache(), newFakeCache()
	l2.getErr = errors.New("connection refused")
	cache := NewCompositeCache[string]([]Cache[string]{l1, l2}, quietLogger(), WithMissCache(10, time.Minute))
	defer cache.Close(context.Background())

	_, _ = getAndSettle(cache, "k")
	_, _ = getAndSettle(cache, "k")
	assert.Equal(t, int32(2), l2.gets.Load(), "a failing cache must not cause a miss to be remembered")

	// Once the cache recovers, its value is returned.
	l2.mu.Lock()
	l2.getErr = nil
	l2.data["k"] = "v"
	l2.mu.Unlock()
	v, err := getAndSettle(cache, "k")
	assert.Nil(t, err)
	assert.Equal(t, "v", v)
}

func TestLocalCache_ReturnsErrNotFound(t *testing.T) {
	local, err := NewLocalCache[string](10, time.Minute)
	assert.Nil(t, err)
	defer local.Close(context.Background())
	_, err = local.Get(context.Background(), "nope")
	assert.ErrorIs(t, err, ErrNotFound)
}
