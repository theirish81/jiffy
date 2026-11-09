package jiffy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// counterCache is a cache that also implements Counter, as LocalCache and RedisCache do.
type counterCache interface {
	Cache[int64]
	Counter
}

// counterImplementation builds a fresh cache, and a function that moves its clock forward.
type counterImplementation func(t *testing.T) (counterCache, func(d time.Duration))

// counterImplementations lists every cache that must behave identically as a Counter. Add new caches here.
var counterImplementations = map[string]counterImplementation{
	"LocalCache": func(t *testing.T) (counterCache, func(d time.Duration)) {
		cache, err := NewLocalCache[int64](1000, time.Minute)
		require.NoError(t, err)
		t.Cleanup(func() { _ = cache.Close(context.Background()) })
		return cache, time.Sleep // ristretto expires against the real clock
	},
	"RedisCache": func(t *testing.T) (counterCache, func(d time.Duration)) {
		mr := miniredis.RunT(t)
		cache, err := NewRedisCache[int64](mr.Addr(), "", 0, time.Minute)
		require.NoError(t, err)
		t.Cleanup(func() { _ = cache.Close(context.Background()) })
		return cache, mr.FastForward
	},
}

// TestCounterConformance runs the same assertions against every implementation.
func TestCounterConformance(t *testing.T) {
	const ttl = 200 * time.Millisecond
	ctx := context.Background()
	for name, newCache := range counterImplementations {
		t.Run(name, func(t *testing.T) {
			t.Run("missing counter counts as zero", func(t *testing.T) {
				c, _ := newCache(t)
				n, err := c.Count(ctx, "c")
				assert.NoError(t, err)
				assert.Equal(t, int64(0), n)
			})

			t.Run("incr returns running totals", func(t *testing.T) {
				c, _ := newCache(t)
				for _, step := range []struct{ n, want int64 }{{1, 1}, {4, 5}, {-2, 3}} {
					n, err := c.Incr(ctx, "c", step.n, ttl)
					assert.NoError(t, err)
					assert.Equal(t, step.want, n)
				}
				assert.Equal(t, int64(3), mustCount(t, c, "c"))
			})

			t.Run("counter expires after ttl", func(t *testing.T) {
				c, advance := newCache(t)
				_, err := c.Incr(ctx, "c", 1, ttl)
				require.NoError(t, err)
				advance(ttl + 100*time.Millisecond)
				assert.Equal(t, int64(0), mustCount(t, c, "c"))
			})

			t.Run("incr refreshes the expiry", func(t *testing.T) {
				c, advance := newCache(t)
				_, err := c.Incr(ctx, "c", 1, ttl)
				require.NoError(t, err)
				advance(ttl / 2)
				_, err = c.Incr(ctx, "c", 1, ttl)
				require.NoError(t, err)
				advance(ttl/2 + ttl/4)
				assert.Equal(t, int64(2), mustCount(t, c, "c"))
			})

			t.Run("non-positive ttl leaves the expiry untouched", func(t *testing.T) {
				c, advance := newCache(t)
				_, err := c.Incr(ctx, "expiring", 1, ttl)
				require.NoError(t, err)
				_, err = c.Incr(ctx, "expiring", 1, 0)
				require.NoError(t, err)
				_, err = c.Incr(ctx, "forever", 1, 0)
				require.NoError(t, err)
				advance(ttl + 100*time.Millisecond)
				assert.Equal(t, int64(0), mustCount(t, c, "expiring"), "an existing expiry is kept")
				assert.Equal(t, int64(1), mustCount(t, c, "forever"), "a new counter without ttl does not expire")
			})

			t.Run("counters share the key space with values", func(t *testing.T) {
				c, _ := newCache(t)
				require.NoError(t, c.Set(ctx, "k", 5))
				n, err := c.Incr(ctx, "k", 1, ttl)
				assert.NoError(t, err)
				assert.Equal(t, int64(6), n)
				v, err := c.Get(ctx, "k")
				assert.NoError(t, err)
				assert.Equal(t, int64(6), v)
			})

			t.Run("delete removes values and counters", func(t *testing.T) {
				c, _ := newCache(t)
				require.NoError(t, c.Set(ctx, "v", 7))
				_, err := c.Incr(ctx, "c", 3, ttl)
				require.NoError(t, err)
				assert.NoError(t, c.Delete(ctx, "v"))
				assert.NoError(t, c.Delete(ctx, "c"))
				assert.NoError(t, c.Delete(ctx, "missing"), "deleting a missing key is not an error")
				_, err = c.Get(ctx, "v")
				assert.ErrorIs(t, err, ErrNotFound)
				assert.Equal(t, int64(0), mustCount(t, c, "c"))
			})

			t.Run("concurrent increments are not lost", func(t *testing.T) {
				c, _ := newCache(t)
				var wg sync.WaitGroup
				for g := 0; g < 20; g++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := 0; i < 25; i++ {
							_, err := c.Incr(ctx, "c", 1, time.Minute)
							assert.NoError(t, err)
						}
					}()
				}
				wg.Wait()
				assert.Equal(t, int64(500), mustCount(t, c, "c"))
			})
		})
	}
}

// TestCounter_RejectsNonIntegerValues checks that a key holding something other than a counter is not overwritten.
func TestCounter_RejectsNonIntegerValues(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalCache[string](10, time.Minute)
	require.NoError(t, err)
	defer local.Close(ctx)
	mr := miniredis.RunT(t)
	remote, err := NewRedisCache[string](mr.Addr(), "", 0, time.Minute)
	require.NoError(t, err)
	defer remote.Close(ctx)

	for name, c := range map[string]interface {
		Cache[string]
		Counter
	}{"LocalCache": local, "RedisCache": remote} {
		require.NoError(t, c.Set(ctx, "k", "hello"), name)
		_, err := c.Incr(ctx, "k", 1, time.Minute)
		assert.Error(t, err, name)
		_, err = c.Count(ctx, "k")
		assert.Error(t, err, name)
		v, err := c.Get(ctx, "k")
		assert.NoError(t, err, name)
		assert.Equal(t, "hello", v, name)
	}
}

func mustCount(t *testing.T, c Counter, key string) int64 {
	t.Helper()
	n, err := c.Count(context.Background(), key)
	require.NoError(t, err)
	return n
}
