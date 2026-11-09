package jiffy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCache is an in-memory Cache used to observe calls made by CompositeCache.
type fakeCache struct {
	mu        sync.Mutex
	data      map[string]string
	gets      atomic.Int32
	sets      atomic.Int32
	deletes   atomic.Int32
	deletedAt atomic.Int64
	closed    atomic.Int32
	getErr    error
	deleteErr error
	closeErr  error
	setDelay  time.Duration
}

func newFakeCache() *fakeCache {
	return &fakeCache{data: map[string]string{}}
}

func (f *fakeCache) Get(_ context.Context, key string) (string, error) {
	f.gets.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return "", f.getErr
	}
	if v, ok := f.data[key]; ok {
		return v, nil
	}
	return "", ErrNotFound
}

func (f *fakeCache) Set(_ context.Context, key string, value string) error {
	time.Sleep(f.setDelay)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = value
	f.sets.Add(1)
	return nil
}

// deleteClock orders Delete calls across fakeCaches.
var deleteClock atomic.Int64

func (f *fakeCache) Delete(_ context.Context, key string) error {
	f.deletes.Add(1)
	f.deletedAt.Store(deleteClock.Add(1))
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, key)
	return nil
}

func (f *fakeCache) Close(_ context.Context) error {
	f.closed.Add(1)
	return f.closeErr
}

func quietLogger() CacheOption {
	return WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestCompositeCache_Get(t *testing.T) {
	local1, err := NewLocalCache[string](10, 2*time.Second)
	assert.Nil(t, err)
	local2, err := NewLocalCache[string](10, 5*time.Second)
	assert.Nil(t, err)
	cache := NewCompositeCache[string]([]Cache[string]{local1, local2}, quietLogger())
	defer cache.Close(context.Background())
	err = cache.Set(context.Background(), "k1", "v1")
	assert.Nil(t, err)
	for i := 0; i < 5; i++ {
		v, err := cache.Get(context.Background(), "k1")
		assert.Nil(t, err)
		assert.Equal(t, "v1", v)
		time.Sleep(1 * time.Second)
	}
}

func TestCompositeCache_SpreadsOnceOnLowerHit(t *testing.T) {
	l1, l2 := newFakeCache(), newFakeCache()
	l2.data["k"] = "v"
	cache := NewCompositeCache[string]([]Cache[string]{l1, l2}, quietLogger())

	v, err := cache.Get(context.Background(), "k")
	assert.Nil(t, err)
	assert.Equal(t, "v", v)

	// Close drains the spread worker, so all spreads have completed afterwards.
	assert.Nil(t, cache.Close(context.Background()))
	assert.Equal(t, int32(1), l1.sets.Load(), "L1 should be warmed exactly once")
	assert.Equal(t, "v", l1.data["k"])
	assert.Equal(t, int32(0), l2.sets.Load())
}

func TestCompositeCache_GetDoesNotBlockWhenBufferFull(t *testing.T) {
	l1, l2 := newFakeCache(), newFakeCache()
	l1.setDelay = 200 * time.Millisecond // keep the worker busy
	l2.data["k"] = "v"
	cache := NewCompositeCache[string]([]Cache[string]{l1, l2}, quietLogger(), WithSpreadBufferSize(1))
	defer cache.Close(context.Background())

	done := make(chan struct{})
	go func() {
		for i := 0; i < 20; i++ {
			_, _ = cache.Get(context.Background(), "k")
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Get blocked on a full spread buffer")
	}
}

func TestCompositeCache_UseAfterClose(t *testing.T) {
	l1, l2 := newFakeCache(), newFakeCache()
	l2.data["k"] = "v"
	cache := NewCompositeCache[string]([]Cache[string]{l1, l2}, quietLogger())
	assert.Nil(t, cache.Close(context.Background()))

	assert.NotPanics(t, func() {
		_, err := cache.Get(context.Background(), "k")
		assert.ErrorIs(t, err, ErrClosed)
		assert.ErrorIs(t, cache.Set(context.Background(), "k", "v"), ErrClosed)
		assert.Nil(t, cache.Close(context.Background()))
	})
	assert.Equal(t, int32(1), l1.closed.Load(), "caches should be closed only once")
}

func TestCompositeCache_CloseClosesAllCachesAndJoinsErrors(t *testing.T) {
	err1, err2 := errors.New("boom1"), errors.New("boom2")
	l1, l2, l3 := newFakeCache(), newFakeCache(), newFakeCache()
	l1.closeErr, l2.closeErr = err1, err2
	cache := NewCompositeCache[string]([]Cache[string]{l1, l2, l3}, quietLogger())

	err := cache.Close(context.Background())
	assert.ErrorIs(t, err, err1)
	assert.ErrorIs(t, err, err2)
	assert.Equal(t, int32(1), l3.closed.Load(), "later caches must be closed even if earlier ones fail")
}

func TestCompositeCache_ConcurrentGetAndClose(t *testing.T) {
	l1, l2 := newFakeCache(), newFakeCache()
	l2.data["k"] = "v"
	cache := NewCompositeCache[string]([]Cache[string]{l1, l2}, quietLogger())

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = cache.Get(context.Background(), "k")
			}
		}()
	}
	time.Sleep(5 * time.Millisecond)
	assert.Nil(t, cache.Close(context.Background()))
	wg.Wait()
}

func TestCompositeCache_Delete(t *testing.T) {
	l1, l2 := newFakeCache(), newFakeCache()
	cache := NewCompositeCache[string]([]Cache[string]{l1, l2}, quietLogger())
	defer cache.Close(context.Background())
	ctx := context.Background()

	require.NoError(t, cache.Set(ctx, "k", "v"))
	assert.NoError(t, cache.Delete(ctx, "k"))
	_, err := cache.Get(ctx, "k")
	assert.ErrorIs(t, err, ErrNotFound, "the key is gone from every cache")
	assert.Less(t, l2.deletedAt.Load(), l1.deletedAt.Load(), "lower caches are cleared first")
	assert.NoError(t, cache.Delete(ctx, "missing"), "deleting a missing key is not an error")
}

func TestCompositeCache_DeleteAttemptsEveryCache(t *testing.T) {
	errA, errB := errors.New("a failed"), errors.New("b failed")
	l1, l2, l3 := newFakeCache(), newFakeCache(), newFakeCache()
	l1.deleteErr, l3.deleteErr = errA, errB
	cache := NewCompositeCache[string]([]Cache[string]{l1, l2, l3}, quietLogger())
	defer cache.Close(context.Background())

	err := cache.Delete(context.Background(), "k")
	assert.ErrorIs(t, err, errA)
	assert.ErrorIs(t, err, errB)
	for _, c := range []*fakeCache{l1, l2, l3} {
		assert.Equal(t, int32(1), c.deletes.Load())
	}
}

func TestCompositeCache_DeleteClearsMiss(t *testing.T) {
	l1 := newFakeCache()
	cache := NewCompositeCache[string]([]Cache[string]{l1}, quietLogger(), WithMissCache(10, time.Minute))
	defer cache.Close(context.Background())
	ctx := context.Background()

	_, err := cache.Get(ctx, "k") // remembered as a miss
	assert.ErrorIs(t, err, ErrNotFound)
	l1.data["k"] = "v" // written behind the composite cache's back
	require.NoError(t, cache.Delete(ctx, "other"))
	_, err = cache.Get(ctx, "k")
	assert.ErrorIs(t, err, ErrNotFound, "deleting another key leaves the miss in place")

	require.NoError(t, cache.Delete(ctx, "k"))
	l1.data["k"] = "v"
	v, err := cache.Get(ctx, "k")
	assert.NoError(t, err, "Delete clears the remembered miss")
	assert.Equal(t, "v", v)
}

func TestCompositeCache_DeleteAfterClose(t *testing.T) {
	cache := NewCompositeCache[string]([]Cache[string]{newFakeCache()}, quietLogger())
	require.NoError(t, cache.Close(context.Background()))
	assert.ErrorIs(t, cache.Delete(context.Background(), "k"), ErrClosed)
}
