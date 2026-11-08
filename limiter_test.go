package jiffy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCounter is an in-memory Counter that can be made to fail.
type fakeCounter struct {
	mu      sync.Mutex
	data    map[string]int64
	incrs   atomic.Int32
	failing atomic.Bool
}

func newFakeCounter() *fakeCounter {
	return &fakeCounter{data: map[string]int64{}}
}

func (f *fakeCounter) Incr(_ context.Context, key string, n int64, _ time.Duration) (int64, error) {
	f.incrs.Add(1)
	if f.failing.Load() {
		return 0, errors.New("counter down")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] += n
	return f.data[key], nil
}

func (f *fakeCounter) Count(_ context.Context, key string) (int64, error) {
	if f.failing.Load() {
		return 0, errors.New("counter down")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data[key], nil
}

// total sums every counter, across all keys and windows.
func (f *fakeCounter) total() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var sum int64
	for _, v := range f.data {
		sum += v
	}
	return sum
}

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
}

func quietLimiterLogger() LimiterOption {
	return WithLimiterLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// newTestLimiter creates a Limiter driven by clock, closed at the end of the test.
func newTestLimiter(t *testing.T, counter Counter, clock *fakeClock, limit int64, window time.Duration,
	options ...LimiterOption) *Limiter {
	t.Helper()
	l, err := NewLimiter(counter, limit, window, append([]LimiterOption{quietLimiterLogger()}, options...)...)
	require.NoError(t, err)
	l.now = clock.Now
	t.Cleanup(func() { _ = l.Close(context.Background()) })
	return l
}

// settle waits until every queued flush has been applied.
func settle(t *testing.T, l *Limiter) {
	t.Helper()
	require.Eventually(t, func() bool { return l.flushes.queued.Load() == 0 }, 2*time.Second, time.Millisecond)
}

// allowSettled calls Allow and waits for the flush it may trigger.
func allowSettled(t *testing.T, l *Limiter, key string) Decision {
	t.Helper()
	d, err := l.Allow(context.Background(), key)
	require.NoError(t, err)
	settle(t, l)
	return d
}

func TestLimiter_LocalOnlyAllowsUpToLimit(t *testing.T) {
	clock := newFakeClock()
	l := newTestLimiter(t, nil, clock, 5, time.Minute)

	for i := int64(0); i < 5; i++ {
		d, err := l.Allow(context.Background(), "k")
		require.NoError(t, err)
		assert.True(t, d.Allowed)
		assert.Equal(t, 4-i, d.Remaining)
	}
	d, err := l.Allow(context.Background(), "k")
	require.NoError(t, err)
	assert.False(t, d.Allowed)
	// The current window alone is full: wait for the next window, then until 5 * weight <= 4, i.e. 12s into it.
	assert.Equal(t, 72*time.Second, d.RetryAfter)

	other, err := l.Allow(context.Background(), "other")
	require.NoError(t, err)
	assert.True(t, other.Allowed, "keys are limited independently")
}

func TestLimiter_SlidingWindowDecays(t *testing.T) {
	clock := newFakeClock()
	l := newTestLimiter(t, nil, clock, 10, time.Minute)
	for i := 0; i < 10; i++ {
		_, _ = l.Allow(context.Background(), "k")
	}

	clock.Advance(time.Minute) // new window: the previous 10 still weigh in full
	d, _ := l.Allow(context.Background(), "k")
	assert.False(t, d.Allowed)
	assert.Equal(t, 6*time.Second, d.RetryAfter, "10 * weight <= 9 once 6s have passed")

	clock.Advance(6 * time.Second)
	d, _ = l.Allow(context.Background(), "k")
	assert.True(t, d.Allowed)

	d, _ = l.Allow(context.Background(), "k")
	assert.False(t, d.Allowed)
	assert.Equal(t, 6*time.Second, d.RetryAfter, "10 * weight + 1 <= 9 once 12s have passed")
}

func TestLimiter_IdleKeyStartsFresh(t *testing.T) {
	clock := newFakeClock()
	l := newTestLimiter(t, nil, clock, 3, time.Minute)
	for i := 0; i < 3; i++ {
		_, _ = l.Allow(context.Background(), "k")
	}
	clock.Advance(2 * time.Minute) // the previous window is now empty
	for i := 0; i < 3; i++ {
		d, _ := l.Allow(context.Background(), "k")
		assert.True(t, d.Allowed)
	}
}

func TestLimiter_SharesCountsAcrossReplicas(t *testing.T) {
	// A threshold of 1 sends every hit, so the only overshoot left is a replica's first hit.
	counter, clock := newFakeCounter(), newFakeClock()
	a := newTestLimiter(t, counter, clock, 100, time.Minute, WithFlushThreshold(1))
	b := newTestLimiter(t, counter, clock, 100, time.Minute, WithFlushThreshold(1))
	c := newTestLimiter(t, counter, clock, 100, time.Minute, WithFlushThreshold(1))

	for i := 0; i < 100; i++ {
		assert.True(t, allowSettled(t, a, "k").Allowed)
	}
	assert.False(t, allowSettled(t, a, "k").Allowed)

	// B and C have never seen the key: each lets one hit through before learning the global count.
	for _, replica := range []*Limiter{b, c} {
		assert.True(t, allowSettled(t, replica, "k").Allowed, "a replica's first hit is decided before it hears from the counter")
		assert.False(t, allowSettled(t, replica, "k").Allowed)
	}
	assert.Equal(t, int64(102), counter.total())
}

func TestLimiter_BatchesFlushes(t *testing.T) {
	counter, clock := newFakeCounter(), newFakeClock()
	l := newTestLimiter(t, counter, clock, 1000, time.Hour, WithFlushThreshold(10), WithFlushInterval(time.Minute))

	for i := 0; i < 25; i++ {
		allowSettled(t, l, "k")
	}
	// The first hit is sent straight away, then every 10th: 1 + 10 + 10 sent, 4 still local.
	assert.Equal(t, int32(3), counter.incrs.Load())
	assert.Equal(t, int64(21), counter.total())

	clock.Advance(time.Minute)
	allowSettled(t, l, "k")
	assert.Equal(t, int32(4), counter.incrs.Load(), "the interval flushes a partial batch")
	assert.Equal(t, int64(26), counter.total())
}

func TestLimiter_DeniedHitsAreNotCountedByDefault(t *testing.T) {
	for _, tc := range []struct {
		countDenied bool
		want        int64
	}{{false, 3}, {true, 13}} {
		counter, clock := newFakeCounter(), newFakeClock()
		l := newTestLimiter(t, counter, clock, 3, time.Minute, WithFlushThreshold(1), WithCountDenied(tc.countDenied))
		for i := 0; i < 13; i++ {
			allowSettled(t, l, "k")
		}
		assert.Equal(t, tc.want, counter.total(), "countDenied=%v", tc.countDenied)
	}
}

func TestLimiter_FallbackLocal(t *testing.T) {
	counter, clock := newFakeCounter(), newFakeClock()
	counter.failing.Store(true)
	l := newTestLimiter(t, counter, clock, 100, time.Minute, WithFallbackLimit(5))

	assert.True(t, allowSettled(t, l, "k").Allowed, "the failure is only noticed once a flush fails")
	for i := 0; i < 4; i++ {
		assert.True(t, allowSettled(t, l, "k").Allowed)
	}
	d := allowSettled(t, l, "k")
	assert.False(t, d.Allowed, "while degraded, the fallback limit applies")
	assert.Equal(t, int64(5), d.Limit)
	assert.Equal(t, int32(1), counter.incrs.Load(), "a failing counter is left alone until the flush interval passes")

	counter.failing.Store(false)
	clock.Advance(time.Second)
	assert.True(t, allowSettled(t, l, "k").Allowed, "after recovering, the normal limit applies again")
	// The hit of the failed send is not retried; the 4 counted while degraded and the new one are sent.
	assert.Equal(t, int64(5), counter.total())
}

func TestLimiter_FallbackAllowAndDeny(t *testing.T) {
	for _, tc := range []struct {
		fallbackLimit int64
		allowed       bool
	}{{math.MaxInt64, true}, {0, false}} {
		counter, clock := newFakeCounter(), newFakeClock()
		counter.failing.Store(true)
		l := newTestLimiter(t, counter, clock, 2, time.Minute, WithFallbackLimit(tc.fallbackLimit))
		allowSettled(t, l, "k") // trips the fallback
		for i := 0; i < 5; i++ {
			d := allowSettled(t, l, "k")
			assert.Equal(t, tc.allowed, d.Allowed, "fallbackLimit=%d", tc.fallbackLimit)
			if !tc.allowed {
				assert.Positive(t, d.RetryAfter)
			}
		}
	}
}

func TestLimiter_InvalidArguments(t *testing.T) {
	_, err := NewLimiter(nil, 0, time.Minute)
	assert.Error(t, err)
	_, err = NewLimiter(nil, 1, 0)
	assert.Error(t, err)
	_, err = NewLimiter(nil, 1, time.Minute, WithFlushThreshold(0))
	assert.Error(t, err)

	l := newTestLimiter(t, nil, newFakeClock(), 5, time.Minute)
	_, err = l.AllowN(context.Background(), "k", 0)
	assert.ErrorIs(t, err, ErrInvalidN)
	_, err = l.AllowN(context.Background(), "k", 6)
	assert.ErrorIs(t, err, ErrInvalidN)
	d, err := l.AllowN(context.Background(), "k", 5)
	assert.NoError(t, err)
	assert.True(t, d.Allowed)
}

func TestLimiter_Close(t *testing.T) {
	counter, clock := newFakeCounter(), newFakeClock()
	l, err := NewLimiter(counter, 10, time.Minute, quietLimiterLogger())
	require.NoError(t, err)
	l.now = clock.Now

	_, err = l.Allow(context.Background(), "k")
	require.NoError(t, err)
	assert.NoError(t, l.Close(context.Background()))
	assert.Equal(t, int64(1), counter.total(), "Close sends queued hits before returning")

	_, err = l.Allow(context.Background(), "k")
	assert.ErrorIs(t, err, ErrClosed)
	assert.NoError(t, l.Close(context.Background()), "closing twice is a no-op")
}

func TestLimiter_ConcurrentReplicasWithRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	counter := NewRedisCacheFromClient[string](client, time.Minute)

	const limit, threshold = 100, 10
	replicas := make([]*Limiter, 3)
	for i := range replicas {
		l, err := NewLimiter(counter, limit, time.Hour, quietLimiterLogger(), WithFlushThreshold(threshold))
		require.NoError(t, err)
		// Warm the key up, so the load below does not race to create its local state, and the connection is open.
		_, err = l.Allow(context.Background(), "k")
		require.NoError(t, err)
		settle(t, l)
		replicas[i] = l
	}

	// The overshoot is bounded by the hits that arrive during one round trip to Redis, so the load is paced: an
	// unpaced burst finishing within a single round trip would be decided on local counts alone.
	var allowed atomic.Int64
	allowed.Store(int64(len(replicas)))
	var wg sync.WaitGroup
	for _, l := range replicas {
		for g := 0; g < 5; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 60; i++ {
					d, err := l.Allow(context.Background(), "k")
					assert.NoError(t, err)
					if d.Allowed {
						allowed.Add(1)
					}
					time.Sleep(time.Millisecond)
				}
			}()
		}
	}
	wg.Wait()

	// Each replica can overshoot by the hits it has not sent yet and those it is sending, which the others cannot
	// see. Replicas limiting on their own would allow up to limit * replicas; sharing counts must do far better.
	assert.GreaterOrEqual(t, allowed.Load(), int64(limit))
	assert.Less(t, allowed.Load(), int64(2*limit))

	for _, l := range replicas {
		assert.NoError(t, l.Close(context.Background()))
	}
	var stored int64
	for _, key := range mr.Keys() {
		if strings.HasPrefix(key, defaultLimiterKeyPrefix+"k:") {
			v, err := counter.Count(context.Background(), key)
			require.NoError(t, err)
			stored += v
		}
	}
	// Hits below the flush threshold may never have been sent.
	assert.LessOrEqual(t, stored, allowed.Load())
	assert.GreaterOrEqual(t, stored, allowed.Load()-int64(len(replicas)*threshold))
}
