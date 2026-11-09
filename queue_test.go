package jiffy

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackgroundQueue_HandlesAndDrains(t *testing.T) {
	var handled atomic.Int32
	q := newBackgroundQueue(10, 2, time.Second, func(ctx context.Context, _ int) {
		_, hasDeadline := ctx.Deadline()
		assert.True(t, hasDeadline, "each command runs under a timeout")
		time.Sleep(10 * time.Millisecond)
		handled.Add(1)
	})
	assert.True(t, q.enter())
	for i := 0; i < 5; i++ {
		assert.True(t, q.tryPush(i))
	}
	q.exit()

	closed, err := q.close(context.Background())
	assert.True(t, closed)
	assert.NoError(t, err)
	assert.Equal(t, int32(5), handled.Load(), "close drains queued commands")
	assert.Equal(t, int64(0), q.queued.Load())
}

func TestBackgroundQueue_DropsWhenFull(t *testing.T) {
	release := make(chan struct{})
	q := newBackgroundQueue(1, 1, time.Second, func(context.Context, int) { <-release })
	assert.True(t, q.enter())
	assert.True(t, q.tryPush(1)) // taken by the worker, which blocks
	assert.Eventually(t, func() bool { return len(q.ch) == 0 }, time.Second, time.Millisecond)
	assert.True(t, q.tryPush(2)) // fills the buffer
	assert.False(t, q.tryPush(3), "a full queue drops commands instead of blocking")
	q.exit()
	close(release)
	_, _ = q.close(context.Background())
}

func TestBackgroundQueue_Close(t *testing.T) {
	release := make(chan struct{})
	q := newBackgroundQueue(1, 1, time.Second, func(context.Context, int) { <-release })
	assert.True(t, q.enter())
	assert.True(t, q.tryPush(1))
	q.exit()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	closed, err := q.close(ctx)
	assert.True(t, closed)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "close gives up on draining once ctx is done")

	assert.False(t, q.enter(), "no call can enter a closed queue")
	closed, err = q.close(context.Background())
	assert.False(t, closed, "closing twice is a no-op")
	assert.NoError(t, err)
	close(release)
}

// TestBackgroundQueue_CloseWhilePushing checks that closing while calls are pushing never panics with a send on a
// closed channel. Run with -race.
func TestBackgroundQueue_CloseWhilePushing(t *testing.T) {
	q := newBackgroundQueue(10, 2, time.Second, func(context.Context, int) {})
	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if !q.enter() {
					return
				}
				q.tryPush(i)
				q.exit()
			}
		}()
	}
	time.Sleep(time.Millisecond)
	closed, err := q.close(context.Background())
	assert.True(t, closed)
	assert.NoError(t, err)
	wg.Wait()
}

func TestBackgroundQueue_WithoutWorkers(t *testing.T) {
	q := newBackgroundQueue(1, 0, time.Second, func(context.Context, int) {})
	assert.True(t, q.enter())
	q.exit()
	closed, err := q.close(context.Background())
	assert.True(t, closed)
	assert.NoError(t, err, "with no workers there is nothing to drain")
}
