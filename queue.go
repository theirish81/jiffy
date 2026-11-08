package jiffy

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// backgroundQueue runs commands on background workers, and coordinates shutdown with the calls that queue them.
//
// Calls that may push must run between enter and exit. close waits for them, so no call can push after the channel
// is closed, then lets the workers drain what is already queued.
type backgroundQueue[C any] struct {
	// mu guards closed. Calls hold the read lock between enter and exit; close takes the write lock.
	mu      sync.RWMutex
	closed  bool
	ch      chan C
	workers sync.WaitGroup
	// queued counts commands queued or being handled.
	queued  atomic.Int64
	timeout time.Duration
	handle  func(ctx context.Context, cmd C)
}

// newBackgroundQueue starts a queue holding up to size commands, handled by the given number of workers. Each
// command is handled under its own timeout. With no workers, the queue only tracks whether it is closed.
func newBackgroundQueue[C any](size int, workers int, timeout time.Duration,
	handle func(ctx context.Context, cmd C)) *backgroundQueue[C] {
	q := &backgroundQueue[C]{
		ch:      make(chan C, size),
		timeout: timeout,
		handle:  handle,
	}
	for i := 0; i < workers; i++ {
		q.workers.Add(1)
		go q.work()
	}
	return q
}

// enter marks the start of a call, and reports false if the queue is closed. A call that entered must exit.
func (q *backgroundQueue[C]) enter() bool {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return false
	}
	return true
}

// exit marks the end of a call started with enter.
func (q *backgroundQueue[C]) exit() {
	q.mu.RUnlock()
}

// tryPush queues cmd without blocking, and reports whether it was queued. It must be called between enter and exit.
func (q *backgroundQueue[C]) tryPush(cmd C) bool {
	q.queued.Add(1)
	select {
	case q.ch <- cmd:
		return true
	default:
		q.queued.Add(-1)
		return false
	}
}

// work handles commands until the channel is closed and drained.
func (q *backgroundQueue[C]) work() {
	defer q.workers.Done()
	for cmd := range q.ch {
		q.run(cmd)
	}
}

// run handles one command under its timeout.
func (q *backgroundQueue[C]) run(cmd C) {
	defer q.queued.Add(-1)
	ctx, cancel := context.WithTimeout(context.Background(), q.timeout)
	defer cancel()
	q.handle(ctx, cmd)
}

// close closes the queue. It waits for calls in progress, then for the workers to drain the queue, until ctx is done.
// It reports whether this call closed the queue: false means it was already closed, and nothing was done.
func (q *backgroundQueue[C]) close(ctx context.Context) (bool, error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false, nil
	}
	q.closed = true
	close(q.ch)
	q.mu.Unlock()

	done := make(chan struct{})
	go func() {
		q.workers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true, nil
	case <-ctx.Done():
		return true, ctx.Err()
	}
}
