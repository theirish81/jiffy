package jiffy

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ErrInvalidN is returned by AllowN when n is not between 1 and the limit.
var ErrInvalidN = errors.New("n must be between 1 and the limit")

const (
	defaultLimiterMaxKeys    = 10_000
	defaultFlushThreshold    = 10
	defaultFlushInterval     = time.Second
	defaultFlushTimeout      = 2 * time.Second
	defaultLimiterKeyPrefix  = "jiffy:rl:"
	limiterFlushWorkers      = 4
	limiterFlushBufferSize   = 1024
	limiterRetryAfterQuantum = time.Millisecond
)

// LimiterOptions are options for a Limiter.
type LimiterOptions struct {
	maxKeys        int
	flushThreshold int64
	flushInterval  time.Duration
	flushTimeout   time.Duration
	countDenied    bool
	fallbackLimit  int64
	keyPrefix      string
	logger         *slog.Logger
}

// LimiterOption is an option for a Limiter.
type LimiterOption func(options *LimiterOptions)

// WithLimiterMaxKeys sets how many keys each replica tracks in memory (default: 10,000). When full, the least
// valuable keys are evicted, and any hits they had not yet sent to the Counter are lost.
func WithLimiterMaxKeys(n int) LimiterOption {
	return func(o *LimiterOptions) {
		o.maxKeys = n
	}
}

// WithFlushThreshold sends a key's local hits to the Counter once this many have accumulated (default: 10).
func WithFlushThreshold(hits int64) LimiterOption {
	return func(o *LimiterOptions) {
		o.flushThreshold = hits
	}
}

// WithFlushInterval sends a key's local hits to the Counter once this much time has passed since the last send
// (default: 1s). It is also how long the Counter is left alone after a failure before being tried again.
func WithFlushInterval(d time.Duration) LimiterOption {
	return func(o *LimiterOptions) {
		o.flushInterval = d
	}
}

// WithFlushTimeout sets the timeout of a single send to the Counter (default: 2s).
func WithFlushTimeout(d time.Duration) LimiterOption {
	return func(o *LimiterOptions) {
		o.flushTimeout = d
	}
}

// WithCountDenied makes denied requests count towards the limit (default: off). With it, a client that keeps
// sending while denied stays denied until it backs off; without it, the client gets roughly the limit's throughput.
func WithCountDenied(enabled bool) LimiterOption {
	return func(o *LimiterOptions) {
		o.countDenied = enabled
	}
}

// WithFallbackLimit sets the limit each replica enforces on its own while the Counter is failing (default: the
// normal limit). Every replica then limits its own share of the traffic, so limit / replicas keeps the global limit
// roughly in place. 0 denies every request, and math.MaxInt64 allows every request. A negative limit means the
// normal limit.
func WithFallbackLimit(limit int64) LimiterOption {
	return func(o *LimiterOptions) {
		o.fallbackLimit = limit
	}
}

// WithKeyPrefix sets the prefix of the keys the Limiter writes to the Counter (default: "jiffy:rl:").
func WithKeyPrefix(prefix string) LimiterOption {
	return func(o *LimiterOptions) {
		o.keyPrefix = prefix
	}
}

// WithLimiterLogger sets the logger of the Limiter (default: slog.Default()).
func WithLimiterLogger(l *slog.Logger) LimiterOption {
	return func(o *LimiterOptions) {
		o.logger = l
	}
}

// Decision is the outcome of a call to Allow.
type Decision struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the limit the decision was made against.
	Limit int64
	// Remaining is how many more hits are expected to be allowed right now. It is an estimate.
	Remaining int64
	// RetryAfter is, for a denied request, how long until a request of the same size is expected to be allowed.
	RetryAfter time.Duration
}

// limiterEntry is the local state of one key. All fields are guarded by mu.
type limiterEntry struct {
	mu sync.Mutex
	// window is the index of the current window: the number of windows since the Unix epoch.
	window int64
	// prev is the best known global count of the previous window.
	prev int64
	// curr is the best known global count of the current window. Hits are added to it as soon as they are sent to
	// the Counter, without waiting for the reply.
	curr int64
	// pending are hits of the current window not yet sent to the Counter.
	pending   int64
	lastFlush time.Time
}

// flushCommand asks a worker to send n hits of key, counted in the given window, to the Counter.
type flushCommand struct {
	key    string
	entry  *limiterEntry
	window int64
	n      int64
}

// Limiter is a sliding window rate limiter. Every replica decides locally, from what it knows of the global count,
// and shares its hits with the other replicas through a Counter in the background, so the Counter is never on the
// path of a request. The price is accuracy: a key can exceed its limit by the hits the replicas have not shared yet.
type Limiter struct {
	counter Counter
	limit   int64
	window  time.Duration
	opts    LimiterOptions
	// entries is the L1: the local state of each key.
	entries LocalCache[*limiterEntry]
	// flushes sends hits to the Counter in the background.
	flushes       *backgroundQueue[flushCommand]
	degradedUntil atomic.Int64
	now           func() time.Time
}

// NewLocalLimiter creates a sliding window rate limiter allowing up to limit hits per key in any window, counted in
// this process only. Each replica limits on its own, so the effective global limit grows with the number of
// replicas. Options about the Counter (flush, fallback and key prefix) have no effect.
func NewLocalLimiter(limit int64, window time.Duration, options ...LimiterOption) (*Limiter, error) {
	return NewLimiter(nil, limit, window, options...)
}

// NewLimiter creates a sliding window rate limiter allowing up to limit hits per key in any window.
// counter shares hits between replicas, and is typically a RedisCache. Any Counter works, including a LocalCache;
// a nil counter makes every replica limit on its own, as NewLocalLimiter does. The Limiter does not close the
// counter.
func NewLimiter(counter Counter, limit int64, window time.Duration, options ...LimiterOption) (*Limiter, error) {
	if limit < 1 {
		return nil, errors.New("jiffy: limit must be positive")
	}
	if window <= 0 {
		return nil, errors.New("jiffy: window must be positive")
	}
	opts := LimiterOptions{
		maxKeys:        defaultLimiterMaxKeys,
		flushThreshold: defaultFlushThreshold,
		flushInterval:  defaultFlushInterval,
		flushTimeout:   defaultFlushTimeout,
		fallbackLimit:  -1,
		keyPrefix:      defaultLimiterKeyPrefix,
		logger:         slog.Default(),
	}
	for _, option := range options {
		option(&opts)
	}
	if opts.maxKeys < 1 || opts.flushThreshold < 1 || opts.flushInterval <= 0 || opts.flushTimeout <= 0 {
		return nil, errors.New("jiffy: invalid limiter options")
	}
	if opts.fallbackLimit < 0 {
		opts.fallbackLimit = limit
	}
	// Local state lives long enough to still serve as the previous window.
	entries, err := NewLocalCache[*limiterEntry](opts.maxKeys, 2*window)
	if err != nil {
		return nil, err
	}
	l := &Limiter{
		counter: counter,
		limit:   limit,
		window:  window,
		opts:    opts,
		entries: entries,
		now:     time.Now,
	}
	workers := limiterFlushWorkers
	if counter == nil {
		workers = 0 // nothing is ever sent
	}
	l.flushes = newBackgroundQueue(limiterFlushBufferSize, workers, opts.flushTimeout, l.flush)
	return l, nil
}

// Allow reports whether one hit for key may proceed, and counts it if so.
func (l *Limiter) Allow(ctx context.Context, key string) (Decision, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN reports whether n hits for key may proceed, and counts them if so. The decision is always taken locally:
// the Counter is only contacted in the background.
func (l *Limiter) AllowN(_ context.Context, key string, n int64) (Decision, error) {
	if n < 1 || n > l.limit {
		return Decision{Limit: l.limit}, ErrInvalidN
	}
	if !l.flushes.enter() {
		return Decision{Limit: l.limit}, ErrClosed
	}
	defer l.flushes.exit()
	now := l.now()
	limit := l.limit
	degraded := l.isDegraded(now)
	if degraded {
		limit = l.opts.fallbackLimit
		if n > limit {
			// Can never fit the fallback limit: deny until the Counter is tried again.
			return Decision{Limit: limit, RetryAfter: time.Duration(l.degradedUntil.Load() - now.UnixNano())}, nil
		}
	}

	e := l.entry(key, now)
	e.mu.Lock()
	defer e.mu.Unlock()
	l.roll(key, e, now, degraded)
	d := l.evaluate(e, now, n, limit)
	if d.Allowed || l.opts.countDenied {
		e.pending += n
	}
	if !degraded {
		l.maybeFlush(key, e, now)
	}
	return d, nil
}

// entry returns the local state of key, creating it if needed. Under a burst of concurrent first hits, a key may get
// more than one entry for a short while; each entry sends its own hits to the Counter, so none are lost.
func (l *Limiter) entry(key string, now time.Time) *limiterEntry {
	if e, err := l.entries.Get(context.Background(), key); err == nil {
		return e
	}
	e := &limiterEntry{window: l.windowIndex(now)}
	_ = l.entries.Set(context.Background(), key, e) // LocalCache.Set never fails
	return e
}

// roll moves e to the window containing now, if the window has changed. The hits of a window that just ended become
// the previous window's count, and any of them not yet sent to the Counter are sent now.
func (l *Limiter) roll(key string, e *limiterEntry, now time.Time, degraded bool) {
	window := l.windowIndex(now)
	if window <= e.window {
		return
	}
	if window == e.window+1 {
		if e.pending > 0 && l.counter != nil && !degraded {
			// If the queue is full these hits are lost, which only makes the limit slightly more lenient.
			l.send(flushCommand{key: key, entry: e, window: e.window, n: e.pending})
		}
		e.prev = e.curr + e.pending
	} else {
		e.prev = 0
	}
	e.window = window
	e.curr, e.pending = 0, 0
	// Send the first hit of the new window straight away, so the replica quickly learns the global counts.
	e.lastFlush = time.Time{}
	// Refresh the entry's expiry, so that keys in constant use keep their state.
	_ = l.entries.Set(context.Background(), key, e)
}

// evaluate decides whether n more hits fit in the sliding window.
func (l *Limiter) evaluate(e *limiterEntry, now time.Time, n int64, limit int64) Decision {
	elapsed := now.UnixNano() - e.window*int64(l.window)
	weight := 1 - float64(elapsed)/float64(l.window)
	current := float64(e.curr + e.pending)
	estimate := float64(e.prev)*weight + current
	if estimate+float64(n) <= float64(limit) {
		return Decision{Allowed: true, Limit: limit, Remaining: limit - n - int64(math.Ceil(estimate))}
	}
	return Decision{Limit: limit, RetryAfter: l.retryAfter(e, now, n, limit, current)}
}

// retryAfter estimates how long until n hits fit, assuming no more hits arrive in the meantime.
func (l *Limiter) retryAfter(e *limiterEntry, now time.Time, n int64, limit int64, current float64) time.Duration {
	budget := float64(limit - n)
	window := float64(l.window)
	start := e.window * int64(l.window)
	var at int64
	if current > budget {
		// The current window alone is over budget: wait for the next window, where it becomes the previous window
		// and its weight decays until current * weight <= budget.
		at = start + int64(l.window) + int64((1-budget/current)*window)
	} else {
		// Wait until the previous window's weight has decayed enough: prev * weight <= budget - current.
		at = start + int64((1-(budget-current)/float64(e.prev))*window)
	}
	d := time.Duration(at - now.UnixNano())
	if d <= 0 {
		return 0
	}
	// Round up, so that retrying after RetryAfter is not denied by a rounding error.
	return (d + limiterRetryAfterQuantum - 1) / limiterRetryAfterQuantum * limiterRetryAfterQuantum
}

// maybeFlush sends e's pending hits once enough have accumulated or enough time has passed. Sent hits count towards
// curr straight away; if the send fails they are not retried, so the other replicas never see them.
func (l *Limiter) maybeFlush(key string, e *limiterEntry, now time.Time) {
	if l.counter == nil || e.pending == 0 {
		return
	}
	if e.pending < l.opts.flushThreshold && now.Sub(e.lastFlush) < l.opts.flushInterval {
		return
	}
	if !l.send(flushCommand{key: key, entry: e, window: e.window, n: e.pending}) {
		return // the hits stay pending and are sent on a later hit
	}
	e.curr += e.pending
	e.pending = 0
	e.lastFlush = now
}

// send queues cmd without blocking, and reports whether it was queued.
func (l *Limiter) send(cmd flushCommand) bool {
	if !l.flushes.tryPush(cmd) {
		l.opts.logger.Warn("limiter flush dropped. Channel full", "key", cmd.key)
		return false
	}
	return true
}

// flush sends cmd's hits to the Counter and updates the entry with the global counts it reports.
func (l *Limiter) flush(ctx context.Context, cmd flushCommand) {
	total, err := l.counter.Incr(ctx, l.counterKey(cmd.key, cmd.window), cmd.n, 2*l.window)
	var prev int64
	if err == nil {
		prev, err = l.counter.Count(ctx, l.counterKey(cmd.key, cmd.window-1))
	}
	if err != nil {
		l.opts.logger.Error("could not flush limiter hits", "key", cmd.key, "err", err.Error())
		l.degradedUntil.Store(l.now().Add(l.opts.flushInterval).UnixNano())
		return
	}

	e := cmd.entry
	e.mu.Lock()
	defer e.mu.Unlock()
	// Counts only grow within a window, so the highest one seen is the most recent.
	switch e.window {
	case cmd.window:
		e.curr = max(e.curr, total)
		e.prev = max(e.prev, prev)
	case cmd.window + 1: // the entry has moved on: the hits were the last of what is now the previous window
		e.prev = max(e.prev, total)
	}
}

// windowIndex is the index of the window containing t.
func (l *Limiter) windowIndex(t time.Time) int64 {
	return t.UnixNano() / int64(l.window)
}

// isDegraded reports whether the Counter has recently failed.
func (l *Limiter) isDegraded(now time.Time) bool {
	return l.counter != nil && now.UnixNano() < l.degradedUntil.Load()
}

// counterKey is the Counter key holding the global count of key for the given window.
func (l *Limiter) counterKey(key string, window int64) string {
	return l.opts.keyPrefix + key + ":" + strconv.FormatInt(window, 10)
}

// Close stops the Limiter. It waits for in-flight calls, then sends any queued hits to the Counter until ctx is
// done. It does not close the Counter. Calling Close more than once is a no-op.
func (l *Limiter) Close(ctx context.Context) error {
	closed, err := l.flushes.close(ctx)
	if !closed {
		return nil
	}
	if err != nil {
		l.opts.logger.Warn("timed out waiting for limiter flushes to drain", "err", err)
	}
	_ = l.entries.Close(ctx) // LocalCache.Close never fails
	return err
}
