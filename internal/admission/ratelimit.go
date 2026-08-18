// Package admission rejects excess control-plane work before the relay
// allocates media resources.
package admission

import (
	"sync"
	"sync/atomic"
	"time"
)

// cleanupInterval is how often the background goroutine sweeps stale entries.
const cleanupInterval = 5 * time.Minute

// IPLimiter enforces a sliding-window limit of MaxCount events per Window
// for each distinct IP address.
//
// The zero value is not usable; construct with New.
type IPLimiter struct {
	maxCount int64
	window   time.Duration

	entries sync.Map // map[string]*ipState

	stopOnce sync.Once
	stop     chan struct{}
}

// ipState holds the sliding-window state for a single IP.
type ipState struct {
	mu          sync.Mutex
	windowStart time.Time
	count       int64
	// lastSeen is updated without the mutex (atomic) so the cleanup goroutine
	// can skip entries that have seen recent activity.
	lastSeen atomic.Int64 // Unix nanoseconds
}

// New creates an IPLimiter and starts the background cleanup goroutine.
// Call Stop when the limiter is no longer needed to release resources.
func New(maxCount int64, window time.Duration) *IPLimiter {
	l := &IPLimiter{
		maxCount: maxCount,
		window:   window,
		stop:     make(chan struct{}),
	}
	go l.cleanup()
	return l
}

// Allow reports whether the request from ip should be allowed.
// It increments the counter for ip within the current window and returns false
// when the count exceeds maxCount.
func (l *IPLimiter) Allow(ip string) bool {
	now := time.Now()
	val, _ := l.entries.LoadOrStore(ip, &ipState{windowStart: now})
	st := val.(*ipState)

	st.mu.Lock()
	defer st.mu.Unlock()

	// Reset the window if it has expired.
	if now.Sub(st.windowStart) >= l.window {
		st.windowStart = now
		st.count = 0
	}

	st.lastSeen.Store(now.UnixNano())

	if st.count >= l.maxCount {
		return false
	}
	st.count++
	return true
}

// Stop shuts down the background cleanup goroutine.
// Safe to call multiple times.
func (l *IPLimiter) Stop() {
	l.stopOnce.Do(func() { close(l.stop) })
}

// cleanup sweeps stale entries every cleanupInterval.
// An entry is stale when it has not been accessed for at least one full window.
func (l *IPLimiter) cleanup() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case now := <-ticker.C:
			l.entries.Range(func(key, val any) bool {
				st := val.(*ipState)
				lastNs := st.lastSeen.Load()
				if lastNs == 0 {
					return true
				}
				if now.Sub(time.Unix(0, lastNs)) >= l.window {
					l.entries.Delete(key)
				}
				return true
			})
		}
	}
}
