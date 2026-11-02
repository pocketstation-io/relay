// Package admission bounds public control-plane work before allocation.
package admission

import (
	"net"
	"sync"
	"time"
)

type entry struct {
	windowStart time.Time
	count       int
}

const defaultMaxIdentities = 4096

// IPLimiter is a fixed-window limiter with a finite identity table. It uses
// the socket peer address; trusted-proxy rewriting belongs at the edge.
type IPLimiter struct {
	mu         sync.Mutex
	entries    map[string]entry
	limit      int
	window     time.Duration
	maxEntries int
}

func NewIPLimiter(limit int, window time.Duration, maxEntries int) *IPLimiter {
	if maxEntries <= 0 {
		maxEntries = defaultMaxIdentities
	}
	if window <= 0 {
		window = time.Minute
	}
	return &IPLimiter{
		entries:    make(map[string]entry),
		limit:      limit,
		window:     window,
		maxEntries: maxEntries,
	}
}

// Allow returns whether the request may proceed and a bounded Retry-After.
func (limiter *IPLimiter) Allow(remoteAddress string, now time.Time) (bool, time.Duration) {
	if limiter == nil || limiter.limit <= 0 {
		return true, 0
	}
	host, _, err := net.SplitHostPort(remoteAddress)
	if err != nil {
		host = remoteAddress
	}
	if host == "" {
		host = "unknown"
	}

	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	current, found := limiter.entries[host]
	if found && now.Sub(current.windowStart) >= limiter.window {
		delete(limiter.entries, host)
		found = false
	}
	if !found && len(limiter.entries) >= limiter.maxEntries {
		limiter.prune(now)
		if len(limiter.entries) >= limiter.maxEntries {
			return false, limiter.window
		}
	}
	if !found {
		limiter.entries[host] = entry{windowStart: now, count: 1}
		return true, 0
	}
	if current.count >= limiter.limit {
		remaining := limiter.window - now.Sub(current.windowStart)
		if remaining < time.Second {
			remaining = time.Second
		}
		return false, remaining
	}
	current.count++
	limiter.entries[host] = current
	return true, 0
}

func (limiter *IPLimiter) prune(now time.Time) {
	for key, current := range limiter.entries {
		if now.Sub(current.windowStart) >= limiter.window {
			delete(limiter.entries, key)
		}
	}
}
