// Package session owns RelaySession lifecycle, named AudioBuses, source
// attachment, subscriptions, and forwarding observations.
package session

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// defaultInactivityTimeout is the time a RelaySession with no active bus source
// will remain open before auto-closing. Configurable via ROOM_EXPIRY_MINUTES.
const defaultInactivityTimeout = 30 * time.Minute

// defaultReconnectWindow is the time a bus keeps subscribers alive after its
// source disconnects. Configurable via SOURCE_RECONNECT_WINDOW_SEC.
const defaultReconnectWindow = 60 * time.Second

// defaultMaxBuses bounds the named source lanes retained by one RelaySession.
// It matches the maximum authenticated multi-bus publisher declaration.
const defaultMaxBuses = 16

// DefaultMediaStallThresholdMs is how long a bus with an attached source may go
// without forwarding an RTP packet before it is considered media-stalled.
// WebSocket/ICE can stay alive while media silently stops (Corrected Audit §6):
// at 50–100 pkt/s, ~2 s with zero packets is unambiguously a stall, not jitter.
const DefaultMediaStallThresholdMs = 2000

var (
	// ErrNoSource is returned when an operation requires a live source and none exists.
	ErrNoSource = errors.New("relay_session: bus has no source")
)

// RelaySession owns the set of AudioBuses for one relay session.
// Each subscriber selects one bus, or BusMix to receive RTP from all buses
// (relay.out("mix") semantics). Buses are created lazily on first PUBLISH.
//
// Invariants:
//   - busesMu guards the buses map for writes; reads inside forwardLoop go
//     through the room's atomic subscription pointer (no room lock on hot path).
//   - subscriptions is copy-on-write behind subscriptionsMu.
//   - done is closed by Close and signals all bus forwardLoops to stop.
type RelaySession struct {
	ID      string
	GraphID string // graph template name from PUBLISH (e.g. "room-demo")

	busesMu sync.RWMutex
	buses   map[BusID]*AudioBus

	// subscriptionsMu and subscriptions are room-wide; every bus forwardLoop
	// writes here tagged with its BusID, and deliver filters per subscriber
	// (BusMix subscribers receive all buses on one track).
	subscriptionsMu sync.Mutex
	subscriptions   atomic.Pointer[[]*subscriptionEntry]

	Public    atomic.Bool
	createdAt time.Time

	keyMu      sync.RWMutex
	currentKey string

	latency      *latencyStore
	controlState controlStateOwner

	stateObserver atomic.Pointer[stateObserver]

	// maxSubscriptions: 0 = unlimited.
	maxSubscriptions int
	maxBuses         int
	pendingSlots     atomic.Int32
	evictionsTotal   atomic.Uint64

	// inactivity/reconnect configuration propagated to each AudioBus.
	inactivityTimeout time.Duration
	reconnectWindow   time.Duration

	// expiryTimer drives active-media-aware room expiry: it periodically closes
	// the room only when it is truly idle (Corrected Audit §6.4).
	expiryMu    sync.Mutex
	expiryTimer *time.Timer

	closeOnce sync.Once
	done      chan struct{}
}

type stateObserver struct {
	notify func()
}

// SetStateObserver installs the nonblocking control-state notification owned
// by the composing server. Reinstalling the same observer is safe.
func (r *RelaySession) SetStateObserver(observer func()) {
	if observer == nil {
		r.stateObserver.Store(nil)
		return
	}
	r.stateObserver.Store(&stateObserver{notify: observer})
}

func (r *RelaySession) notifyStateChange() {
	r.controlState.revision.Add(1)
	observer := r.stateObserver.Load()
	if observer != nil {
		observer.notify()
	}
}

// New returns an open RelaySession with default timeouts.
func New(id string) *RelaySession {
	return newWithTimeouts(id, defaultInactivityTimeout, defaultReconnectWindow)
}

func newWithTimeout(id string, timeout time.Duration) *RelaySession {
	return newWithTimeouts(id, timeout, defaultReconnectWindow)
}

func newWithTimeouts(id string, inactivityTimeout, reconnectWindow time.Duration) *RelaySession {
	r := &RelaySession{
		ID:                id,
		buses:             make(map[BusID]*AudioBus),
		done:              make(chan struct{}),
		inactivityTimeout: inactivityTimeout,
		reconnectWindow:   reconnectWindow,
		maxBuses:          defaultMaxBuses,
		latency:           newLatencyStore(),
		createdAt:         time.Now().UTC(),
	}
	r.controlState.revision.Store(1)
	empty := make([]*subscriptionEntry, 0)
	r.subscriptions.Store(&empty)
	r.expiryMu.Lock()
	r.expiryTimer = time.AfterFunc(inactivityTimeout, r.checkExpiry)
	r.expiryMu.Unlock()
	return r
}

// deliver is the hot-path function passed to each AudioBus.forwardLoop. It
// writes pkt to every subscriber that selected this bus (entry.busID == busID)
// or the virtual mix (entry.busID == BusMix). A BusMix subscriber therefore
// receives RTP from all buses — byte-identical to the room-wide behavior.
//
// errCounts and deadSubs are pre-allocated per bus. A subscriber with
// negotiated packet mutation currently requires a deep RTP header clone; that
// clone allocates extension storage and remains a measured optimization target.
// Close terminates the room and all its buses. Safe to call multiple times.
func (r *RelaySession) Close() {
	r.closeOnce.Do(func() {
		close(r.done)
		for _, e := range *r.subscriptions.Load() {
			e.subscription.StopForwarding()
		}
		r.expiryMu.Lock()
		if r.expiryTimer != nil {
			r.expiryTimer.Stop()
		}
		r.expiryMu.Unlock()
		r.busesMu.RLock()
		defer r.busesMu.RUnlock()
		for _, b := range r.buses {
			b.close()
		}
	})
}
