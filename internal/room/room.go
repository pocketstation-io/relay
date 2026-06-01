package room

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
)

// defaultInactivityTimeout is the time a room without an active publisher will
// remain open before being automatically closed. Configurable via
// ROOM_EXPIRY_MINUTES env (default 30).
const defaultInactivityTimeout = 30 * time.Minute

// defaultReconnectWindow is the time a source has to reconnect after an
// unexpected disconnect before the room is closed. Configurable via
// SOURCE_RECONNECT_WINDOW_SEC env (default 60 s).
const defaultReconnectWindow = 60 * time.Second

// ErrNoSource is returned when an operation requires a live source and none is set.
var ErrNoSource = errors.New("room has no source")

// ErrRoomFull is returned by AddListener when the room has reached its
// maximum listener capacity (configured via MaxListeners in ManagerConfig).
var ErrRoomFull = errors.New("room has reached maximum listener capacity")

// Source is the readable side of a live audio stream.
// Using an interface here decouples room from *webrtc.TrackRemote and allows
// the forward loop to be unit-tested without a live Pion setup.
type Source interface {
	ReadRTP() (*rtp.Packet, error)
}

// Listener is the write-side of a delivered audio stream.
// *webrtc.TrackLocalStaticRTP satisfies this interface directly.
// The interface boundary also enables ADR-009 benchmarking with mock listeners.
type Listener interface {
	WriteRTP(pkt *rtp.Packet) error
}

// listenerEntry pairs a peer ID with its Listener.
// Stored by value in the copy-on-write slice so the slice itself holds no
// extra pointer indirection beyond the interface header.
type listenerEntry struct {
	peerID string
	l      Listener
}

// Room holds one live audio source and its set of listeners.
//
// Invariants:
//   - listeners is a copy-on-write atomic pointer to an immutable []listenerEntry.
//     forwardLoop reads it with one Load and holds no lock during WriteRTP.
//   - listenersMu is a short-lived write lock used only by AddListener and
//     RemoveListener to serialise slice copies. It is never held during I/O.
//   - source and sourceMu are separate because SetSource must be able to change
//     the source without blocking the listener snapshot path.
//   - closeOnce and done provide an idempotent close signal.
//   - inactivityTimer fires when no PUBLISH arrives within inactivityTimeout
//     before any source has ever connected.
//   - reconnectTimer fires when a source disconnects and no new PUBLISH arrives
//     within reconnectWindow. Both timers call Close; only one fires per lifecycle.
//     timerMu serialises all Reset/Stop calls across both timers.
//
// Ownership: Room is created open. Callers call Close to terminate it.
// Failure: forwardLoop exits on source EOF or done close; source is cleared atomically.
// Phase scope: ADR-005 copy-on-write + room expiry + source reconnect (Phase 2).
//
// Intentionally not implemented: automatic room cleanup on last-peer-leave.
type Room struct {
	ID string

	// listeners is a copy-on-write pointer to an immutable snapshot.
	// The zero value (nil pointer) is treated as an empty slice in forwardLoop.
	// Write ops (AddListener, RemoveListener) take listenersMu, copy the slice,
	// then atomically store the new pointer.
	listenersMu sync.Mutex
	listeners   atomic.Pointer[[]listenerEntry]

	// sourceMu guards source, sourceCloser, and loopDone.
	// sourceCloser, when non-nil, terminates the previous source's underlying
	// connection (e.g. closes the PeerConnection). Called by SetSource on
	// reconnect so the old forwardLoop exits and listeners receive RTP from
	// the new source without re-subscribing.
	//
	// loopDone is closed by the running forwardLoop goroutine when it exits.
	// SetSource waits on prevLoopDone before launching the new loop, ensuring
	// only one forwardLoop writes to listeners at a time.
	sourceMu     sync.Mutex
	source       Source
	sourceCloser func()
	loopDone     chan struct{} // closed by the current forwardLoop on exit; nil if none

	closeOnce sync.Once
	done      chan struct{}

	// timerMu serialises Reset/Stop calls on inactivityTimer and reconnectTimer
	// to avoid races between timer callbacks and SetSource.
	timerMu sync.Mutex

	// inactivityTimer auto-closes the room after inactivityTimeout when no
	// publisher has ever connected. Cancelled by the first SetSource call.
	inactivityTimer   *time.Timer
	inactivityTimeout time.Duration

	// reconnectTimer auto-closes the room after reconnectWindow when a source
	// disconnects and no replacement PUBLISH arrives in time. Started by
	// forwardLoop on exit (after the first publisher has connected); cancelled
	// by the next SetSource call.
	reconnectTimer  *time.Timer
	reconnectWindow time.Duration

	// sourceEverConnected tracks whether SetSource has been called at least once.
	// Used to choose between inactivity semantics (no source yet) and reconnect
	// semantics (source previously connected).
	sourceEverConnected atomic.Bool

	// maxListeners is the maximum number of listeners allowed in this room.
	// Zero means unlimited. Set once at creation from ManagerConfig.MaxListeners.
	maxListeners int

	PacketCount     atomic.Uint64
	ByteCount       atomic.Uint64
	PacketDropCount atomic.Uint64
}

// New returns an open room with the given id and default timeouts.
func New(id string) *Room {
	return newWithTimeouts(id, defaultInactivityTimeout, defaultReconnectWindow)
}

// newWithTimeout creates a room with a configurable inactivity timeout and the
// default reconnect window. Used by existing tests that pre-date the reconnect
// window feature.
func newWithTimeout(id string, timeout time.Duration) *Room {
	return newWithTimeouts(id, timeout, defaultReconnectWindow)
}

// newWithTimeouts creates a room with configurable inactivity timeout and
// reconnect window. Used by tests to exercise expiry / reconnect with short
// durations without sleeping through production values.
func newWithTimeouts(id string, inactivityTimeout, reconnectWindow time.Duration) *Room {
	r := &Room{
		ID:                id,
		done:              make(chan struct{}),
		inactivityTimeout: inactivityTimeout,
		reconnectWindow:   reconnectWindow,
	}
	// Initialise atomic pointer to an empty (non-nil) slice so Load always
	// returns a valid pointer. This avoids nil-dereference in forwardLoop.
	empty := make([]listenerEntry, 0)
	r.listeners.Store(&empty)

	// Start the inactivity timer. The timer fires if no publisher attaches within
	// inactivityTimeout. SetSource cancels this timer on the first connection.
	//
	// Race-free construction: both the timer field write and the timerMu lock
	// happen under timerMu. The callback also acquires timerMu before calling
	// Close, establishing a happens-before relationship that satisfies the Go
	// memory model.
	r.timerMu.Lock()
	r.inactivityTimer = time.AfterFunc(inactivityTimeout, func() {
		// Acquire timerMu before calling Close so there is a happens-before edge
		// between the r.inactivityTimer assignment in newWithTimeouts and this
		// goroutine reading it through r.Close → r.inactivityTimer.Stop().
		r.timerMu.Lock()
		r.timerMu.Unlock()
		r.Close()
	})
	r.timerMu.Unlock()
	return r
}

// SetSource sets the audio source and starts the forward loop.
// Resets the inactivity timer to give the new publisher a fresh window.
//
// If a previous source is active (ICE restart / reconnect), SetSource closes
// it via the registered sourceCloser before starting the new forwardLoop.
// closer, if non-nil, will be called when the next SetSource call replaces
// this source, or when the room closes; pass nil if no cleanup is needed.
//
// Existing listeners are not affected: they continue to receive RTP from the
// new source without re-subscribing, satisfying the ICE restart requirement.
func (r *Room) SetSource(src Source, closer func()) {
	newLoopDone := make(chan struct{})

	r.sourceMu.Lock()
	prevCloser := r.sourceCloser
	prevLoopDone := r.loopDone
	r.source = src
	r.sourceCloser = closer
	r.loopDone = newLoopDone
	r.sourceMu.Unlock()

	// Initiate teardown of the previous source's connection so its ReadRTP
	// starts returning errors, unblocking the old forwardLoop.
	if prevCloser != nil {
		prevCloser()
	}

	// Wait for the old forwardLoop to exit before launching the new one.
	// This ensures only one loop calls WriteRTP on the listener slice at a time,
	// preventing double-write races on reconnect (BLOCK-1 fix).
	if prevLoopDone != nil {
		<-prevLoopDone
	}

	// On the first publisher: stop the pre-connection inactivity timer so it
	// does not fire after a source has successfully joined. On subsequent
	// publishers (reconnect): stop any pending reconnect timer.
	// timerMu prevents a race between these Stop/Reset calls and the callback.
	r.timerMu.Lock()
	r.inactivityTimer.Stop()
	if r.reconnectTimer != nil {
		r.reconnectTimer.Stop()
	}
	r.timerMu.Unlock()

	r.sourceEverConnected.Store(true)

	go r.forwardLoop(src, newLoopDone)
}

// AddListener registers listener l under peerID.
// Returns ErrRoomFull when r.maxListeners > 0 and the room is at capacity.
// Takes listenersMu briefly to copy the slice, then stores the new pointer.
func (r *Room) AddListener(peerID string, l Listener) error {
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()

	old := *r.listeners.Load()
	if r.maxListeners > 0 && len(old) >= r.maxListeners {
		return ErrRoomFull
	}
	next := make([]listenerEntry, len(old)+1)
	copy(next, old)
	next[len(old)] = listenerEntry{peerID: peerID, l: l}
	r.listeners.Store(&next)
	return nil
}

// RemoveListener deregisters the listener for peerID. No-op if absent.
// Takes listenersMu briefly to copy the slice, then stores the new pointer.
func (r *Room) RemoveListener(peerID string) {
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()

	old := *r.listeners.Load()
	next := make([]listenerEntry, 0, len(old))
	for _, e := range old {
		if e.peerID != peerID {
			next = append(next, e)
		}
	}
	r.listeners.Store(&next)
}

// ListenerCount returns the current number of registered listeners.
func (r *Room) ListenerCount() int {
	return len(*r.listeners.Load())
}

// SourceActive reports whether a source is currently attached.
func (r *Room) SourceActive() bool {
	r.sourceMu.Lock()
	defer r.sourceMu.Unlock()
	return r.source != nil
}

// Close terminates the room. Safe to call multiple times.
// Stops both the inactivity timer and the reconnect timer.
func (r *Room) Close() {
	r.closeOnce.Do(func() {
		r.timerMu.Lock()
		r.inactivityTimer.Stop()
		if r.reconnectTimer != nil {
			r.reconnectTimer.Stop()
		}
		r.timerMu.Unlock()
		close(r.done)
	})
}

// forwardLoop reads RTP from src and writes to each listener until src
// returns an error or the room is closed. loopDone is closed when the
// goroutine exits, allowing SetSource to synchronise on reconnect.
//
// Hot-path invariants (ADR-005, ADR-009):
//   - One atomic.Load per packet; no lock held during WriteRTP.
//   - No heap allocation beyond what the loaded slice pointer itself costs.
//
// Goroutine teardown:
//  1. src.ReadRTP errors (peer connection closed) → return.
//  2. r.done closed (Room.Close called) → return after the next packet read.
//
// In both cases the deferred block clears r.source and closes loopDone.
func (r *Room) forwardLoop(src Source, loopDone chan struct{}) {
	defer close(loopDone)
	defer func() {
		r.sourceMu.Lock()
		if r.source == src {
			r.source = nil
			r.sourceCloser = nil
		}
		r.sourceMu.Unlock()

		// Source disconnected (EOF or room closed). If the room is still open,
		// start the reconnect window timer: give the source reconnectWindow to
		// re-PUBLISH before the room is closed. This allows listeners to remain
		// connected across a brief source ICE failure or network blip.
		select {
		case <-r.done:
			// Room already closed — no reconnect timer needed.
		default:
			r.timerMu.Lock()
			r.reconnectTimer = time.AfterFunc(r.reconnectWindow, func() {
				// Acquire timerMu before calling Close to establish a happens-before
				// edge between the reconnectTimer field assignment and this callback
				// reading it through r.Close → r.reconnectTimer.Stop().
				r.timerMu.Lock()
				r.timerMu.Unlock()
				r.Close()
			})
			r.timerMu.Unlock()
		}
	}()

	for {
		pkt, err := src.ReadRTP()
		if err != nil {
			return
		}

		select {
		case <-r.done:
			return
		default:
		}

		r.PacketCount.Add(1)
		r.ByteCount.Add(uint64(len(pkt.Payload)))

		// ADR-005: one atomic load; no lock held during WriteRTP.
		ls := *r.listeners.Load()
		for _, e := range ls {
			if err := e.l.WriteRTP(pkt); err != nil {
				r.PacketDropCount.Add(1)
			}
		}
	}
}

// Manager owns the set of active rooms.
// The name "Manager" is used here because Go convention for a type that owns
// a keyed set of live objects is well-established; a more specific name would
// not be clearer at this phase.
type Manager struct {
	mu    sync.RWMutex
	rooms map[string]*Room

	// inactivityTimeout, reconnectWindow, and maxListeners are applied to every
	// room created by GetOrCreate. Zero values use the package defaults.
	inactivityTimeout time.Duration
	reconnectWindow   time.Duration
	maxListeners      int
}

// ManagerConfig holds configurable timeouts for the Manager.
type ManagerConfig struct {
	// InactivityTimeout is the time a room waits for its first publisher before
	// auto-closing. Zero means use defaultInactivityTimeout (30 min).
	// Configurable via ROOM_EXPIRY_MINUTES env in main.go.
	InactivityTimeout time.Duration
	// ReconnectWindow is the time a room keeps listeners alive after its source
	// disconnects, waiting for a re-PUBLISH on the same room_id.
	// Zero means use defaultReconnectWindow (60 s).
	// Configurable via SOURCE_RECONNECT_WINDOW_SEC env in main.go.
	ReconnectWindow time.Duration
	// MaxListeners is the maximum number of listeners allowed per room.
	// Zero means unlimited.
	// Configurable via MAX_LISTENERS_PER_ROOM env in main.go.
	MaxListeners int
}

// NewManager returns an empty Manager with default timeouts.
func NewManager() *Manager {
	return NewManagerWithConfig(ManagerConfig{})
}

// NewManagerWithConfig returns an empty Manager configured with the given timeouts.
func NewManagerWithConfig(cfg ManagerConfig) *Manager {
	inactivity := cfg.InactivityTimeout
	if inactivity <= 0 {
		inactivity = defaultInactivityTimeout
	}
	reconnect := cfg.ReconnectWindow
	if reconnect <= 0 {
		reconnect = defaultReconnectWindow
	}
	return &Manager{
		rooms:             make(map[string]*Room),
		inactivityTimeout: inactivity,
		reconnectWindow:   reconnect,
		maxListeners:      cfg.MaxListeners,
	}
}

// GetOrCreate returns the room for id, creating it if absent.
func (m *Manager) GetOrCreate(id string) *Room {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r := m.rooms[id]; r != nil {
		return r
	}
	r := newWithTimeouts(id, m.inactivityTimeout, m.reconnectWindow)
	r.maxListeners = m.maxListeners
	m.rooms[id] = r
	return r
}

// Get returns the room for id and a boolean indicating whether it was found.
func (m *Manager) Get(id string) (*Room, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.rooms[id]
	return r, ok
}

// Delete closes and removes the room for id. No-op if absent.
func (m *Manager) Delete(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.rooms[id]; ok {
		r.Close()
		delete(m.rooms, id)
	}
}

// PacketStats returns the aggregate forwarded and dropped packet counts
// across all currently active rooms.
func (m *Manager) PacketStats() (forwarded, dropped uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, r := range m.rooms {
		forwarded += r.PacketCount.Load()
		dropped += r.PacketDropCount.Load()
	}
	return
}

// RoomCount returns the number of active rooms.
func (m *Manager) RoomCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rooms)
}

// CloseAll closes every active room and removes it from the manager.
// Used by the graceful shutdown path to drain all sessions before the
// process exits. Safe to call multiple times.
func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, r := range m.rooms {
		r.Close()
		delete(m.rooms, id)
	}
}
