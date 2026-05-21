package room

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
)

// defaultInactivityTimeout is the time a room without an active publisher will
// remain open before being automatically closed.
const defaultInactivityTimeout = 30 * time.Minute

// ErrNoSource is returned when an operation requires a live source and none is set.
var ErrNoSource = errors.New("room has no source")

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
//   - inactivityTimer fires when no PUBLISH arrives within inactivityTimeout.
//     SetSource resets the timer on every successful source attachment.
//
// Ownership: Room is created open. Callers call Close to terminate it.
// Failure: forwardLoop exits on source EOF or done close; source is cleared atomically.
// Phase scope: ADR-005 copy-on-write + room expiry (Phase 2 Tasks 1 and 2).
//
//	Source reconnect handled in Task 4.
//
// Intentionally not implemented: automatic room cleanup on last-peer-leave (Phase 2 Task 4).
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

	// inactivityTimer auto-closes the room after inactivityTimeout of no publisher.
	// timerMu serialises Reset/Stop calls on the timer to avoid a race between
	// the timer firing and SetSource resetting it.
	timerMu           sync.Mutex
	inactivityTimer   *time.Timer
	inactivityTimeout time.Duration

	PacketCount     atomic.Uint64
	ByteCount       atomic.Uint64
	PacketDropCount atomic.Uint64
}

// New returns an open room with the given id and the default inactivity timeout.
func New(id string) *Room {
	return newWithTimeout(id, defaultInactivityTimeout)
}

// newWithTimeout creates a room with a configurable inactivity timeout.
// Used by tests to exercise expiry with short durations without sleeping 30 min.
func newWithTimeout(id string, timeout time.Duration) *Room {
	r := &Room{
		ID:                id,
		done:              make(chan struct{}),
		inactivityTimeout: timeout,
	}
	// Initialise atomic pointer to an empty (non-nil) slice so Load always
	// returns a valid pointer. This avoids nil-dereference in forwardLoop.
	empty := make([]listenerEntry, 0)
	r.listeners.Store(&empty)

	// Start the inactivity timer. The timer fires if no publisher attaches within
	// inactivityTimeout. SetSource resets the timer to give the publisher a
	// fresh window on every reconnect.
	//
	// Race-free construction: both the timer field write and the Reset call
	// happen under timerMu. The callback also acquires timerMu before reading
	// the field (via Stop inside Close). This establishes a happens-before
	// relationship between the write in newWithTimeout and any subsequent read
	// in the callback, satisfying the Go memory model.
	r.timerMu.Lock()
	r.inactivityTimer = time.AfterFunc(timeout, func() {
		// Acquire timerMu before calling Close so there is a happens-before edge
		// between the r.inactivityTimer assignment in newWithTimeout and this
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

	// Reset the inactivity timer: a publisher is now active.
	// timerMu prevents a race between this Reset and the timer callback
	// (which calls Close) firing concurrently.
	r.timerMu.Lock()
	r.inactivityTimer.Stop()
	r.inactivityTimer.Reset(r.inactivityTimeout)
	r.timerMu.Unlock()

	go r.forwardLoop(src, newLoopDone)
}

// AddListener registers listener l under peerID.
// Takes listenersMu briefly to copy the slice, then stores the new pointer.
func (r *Room) AddListener(peerID string, l Listener) {
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()

	old := *r.listeners.Load()
	next := make([]listenerEntry, len(old)+1)
	copy(next, old)
	next[len(old)] = listenerEntry{peerID: peerID, l: l}
	r.listeners.Store(&next)
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
// Stops the inactivity timer so it does not fire after an explicit close.
func (r *Room) Close() {
	r.closeOnce.Do(func() {
		r.timerMu.Lock()
		r.inactivityTimer.Stop()
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
}

// NewManager returns an empty Manager.
func NewManager() *Manager {
	return &Manager{rooms: make(map[string]*Room)}
}

// GetOrCreate returns the room for id, creating it if absent.
func (m *Manager) GetOrCreate(id string) *Room {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r := m.rooms[id]; r != nil {
		return r
	}
	r := New(id)
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
