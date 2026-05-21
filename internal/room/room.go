package room

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/pion/rtp"
)

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
//
// Ownership: Room is created open. Callers call Close to terminate it.
// Failure: forwardLoop exits on source EOF or done close; source is cleared atomically.
// Phase scope: ADR-005 copy-on-write (Phase 2). Source reconnect handled in Task 4.
// Intentionally not implemented: automatic room cleanup on last-peer-leave (Phase 2 Task 4).
type Room struct {
	ID string

	// listeners is a copy-on-write pointer to an immutable snapshot.
	// The zero value (nil pointer) is treated as an empty slice in forwardLoop.
	// Write ops (AddListener, RemoveListener) take listenersMu, copy the slice,
	// then atomically store the new pointer.
	listenersMu sync.Mutex
	listeners   atomic.Pointer[[]listenerEntry]

	// sourceMu guards source field for SetSource and SourceActive.
	sourceMu sync.Mutex
	source   Source

	closeOnce sync.Once
	done      chan struct{}

	PacketCount     atomic.Uint64
	ByteCount       atomic.Uint64
	PacketDropCount atomic.Uint64
}

// New returns an open room with the given id.
func New(id string) *Room {
	r := &Room{
		ID:   id,
		done: make(chan struct{}),
	}
	// Initialise atomic pointer to an empty (non-nil) slice so Load always
	// returns a valid pointer. This avoids nil-dereference in forwardLoop.
	empty := make([]listenerEntry, 0)
	r.listeners.Store(&empty)
	return r
}

// SetSource sets the audio source and starts the forward loop.
// Calling SetSource a second time replaces the source (ICE restart path, Task 4).
func (r *Room) SetSource(src Source) {
	r.sourceMu.Lock()
	r.source = src
	r.sourceMu.Unlock()
	go r.forwardLoop(src)
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
func (r *Room) Close() {
	r.closeOnce.Do(func() { close(r.done) })
}

// forwardLoop reads RTP from src and writes to each listener until src
// returns an error or the room is closed.
//
// Hot-path invariants (ADR-005, ADR-009):
//   - One atomic.Load per packet; no lock held during WriteRTP.
//   - No heap allocation beyond what the loaded slice pointer itself costs.
//
// Goroutine teardown:
//  1. src.ReadRTP errors (peer connection closed) → return.
//  2. r.done closed (Room.Close called) → return after the next packet read.
//
// In both cases the deferred block clears r.source so SourceActive() returns false.
func (r *Room) forwardLoop(src Source) {
	defer func() {
		r.sourceMu.Lock()
		if r.source == src {
			r.source = nil
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
