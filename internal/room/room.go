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

// Room holds one live audio source and its set of listeners.
// A room is created open and closed explicitly via Close.
type Room struct {
	ID string

	mu        sync.RWMutex
	source    Source
	listeners map[string]Listener

	closeOnce sync.Once
	done      chan struct{}

	PacketCount atomic.Uint64
	ByteCount   atomic.Uint64
}

// New returns an open room with the given id.
func New(id string) *Room {
	return &Room{
		ID:        id,
		listeners: make(map[string]Listener),
		done:      make(chan struct{}),
	}
}

// SetSource sets the audio source and starts the forward loop.
// Calling SetSource a second time is undefined; the caller must not do this.
func (r *Room) SetSource(src Source) {
	r.mu.Lock()
	r.source = src
	r.mu.Unlock()
	go r.forwardLoop(src)
}

// AddListener registers listener l under peerID.
func (r *Room) AddListener(peerID string, l Listener) {
	r.mu.Lock()
	r.listeners[peerID] = l
	r.mu.Unlock()
}

// RemoveListener deregisters the listener for peerID. No-op if absent.
func (r *Room) RemoveListener(peerID string) {
	r.mu.Lock()
	delete(r.listeners, peerID)
	r.mu.Unlock()
}

// ListenerCount returns the current number of registered listeners.
func (r *Room) ListenerCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.listeners)
}

// SourceActive reports whether a source is currently attached.
func (r *Room) SourceActive() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.source != nil
}

// Close terminates the room. Safe to call multiple times.
func (r *Room) Close() {
	r.closeOnce.Do(func() { close(r.done) })
}

// forwardLoop reads RTP from src and writes to each listener until src
// returns an error or the room is closed. The goroutine teardown path:
//  1. src.ReadRTP errors (peer connection closed) → return.
//  2. r.done closed (Room.Close called) → return after the next packet read.
//
// In both cases the deferred block clears r.source so SourceActive() returns false.
func (r *Room) forwardLoop(src Source) {
	defer func() {
		r.mu.Lock()
		if r.source == src {
			r.source = nil
		}
		r.mu.Unlock()
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

		// Snapshot listener set under read lock to minimise lock hold time.
		r.mu.RLock()
		ls := make([]Listener, 0, len(r.listeners))
		for _, l := range r.listeners {
			ls = append(ls, l)
		}
		r.mu.RUnlock()

		for _, l := range ls {
			// TODO(Phase 1, ADR-009): measure WriteRTP allocation/mutation profile
			// before claiming zero-alloc forwarding. See forward_bench_test.go.
			_ = l.WriteRTP(pkt)
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
