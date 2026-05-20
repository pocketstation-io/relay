// White-box tests for room lifecycle. Package room (not room_test) so we can
// inject mock implementations via the unexported source/listener interfaces.
package room

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
)

// --- mock types ---

// mockSource delivers packets from a channel. Close signals EOF.
type mockSource struct {
	ch chan *rtp.Packet
}

func newMockSource() *mockSource { return &mockSource{ch: make(chan *rtp.Packet, 16)} }

func (m *mockSource) send(pkt *rtp.Packet) { m.ch <- pkt }

func (m *mockSource) close() { close(m.ch) }

func (m *mockSource) ReadRTP() (*rtp.Packet, error) {
	pkt, ok := <-m.ch
	if !ok {
		return nil, io.EOF
	}
	return pkt, nil
}

// mockListener records every packet it receives.
type mockListener struct {
	mu      sync.Mutex
	packets []*rtp.Packet
}

func (m *mockListener) WriteRTP(pkt *rtp.Packet) error {
	m.mu.Lock()
	m.packets = append(m.packets, pkt)
	m.mu.Unlock()
	return nil
}

func (m *mockListener) received() []*rtp.Packet {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*rtp.Packet, len(m.packets))
	copy(out, m.packets)
	return out
}

// waitFor polls fn until it returns true or deadline elapses.
func waitFor(t *testing.T, deadline time.Duration, fn func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %v", deadline)
}

// --- tests ---

func TestNew_EmptyRoom(t *testing.T) {
	// Given / When
	r := New("room-1")
	// Then
	if r.ID != "room-1" {
		t.Errorf("got ID %q, want %q", r.ID, "room-1")
	}
	if r.ListenerCount() != 0 {
		t.Errorf("got %d listeners, want 0", r.ListenerCount())
	}
	if r.SourceActive() {
		t.Error("new room must not report source active")
	}
}

func TestAddListener_IncreasesCount(t *testing.T) {
	// Given
	r := New("room-1")
	// When
	r.AddListener("peer-1", &mockListener{})
	// Then
	if r.ListenerCount() != 1 {
		t.Errorf("got %d listeners, want 1", r.ListenerCount())
	}
}

func TestAddListener_MultipleListeners(t *testing.T) {
	// Given
	r := New("room-1")
	// When
	r.AddListener("peer-1", &mockListener{})
	r.AddListener("peer-2", &mockListener{})
	r.AddListener("peer-3", &mockListener{})
	// Then
	if r.ListenerCount() != 3 {
		t.Errorf("got %d listeners, want 3", r.ListenerCount())
	}
}

func TestRemoveListener_DecreasesCount(t *testing.T) {
	// Given
	r := New("room-1")
	r.AddListener("peer-1", &mockListener{})
	// When
	r.RemoveListener("peer-1")
	// Then
	if r.ListenerCount() != 0 {
		t.Errorf("got %d listeners, want 0", r.ListenerCount())
	}
}

func TestRemoveListener_UnknownPeer_IsNoop(t *testing.T) {
	// Given
	r := New("room-1")
	// When / Then — must not panic
	r.RemoveListener("does-not-exist")
}

func TestClose_Idempotent(t *testing.T) {
	// Given
	r := New("room-1")
	// When / Then — multiple closes must not panic
	r.Close()
	r.Close()
	r.Close()
}

func TestSetSource_SourceActiveBecomesTrue(t *testing.T) {
	// Given
	r := New("room-1")
	src := newMockSource()
	// When
	r.SetSource(src)
	// Then
	if !r.SourceActive() {
		t.Error("expected source active after SetSource")
	}
	src.close()
}

func TestForwardLoop_DeliversSinglePacketToOneListener(t *testing.T) {
	// Given
	r := New("room-1")
	src := newMockSource()
	l := &mockListener{}
	r.AddListener("peer-1", l)
	r.SetSource(src)

	pkt := &rtp.Packet{Payload: []byte{0xDE, 0xAD, 0xBE, 0xEF}}

	// When
	src.send(pkt)

	// Then
	waitFor(t, 100*time.Millisecond, func() bool { return len(l.received()) == 1 })
	if got := l.received()[0]; got != pkt {
		t.Errorf("listener received wrong packet pointer")
	}

	src.close()
}

func TestForwardLoop_DeliversSinglePacketToMultipleListeners(t *testing.T) {
	// Given
	r := New("room-1")
	src := newMockSource()
	l1, l2 := &mockListener{}, &mockListener{}
	r.AddListener("peer-1", l1)
	r.AddListener("peer-2", l2)
	r.SetSource(src)

	pkt := &rtp.Packet{Payload: []byte{0x01}}

	// When
	src.send(pkt)

	// Then
	waitFor(t, 100*time.Millisecond, func() bool {
		return len(l1.received()) == 1 && len(l2.received()) == 1
	})

	src.close()
}

func TestForwardLoop_SourceClearsAfterEOF(t *testing.T) {
	// Given
	r := New("room-1")
	src := newMockSource()
	r.SetSource(src)

	// When — close source channel to trigger EOF
	src.close()

	// Then — source must clear once forwardLoop exits
	waitFor(t, 100*time.Millisecond, func() bool { return !r.SourceActive() })
}

func TestForwardLoop_StopsAfterRoomClosed(t *testing.T) {
	// Given
	r := New("room-1")
	src := newMockSource()
	l := &mockListener{}
	r.AddListener("peer-1", l)
	r.SetSource(src)

	// When — close room, then send a packet
	r.Close()
	// Send one packet to unblock the ReadRTP call in forwardLoop.
	src.send(&rtp.Packet{Payload: []byte{0x01}})

	// Then — forwardLoop exits; source clears
	waitFor(t, 100*time.Millisecond, func() bool { return !r.SourceActive() })
	src.close()
}

func TestForwardLoop_CountersIncrementPerPacket(t *testing.T) {
	// Given
	r := New("room-1")
	src := newMockSource()
	r.SetSource(src)

	payload := []byte{0xAA, 0xBB, 0xCC}
	pkt := &rtp.Packet{Payload: payload}

	// When
	src.send(pkt)

	// Then
	waitFor(t, 100*time.Millisecond, func() bool { return r.PacketCount.Load() == 1 })
	if got := r.ByteCount.Load(); got != uint64(len(payload)) {
		t.Errorf("ByteCount = %d, want %d", got, len(payload))
	}

	src.close()
}

func TestManager_GetOrCreate_ReusesSameRoom(t *testing.T) {
	// Given
	m := NewManager()
	// When
	r1 := m.GetOrCreate("r1")
	r2 := m.GetOrCreate("r1")
	// Then
	if r1 != r2 {
		t.Error("GetOrCreate returned different pointers for same ID")
	}
}

func TestManager_GetOrCreate_CreatesDistinctRooms(t *testing.T) {
	// Given
	m := NewManager()
	// When
	r1 := m.GetOrCreate("r1")
	r2 := m.GetOrCreate("r2")
	// Then
	if r1 == r2 {
		t.Error("GetOrCreate returned same pointer for different IDs")
	}
}

func TestManager_Get_ReturnsFalseForUnknownRoom(t *testing.T) {
	// Given
	m := NewManager()
	// When
	_, ok := m.Get("unknown")
	// Then
	if ok {
		t.Error("Get returned true for a room that was never created")
	}
}

func TestManager_Delete_RemovesRoom(t *testing.T) {
	// Given
	m := NewManager()
	m.GetOrCreate("r1")
	// When
	m.Delete("r1")
	// Then
	_, ok := m.Get("r1")
	if ok {
		t.Error("room still present after Delete")
	}
}

func TestManager_Delete_UnknownRoom_IsNoop(t *testing.T) {
	// Given
	m := NewManager()
	// When / Then — must not panic
	m.Delete("does-not-exist")
}
