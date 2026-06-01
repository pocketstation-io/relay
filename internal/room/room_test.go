// White-box tests for room lifecycle. Package room (not room_test) so we can
// inject mock implementations via the unexported source/listener interfaces.
package room

import (
	"fmt"
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
	r.SetSource(src, nil)
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
	r.SetSource(src, nil)

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
	r.SetSource(src, nil)

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
	r.SetSource(src, nil)

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
	r.SetSource(src, nil)

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
	r.SetSource(src, nil)

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

// TestGiven_Room_When_InactivityTimerExpires_Then_RoomClosed verifies that a
// room auto-closes after its inactivity timeout elapses with no publisher.
func TestGiven_Room_When_InactivityTimerExpires_Then_RoomClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("timer expiry test sleeps up to 150ms — skipped in -short mode")
	}

	// Given — a room with a very short inactivity timeout and no publisher.
	const timeout = 50 * time.Millisecond
	r := newWithTimeout("expiry-room", timeout)

	// When — we wait longer than the inactivity timeout without calling SetSource.

	// Then — done channel is closed, meaning Room.Close was called by the timer.
	waitFor(t, 500*time.Millisecond, func() bool {
		select {
		case <-r.done:
			return true
		default:
			return false
		}
	})
}

// TestGiven_Room_When_SourceSetsBeforeTimeout_Then_TimerReset verifies that
// calling SetSource resets the inactivity timer so the room stays open.
func TestGiven_Room_When_SourceSetsBeforeTimeout_Then_TimerReset(t *testing.T) {
	// Given — a room with a short inactivity timeout.
	const timeout = 80 * time.Millisecond
	r := newWithTimeout("reset-room", timeout)
	src := newMockSource()

	// When — attach a source before the timer fires; the timer must reset.
	// We sleep half the timeout, then attach, then check the room is still open.
	time.Sleep(timeout / 2)
	r.SetSource(src, nil)

	// Then — the room should still be open shortly after the original timeout.
	// The timer was reset, so the room should remain open for another `timeout`.
	time.Sleep(timeout / 2)
	select {
	case <-r.done:
		t.Fatal("room closed too early: timer was not reset by SetSource")
	default:
	}

	src.close()
	r.Close()
}

// TestGiven_SourcePublishing_When_SourceReconnects_Then_ListenerReceivesRTPAfterReconnect
// verifies that when a new source replaces an existing one (ICE restart), the
// existing listener continues receiving RTP without re-subscribing.
func TestGiven_SourcePublishing_When_SourceReconnects_Then_ListenerReceivesRTPAfterReconnect(t *testing.T) {
	// Given — a room with a source and a listener.
	r := New("reconnect-room")
	src1 := newMockSource()
	l := &mockListener{}
	r.AddListener("peer-1", l)

	closerCalled := false
	r.SetSource(src1, func() {
		closerCalled = true
		src1.close() // unblocks the old forwardLoop so SetSource can wait for it
	})

	// Send a packet from src1 to confirm the initial forward path works.
	pkt1 := &rtp.Packet{Payload: []byte{0x01}}
	src1.send(pkt1)
	waitFor(t, 100*time.Millisecond, func() bool { return len(l.received()) == 1 })

	// When — reconnect: a new source replaces the old one.
	// SetSource must call the prevCloser (simulating PC.Close on the old source).
	src2 := newMockSource()
	r.SetSource(src2, nil)

	// The prevCloser (from src1's SetSource) must have been called.
	waitFor(t, 100*time.Millisecond, func() bool { return closerCalled })

	// Send a packet from src2 — the listener should receive it without
	// having called AddListener again.
	pkt2 := &rtp.Packet{Payload: []byte{0x02}}
	src2.send(pkt2)
	waitFor(t, 100*time.Millisecond, func() bool { return len(l.received()) == 2 })

	// Then — listener received both pkt1 and pkt2 in order.
	pkts := l.received()
	if len(pkts) < 2 {
		t.Fatalf("expected at least 2 packets, got %d", len(pkts))
	}
	if pkts[0] != pkt1 {
		t.Error("first packet from src1 not received correctly")
	}
	if pkts[1] != pkt2 {
		t.Error("first packet from src2 not received after reconnect")
	}

	// src1 was already closed by the closer registered with SetSource.
	src2.close()
	r.Close()
}

// TestGiven_CopyOnWriteListeners_When_ConcurrentAddAndForward_Then_NoRace
// verifies that concurrent AddListener calls and forwardLoop do not race.
// Run with -race; the race detector fires immediately on any unsynchronised
// access to the listener slice.
func TestGiven_CopyOnWriteListeners_When_ConcurrentAddAndForward_Then_NoRace(t *testing.T) {
	// Given — a room with a live source sending packets continuously.
	r := New("race-room")
	src := newMockSource()
	r.SetSource(src, nil)

	// stopProducer is closed first to stop the producer goroutine before
	// src.close() is called, preventing a channel-close-while-sending race.
	stopProducer := make(chan struct{})
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		pkt := &rtp.Packet{Payload: []byte{0x01}}
		for {
			select {
			case <-stopProducer:
				return
			default:
				// Non-blocking send: drop if channel is full so the producer
				// never blocks after stopProducer is closed.
				select {
				case src.ch <- pkt:
				default:
				}
			}
		}
	}()

	// When — concurrently add and remove listeners while forwardLoop is running.
	var wg sync.WaitGroup
	const goroutines = 8
	const opsPerGoroutine = 50
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				id := fmt.Sprintf("peer-%d-%d", g, i)
				r.AddListener(id, &mockListener{})
				r.RemoveListener(id)
			}
		}(g)
	}

	// Then — all goroutines complete without the race detector firing.
	wg.Wait()

	// Stop producer before closing the source channel to avoid a race between
	// a concurrent send on src.ch and src.close() (which closes the channel).
	close(stopProducer)
	<-producerDone
	src.close()
}

// Phase 2 exit-criterion tests — named for the three scenarios called out in the
// Phase 2 hardening spec. They use newWithTimeouts to exercise short durations
// without waiting for production defaults (30 min inactivity / 60 s reconnect).

// TestRoomExpiryAfterTimeout verifies that a room with no publisher auto-closes
// after its configured inactivity timeout. This satisfies the Phase 2 exit
// criterion: "Relay survives source disconnect + reconnect without losing listeners."
// The complementary side is that rooms without any publisher are reclaimed promptly.
func TestRoomExpiryAfterTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("timer expiry test sleeps up to 250 ms — skipped in -short mode")
	}

	// Given — a room with a very short inactivity timeout and no reconnect window.
	const inactivity = 50 * time.Millisecond
	r := newWithTimeouts("expiry-room", inactivity, time.Hour)

	// When — no publisher connects; we simply wait.

	// Then — the room must auto-close after the inactivity timeout.
	waitFor(t, 500*time.Millisecond, func() bool {
		select {
		case <-r.done:
			return true
		default:
			return false
		}
	})
}

// TestSourceReconnectWithinWindow verifies that when a source disconnects and a
// replacement source re-PUBLISHes within the reconnect window, existing listeners
// keep receiving RTP without re-subscribing.
func TestSourceReconnectWithinWindow(t *testing.T) {
	if testing.Short() {
		t.Skip("reconnect window test sleeps up to 200 ms — skipped in -short mode")
	}

	// Given — a room with a generous reconnect window and a listener.
	const inactivity = time.Hour    // never fires during this test
	const reconnect = 200 * time.Millisecond
	r := newWithTimeouts("reconnect-window-room", inactivity, reconnect)

	src1 := newMockSource()
	l := &mockListener{}
	r.AddListener("peer-1", l)

	// Attach src1 and confirm the listener receives a packet.
	r.SetSource(src1, func() { src1.close() })
	pkt1 := &rtp.Packet{Payload: []byte{0xAA}}
	src1.send(pkt1)
	waitFor(t, 100*time.Millisecond, func() bool { return len(l.received()) == 1 })

	// When — source1 disconnects. The reconnect window timer starts.
	// Simulate ICE failure: the closer is called, which closes src1's channel,
	// causing the forwardLoop to exit via ReadRTP → EOF.
	src1.close()

	// Wait for forwardLoop to exit and source to clear.
	waitFor(t, 100*time.Millisecond, func() bool { return !r.SourceActive() })

	// Reconnect a new source within the window.
	src2 := newMockSource()
	r.SetSource(src2, nil)

	// Then — listener receives a packet from src2 without having re-subscribed.
	pkt2 := &rtp.Packet{Payload: []byte{0xBB}}
	src2.send(pkt2)
	waitFor(t, 100*time.Millisecond, func() bool { return len(l.received()) == 2 })

	pkts := l.received()
	if len(pkts) < 2 || pkts[0] != pkt1 || pkts[1] != pkt2 {
		t.Errorf("listener did not receive both packets; got %d", len(pkts))
	}

	// Room must still be open.
	select {
	case <-r.done:
		t.Fatal("room was closed unexpectedly during reconnect window")
	default:
	}

	src2.close()
	r.Close()
}

// TestSourceReconnectWindowExpired verifies that when a source disconnects and no
// replacement arrives within the reconnect window, the room auto-closes so
// listeners are not held open indefinitely.
func TestSourceReconnectWindowExpired(t *testing.T) {
	if testing.Short() {
		t.Skip("reconnect window expiry test sleeps up to 300 ms — skipped in -short mode")
	}

	// Given — a room with a short reconnect window.
	const inactivity = time.Hour    // never fires during this test
	const reconnect = 50 * time.Millisecond
	r := newWithTimeouts("reconnect-expired-room", inactivity, reconnect)

	src := newMockSource()
	r.AddListener("peer-1", &mockListener{})
	r.SetSource(src, func() { src.close() })

	// Confirm source is active.
	if !r.SourceActive() {
		t.Fatal("source must be active after SetSource")
	}

	// When — source disconnects.
	src.close()

	// Wait for forwardLoop to clear the source.
	waitFor(t, 100*time.Millisecond, func() bool { return !r.SourceActive() })

	// Then — room must close after the reconnect window expires (no re-PUBLISH).
	waitFor(t, 500*time.Millisecond, func() bool {
		select {
		case <-r.done:
			return true
		default:
			return false
		}
	})
}
