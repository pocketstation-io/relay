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
	if err := r.AddListener("peer-1", &mockListener{}); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
	// Then
	if r.ListenerCount() != 1 {
		t.Errorf("got %d listeners, want 1", r.ListenerCount())
	}
}

func TestAddListener_MultipleListeners(t *testing.T) {
	// Given
	r := New("room-1")
	// When
	for _, id := range []string{"peer-1", "peer-2", "peer-3"} {
		if err := r.AddListener(id, &mockListener{}); err != nil {
			t.Fatalf("AddListener %s: %v", id, err)
		}
	}
	// Then
	if r.ListenerCount() != 3 {
		t.Errorf("got %d listeners, want 3", r.ListenerCount())
	}
}

func TestRemoveListener_DecreasesCount(t *testing.T) {
	// Given
	r := New("room-1")
	if err := r.AddListener("peer-1", &mockListener{}); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
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
	if err := r.AddListener("peer-1", l); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
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
	if err := r.AddListener("peer-1", l1); err != nil {
		t.Fatalf("AddListener peer-1: %v", err)
	}
	if err := r.AddListener("peer-2", l2); err != nil {
		t.Fatalf("AddListener peer-2: %v", err)
	}
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
	if err := r.AddListener("peer-1", l); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
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
	if err := r.AddListener("peer-1", l); err != nil {
		t.Fatalf("AddListener: %v", err)
	}

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

// TestGiven_SourceReconnects_When_NewSessionHasDifferentInitialSeq_Then_RelaySeqIsMonotonic
// verifies that relay-local resequencing prevents sequence number gaps when the
// CLI source reconnects (new str0m session = new random initial RTP sequence).
// This is the root cause of the 47% apparent packet loss observed in Chrome's
// WebRTC stats: pion rewrites the SSRC to the relay track's SSRC, so Chrome sees
// one continuous SSRC with a sequence gap — interpreted as massive packet loss.
func TestGiven_SourceReconnects_When_NewSessionHasDifferentInitialSeq_Then_RelaySeqIsMonotonic(t *testing.T) {
	r := New("reseq-room")
	l := &mockListener{}
	if err := r.AddListener("peer-1", l); err != nil {
		t.Fatalf("AddListener: %v", err)
	}

	// Source 1 sends 3 packets with a high initial sequence number (e.g., str0m random start).
	src1 := newMockSource()
	r.SetSource(src1, func() { src1.close() })

	for seq := uint16(50000); seq < 50003; seq++ {
		src1.send(&rtp.Packet{Header: rtp.Header{SequenceNumber: seq, Timestamp: uint32(seq) * 960}})
	}
	waitFor(t, 100*time.Millisecond, func() bool { return len(l.received()) == 3 })

	// Source 2 reconnects with a new random initial sequence (0 — far from 50002).
	src2 := newMockSource()
	r.SetSource(src2, nil)
	for seq := uint16(0); seq < 3; seq++ {
		src2.send(&rtp.Packet{Header: rtp.Header{SequenceNumber: seq, Timestamp: uint32(seq) * 960}})
	}
	waitFor(t, 100*time.Millisecond, func() bool { return len(l.received()) == 6 })

	// Then — all 6 packets arrive at the listener with relay-local monotonically
	// increasing sequence numbers (0, 1, 2, 3, 4, 5). No gap of ~50000 between
	// the two source sessions.
	pkts := l.received()
	for i, pkt := range pkts {
		if pkt.SequenceNumber != uint16(i) {
			t.Errorf("packet[%d]: got SequenceNumber=%d, want %d", i, pkt.SequenceNumber, i)
		}
	}

	// Verify monotonic timestamp (960 samples per 20ms Opus frame at 48 kHz).
	for i, pkt := range pkts {
		wantTS := uint32(i) * 960
		if pkt.Timestamp != wantTS {
			t.Errorf("packet[%d]: got Timestamp=%d, want %d", i, pkt.Timestamp, wantTS)
		}
	}

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
				_ = r.AddListener(id, &mockListener{})
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
	const inactivity = time.Hour // never fires during this test
	const reconnect = 200 * time.Millisecond
	r := newWithTimeouts("reconnect-window-room", inactivity, reconnect)

	src1 := newMockSource()
	l := &mockListener{}
	if err := r.AddListener("peer-1", l); err != nil {
		t.Fatalf("AddListener: %v", err)
	}

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
	const inactivity = time.Hour // never fires during this test
	const reconnect = 50 * time.Millisecond
	r := newWithTimeouts("reconnect-expired-room", inactivity, reconnect)

	src := newMockSource()
	if err := r.AddListener("peer-1", &mockListener{}); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
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

// TestMaxListenersEnforced verifies that AddListener returns ErrRoomFull when
// the room has reached its configured maxListeners ceiling.
func TestMaxListenersEnforced(t *testing.T) {
	// Given — a manager configured with a max of 2 listeners per room.
	cfg := ManagerConfig{
		InactivityTimeout: time.Hour,
		ReconnectWindow:   time.Hour,
		MaxListeners:      2,
	}
	m := NewManagerWithConfig(cfg)
	r := m.GetOrCreate("capacity-room")

	// When — fill the room to its limit.
	if err := r.AddListener("peer-1", &mockListener{}); err != nil {
		t.Fatalf("AddListener peer-1: unexpected error: %v", err)
	}
	if err := r.AddListener("peer-2", &mockListener{}); err != nil {
		t.Fatalf("AddListener peer-2: unexpected error: %v", err)
	}

	// Then — the next AddListener must return ErrRoomFull.
	err := r.AddListener("peer-3", &mockListener{})
	if err == nil {
		t.Fatal("expected ErrRoomFull, got nil")
	}
	if err != ErrRoomFull {
		t.Errorf("expected ErrRoomFull, got %v", err)
	}

	// And — the listener count must remain at the ceiling.
	if got := r.ListenerCount(); got != 2 {
		t.Errorf("ListenerCount = %d, want 2", got)
	}

	r.Close()
}

// TestLatencyReportAccumulatesAndReturnsP50 verifies that RecordLatency populates
// the rolling window and GetLatencyStats returns the P50 median of each dimension
// over the recorded samples (spec §13.4).
func TestLatencyReportAccumulatesAndReturnsP50(t *testing.T) {
	// Given — a fresh room and 5 latency samples with known, distinct values.
	r := New("latency-room")
	defer r.Close()

	type sample struct {
		capture, encode, relayRtt, jitter, decode, loss float64
	}
	samples := []sample{
		{capture: 1, encode: 1, relayRtt: 10, jitter: 20, decode: 1, loss: 0.1},
		{capture: 2, encode: 2, relayRtt: 20, jitter: 40, decode: 2, loss: 0.2},
		{capture: 3, encode: 3, relayRtt: 30, jitter: 60, decode: 3, loss: 0.3},
		{capture: 4, encode: 4, relayRtt: 40, jitter: 80, decode: 4, loss: 0.4},
		{capture: 5, encode: 5, relayRtt: 50, jitter: 100, decode: 5, loss: 0.5},
	}

	// When — all samples are recorded.
	for _, s := range samples {
		r.RecordLatency(s.capture, s.encode, s.relayRtt, s.jitter, s.decode, s.loss)
	}

	// Then — GetLatencyStats returns P50 medians.
	// For 5 sorted values [1,2,3,4,5] the median (P50) is index 2 = 3.
	// For packet_loss_pct the result is the mean of all samples (0.3).
	stats := r.GetLatencyStats()

	if stats.SampleCount != 5 {
		t.Errorf("SampleCount = %d, want 5", stats.SampleCount)
	}
	if stats.CaptureP50Ms != 3 {
		t.Errorf("CaptureP50Ms = %v, want 3", stats.CaptureP50Ms)
	}
	if stats.EncodeP50Ms != 3 {
		t.Errorf("EncodeP50Ms = %v, want 3", stats.EncodeP50Ms)
	}
	if stats.RelayRttP50Ms != 30 {
		t.Errorf("RelayRttP50Ms = %v, want 30", stats.RelayRttP50Ms)
	}
	if stats.JitterBufferP50Ms != 60 {
		t.Errorf("JitterBufferP50Ms = %v, want 60", stats.JitterBufferP50Ms)
	}
	if stats.DecodeP50Ms != 3 {
		t.Errorf("DecodeP50Ms = %v, want 3", stats.DecodeP50Ms)
	}

	const wantLoss = 0.3
	const epsilon = 1e-9
	if diff := stats.PacketLossPct - wantLoss; diff > epsilon || diff < -epsilon {
		t.Errorf("PacketLossPct = %v, want %v", stats.PacketLossPct, wantLoss)
	}
}

// TestLatencyReportRollingWindowEvictsOldestSamples verifies that once the
// window fills (latencyWindowSize = 100), new samples overwrite the oldest ones
// so SampleCount stays at the window size.
func TestLatencyReportRollingWindowEvictsOldestSamples(t *testing.T) {
	// Given — a fresh room.
	r := New("latency-window-room")
	defer r.Close()

	// When — record more samples than the window size.
	for i := 0; i < latencyWindowSize+10; i++ {
		r.RecordLatency(float64(i), 0, 0, 0, 0, 0)
	}

	// Then — SampleCount is capped at the window size.
	stats := r.GetLatencyStats()
	if stats.SampleCount != latencyWindowSize {
		t.Errorf("SampleCount = %d, want %d", stats.SampleCount, latencyWindowSize)
	}
}

// TestGiven_KeyExchangeStored_When_GetKey_Then_ReturnsBase64 verifies that
// SetKey stores the opaque base64 string and GetKey returns it unchanged (RELAY-014).
// The relay stores the raw base64 it received from the source without decoding,
// so it never holds raw key material.
func TestGiven_KeyExchangeStored_When_GetKey_Then_ReturnsBase64(t *testing.T) {
	// Given — a base64-encoded key as received over the wire.
	r := New("key-room")
	want := "AQIDBAUGBwgJCgsMDQ4PEA==" // base64 of 0x01..0x10

	// When
	r.SetKey(want)
	got := r.GetKey()

	// Then — returned string matches exactly what was stored.
	if got != want {
		t.Fatalf("GetKey = %q, want %q", got, want)
	}
}

// TestGiven_NoKeyExchangeYet_When_GetKey_Then_ReturnsEmpty verifies that GetKey
// returns "" before any KEY_EXCHANGE has been received (RELAY-014).
func TestGiven_NoKeyExchangeYet_When_GetKey_Then_ReturnsEmpty(t *testing.T) {
	// Given / When
	r := New("no-key-room")

	// Then
	if got := r.GetKey(); got != "" {
		t.Errorf("GetKey = %q, want empty string", got)
	}
}

// TestGiven_KeyExchangeStored_When_ListenerJoinsLate_Then_KeyRetrievable
// verifies the end-to-end room-level contract: after SetKey is called,
// GetKey returns the key so server.go can forward it to late-joining listeners.
func TestGiven_KeyExchangeStored_When_ListenerJoinsLate_Then_KeyRetrievable(t *testing.T) {
	// Given — a key has been stored before the listener joins.
	r := New("late-join-room")
	key := "3q2+78r+ur4BAgMEBQYHCA==" // base64 of 0xde 0xad 0xbe 0xef ... 0x08
	r.SetKey(key)

	// When — a listener joins after the key has been stored.
	l := &mockListener{}
	if err := r.AddListener("late-peer", l); err != nil {
		t.Fatalf("AddListener: %v", err)
	}

	// Then — GetKey returns the stored key so the server can forward it.
	retrieved := r.GetKey()
	if retrieved == "" {
		t.Fatal("GetKey returned empty string; late-joining listener would not receive key")
	}
	if retrieved != key {
		t.Errorf("GetKey = %q, want %q", retrieved, key)
	}
}
