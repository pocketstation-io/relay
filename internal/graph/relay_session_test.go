// White-box tests for RelaySession lifecycle. Package graph (not graph_test) so we
// can inject mock implementations via the unexported SourceSession/BusSubscription
// interfaces.
package graph

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
)

// --- mock types ---

type mockSource struct {
	ch chan *rtp.Packet
}

func newMockSource() *mockSource           { return &mockSource{ch: make(chan *rtp.Packet, 16)} }
func (m *mockSource) send(pkt *rtp.Packet) { m.ch <- pkt }
func (m *mockSource) close()               { close(m.ch) }
func (m *mockSource) ReadRTP() (*rtp.Packet, error) {
	pkt, ok := <-m.ch
	if !ok {
		return nil, io.EOF
	}
	return pkt, nil
}

type mockSubscription struct {
	mu      sync.Mutex
	packets []*rtp.Packet
}

func (m *mockSubscription) WriteRTP(pkt *rtp.Packet) error {
	m.mu.Lock()
	m.packets = append(m.packets, pkt)
	m.mu.Unlock()
	return nil
}

func (m *mockSubscription) received() []*rtp.Packet {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*rtp.Packet, len(m.packets))
	copy(out, m.packets)
	return out
}

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

// --- RelaySession tests ---

func given_new_relay_session_when_inspected_then_empty(t *testing.T) {
	r := New("room-1")
	if r.ID != "room-1" {
		t.Errorf("got ID %q, want %q", r.ID, "room-1")
	}
	if r.SubscriptionCount() != 0 {
		t.Errorf("got %d subscriptions, want 0", r.SubscriptionCount())
	}
	if r.SourceActive() {
		t.Error("new RelaySession must not report source active")
	}
}

func TestGiven_NewRelaySession_When_Inspected_Then_Empty(t *testing.T) {
	given_new_relay_session_when_inspected_then_empty(t)
}

func TestGiven_RelaySession_When_AddSubscription_Then_CountIncreases(t *testing.T) {
	r := New("room-1")
	if err := r.AddSubscription("peer-1", BusMix, &mockSubscription{}); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	if r.SubscriptionCount() != 1 {
		t.Errorf("got %d subscriptions, want 1", r.SubscriptionCount())
	}
}

func TestGiven_RelaySession_When_MultipleSubscriptions_Then_AllCounted(t *testing.T) {
	r := New("room-1")
	for _, id := range []string{"peer-1", "peer-2", "peer-3"} {
		if err := r.AddSubscription(id, BusMix, &mockSubscription{}); err != nil {
			t.Fatalf("AddSubscription %s: %v", id, err)
		}
	}
	if r.SubscriptionCount() != 3 {
		t.Errorf("got %d subscriptions, want 3", r.SubscriptionCount())
	}
}

func TestGiven_RelaySession_When_RemoveSubscription_Then_CountDecreases(t *testing.T) {
	r := New("room-1")
	if err := r.AddSubscription("peer-1", BusMix, &mockSubscription{}); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	r.RemoveSubscription("peer-1")
	if r.SubscriptionCount() != 0 {
		t.Errorf("got %d subscriptions, want 0", r.SubscriptionCount())
	}
}

func TestGiven_RelaySession_When_RemoveUnknownSubscription_Then_Noop(t *testing.T) {
	r := New("room-1")
	r.RemoveSubscription("does-not-exist") // must not panic
}

func TestGiven_RelaySession_When_ClosedMultipleTimes_Then_Idempotent(t *testing.T) {
	r := New("room-1")
	r.Close()
	r.Close()
	r.Close() // must not panic
}

func TestGiven_RelaySession_When_SetSource_Then_SourceActive(t *testing.T) {
	r := New("room-1")
	src := newMockSource()
	r.SetSource("voice", BusRoleVoice, src, nil)
	if !r.SourceActive() {
		t.Error("expected source active after SetSource")
	}
	src.close()
}

func TestGiven_RelaySession_When_SetSource_Then_BusSourceActive(t *testing.T) {
	r := New("room-1")
	src := newMockSource()
	r.SetSource("music", BusRoleMusic, src, nil)
	if !r.BusSourceActive("music") {
		t.Error("expected bus source active after SetSource on music bus")
	}
	if r.BusSourceActive("voice") {
		t.Error("voice bus should not be active when only music bus has a source")
	}
	src.close()
}

func TestGiven_ActiveBus_When_PacketPublished_Then_SubscriberReceives(t *testing.T) {
	r := New("room-1")
	src := newMockSource()
	sub := &mockSubscription{}
	if err := r.AddSubscription("peer-1", BusMix, sub); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	r.SetSource("voice", BusRoleVoice, src, nil)

	pkt := &rtp.Packet{Payload: []byte{0xDE, 0xAD, 0xBE, 0xEF}}
	src.send(pkt)

	waitFor(t, 100*time.Millisecond, func() bool { return len(sub.received()) == 1 })
	src.close()
}

func TestGiven_MultipleBuses_When_EachPublishes_Then_SubscriberReceivesAll(t *testing.T) {
	r := New("room-1")
	voiceSrc := newMockSource()
	musicSrc := newMockSource()
	sub := &mockSubscription{}
	if err := r.AddSubscription("peer-1", BusMix, sub); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	r.SetSource("voice", BusRoleVoice, voiceSrc, nil)
	r.SetSource("music", BusRoleMusic, musicSrc, nil)

	voicePkt := &rtp.Packet{Payload: []byte{0x01}}
	musicPkt := &rtp.Packet{Payload: []byte{0x02}}
	voiceSrc.send(voicePkt)
	musicSrc.send(musicPkt)

	// Subscriber receives from both buses (relay.out("mix") semantics).
	waitFor(t, 100*time.Millisecond, func() bool { return len(sub.received()) == 2 })

	voiceSrc.close()
	musicSrc.close()
}

func TestGiven_BusSpecificSubscriber_When_OtherBusPublishes_Then_NotReceived(t *testing.T) {
	r := New("room-1")
	voiceSrc := newMockSource()
	musicSrc := newMockSource()

	mixSub := &mockSubscription{}   // BusMix: must receive both buses
	voiceSub := &mockSubscription{} // "voice": must receive only voice
	musicSub := &mockSubscription{} // "music": must receive only music
	if err := r.AddSubscription("mix-peer", BusMix, mixSub); err != nil {
		t.Fatalf("AddSubscription mix: %v", err)
	}
	if err := r.AddSubscription("voice-peer", "voice", voiceSub); err != nil {
		t.Fatalf("AddSubscription voice: %v", err)
	}
	if err := r.AddSubscription("music-peer", "music", musicSub); err != nil {
		t.Fatalf("AddSubscription music: %v", err)
	}

	r.SetSource("voice", BusRoleVoice, voiceSrc, nil)
	r.SetSource("music", BusRoleMusic, musicSrc, nil)

	voicePkt := &rtp.Packet{Payload: []byte{0x01}}
	musicPkt := &rtp.Packet{Payload: []byte{0x02}}
	voiceSrc.send(voicePkt)
	musicSrc.send(musicPkt)

	// BusMix subscriber receives both buses (byte-identical to room-wide behavior).
	waitFor(t, 100*time.Millisecond, func() bool { return len(mixSub.received()) == 2 })
	// Per-bus subscribers each receive exactly their own bus.
	waitFor(t, 100*time.Millisecond, func() bool { return len(voiceSub.received()) == 1 })
	waitFor(t, 100*time.Millisecond, func() bool { return len(musicSub.received()) == 1 })

	if got := voiceSub.received()[0].Payload[0]; got != 0x01 {
		t.Errorf("voice subscriber got payload %#x, want 0x01 (must not receive music)", got)
	}
	if got := musicSub.received()[0].Payload[0]; got != 0x02 {
		t.Errorf("music subscriber got payload %#x, want 0x02 (must not receive voice)", got)
	}

	// Give any erroneous cross-bus delivery a chance to land, then assert it did not.
	time.Sleep(20 * time.Millisecond)
	if n := len(voiceSub.received()); n != 1 {
		t.Errorf("voice subscriber received %d packets, want exactly 1", n)
	}
	if n := len(musicSub.received()); n != 1 {
		t.Errorf("music subscriber received %d packets, want exactly 1", n)
	}

	voiceSrc.close()
	musicSrc.close()
}

func TestGiven_MultipleSubscribers_When_BusPublishes_Then_AllReceive(t *testing.T) {
	r := New("room-1")
	src := newMockSource()
	sub1, sub2 := &mockSubscription{}, &mockSubscription{}
	if err := r.AddSubscription("peer-1", BusMix, sub1); err != nil {
		t.Fatalf("AddSubscription peer-1: %v", err)
	}
	if err := r.AddSubscription("peer-2", BusMix, sub2); err != nil {
		t.Fatalf("AddSubscription peer-2: %v", err)
	}
	r.SetSource("voice", BusRoleVoice, src, nil)

	pkt := &rtp.Packet{Payload: []byte{0x01}}
	src.send(pkt)

	waitFor(t, 100*time.Millisecond, func() bool {
		return len(sub1.received()) == 1 && len(sub2.received()) == 1
	})
	src.close()
}

func TestGiven_Bus_When_SourceEOF_Then_SourceClears(t *testing.T) {
	r := New("room-1")
	src := newMockSource()
	r.SetSource("voice", BusRoleVoice, src, nil)
	src.close()
	waitFor(t, 100*time.Millisecond, func() bool { return !r.SourceActive() })
}

func TestGiven_Bus_When_PacketPublished_Then_CountersIncrement(t *testing.T) {
	r := New("room-1")
	src := newMockSource()
	r.SetSource("voice", BusRoleVoice, src, nil)

	payload := []byte{0xAA, 0xBB, 0xCC}
	src.send(&rtp.Packet{Payload: payload})

	b := r.GetOrCreateBus("voice", BusRoleVoice)
	waitFor(t, 100*time.Millisecond, func() bool { return b.PacketCount.Load() == 1 })
	if got := b.ByteCount.Load(); got != uint64(len(payload)) {
		t.Errorf("ByteCount = %d, want %d", got, len(payload))
	}
	src.close()
}

func TestGiven_SourcePublishing_When_SourceReconnects_Then_SubscriberReceivesAfterReconnect(t *testing.T) {
	r := New("reconnect-room")
	src1 := newMockSource()
	sub := &mockSubscription{}
	if err := r.AddSubscription("peer-1", BusMix, sub); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}

	closerCalled := false
	r.SetSource("voice", BusRoleVoice, src1, func() {
		closerCalled = true
		src1.close()
	})

	src1.send(&rtp.Packet{Payload: []byte{0x01}})
	waitFor(t, 100*time.Millisecond, func() bool { return len(sub.received()) == 1 })

	src2 := newMockSource()
	r.SetSource("voice", BusRoleVoice, src2, nil)

	waitFor(t, 100*time.Millisecond, func() bool { return closerCalled })

	src2.send(&rtp.Packet{Payload: []byte{0x02}})
	waitFor(t, 100*time.Millisecond, func() bool { return len(sub.received()) == 2 })
	src2.close()
}

func TestGiven_RTPPaddingBitSet_When_Forwarded_Then_PaddingBitCleared(t *testing.T) {
	r := New("padding-room")
	src := newMockSource()
	sub := &mockSubscription{}
	if err := r.AddSubscription("peer-1", BusMix, sub); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	r.SetSource("voice", BusRoleVoice, src, nil)

	paddedPkt := &rtp.Packet{}
	paddedPkt.Header.Padding = true
	paddedPkt.PaddingSize = 4
	paddedPkt.Payload = []byte{0xAA, 0xBB}
	src.send(paddedPkt)

	waitFor(t, 100*time.Millisecond, func() bool { return len(sub.received()) == 1 })
	received := sub.received()[0]
	if received.Header.Padding {
		t.Error("forwarded packet must not have padding bit set")
	}
	if received.PaddingSize != 0 {
		t.Errorf("forwarded PaddingSize = %d, want 0", received.PaddingSize)
	}
	src.close()
}

// --- SessionRegistry tests ---

func TestGiven_SessionRegistry_When_GetOrCreate_Then_SameRoomReturned(t *testing.T) {
	reg := NewRegistry()
	r1 := reg.GetOrCreate("r1")
	r2 := reg.GetOrCreate("r1")
	if r1 != r2 {
		t.Error("GetOrCreate returned different pointers for same ID")
	}
}

func TestGiven_SessionRegistry_When_GetOrCreateDifferentIDs_Then_DistinctRooms(t *testing.T) {
	reg := NewRegistry()
	r1 := reg.GetOrCreate("r1")
	r2 := reg.GetOrCreate("r2")
	if r1 == r2 {
		t.Error("GetOrCreate returned same pointer for different IDs")
	}
}

func TestGiven_SessionRegistry_When_GetUnknownID_Then_NotFound(t *testing.T) {
	reg := NewRegistry()
	_, ok := reg.Get("unknown")
	if ok {
		t.Error("Get returned true for a room that was never created")
	}
}

func TestGiven_SessionRegistry_When_Delete_Then_RoomRemoved(t *testing.T) {
	reg := NewRegistry()
	reg.GetOrCreate("r1")
	reg.Delete("r1")
	_, ok := reg.Get("r1")
	if ok {
		t.Error("room still present after Delete")
	}
}

func TestGiven_SessionRegistry_When_DeleteUnknown_Then_Noop(t *testing.T) {
	reg := NewRegistry()
	reg.Delete("does-not-exist") // must not panic
}

func TestGiven_SessionRegistry_When_ListPublic_Then_OnlyPublicReturned(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	pub := reg.GetOrCreate("pub-room")
	pub.Public.Store(true)
	_ = reg.GetOrCreate("priv-room")

	summaries := reg.ListPublic()
	if len(summaries) != 1 {
		t.Fatalf("ListPublic: got %d results, want 1", len(summaries))
	}
	if summaries[0].SessionID != "pub-room" {
		t.Errorf("ListPublic: got session_id %q, want %q", summaries[0].SessionID, "pub-room")
	}
}

func TestGiven_EmptySessionRegistry_When_ListPublic_Then_NonNilEmpty(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	summaries := reg.ListPublic()
	if summaries == nil {
		t.Error("ListPublic: returned nil, want non-nil empty slice")
	}
	if len(summaries) != 0 {
		t.Errorf("ListPublic: got %d results, want 0", len(summaries))
	}
}

func TestGiven_RelaySession_When_InactivityTimerExpires_Then_RoomClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("timer expiry test — skipped in -short mode")
	}
	const timeout = 50 * time.Millisecond
	r := newWithTimeout("expiry-room", timeout)
	waitFor(t, 500*time.Millisecond, func() bool {
		select {
		case <-r.done:
			return true
		default:
			return false
		}
	})
}

// --- BusRole classification tests (LAW 8) ---

func TestGiven_BusRoles_When_LatencyRank_Then_VoiceAndAgentAreLowest(t *testing.T) {
	if BusRoleVoice.LatencyRank() != 0 {
		t.Errorf("voice latency rank = %d, want 0", BusRoleVoice.LatencyRank())
	}
	if BusRoleAgentOutput.LatencyRank() != 0 {
		t.Errorf("agent_output latency rank = %d, want 0", BusRoleAgentOutput.LatencyRank())
	}
	if BusRoleMusic.LatencyRank() <= BusRoleVoice.LatencyRank() {
		t.Error("music must have higher latency rank than voice")
	}
	if BusRoleEvents.LatencyRank() <= BusRoleMusic.LatencyRank() {
		t.Error("events must have higher latency rank than music")
	}
}

// --- media watchdog (Corrected Audit §6) ---

func TestGiven_BusWithNoSource_When_StalledChecked_Then_NotStalled(t *testing.T) {
	r := New("room-wd")
	defer r.Close()
	bus := r.GetOrCreateBus("voice", BusRoleVoice)

	if bus.SourceActive() {
		t.Error("a bus with no source must not report source active")
	}
	if bus.Stalled(time.Second) {
		t.Error("a bus with no source is idle, not stalled")
	}
}

func TestGiven_ActiveSourceWithRecentRTP_When_StalledChecked_Then_NotStalled(t *testing.T) {
	r := New("room-wd")
	src := newMockSource()
	r.SetSource("voice", BusRoleVoice, src, nil)
	bus := r.GetOrCreateBus("voice", BusRoleVoice)

	src.send(&rtp.Packet{})
	waitFor(t, time.Second, func() bool { return bus.PacketCount.Load() == 1 })

	if bus.Stalled(time.Second) {
		t.Errorf("a bus forwarding RTP must not be stalled (age=%v)", bus.LastRTPAge())
	}

	r.Close()
	src.close()
}

func TestGiven_ActiveSourceButSilent_When_StalledChecked_Then_Stalled(t *testing.T) {
	r := New("room-wd")
	src := newMockSource()
	r.SetSource("voice", BusRoleVoice, src, nil) // seeds last-RTP=now, then blocks on ReadRTP
	bus := r.GetOrCreateBus("voice", BusRoleVoice)

	const threshold = 10 * time.Millisecond
	time.Sleep(5 * threshold) // let the seeded timestamp age past the threshold

	if !bus.Stalled(threshold) {
		t.Errorf("a live source silent past %v must be stalled (age=%v)", threshold, bus.LastRTPAge())
	}
	if !r.AnyBusStalled(threshold) {
		t.Error("room must report a stalled bus")
	}

	r.Close()
	src.close()
}

func TestGiven_ForwardingBus_When_HealthSnapshot_Then_ReflectsCounters(t *testing.T) {
	r := New("room-wd")
	src := newMockSource()
	r.SetSource("music", BusRoleMusic, src, nil)
	bus := r.GetOrCreateBus("music", BusRoleMusic)

	src.send(&rtp.Packet{Payload: []byte{1, 2, 3, 4}})
	waitFor(t, time.Second, func() bool { return bus.PacketCount.Load() == 1 })

	h := bus.Health(time.Second)
	if h.BusID != "music" || h.Role != "music" {
		t.Errorf("unexpected health identity: %+v", h)
	}
	if !h.SourceActive || h.Stalled {
		t.Errorf("a forwarding bus must be source-active and not stalled: %+v", h)
	}
	if h.PacketCount != 1 || h.ByteCount != 4 {
		t.Errorf("health counters wrong: %+v", h)
	}

	list := r.BusHealthList(time.Second)
	if len(list) != 1 || list[0].BusID != "music" {
		t.Errorf("BusHealthList should hold the one music bus, got %+v", list)
	}

	r.Close()
	src.close()
}

func TestGiven_RoomWithActiveSource_When_PastInactivityTimeout_Then_NotExpired(t *testing.T) {
	const timeout = 20 * time.Millisecond
	r := newWithTimeout("active-room", timeout)
	src := newMockSource()
	r.SetSource("voice", BusRoleVoice, src, nil)

	// Keep a live source attached well past several inactivity windows; a
	// WebSocket-open-but-media-aware room must stay open while media is live.
	time.Sleep(6 * timeout)

	select {
	case <-r.done:
		t.Error("a room with an active source must not expire on the inactivity timer")
	default:
	}

	r.Close()
	src.close()
}

// --- packetLogStore tests ---

func TestGiven_EmptyPacketLog_When_LastCalled_Then_EmptySlice(t *testing.T) {
	pl := newPacketLogStore()
	entries := pl.last(10)
	if entries == nil {
		t.Error("last() must return empty slice, not nil, when no entries exist")
	}
	if len(entries) != 0 {
		t.Errorf("got %d entries, want 0", len(entries))
	}
}

func TestGiven_PacketLog_When_FewerThanLimitRecorded_Then_AllReturned(t *testing.T) {
	pl := newPacketLogStore()
	pl.record(PacketLogEntry{Seq: 1, RxTsNs: 100, TxTsNs: 110})
	pl.record(PacketLogEntry{Seq: 2, RxTsNs: 120, TxTsNs: 130})

	entries := pl.last(10)
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if entries[0].Seq != 1 || entries[1].Seq != 2 {
		t.Errorf("got seqs %d,%d, want 1,2", entries[0].Seq, entries[1].Seq)
	}
}

func TestGiven_PacketLog_When_LimitRequested_Then_MostRecentReturned(t *testing.T) {
	pl := newPacketLogStore()
	for i := 0; i < 5; i++ {
		pl.record(PacketLogEntry{Seq: uint16(i), RxTsNs: int64(i * 10), TxTsNs: int64(i*10 + 1)})
	}

	entries := pl.last(3)
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	// Most recent 3: seq 2, 3, 4
	if entries[0].Seq != 2 || entries[1].Seq != 3 || entries[2].Seq != 4 {
		t.Errorf("got seqs %d,%d,%d, want 2,3,4", entries[0].Seq, entries[1].Seq, entries[2].Seq)
	}
}

func TestGiven_PacketLog_When_WindowFull_Then_OldestOverwritten(t *testing.T) {
	pl := newPacketLogStore()
	// Fill the ring completely.
	for i := 0; i < packetLogWindowSize+10; i++ {
		pl.record(PacketLogEntry{Seq: uint16(i % 65536), RxTsNs: int64(i), TxTsNs: int64(i + 1)})
	}
	entries := pl.last(5)
	if len(entries) != 5 {
		t.Fatalf("got %d entries, want 5 after ring wrap", len(entries))
	}
	// Last 5 entries should be the most recently recorded.
	last := packetLogWindowSize + 10 - 1
	for i, e := range entries {
		wantRx := int64(last - (4 - i))
		if e.RxTsNs != wantRx {
			t.Errorf("entry[%d]: got RxTsNs=%d, want %d", i, e.RxTsNs, wantRx)
		}
	}
}

// --- BusPacketLog integration tests ---

func TestGiven_RelaySession_When_BusNotFound_Then_PacketLogNil(t *testing.T) {
	r := New("room-pl-1")
	entries := r.BusPacketLog("voice", 10)
	if entries != nil {
		t.Errorf("BusPacketLog on non-existent bus must return nil, got %v", entries)
	}
}

func TestGiven_ActiveBus_When_PacketForwarded_Then_PacketLogPopulated(t *testing.T) {
	r := New("room-pl-2")
	src := newMockSource()
	sub := &mockSubscription{}
	if err := r.AddSubscription("peer-1", BusMix, sub); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	r.SetSource("voice", BusRoleVoice, src, nil)

	pkt := &rtp.Packet{}
	pkt.Header.SequenceNumber = 42
	pkt.Payload = []byte{0x01}
	src.send(pkt)

	// Wait for the subscriber to receive the packet (proves forwardLoop ran).
	waitFor(t, 100*time.Millisecond, func() bool { return len(sub.received()) == 1 })

	entries := r.BusPacketLog("voice", 10)
	if len(entries) != 1 {
		t.Fatalf("got %d packet log entries, want 1", len(entries))
	}
	if entries[0].Seq != 42 {
		t.Errorf("got seq %d, want 42", entries[0].Seq)
	}
	if entries[0].RxTsNs == 0 {
		t.Error("RxTsNs must not be zero after packet forwarded")
	}
	if entries[0].TxTsNs == 0 {
		t.Error("TxTsNs must not be zero after packet forwarded")
	}
	if entries[0].TxTsNs < entries[0].RxTsNs {
		t.Errorf("TxTsNs (%d) must be >= RxTsNs (%d)", entries[0].TxTsNs, entries[0].RxTsNs)
	}

	r.Close()
	src.close()
}
