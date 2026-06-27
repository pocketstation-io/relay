// White-box tests for GraphRoom lifecycle. Package graph (not graph_test) so we
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

func newMockSource() *mockSource { return &mockSource{ch: make(chan *rtp.Packet, 16)} }
func (m *mockSource) send(pkt *rtp.Packet)   { m.ch <- pkt }
func (m *mockSource) close()                 { close(m.ch) }
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

// --- GraphRoom tests ---

func given_new_graph_room_when_inspected_then_empty(t *testing.T) {
	r := New("room-1")
	if r.ID != "room-1" {
		t.Errorf("got ID %q, want %q", r.ID, "room-1")
	}
	if r.SubscriptionCount() != 0 {
		t.Errorf("got %d subscriptions, want 0", r.SubscriptionCount())
	}
	if r.SourceActive() {
		t.Error("new GraphRoom must not report source active")
	}
}

func TestGiven_NewGraphRoom_When_Inspected_Then_Empty(t *testing.T) {
	given_new_graph_room_when_inspected_then_empty(t)
}

func TestGiven_GraphRoom_When_AddSubscription_Then_CountIncreases(t *testing.T) {
	r := New("room-1")
	if err := r.AddSubscription("peer-1", &mockSubscription{}); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	if r.SubscriptionCount() != 1 {
		t.Errorf("got %d subscriptions, want 1", r.SubscriptionCount())
	}
}

func TestGiven_GraphRoom_When_MultipleSubscriptions_Then_AllCounted(t *testing.T) {
	r := New("room-1")
	for _, id := range []string{"peer-1", "peer-2", "peer-3"} {
		if err := r.AddSubscription(id, &mockSubscription{}); err != nil {
			t.Fatalf("AddSubscription %s: %v", id, err)
		}
	}
	if r.SubscriptionCount() != 3 {
		t.Errorf("got %d subscriptions, want 3", r.SubscriptionCount())
	}
}

func TestGiven_GraphRoom_When_RemoveSubscription_Then_CountDecreases(t *testing.T) {
	r := New("room-1")
	if err := r.AddSubscription("peer-1", &mockSubscription{}); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	r.RemoveSubscription("peer-1")
	if r.SubscriptionCount() != 0 {
		t.Errorf("got %d subscriptions, want 0", r.SubscriptionCount())
	}
}

func TestGiven_GraphRoom_When_RemoveUnknownSubscription_Then_Noop(t *testing.T) {
	r := New("room-1")
	r.RemoveSubscription("does-not-exist") // must not panic
}

func TestGiven_GraphRoom_When_ClosedMultipleTimes_Then_Idempotent(t *testing.T) {
	r := New("room-1")
	r.Close()
	r.Close()
	r.Close() // must not panic
}

func TestGiven_GraphRoom_When_SetSource_Then_SourceActive(t *testing.T) {
	r := New("room-1")
	src := newMockSource()
	r.SetSource("voice", BusRoleVoice, src, nil)
	if !r.SourceActive() {
		t.Error("expected source active after SetSource")
	}
	src.close()
}

func TestGiven_GraphRoom_When_SetSource_Then_BusSourceActive(t *testing.T) {
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
	if err := r.AddSubscription("peer-1", sub); err != nil {
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
	if err := r.AddSubscription("peer-1", sub); err != nil {
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

func TestGiven_MultipleSubscribers_When_BusPublishes_Then_AllReceive(t *testing.T) {
	r := New("room-1")
	src := newMockSource()
	sub1, sub2 := &mockSubscription{}, &mockSubscription{}
	if err := r.AddSubscription("peer-1", sub1); err != nil {
		t.Fatalf("AddSubscription peer-1: %v", err)
	}
	if err := r.AddSubscription("peer-2", sub2); err != nil {
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
	if err := r.AddSubscription("peer-1", sub); err != nil {
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
	if err := r.AddSubscription("peer-1", sub); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	r.SetSource("voice", BusRoleVoice, src, nil)

	paddedPkt := &rtp.Packet{}
	paddedPkt.Header.Padding = true
	paddedPkt.PaddingSize    = 4
	paddedPkt.Payload        = []byte{0xAA, 0xBB}
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

func TestGiven_GraphRoom_When_InactivityTimerExpires_Then_RoomClosed(t *testing.T) {
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
