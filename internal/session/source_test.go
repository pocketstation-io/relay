package session

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/media/clocklineage"
)

func TestGivenRelaySessionWhenSetSourceThenSourceActive(t *testing.T) {
	r := New("room-1")
	src := newMockSource()
	r.SetSource("voice", BusRoleVoice, src, nil)
	if !r.SourceActive() {
		t.Error("expected source active after SetSource")
	}
	src.close()
}

func TestGivenSourceReconnectWhenNewPublisherHasNoSenderReportThenOldClockIsRejected(t *testing.T) {
	r := New("session-clock-reconnect")
	base := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	oldTimeline := clocklineage.NewTimeline(1)
	oldTimeline.Observe(&rtcp.SenderReport{SSRC: 1, NTPTime: testNTP(base), RTPTime: 1000}, base)
	oldSource := &mockClockLineageSource{mockSource: newMockSource(), timeline: oldTimeline}
	r.SetSource("voice", BusRoleVoice, oldSource, oldSource.close)
	bus := r.GetOrCreateBus("voice", BusRoleVoice)
	if _, ok := bus.CaptureTime(1000, base); !ok {
		t.Fatal("old publisher clock was not available before reconnect")
	}

	newSource := &mockClockLineageSource{mockSource: newMockSource(), timeline: clocklineage.NewTimeline(2)}
	r.SetSource("voice", BusRoleVoice, newSource, newSource.close)
	if _, ok := bus.CaptureTime(1000, base); ok {
		t.Fatal("new publisher reused the previous publisher clock mapping")
	}
	newSource.close()
}

func testNTP(value time.Time) uint64 {
	const offset = uint64(2_208_988_800)
	seconds := uint64(value.Unix()) + offset
	fraction := uint64(value.Nanosecond()) * (uint64(1) << 32) / uint64(time.Second)
	return seconds<<32 | fraction
}

func TestGivenRelaySessionWhenSetSourceThenBusSourceActive(t *testing.T) {
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

func TestGivenActiveBusWhenPacketPublishedThenSubscriberReceives(t *testing.T) {
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

func TestGivenMultipleBusesWhenEachPublishesThenSubscriberReceivesAll(t *testing.T) {
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

func TestGivenBusSpecificSubscriberWhenOtherBusPublishesThenNotReceived(t *testing.T) {
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

func TestGivenMultipleSubscribersWhenBusPublishesThenAllReceive(t *testing.T) {
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

func TestGivenBusWhenSourceEOFThenSourceClears(t *testing.T) {
	r := New("room-1")
	src := newMockSource()
	r.SetSource("voice", BusRoleVoice, src, nil)
	src.close()
	waitFor(t, 100*time.Millisecond, func() bool { return !r.SourceActive() })
}

func TestGivenBusWhenPacketPublishedThenCountersIncrement(t *testing.T) {
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

func TestGivenSourcePublishingWhenSourceReconnectsThenSubscriberReceivesAfterReconnect(t *testing.T) {
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

func TestGivenValidRTPPaddingWhenForwardedThenWireRoundTripPreservesPadding(t *testing.T) {
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
	if !received.Header.Padding {
		t.Error("forwarded packet lost padding bit")
	}
	if received.PaddingSize != 4 {
		t.Errorf("forwarded PaddingSize = %d, want 4", received.PaddingSize)
	}
	wire, err := received.Marshal()
	if err != nil {
		t.Fatalf("marshal forwarded padded packet: %v", err)
	}
	var decoded rtp.Packet
	if err := decoded.Unmarshal(wire); err != nil {
		t.Fatalf("unmarshal forwarded padded packet: %v", err)
	}
	if !decoded.Padding || decoded.PaddingSize != 4 || !bytes.Equal(decoded.Payload, []byte{0xAA, 0xBB}) {
		t.Fatalf("wire round-trip corrupted padded packet: %+v", decoded)
	}
	src.close()
}

func TestGivenRelaySessionAtBusLimitWhenAnotherSourceAttachesThenItIsRejected(t *testing.T) {
	relaySession := New("bounded-buses")
	relaySession.maxBuses = 2
	first := newMockSource()
	second := newMockSource()
	third := newMockSource()
	t.Cleanup(first.close)
	t.Cleanup(second.close)
	t.Cleanup(third.close)

	if err := relaySession.SetSource("application", BusRoleMusic, first, nil); err != nil {
		t.Fatalf("attach application source: %v", err)
	}
	if err := relaySession.SetSource("microphone", BusRoleVoice, second, nil); err != nil {
		t.Fatalf("attach microphone source: %v", err)
	}
	if err := relaySession.SetSource("generated", BusRoleAgentOutput, third, nil); !errors.Is(err, ErrBusLimitExceeded) {
		t.Fatalf("third source error = %v, want %v", err, ErrBusLimitExceeded)
	}
	if got := len(relaySession.buses); got != 2 {
		t.Fatalf("bus count = %d, want 2", got)
	}
}
