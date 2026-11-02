package downlink

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/session"
)

// borrowedDispatchSource reuses one source-owned packet only after the actual
// RelaySession dispatch has returned and requested the next packet.
type borrowedDispatchSource struct {
	packet     *rtp.Packet
	dispatched chan struct{}
	closed     chan struct{}
	readCount  int
}

func (source *borrowedDispatchSource) ReadRTP() (*rtp.Packet, error) {
	source.readCount++
	if source.readCount == 1 {
		return source.packet, nil
	}
	close(source.dispatched)
	<-source.closed
	return nil, io.EOF
}

// delayedOwnershipWriter is a test-only transport boundary. The first write
// blocks the real pacer worker so source reuse precedes the second consumption.
type delayedOwnershipWriter struct {
	entered chan struct{}
	release chan struct{}
	written chan *rtp.Packet
	mutate  bool
}

func (writer *delayedOwnershipWriter) WriteRTP(packet *rtp.Packet) error {
	if packet.SequenceNumber == 100 {
		close(writer.entered)
		<-writer.release
		return nil
	}
	writer.written <- packet.Clone()
	if writer.mutate {
		packet.SSRC = 0
		packet.CSRC[0] = 0
		packet.GetExtension(8)[0] = 0
		packet.Payload[0] = 0
	}
	return nil
}

func TestGivenBorrowedSourcePacketWhenReusedBeforePacedConsumeThenSubscribersKeepOriginalBytes(t *testing.T) {
	wantPayload := []byte{0xf8, 0x11, 0x22, 0x33}
	wantExtension := []byte{0x41, 0x42, 0x43}
	packet := &rtp.Packet{Header: rtp.Header{
		Version: 2, Marker: true, PayloadType: 111, SequenceNumber: 101,
		Timestamp: 1960, SSRC: 0x12345678, CSRC: []uint32{0x11223344},
	}, Payload: append([]byte(nil), wantPayload...)}
	if err := packet.SetExtension(8, append([]byte(nil), wantExtension...)); err != nil {
		t.Fatal(err)
	}
	source := &borrowedDispatchSource{
		packet: packet, dispatched: make(chan struct{}), closed: make(chan struct{}),
	}
	writers := [2]*delayedOwnershipWriter{}
	subscriptions := [2]*Downlink{}
	relaySession := session.New("borrowed-payload-ownership")
	defer func() {
		for _, writer := range writers {
			if writer != nil {
				select {
				case <-writer.release:
				default:
					close(writer.release)
				}
			}
		}
		relaySession.Close()
		for _, subscription := range subscriptions {
			if subscription != nil {
				subscription.StopForwarding()
			}
		}
	}()
	for subscriberIndex := range writers {
		writer := &delayedOwnershipWriter{
			entered: make(chan struct{}), release: make(chan struct{}),
			written: make(chan *rtp.Packet, 1), mutate: subscriberIndex == 0,
		}
		writers[subscriberIndex] = writer
		subscription := NewForwardingDownlink(string(rune('a'+subscriberIndex)), writer, nil)
		subscriptions[subscriberIndex] = subscription
		// The fresh music bus's first attachment has generation one. Seed the
		// same identity so reconnect timing cannot alter the ownership assertion.
		if err := subscription.WriteRTPWithSource(&rtp.Packet{Header: rtp.Header{
			Version: 2, SequenceNumber: 100, Timestamp: 1000,
		}}, time.Time{}, false, session.SourceIdentity{BusID: "music", Generation: 1, SSRC: 0x12345678}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-writer.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("pacer writer did not enter the deterministic barrier")
		}
		if err := relaySession.AddBusSubscription(string(rune('a'+subscriberIndex)), "music", subscription); err != nil {
			t.Fatal(err)
		}
	}
	if err := relaySession.SetSource("music", session.BusRoleMusic, source, func() { close(source.closed) }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-source.dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("actual RelaySession dispatch did not return")
	}
	// The source is now entitled to reuse every borrowed field. Neither pacer
	// can consume it yet because both actual writers remain behind the barrier.
	for byteIndex := range packet.Payload {
		packet.Payload[byteIndex] = 0xee
	}
	packet.CSRC[0] = 0xeeeeeeee
	packet.GetExtension(8)[0] = 0xee
	packet.SequenceNumber = 900
	packet.Timestamp = 9000
	packet.SSRC = 0xeeeeeeee
	for _, writer := range writers {
		close(writer.release)
	}
	for subscriberIndex, writer := range writers {
		select {
		case received := <-writer.written:
			if !bytes.Equal(received.Payload, wantPayload) {
				t.Errorf("subscriber %d payload = %x, want original %x", subscriberIndex, received.Payload, wantPayload)
			}
			if received.SequenceNumber != 101 || received.Timestamp != 1960 || received.SSRC != 0x12345678 ||
				!received.Marker || received.PayloadType != 111 || received.CSRC[0] != 0x11223344 ||
				!bytes.Equal(received.GetExtension(8), wantExtension) {
				t.Errorf("subscriber %d lost original RTP/header-extension identity", subscriberIndex)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %d did not consume its queued packet", subscriberIndex)
		}
	}
	// The primary writer mutated its scratch. Both actual NACKs must still use
	// the original canonical cache bytes, including private header storage.
	for subscriberIndex, subscription := range subscriptions {
		subscription.HandleNACK(101)
		select {
		case received := <-writers[subscriberIndex].written:
			if !bytes.Equal(received.Payload, wantPayload) || received.SequenceNumber != 101 ||
				received.Timestamp != 1960 || received.SSRC != 0x12345678 ||
				received.CSRC[0] != 0x11223344 || !bytes.Equal(received.GetExtension(8), wantExtension) {
				t.Errorf("subscriber %d retransmission changed after mutable writer", subscriberIndex)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %d did not retransmit its cached packet", subscriberIndex)
		}
	}
}

func TestGivenSequenceWrapWhenUnwrappedThenExtendedOrderIsMonotonic(t *testing.T) {
	var unwrapper sequenceUnwrapper
	want := []int64{65_534, 65_535, 65_536, 65_537}
	for i, seq := range []uint16{65_534, 65_535, 0, 1} {
		if got := unwrapper.unwrap(seq); got != want[i] {
			t.Fatalf("unwrap(%d) = %d, want %d", seq, got, want[i])
		}
	}
}

func TestGivenReorderedSequenceWhenUnwrappedThenLatePacketKeepsOlderExtendedValue(t *testing.T) {
	var unwrapper sequenceUnwrapper
	_ = unwrapper.unwrap(100)
	if got := unwrapper.unwrap(102); got != 102 {
		t.Fatalf("unwrap(102) = %d, want 102", got)
	}
	if got := unwrapper.unwrap(101); got != 101 {
		t.Fatalf("unwrap(101) = %d, want 101", got)
	}
}

func TestGivenOrderedPaddingWhenReleasedThenMediaOutputSequenceIsContiguous(t *testing.T) {
	var forwarder sequenceForwarder
	first := &rtp.Packet{Header: rtp.Header{SequenceNumber: 100}}
	padding := &rtp.Packet{Header: rtp.Header{SequenceNumber: 101, Padding: true}}
	second := &rtp.Packet{Header: rtp.Header{SequenceNumber: 102}}

	if got := forwarder.release(first, 100); got != forwardMedia {
		t.Fatalf("first decision = %d, want forwardMedia", got)
	}
	if got := forwarder.release(padding, 101); got != dropPadding {
		t.Fatalf("padding decision = %d, want dropPadding", got)
	}
	if got := forwarder.release(second, 102); got != forwardMedia {
		t.Fatalf("second decision = %d, want forwardMedia", got)
	}
	if first.SequenceNumber != 100 || second.SequenceNumber != 101 {
		t.Fatalf("output sequences = %d/%d, want 100/101", first.SequenceNumber, second.SequenceNumber)
	}
}

func TestGivenPacketOlderThanReleasedFrontierWhenReleasedThenItIsDropped(t *testing.T) {
	var forwarder sequenceForwarder
	if got := forwarder.release(&rtp.Packet{}, 100); got != forwardMedia {
		t.Fatalf("first decision = %d, want forwardMedia", got)
	}
	if got := forwarder.release(&rtp.Packet{}, 99); got != dropLate {
		t.Fatalf("late decision = %d, want dropLate", got)
	}
}

func TestGivenPublisherReconnectWhenForwardTranslatedThenSequenceAndTimestampStayContinuous(t *testing.T) {
	var translator rtpForwardTranslator
	firstSource := session.SourceIdentity{BusID: "voice", Generation: 1, SSRC: 7}
	secondSource := session.SourceIdentity{BusID: "voice", Generation: 2, SSRC: 7}
	first := &rtp.Packet{Header: rtp.Header{SequenceNumber: 100, Timestamp: 1000}}
	second := &rtp.Packet{Header: rtp.Header{SequenceNumber: 101, Timestamp: 1960}}
	reconnected := &rtp.Packet{Header: rtp.Header{SequenceNumber: 5, Timestamp: 100}}

	if translator.translate(first, firstSource) {
		t.Fatal("first packet reported a timeline reset")
	}
	if translator.translate(second, firstSource) {
		t.Fatal("same source reported a timeline reset")
	}
	if !translator.translate(reconnected, secondSource) {
		t.Fatal("reconnected source did not report a timeline reset")
	}
	if reconnected.SequenceNumber != 102 || reconnected.Timestamp != 2920 {
		t.Fatalf("reconnected output = seq %d ts %d, want seq 102 ts 2920", reconnected.SequenceNumber, reconnected.Timestamp)
	}
}

func TestGivenPublisherOutageWhenForwardTranslatedThenTimestampRepresentsElapsedMediaTime(t *testing.T) {
	var translator rtpForwardTranslator
	base := time.Now()
	firstSource := session.SourceIdentity{BusID: "voice", Generation: 1, SSRC: 7}
	secondSource := session.SourceIdentity{BusID: "voice", Generation: 2, SSRC: 8}
	first := &rtp.Packet{Header: rtp.Header{SequenceNumber: 100, Timestamp: 1000}}
	reconnected := &rtp.Packet{Header: rtp.Header{SequenceNumber: 5, Timestamp: 100}}

	translator.translateAt(first, firstSource, base)
	translator.translateAt(reconnected, secondSource, base.Add(300*time.Millisecond))
	if reconnected.SequenceNumber != 101 || reconnected.Timestamp != 15_400 {
		t.Fatalf("reconnected output = seq %d ts %d, want seq 101 ts 15400", reconnected.SequenceNumber, reconnected.Timestamp)
	}
}

func TestGivenSequenceAndTimestampWrapWhenForwardTranslatedThenNaturalWrapIsPreserved(t *testing.T) {
	var translator rtpForwardTranslator
	source := session.SourceIdentity{BusID: "voice", Generation: 1, SSRC: 9}
	first := &rtp.Packet{Header: rtp.Header{SequenceNumber: 65_535, Timestamp: 0xFFFF_FF00}}
	second := &rtp.Packet{Header: rtp.Header{SequenceNumber: 0, Timestamp: 0x0000_02C0}}

	translator.translate(first, source)
	if translator.translate(second, source) {
		t.Fatal("natural wrap reported a timeline reset")
	}
	if second.SequenceNumber != 0 || second.Timestamp != 0x0000_02C0 {
		t.Fatalf("wrapped output = seq %d ts %#x, want seq 0 ts 0x2c0", second.SequenceNumber, second.Timestamp)
	}
}

func TestGivenSourcePacketGapWhenForwardTranslatedThenGapIsPreservedForRepair(t *testing.T) {
	var translator rtpForwardTranslator
	source := session.SourceIdentity{BusID: "voice", Generation: 1, SSRC: 11}
	first := &rtp.Packet{Header: rtp.Header{SequenceNumber: 100, Timestamp: 1000}}
	afterGap := &rtp.Packet{Header: rtp.Header{SequenceNumber: 102, Timestamp: 2920}}

	translator.translate(first, source)
	translator.translate(afterGap, source)
	if afterGap.SequenceNumber != 102 || afterGap.Timestamp != 2920 {
		t.Fatalf("gap output = seq %d ts %d, want source gap preserved", afterGap.SequenceNumber, afterGap.Timestamp)
	}
}

func TestGivenBusMixSourceSwitchesWhenForwardTranslatedThenOneOutputTimelineIsContinuous(t *testing.T) {
	var translator rtpForwardTranslator
	voice := session.SourceIdentity{BusID: "voice", Generation: 1, SSRC: 1}
	music := session.SourceIdentity{BusID: "music", Generation: 1, SSRC: 2}
	packets := []*rtp.Packet{
		{Header: rtp.Header{SequenceNumber: 100, Timestamp: 1000}},
		{Header: rtp.Header{SequenceNumber: 500, Timestamp: 9000}},
		{Header: rtp.Header{SequenceNumber: 101, Timestamp: 1960}},
	}

	translator.translate(packets[0], voice)
	translator.translate(packets[1], music)
	translator.translate(packets[2], voice)
	for i, packet := range packets {
		wantSequence := uint16(100 + i)
		wantTimestamp := uint32(1000 + i*960)
		if packet.SequenceNumber != wantSequence || packet.Timestamp != wantTimestamp {
			t.Fatalf("packet %d = seq %d ts %d, want seq %d ts %d", i, packet.SequenceNumber, packet.Timestamp, wantSequence, wantTimestamp)
		}
	}
}
