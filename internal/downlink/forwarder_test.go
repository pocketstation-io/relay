package downlink

import (
	"testing"
	"time"

	"github.com/pion/rtp"
)

func TestGivenSequenceWrap_WhenUnwrapped_ThenExtendedOrderIsMonotonic(t *testing.T) {
	var unwrapper sequenceUnwrapper
	want := []int64{65_534, 65_535, 65_536, 65_537}
	for i, seq := range []uint16{65_534, 65_535, 0, 1} {
		if got := unwrapper.unwrap(seq); got != want[i] {
			t.Fatalf("unwrap(%d) = %d, want %d", seq, got, want[i])
		}
	}
}

func TestGivenReorderedSequence_WhenUnwrapped_ThenLatePacketKeepsOlderExtendedValue(t *testing.T) {
	var unwrapper sequenceUnwrapper
	_ = unwrapper.unwrap(100)
	if got := unwrapper.unwrap(102); got != 102 {
		t.Fatalf("unwrap(102) = %d, want 102", got)
	}
	if got := unwrapper.unwrap(101); got != 101 {
		t.Fatalf("unwrap(101) = %d, want 101", got)
	}
}

func TestGivenOrderedPadding_WhenReleased_ThenMediaOutputSequenceIsContiguous(t *testing.T) {
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

func TestGivenPacketOlderThanReleasedFrontier_WhenReleased_ThenItIsDropped(t *testing.T) {
	var forwarder sequenceForwarder
	if got := forwarder.release(&rtp.Packet{}, 100); got != forwardMedia {
		t.Fatalf("first decision = %d, want forwardMedia", got)
	}
	if got := forwarder.release(&rtp.Packet{}, 99); got != dropLate {
		t.Fatalf("late decision = %d, want dropLate", got)
	}
}

func TestGivenPublisherReconnect_WhenForwardTranslated_ThenSequenceAndTimestampStayContinuous(t *testing.T) {
	var translator rtpForwardTranslator
	firstSource := SourceIdentity{BusID: "voice", Generation: 1, SSRC: 7}
	secondSource := SourceIdentity{BusID: "voice", Generation: 2, SSRC: 7}
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

func TestGivenPublisherOutage_WhenForwardTranslated_ThenTimestampRepresentsElapsedMediaTime(t *testing.T) {
	var translator rtpForwardTranslator
	base := time.Now()
	firstSource := SourceIdentity{BusID: "voice", Generation: 1, SSRC: 7}
	secondSource := SourceIdentity{BusID: "voice", Generation: 2, SSRC: 8}
	first := &rtp.Packet{Header: rtp.Header{SequenceNumber: 100, Timestamp: 1000}}
	reconnected := &rtp.Packet{Header: rtp.Header{SequenceNumber: 5, Timestamp: 100}}

	translator.translateAt(first, firstSource, base)
	translator.translateAt(reconnected, secondSource, base.Add(300*time.Millisecond))
	if reconnected.SequenceNumber != 101 || reconnected.Timestamp != 15_400 {
		t.Fatalf("reconnected output = seq %d ts %d, want seq 101 ts 15400", reconnected.SequenceNumber, reconnected.Timestamp)
	}
}

func TestGivenSequenceAndTimestampWrap_WhenForwardTranslated_ThenNaturalWrapIsPreserved(t *testing.T) {
	var translator rtpForwardTranslator
	source := SourceIdentity{BusID: "voice", Generation: 1, SSRC: 9}
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

func TestGivenSourcePacketGap_WhenForwardTranslated_ThenGapIsPreservedForRepair(t *testing.T) {
	var translator rtpForwardTranslator
	source := SourceIdentity{BusID: "voice", Generation: 1, SSRC: 11}
	first := &rtp.Packet{Header: rtp.Header{SequenceNumber: 100, Timestamp: 1000}}
	afterGap := &rtp.Packet{Header: rtp.Header{SequenceNumber: 102, Timestamp: 2920}}

	translator.translate(first, source)
	translator.translate(afterGap, source)
	if afterGap.SequenceNumber != 102 || afterGap.Timestamp != 2920 {
		t.Fatalf("gap output = seq %d ts %d, want source gap preserved", afterGap.SequenceNumber, afterGap.Timestamp)
	}
}

func TestGivenBusMixSourceSwitches_WhenForwardTranslated_ThenOneOutputTimelineIsContinuous(t *testing.T) {
	var translator rtpForwardTranslator
	voice := SourceIdentity{BusID: "voice", Generation: 1, SSRC: 1}
	music := SourceIdentity{BusID: "music", Generation: 1, SSRC: 2}
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
