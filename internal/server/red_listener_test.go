package server

import (
	"bytes"
	"testing"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/media/red"
)

// captureListener records the packets written to it (the underlying audio/red track).
type captureListener struct {
	packets []*rtp.Packet
}

func (c *captureListener) WriteRTP(pkt *rtp.Packet) error {
	cp := *pkt
	cp.Payload = append([]byte(nil), pkt.Payload...)
	c.packets = append(c.packets, &cp)
	return nil
}

// TestGivenREDListenerWhenConsecutiveFramesThenSecondCarriesFirstAsRedundancy
// verifies the redListener wraps Opus in RFC 2198 RED: the first frame is sent
// primary-only; the second carries the first as a redundant block with the
// correct 20 ms (960-sample) timestamp offset. The RTP header (seq/ts/ssrc) is
// preserved so the receiver's clock is unaffected.
func TestGivenREDListenerWhenConsecutiveFramesThenSecondCarriesFirstAsRedundancy(t *testing.T) {
	const opusPT = 111
	inner := &captureListener{}
	rl := newREDListener(inner, opusPT)

	frame1 := []byte{0xA1, 0xA2, 0xA3}
	frame2 := []byte{0xB1, 0xB2}

	if err := rl.WriteRTP(&rtp.Packet{Header: rtp.Header{SequenceNumber: 1, Timestamp: 1000, SSRC: 7}, Payload: frame1}); err != nil {
		t.Fatalf("WriteRTP frame1: %v", err)
	}
	if err := rl.WriteRTP(&rtp.Packet{Header: rtp.Header{SequenceNumber: 2, Timestamp: 1960, SSRC: 7}, Payload: frame2}); err != nil {
		t.Fatalf("WriteRTP frame2: %v", err)
	}

	if len(inner.packets) != 2 {
		t.Fatalf("expected 2 packets written, got %d", len(inner.packets))
	}

	// Packet 1: primary only (no prior frame to be redundant).
	p1, err := red.Parse(inner.packets[0].Payload)
	if err != nil {
		t.Fatalf("parse packet1: %v", err)
	}
	if len(p1.Redundant) != 0 || !bytes.Equal(p1.PrimaryPayload, frame1) {
		t.Errorf("packet1 = %+v, want primary-only %x", p1, frame1)
	}

	// Packet 2: primary frame2 + frame1 as redundancy at offset 960.
	if inner.packets[1].Header.SequenceNumber != 2 || inner.packets[1].Header.Timestamp != 1960 {
		t.Errorf("packet2 RTP header not preserved: seq=%d ts=%d", inner.packets[1].Header.SequenceNumber, inner.packets[1].Header.Timestamp)
	}
	p2, err := red.Parse(inner.packets[1].Payload)
	if err != nil {
		t.Fatalf("parse packet2: %v", err)
	}
	if !bytes.Equal(p2.PrimaryPayload, frame2) {
		t.Errorf("packet2 primary = %x, want %x", p2.PrimaryPayload, frame2)
	}
	if len(p2.Redundant) != 1 {
		t.Fatalf("packet2 redundant blocks = %d, want 1", len(p2.Redundant))
	}
	r := p2.Redundant[0]
	if r.PayloadType != opusPT || r.TimestampOffsetSamples != 960 || !bytes.Equal(r.Payload, frame1) {
		t.Errorf("packet2 redundant = %+v, want pt=%d off=960 samples payload=%x", r, opusPT, frame1)
	}
}
