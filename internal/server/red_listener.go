package server

import (
	"os"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/red"
	"github.com/pocketstation-io/relay/internal/room"
)

// redMaxTimestampOffset is the largest redundant-block timestamp offset RFC 2198
// can encode (14 bits). If the gap to the previous frame exceeds this (a long
// pause / discontinuity), we forward the current frame without redundancy rather
// than mis-encode it.
const redMaxTimestampOffset = 1<<14 - 1

// redEnabled reports whether the relay should wrap forwarded Opus in RFC 2198 RED
// on the listener-facing leg. Gated by env so the default path remains the
// verified stereo-Opus path; RED is opt-in until its loss-recovery is proven with
// an injection harness.
func redEnabled() bool {
	return os.Getenv("RELAY_ENABLE_RED") == "1"
}

// redListener is a room.Listener decorator that wraps each forwarded Opus packet
// in an RFC 2198 RED payload carrying the previous frame as redundancy, then
// writes it to the underlying audio/red track. If a packet is lost, the next
// packet's redundant copy lets the browser reconstruct it without retransmission
// (combined with Opus in-band FEC this rides out 20-50% loss).
//
// One redListener per listener: the previous-frame state is per output stream.
// room.forwardLoop stays codec-agnostic — it just calls WriteRTP; the RED framing
// lives here.
type redListener struct {
	inner  room.Listener // the underlying audio/red TrackLocalStaticRTP
	opusPT uint8         // payload type of the inner Opus blocks (the browser's Opus PT)

	prevPayload []byte // previous Opus payload, retained as the redundant block
	prevTS      uint32 // previous packet's RTP timestamp
	havePrev    bool
}

// newREDListener wraps inner so forwarded Opus is RED-encoded. opusPT is the Opus
// payload type carried inside the RED blocks (what the receiver decodes them as).
func newREDListener(inner room.Listener, opusPT uint8) *redListener {
	return &redListener{inner: inner, opusPT: opusPT}
}

// WriteRTP builds the RED payload for pkt (previous frame as redundancy + this
// frame as primary) and writes it to the underlying track with pkt's RTP header.
func (r *redListener) WriteRTP(pkt *rtp.Packet) error {
	var redundant []red.Block
	if r.havePrev {
		offset := pkt.Timestamp - r.prevTS
		if offset > 0 && offset <= redMaxTimestampOffset {
			redundant = []red.Block{{
				PayloadType:     r.opusPT,
				TimestampOffset: offset,
				Payload:         r.prevPayload,
			}}
		}
	}

	redPayload, err := red.Encode(r.opusPT, pkt.Payload, redundant)
	if err != nil {
		return err
	}

	// Retain this frame as the next packet's redundancy. Copy because pkt.Payload
	// may alias a buffer the caller reuses after WriteRTP returns.
	r.prevPayload = append(r.prevPayload[:0], pkt.Payload...)
	r.prevTS = pkt.Timestamp
	r.havePrev = true

	// Same RTP header (seq/ts/ssrc); RED payload. The underlying audio/red track
	// rewrites the payload type to the negotiated RED PT on send.
	out := *pkt
	out.Payload = redPayload
	return r.inner.WriteRTP(&out)
}
