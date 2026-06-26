// Package red implements the RTP RED (REDundant coding) payload format,
// RFC 2198, for Opus audio loss resilience.
//
// RED carries one or more older media payloads alongside the current one inside a
// single RTP packet. If a packet is lost, the next packet's redundant copy lets
// the receiver reconstruct it without retransmission. Combined with Opus in-band
// FEC this lets an audio stream ride out 20–50% loss cleanly — the technique
// LiveKit and Chrome (opus+red, default since M96) use. pion has no built-in RED,
// so the relay builds RED packets itself on the listener-facing leg.
//
// Wire format (RFC 2198 §3):
//
//	redundant block header (4 octets), one per redundant block:
//	 0                   1                   2                   3
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|F|   block PT  |  timestamp offset         |   block length    |
//	F=1, PT=7 bits, timestamp offset=14 bits, block length=10 bits.
//
//	primary block header (1 octet), last block:
//	|F|   block PT  |   F=0 (no further blocks).
//
//	Then the block data in order: redundant payloads first, primary payload last.
package red

import (
	"errors"
	"fmt"
)

const (
	// maxBlockLength is the largest payload a redundant block can declare: the
	// block-length field is 10 bits.
	maxBlockLength = 1<<10 - 1
	// maxTimestampOffset is the largest representable redundant-block timestamp
	// offset: the field is 14 bits.
	maxTimestampOffset = 1<<14 - 1
	// redundantHeaderLen is the size of a redundant block header in octets.
	redundantHeaderLen = 4
	// primaryHeaderLen is the size of the final (primary) block header in octets.
	primaryHeaderLen = 1
	// payloadTypeMask masks the 7-bit payload type out of a header octet.
	payloadTypeMask = 0x7f
	// fBit marks a non-final (redundant) block header.
	fBit = 0x80
)

// ErrEmptyPrimary is returned when Encode is asked to build a packet with no
// primary payload.
var ErrEmptyPrimary = errors.New("red: primary payload must not be empty")

// Block is one redundant media payload carried ahead of the primary.
type Block struct {
	// PayloadType is the media payload type (e.g. the Opus PT) of this block.
	PayloadType uint8
	// TimestampOffset is the RTP-clock distance of this redundant payload BEFORE
	// the RED packet's own timestamp (RFC 2198: "timestamp offset"). For
	// consecutive 20 ms Opus frames at 48 kHz this is a multiple of 960.
	TimestampOffset uint32
	// Payload is the redundant media payload (e.g. an earlier Opus frame).
	Payload []byte
}

// Encode builds an RFC 2198 RED payload from ordered redundant blocks (oldest
// first) plus the current primary payload and its payload type. The redundant
// slice may be empty (a RED packet that carries only the primary). The returned
// bytes are the RTP payload to send under the negotiated RED payload type.
func Encode(primaryPayloadType uint8, primary []byte, redundant []Block) ([]byte, error) {
	if len(primary) == 0 {
		return nil, ErrEmptyPrimary
	}

	size := primaryHeaderLen + len(primary)
	for i := range redundant {
		b := &redundant[i]
		if len(b.Payload) > maxBlockLength {
			return nil, fmt.Errorf("red: redundant block %d payload %d exceeds %d", i, len(b.Payload), maxBlockLength)
		}
		if b.TimestampOffset > maxTimestampOffset {
			return nil, fmt.Errorf("red: redundant block %d timestamp offset %d exceeds %d", i, b.TimestampOffset, maxTimestampOffset)
		}
		size += redundantHeaderLen + len(b.Payload)
	}

	out := make([]byte, 0, size)

	// Redundant block headers (F=1), in order.
	for i := range redundant {
		b := &redundant[i]
		out = append(out, fBit|(b.PayloadType&payloadTypeMask))
		// 14-bit timestamp offset followed by 10-bit length, packed into 3 octets.
		v := (b.TimestampOffset << 10) | uint32(len(b.Payload))
		out = append(out, byte(v>>16), byte(v>>8), byte(v))
	}

	// Primary block header (F=0).
	out = append(out, primaryPayloadType&payloadTypeMask)

	// Block data: redundant payloads (in order) then the primary payload.
	for i := range redundant {
		out = append(out, redundant[i].Payload...)
	}
	out = append(out, primary...)

	return out, nil
}

// Parsed is the result of decoding a RED payload: the ordered redundant blocks
// and the primary payload with its payload type. Used to verify Encode and by any
// receive-side handling.
type Parsed struct {
	Redundant      []Block
	PrimaryType    uint8
	PrimaryPayload []byte
}

// ErrMalformed is returned when a RED payload cannot be parsed per RFC 2198.
var ErrMalformed = errors.New("red: malformed payload")

// Parse decodes an RFC 2198 RED payload. It is the inverse of Encode and exists
// primarily so the encoder can be characterised by round-trip tests.
func Parse(payload []byte) (Parsed, error) {
	var p Parsed
	pos := 0
	// Read block headers until the final (F=0) header.
	type hdr struct {
		pt     uint8
		tsOff  uint32
		length int
	}
	var redHdrs []hdr
	for {
		if pos >= len(payload) {
			return Parsed{}, ErrMalformed
		}
		first := payload[pos]
		if first&fBit == 0 {
			// Primary header: 1 octet, no length (extends to end of packet).
			p.PrimaryType = first & payloadTypeMask
			pos++
			break
		}
		// Redundant header: 4 octets.
		if pos+redundantHeaderLen > len(payload) {
			return Parsed{}, ErrMalformed
		}
		pt := first & payloadTypeMask
		v := uint32(payload[pos+1])<<16 | uint32(payload[pos+2])<<8 | uint32(payload[pos+3])
		tsOff := v >> 10
		length := int(v & maxBlockLength)
		redHdrs = append(redHdrs, hdr{pt: pt, tsOff: tsOff, length: length})
		pos += redundantHeaderLen
	}

	// Redundant payloads follow, in header order.
	for _, h := range redHdrs {
		if pos+h.length > len(payload) {
			return Parsed{}, ErrMalformed
		}
		p.Redundant = append(p.Redundant, Block{
			PayloadType:     h.pt,
			TimestampOffset: h.tsOff,
			Payload:         payload[pos : pos+h.length],
		})
		pos += h.length
	}

	// Whatever remains is the primary payload.
	p.PrimaryPayload = payload[pos:]
	if len(p.PrimaryPayload) == 0 {
		return Parsed{}, ErrMalformed
	}
	return p, nil
}
