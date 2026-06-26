package red

import (
	"bytes"
	"testing"
)

const opusPT = 111

// TestGiven_PrimaryOnly_When_Encoded_Then_HeaderIsSingleOctet verifies a RED
// packet with no redundancy is just a 1-octet primary header (F=0, PT) followed
// by the payload — the minimal valid RED packet.
func TestGiven_PrimaryOnly_When_Encoded_Then_HeaderIsSingleOctet(t *testing.T) {
	primary := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	got, err := Encode(opusPT, primary, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	want := append([]byte{opusPT & 0x7f}, primary...)
	if !bytes.Equal(got, want) {
		t.Errorf("primary-only RED = % x, want % x", got, want)
	}
}

// TestGiven_OneRedundantBlock_When_Encoded_Then_HeaderMatchesRFC2198 verifies the
// exact 4-octet redundant header layout: F=1, 7-bit PT, 14-bit timestamp offset,
// 10-bit length.
func TestGiven_OneRedundantBlock_When_Encoded_Then_HeaderMatchesRFC2198(t *testing.T) {
	prev := []byte{0x01, 0x02, 0x03} // length 3
	primary := []byte{0xAA, 0xBB}
	const tsOff = 960 // one 20 ms Opus frame at 48 kHz

	got, err := Encode(opusPT, primary, []Block{{PayloadType: opusPT, TimestampOffset: tsOff, Payload: prev}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Redundant header: byte0 = 0x80|PT; then (tsOff<<10 | len) as 3 octets.
	v := uint32(tsOff)<<10 | uint32(len(prev))
	want := []byte{
		0x80 | (opusPT & 0x7f),
		byte(v >> 16), byte(v >> 8), byte(v),
		opusPT & 0x7f, // primary header, F=0
	}
	want = append(want, prev...)    // redundant data
	want = append(want, primary...) // primary data
	if !bytes.Equal(got, want) {
		t.Errorf("RED = % x, want % x", got, want)
	}

	// Decode the header back out to confirm the field packing.
	p, err := Parse(got)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(p.Redundant) != 1 {
		t.Fatalf("got %d redundant blocks, want 1", len(p.Redundant))
	}
	r := p.Redundant[0]
	if r.PayloadType != opusPT || r.TimestampOffset != tsOff || !bytes.Equal(r.Payload, prev) {
		t.Errorf("redundant block = %+v, want pt=%d tsOff=%d payload=% x", r, opusPT, tsOff, prev)
	}
}

// TestGiven_AnyBlocks_When_RoundTripped_Then_Preserved exercises Encode→Parse with
// 0, 1, and 2 redundant blocks and confirms every field survives.
func TestGiven_AnyBlocks_When_RoundTripped_Then_Preserved(t *testing.T) {
	primary := []byte{0x10, 0x11, 0x12, 0x13}
	cases := [][]Block{
		nil,
		{{PayloadType: opusPT, TimestampOffset: 960, Payload: []byte{0x20, 0x21}}},
		{
			{PayloadType: opusPT, TimestampOffset: 1920, Payload: []byte{0x30}},
			{PayloadType: opusPT, TimestampOffset: 960, Payload: []byte{0x40, 0x41, 0x42}},
		},
	}
	for i, redundant := range cases {
		enc, err := Encode(opusPT, primary, redundant)
		if err != nil {
			t.Fatalf("case %d Encode: %v", i, err)
		}
		p, err := Parse(enc)
		if err != nil {
			t.Fatalf("case %d Parse: %v", i, err)
		}
		if p.PrimaryType != opusPT || !bytes.Equal(p.PrimaryPayload, primary) {
			t.Errorf("case %d primary = (pt=%d,% x), want (pt=%d,% x)", i, p.PrimaryType, p.PrimaryPayload, opusPT, primary)
		}
		if len(p.Redundant) != len(redundant) {
			t.Fatalf("case %d got %d redundant, want %d", i, len(p.Redundant), len(redundant))
		}
		for j := range redundant {
			if p.Redundant[j].TimestampOffset != redundant[j].TimestampOffset ||
				!bytes.Equal(p.Redundant[j].Payload, redundant[j].Payload) {
				t.Errorf("case %d block %d = %+v, want %+v", i, j, p.Redundant[j], redundant[j])
			}
		}
	}
}

// TestGiven_InvalidInput_When_Encoded_Then_Errors covers the guarded edges.
func TestGiven_InvalidInput_When_Encoded_Then_Errors(t *testing.T) {
	if _, err := Encode(opusPT, nil, nil); err == nil {
		t.Error("empty primary must error")
	}
	big := make([]byte, maxBlockLength+1)
	if _, err := Encode(opusPT, []byte{1}, []Block{{PayloadType: opusPT, Payload: big}}); err == nil {
		t.Error("oversized redundant payload must error")
	}
	if _, err := Encode(opusPT, []byte{1}, []Block{{PayloadType: opusPT, TimestampOffset: maxTimestampOffset + 1, Payload: []byte{1}}}); err == nil {
		t.Error("oversized timestamp offset must error")
	}
}

// TestGiven_TruncatedPayload_When_Parsed_Then_Errors confirms malformed RED is
// rejected rather than panicking.
func TestGiven_TruncatedPayload_When_Parsed_Then_Errors(t *testing.T) {
	for _, b := range [][]byte{
		{},                       // empty
		{0x80, 0x00},             // redundant header truncated (needs 4)
		{0x80, 0x00, 0x00, 0x05}, // declares 5-byte redundant block, none present
	} {
		if _, err := Parse(b); err == nil {
			t.Errorf("Parse(% x) must error", b)
		}
	}
}
