package downlink

import "github.com/pion/rtp"

// BusSubscription is the write side of a subscriber's audio stream.
// Structurally identical to graph.BusSubscription; redeclared here so the
// downlink package has no import cycle with graph.
type BusSubscription interface {
	WriteRTP(pkt *rtp.Packet) error
}
