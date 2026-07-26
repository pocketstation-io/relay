package graph

import "sync/atomic"

// packetLogWindowSize is the number of per-packet entries retained per bus.
// At 50 pkt/s this covers 20 seconds of audio history.
const packetLogWindowSize = 1000

// PacketLogEntry holds the relay-side timestamps for one forwarded RTP packet.
// RxTsNs is stamped immediately after ReadRTP returns (ingress).
// TxTsNs is retained for artifact compatibility and stamped after fanout
// enqueue. With asynchronous downlink pacing it is not a network-write time.
type PacketLogEntry struct {
	Seq          uint16 `json:"seq"`
	RtpTsSamples uint32 `json:"rtp_ts_samples"`
	PayloadType  uint8  `json:"payload_type"`
	Ssrc         uint32 `json:"ssrc"`
	PayloadBytes uint64 `json:"payload_bytes"`
	Padding      bool   `json:"padding"`
	PaddingBytes uint8  `json:"padding_bytes"`
	RxTsNs       int64  `json:"rx_ts_ns"`
	TxTsNs       int64  `json:"tx_ts_ns"`
}

// packetLogStore accumulates PacketLogEntry values in a fixed-size ring.
// One AudioBus forwarding goroutine writes each store. Readers may snapshot it
// concurrently. Slots use a publication generation so record performs no
// allocation, locking, or blocking on the RTP hot path.
type packetLogStore struct {
	entries    [packetLogWindowSize]packetLogSlot
	writeCount atomic.Uint64
}

type packetLogSlot struct {
	generation   atomic.Uint64
	seq          atomic.Uint32
	rtpTsSamples atomic.Uint32
	payloadType  atomic.Uint32
	ssrc         atomic.Uint32
	payloadBytes atomic.Uint64
	padding      atomic.Bool
	paddingBytes atomic.Uint32
	rxTsNs       atomic.Int64
	txTsNs       atomic.Int64
}

func newPacketLogStore() *packetLogStore {
	return &packetLogStore{}
}

func (pl *packetLogStore) record(entry PacketLogEntry) {
	position := pl.writeCount.Load()
	slot := &pl.entries[position%packetLogWindowSize]
	publishedGeneration := position*2 + 2
	slot.generation.Store(publishedGeneration - 1)
	slot.seq.Store(uint32(entry.Seq))
	slot.rtpTsSamples.Store(entry.RtpTsSamples)
	slot.payloadType.Store(uint32(entry.PayloadType))
	slot.ssrc.Store(entry.Ssrc)
	slot.payloadBytes.Store(entry.PayloadBytes)
	slot.padding.Store(entry.Padding)
	slot.paddingBytes.Store(uint32(entry.PaddingBytes))
	slot.rxTsNs.Store(entry.RxTsNs)
	slot.txTsNs.Store(entry.TxTsNs)
	slot.generation.Store(publishedGeneration)
	pl.writeCount.Store(position + 1)
}

// last returns the most recent limit entries in chronological order.
// Returns an empty slice when no entries exist yet.
func (pl *packetLogStore) last(limit int) []PacketLogEntry {
	writeCount := pl.writeCount.Load()
	count := min(writeCount, uint64(packetLogWindowSize))
	if uint64(limit) > count {
		limit = int(count)
	}
	if limit == 0 {
		return []PacketLogEntry{}
	}

	entries := make([]PacketLogEntry, 0, limit)
	start := writeCount - uint64(limit)
	for position := start; position < writeCount; position++ {
		slot := &pl.entries[position%packetLogWindowSize]
		expectedGeneration := position*2 + 2
		if slot.generation.Load() != expectedGeneration {
			continue
		}
		entry := PacketLogEntry{
			Seq:          uint16(slot.seq.Load()),
			RtpTsSamples: slot.rtpTsSamples.Load(),
			PayloadType:  uint8(slot.payloadType.Load()),
			Ssrc:         slot.ssrc.Load(),
			PayloadBytes: slot.payloadBytes.Load(),
			Padding:      slot.padding.Load(),
			PaddingBytes: uint8(slot.paddingBytes.Load()),
			RxTsNs:       slot.rxTsNs.Load(),
			TxTsNs:       slot.txTsNs.Load(),
		}
		if slot.generation.Load() == expectedGeneration {
			entries = append(entries, entry)
		}
	}
	return entries
}
