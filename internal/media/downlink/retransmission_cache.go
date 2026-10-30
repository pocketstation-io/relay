package downlink

import (
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
)

const (
	retransmissionCachePackets  = 512 // 10.24 s at the 20 ms Opus cadence
	minRetransmissionThrottle   = 20 * time.Millisecond
	maxRetransmissionThrottle   = 100 * time.Millisecond
	maxRetransmissionsPerPacket = 3
)

type retransmissionSlot struct {
	packet             atomic.Pointer[rtp.Packet]
	owned              *ownedPacerPacket // Worker-only; NACK handlers enqueue IDs.
	lastRetransmitAtNs atomic.Int64
	retransmitCount    atomic.Uint32
}

// retransmissionCache retains immutable packets already owned by a downlink.
// Stores add no allocation or lock to the forwarding path.
type retransmissionCache struct {
	slots [retransmissionCachePackets]retransmissionSlot
	rttNs atomic.Int64
}

func (c *retransmissionCache) store(pkt *rtp.Packet) {
	c.replace(pkt, nil)
}

func (c *retransmissionCache) storeOwned(owned *ownedPacerPacket) {
	c.replace(&owned.packet, owned)
}

func (c *retransmissionCache) replace(pkt *rtp.Packet, owned *ownedPacerPacket) {
	slot := &c.slots[int(pkt.SequenceNumber)%len(c.slots)]
	previous := slot.owned
	slot.owned = owned
	slot.lastRetransmitAtNs.Store(0)
	slot.retransmitCount.Store(0)
	slot.packet.Store(pkt)
	if previous != nil {
		previous.release()
	}
}

// clear runs only on the pacer worker, before workerDone is closed. No cache
// reader can retain a slot past this boundary: retransmits use worker scratch.
func (c *retransmissionCache) clear() {
	for slotIndex := range c.slots {
		slot := &c.slots[slotIndex]
		slot.packet.Store(nil)
		if slot.owned != nil {
			slot.owned.release()
			slot.owned = nil
		}
	}
}

func (c *retransmissionCache) setRTT(rtt time.Duration) {
	if rtt >= 0 {
		c.rttNs.Store(rtt.Nanoseconds())
	}
}

func (c *retransmissionCache) load(seq uint16, now time.Time) (*rtp.Packet, bool, bool) {
	slot := &c.slots[int(seq)%len(c.slots)]
	pkt := slot.packet.Load()
	if pkt == nil || pkt.SequenceNumber != seq {
		return nil, false, false
	}
	if slot.retransmitCount.Load() >= maxRetransmissionsPerPacket {
		return nil, true, true
	}

	throttle := maxRetransmissionThrottle
	if rttNs := c.rttNs.Load(); rttNs > 0 {
		throttle = min(max(2*time.Duration(rttNs), minRetransmissionThrottle), maxRetransmissionThrottle)
	}

	nowNs := now.UnixNano()
	for previousNs := slot.lastRetransmitAtNs.Load(); ; previousNs = slot.lastRetransmitAtNs.Load() {
		if previousNs != 0 && nowNs-previousNs < throttle.Nanoseconds() {
			return nil, true, true
		}
		if slot.lastRetransmitAtNs.CompareAndSwap(previousNs, nowNs) {
			slot.retransmitCount.Add(1)
			return pkt, true, false
		}
	}
}
