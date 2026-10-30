package downlink

import (
	"errors"
	"time"

	"github.com/pion/rtp"
)

var errPacerPacketBounds = errors.New("downlink: RTP packet exceeds owned storage")

// One worker-owned wire copy isolates primary and NACK transport writes from
// canonical retained packets. Only this scratch needs private parser metadata.
type pacerWritePacket struct {
	packet     rtp.Packet
	wire       [maxPacerPacketBytes + 1]byte
	csrc       [15]uint32
	extensions [maxPacerHeaderExtensionsCount]rtp.Extension
}

func (scratch *pacerWritePacket) copy(packet *rtp.Packet) bool {
	if !withinPacerPacketBounds(packet) {
		return false
	}
	countBytes, err := packet.MarshalTo(scratch.wire[:maxPacerPacketBytes])
	if err != nil {
		return false
	}
	scratch.packet.CSRC = scratch.csrc[:0]
	scratch.packet.Extensions = scratch.extensions[:0]
	scratch.packet.ExtensionProfile = 0
	// Pion1.8.11 Header.Unmarshal rejects an extension ending exactly at the
	// buffer end. One owned sentinel lets it parse an empty media payload; the
	// sentinel is never part of the emitted payload or packet length.
	scratch.wire[countBytes] = 0
	headerBytes, err := scratch.packet.Header.Unmarshal(scratch.wire[:countBytes+1])
	if err != nil || headerBytes != packet.Header.MarshalSize() || headerBytes+len(packet.Payload) > countBytes {
		return false
	}
	scratch.packet.Payload = scratch.wire[headerBytes : headerBytes+len(packet.Payload)]
	scratch.packet.PaddingSize = packet.PaddingSize
	scratch.packet.Raw = nil
	return true
}

func (p *AudioCadencePacer) writeOwnedPacket(packet *rtp.Packet) error {
	if !p.writeScratch.copy(packet) {
		return errPacerPacketBounds
	}
	return p.write(&p.writeScratch.packet)
}

func (p *AudioCadencePacer) retransmit(seq uint16) {
	pkt, hit, throttled := p.retransmissionCache.load(seq, time.Now())
	if !hit {
		p.nackCacheMisses.Add(1)
		return
	}
	p.nackCacheHits.Add(1)
	if throttled {
		p.nackThrottled.Add(1)
		return
	}
	if err := p.writeOwnedPacket(pkt); err != nil {
		p.retransmitErrorCount.Add(1)
		return
	}
	p.retransmitSentCount.Add(1)
}
