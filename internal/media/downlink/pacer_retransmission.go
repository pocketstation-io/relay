package downlink

import "time"

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
	if err := p.write(pkt.Clone()); err != nil {
		p.retransmitErrorCount.Add(1)
		return
	}
	p.retransmitSentCount.Add(1)
}
