package session

import "time"

func (relaySession *RelaySession) PacketStats() (packets, bytes, dropped uint64) {
	relaySession.busesMu.RLock()
	defer relaySession.busesMu.RUnlock()
	for _, bus := range relaySession.buses {
		packets += bus.PacketCount.Load()
		bytes += bus.ByteCount.Load()
		dropped += bus.PacketDropCount.Load()
	}
	return packets, bytes, dropped
}

func (relaySession *RelaySession) BusHealthList(threshold time.Duration) []BusHealth {
	relaySession.busesMu.RLock()
	defer relaySession.busesMu.RUnlock()
	health := make([]BusHealth, 0, len(relaySession.buses))
	for _, bus := range relaySession.buses {
		health = append(health, bus.Health(threshold))
	}
	return health
}

func (relaySession *RelaySession) BusPacketLog(busID BusID, limit int) []PacketLogEntry {
	relaySession.busesMu.RLock()
	bus, found := relaySession.buses[busID]
	relaySession.busesMu.RUnlock()
	if !found {
		return nil
	}
	return bus.packetLog.last(limit)
}

func (relaySession *RelaySession) AnyBusStalled(threshold time.Duration) bool {
	relaySession.busesMu.RLock()
	defer relaySession.busesMu.RUnlock()
	for _, bus := range relaySession.buses {
		if bus.Stalled(threshold) {
			return true
		}
	}
	return false
}

func (relaySession *RelaySession) RecordLatency(
	captureMs,
	encodeMs,
	relayRTTMs,
	jitterBufferMs,
	decodeMs,
	packetLossPct float64,
) {
	relaySession.latency.record(latencySample{
		captureMs:      captureMs,
		encodeMs:       encodeMs,
		relayRttMs:     relayRTTMs,
		jitterBufferMs: jitterBufferMs,
		decodeMs:       decodeMs,
		packetLossPct:  packetLossPct,
	})
}

func (relaySession *RelaySession) GetLatencyStats() LatencyStats {
	return relaySession.latency.stats()
}
