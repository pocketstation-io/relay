package session

import "time"

func (relaySession *RelaySession) checkExpiry() {
	if relaySession.isInactive() {
		relaySession.Close()
		return
	}
	relaySession.expiryMu.Lock()
	if relaySession.expiryTimer != nil {
		relaySession.expiryTimer.Reset(relaySession.inactivityTimeout)
	}
	relaySession.expiryMu.Unlock()
}

func (relaySession *RelaySession) isInactive() bool {
	if relaySession.SourceActive() || relaySession.SubscriptionCount() > 0 {
		return false
	}
	return !relaySession.hasRecentRTP(relaySession.inactivityTimeout)
}

func (relaySession *RelaySession) hasRecentRTP(window time.Duration) bool {
	relaySession.busesMu.RLock()
	defer relaySession.busesMu.RUnlock()
	for _, bus := range relaySession.buses {
		if bus.lastRTPAtNanos.Load() != 0 && bus.LastRTPAge() < window {
			return true
		}
	}
	return false
}
