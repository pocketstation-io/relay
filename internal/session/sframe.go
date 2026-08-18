package session

func (relaySession *RelaySession) SetKey(keyBase64 string) {
	relaySession.keyMu.Lock()
	defer relaySession.keyMu.Unlock()
	relaySession.currentKey = keyBase64
}

func (relaySession *RelaySession) GetKey() string {
	relaySession.keyMu.RLock()
	defer relaySession.keyMu.RUnlock()
	return relaySession.currentKey
}
