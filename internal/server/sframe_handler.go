package server

import (
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// handleKeyExchange forwards an SFrame KEY_EXCHANGE message from the source to
// all subscriber sessions in the room (RELAY-014). The relay forwards SFrameKey
// verbatim without reading the key material, preserving the SFrame guarantee
// that the relay never holds plaintext audio.
//
// Invariant: only the source role may send KEY_EXCHANGE.
func (s *signalPeer) handleKeyExchange(msg signaling.ClientMessage) {
	if s.role != auth.RoleSource {
		s.sendError(signaling.ErrCodeRoleMismatch, "KEY_EXCHANGE requires a source token")
		return
	}
	if s.room == nil {
		s.sendError(signaling.ErrCodeNotJoined, "join a session before sending KEY_EXCHANGE")
		return
	}

	forward := signaling.ServerMessage{
		Type:      signaling.TypeKeyExchange,
		SFrameKey: msg.SFrameKey,
	}

	s.srv.mu.RLock()
	peers := make([]*signalPeer, 0, len(s.srv.signalPeers))
	for _, peer := range s.srv.signalPeers {
		peers = append(peers, peer)
	}
	s.srv.mu.RUnlock()

	for _, peer := range peers {
		if peer.id == s.id || peer.room == nil || peer.room.ID != s.room.ID {
			continue
		}
		if peer.role == auth.RoleSubscriber {
			_ = peer.send(forward)
		}
	}

	s.srv.Metrics.KeyExchangeTotal.Add(1)

	if msg.SFrameKey != "" {
		s.room.SetKey(msg.SFrameKey)
	}
}
