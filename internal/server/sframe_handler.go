package server

import (
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// handleKeyExchange forwards an SFrame KEY_EXCHANGE message from the source to
// all listeners in the room (RELAY-014). The relay does NOT read the key material:
// it copies SFrameKey verbatim into a ServerMessage and sends it to every
// current listener session. This preserves the SFrame guarantee that the relay
// is never in possession of plaintext audio.
//
// Invariant: only the source role may send KEY_EXCHANGE. Listener-sourced
// KEY_EXCHANGE messages are rejected with a role_mismatch error.
func (s *session) handleKeyExchange(msg signaling.ClientMessage) {
	if s.role != auth.RoleSource {
		s.sendError(signaling.ErrCodeRoleMismatch, "KEY_EXCHANGE requires a source token")
		return
	}
	if s.rm == nil {
		s.sendError(signaling.ErrCodeNotJoined, "join a room before sending KEY_EXCHANGE")
		return
	}

	forward := signaling.ServerMessage{
		Type:      signaling.TypeKeyExchange,
		SFrameKey: msg.SFrameKey,
	}

	// TODO(Phase 3, RELAY-014): Room should own its listener session map; see handleKeyExchange O(N) note.
	s.srv.mu.RLock()
	sessions := make([]*session, 0, len(s.srv.sessions))
	for _, sess := range s.srv.sessions {
		sessions = append(sessions, sess)
	}
	s.srv.mu.RUnlock()

	// Deliver to all current listener sessions in this room.
	for _, sess := range sessions {
		if sess.id != s.id && sess.rm != nil && sess.rm.ID == s.rm.ID &&
			sess.role == auth.RoleListener {
			_ = sess.send(forward)
		}
	}

	// Count the successful forward regardless of individual send errors; the
	// metric tracks KEY_EXCHANGE dispatch attempts, not per-listener delivery.
	s.srv.Metrics.KeyExchangeTotal.Add(1)

	// Persist the key so late-joining listeners receive it immediately on
	// SUBSCRIBE (RELAY-014). Store the opaque base64 string as-is; the relay
	// never decodes it to raw key material so it never holds plaintext bytes.
	if msg.SFrameKey != "" {
		s.rm.SetKey(msg.SFrameKey)
	}
}
