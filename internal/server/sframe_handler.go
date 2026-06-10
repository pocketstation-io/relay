package server

import (
	"encoding/base64"

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
		s.sendError("role_mismatch", "KEY_EXCHANGE requires a source token")
		return
	}
	if s.rm == nil {
		s.sendError("not_joined", "join a room before sending KEY_EXCHANGE")
		return
	}

	forward := signaling.ServerMessage{
		Type:      signaling.TypeKeyExchange,
		SFrameKey: msg.SFrameKey,
	}

	s.srv.mu.Lock()
	sessions := make([]*session, 0, len(s.srv.sessions))
	for _, sess := range s.srv.sessions {
		sessions = append(sessions, sess)
	}
	s.srv.mu.Unlock()

	// Deliver to all current listener sessions in this room.
	for _, sess := range sessions {
		if sess.id != s.id && sess.rm != nil && sess.rm.ID == s.rm.ID &&
			sess.role == auth.RoleListener {
			sess.send(forward)
		}
	}

	// Persist the key so late-joining listeners receive it immediately on
	// SUBSCRIBE (RELAY-014). Decode from base64 to raw bytes for storage;
	// re-encode on delivery so the wire format is always base64.
	if msg.SFrameKey != "" {
		keyBytes, err := base64.StdEncoding.DecodeString(msg.SFrameKey)
		if err == nil {
			s.rm.SetKey(keyBytes)
		}
	}
}
