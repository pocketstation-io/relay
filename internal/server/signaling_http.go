package server

import (
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/pocketstation-io/relay/internal/session"
	"github.com/pocketstation-io/relay/internal/signaling"
)

var allowedOrigins = parseOrigins(os.Getenv("ALLOWED_ORIGINS"))

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		if len(allowedOrigins) == 0 {
			return true
		}
		origin := r.Header.Get("Origin")
		// Origin is a browser security boundary. Native clients normally omit
		// it and still authenticate with a scoped capability.
		if origin == "" {
			return true
		}
		for _, allowed := range allowedOrigins {
			if origin == allowed {
				return true
			}
		}
		return false
	},
}

func parseOrigins(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	origins := make([]string, 0, len(parts))
	for _, part := range parts {
		if origin := strings.TrimSpace(part); origin != "" {
			origins = append(origins, origin)
		}
	}
	return origins
}

func (s *Server) signal(w http.ResponseWriter, r *http.Request) {
	if !s.handshakeAdmission.TryAcquire() {
		s.Metrics.HandshakeRejectedTotal.Add(1)
		http.Error(w, "relay handshake capacity exceeded", http.StatusServiceUnavailable)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.handshakeAdmission.Release()
		return
	}
	peer := &signalPeer{
		id:               newID(),
		srv:              s,
		conn:             conn,
		done:             make(chan struct{}),
		releaseHandshake: s.handshakeAdmission.Release,
	}
	s.mu.Lock()
	s.signalPeers[peer.id] = peer
	s.mu.Unlock()

	s.Metrics.SessionsTotal.Add(1)
	defer func() {
		s.mu.Lock()
		delete(s.signalPeers, peer.id)
		s.mu.Unlock()
		peer.cleanup()
	}()
	peer.run()
}

func (s *Server) broadcastSessionState(relaySession *session.RelaySession) {
	if relaySession == nil {
		return
	}
	message := signaling.ServerMessage{
		Type:              signaling.TypeSessionState,
		SourceActive:      relaySession.SourceActive(),
		SubscriptionCount: relaySession.SubscriptionCount(),
		Codec:             "opus",
		SessionID:         relaySession.ID,
	}

	s.mu.RLock()
	targets := make([]*signalPeer, 0, len(s.signalPeers))
	for _, peer := range s.signalPeers {
		if peer.room == relaySession {
			targets = append(targets, peer)
		}
	}
	s.mu.RUnlock()

	for _, peer := range targets {
		_ = peer.send(message)
	}
}
