package server

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/pocketstation-io/relay/internal/auth"
)

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.authorityMode != "standalone" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "control_plane_authority_required"})
		return
	}

	if s.ipLimiter != nil {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		if !s.ipLimiter.Allow(ip) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "rate_limit_exceeded"})
			return
		}
	}

	id := newID()
	_, _, accepted := s.relaySessions.GetOrCreateWithinLimit(id, s.maxRooms)
	if !accepted {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "room_limit_exceeded"})
		return
	}
	s.Metrics.RoomsActive.Add(1)
	slog.Info("session created", "session_id", id)
	requiredBuses := []string{"application", "microphone"}
	sourceToken, err := auth.SignSource(s.jwtSecret, auth.RelayIssuer, id, requiredBuses, 2*time.Hour)
	if err != nil {
		http.Error(w, "token error", http.StatusInternalServerError)
		return
	}
	subscriberToken, err := auth.SignSubscriber(s.subscriberJWTSecret, auth.RelayIssuer, id, "mix", 2*time.Hour)
	if err != nil {
		http.Error(w, "token error", http.StatusInternalServerError)
		return
	}
	response := map[string]any{
		"session_id":       id,
		"source_token":     sourceToken,
		"subscriber_token": subscriberToken,
		"relay_region":     os.Getenv("FLY_REGION"),
		"relay_app":        os.Getenv("FLY_APP_NAME"),
	}
	if len(s.clientICEServers) > 0 {
		response["ice_servers"] = s.clientICEServers
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}
