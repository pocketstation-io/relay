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

func (s *Server) createRoom(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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
	sourceToken, err := auth.Sign(s.jwtSecret, id, auth.RoleSource, 2*time.Hour)
	if err != nil {
		http.Error(w, "token error", http.StatusInternalServerError)
		return
	}
	subscriberToken, err := auth.Sign(s.jwtSecret, id, auth.RoleSubscriber, 2*time.Hour)
	if err != nil {
		http.Error(w, "token error", http.StatusInternalServerError)
		return
	}
	joinCode, joinURL := s.issueJoinInvitation(r, id)
	response := map[string]any{
		"session_id":       id,
		"room_id":          id,
		"source_token":     sourceToken,
		"subscriber_token": subscriberToken,
		"listener_token":   subscriberToken,
		"join_code":        joinCode,
		"join_url":         joinURL,
		"qr_url":           joinURL,
		"relay_region":     os.Getenv("FLY_REGION"),
		"relay_app":        os.Getenv("FLY_APP_NAME"),
	}
	if len(s.clientICEServers) > 0 {
		response["ice_servers"] = s.clientICEServers
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Server) listChannels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.relaySessions.ListPublic())
}
