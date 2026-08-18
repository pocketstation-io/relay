package server

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pocketstation-io/relay/internal/auth"
)

const joinCodeTTL = 2 * time.Hour

type joinInvite struct {
	sessionID string
	expiresAt time.Time
}

type joinInvitationResponse struct {
	SessionID string `json:"session_id"`
	JoinCode  string `json:"join_code"`
	JoinURL   string `json:"join_url"`
}

func (s *Server) createJoinInvitation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rawToken := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	claims, err := auth.Verify(s.jwtSecret, rawToken)
	if err != nil || claims.EffectiveSessionID() != id || claims.Role != auth.RoleSource {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return
	}
	relaySession, found := s.relaySessions.Get(id)
	if !found {
		http.Error(w, `{"error":"session_not_found"}`, http.StatusNotFound)
		return
	}
	if !relaySession.SourceActive() {
		http.Error(w, `{"error":"source_not_active"}`, http.StatusConflict)
		return
	}
	joinCode, joinURL := s.issueJoinInvitation(r, id)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(joinInvitationResponse{
		SessionID: id,
		JoinCode:  joinCode,
		JoinURL:   joinURL,
	})
}

func (s *Server) issueJoinInvitation(r *http.Request, sessionID string) (string, string) {
	joinCode := newID()
	s.joinMu.Lock()
	s.joinInvites[joinCode] = joinInvite{
		sessionID: sessionID,
		expiresAt: time.Now().Add(joinCodeTTL),
	}
	s.joinMu.Unlock()

	receiverURL, err := url.Parse(s.publicReceiverURL)
	if err != nil {
		return joinCode, s.publicReceiverURL + "?join=" + url.QueryEscape(joinCode)
	}
	query := receiverURL.Query()
	query.Set("join", joinCode)
	query.Set("relay", s.publicRelayHTTPURL(r))
	receiverURL.RawQuery = query.Encode()
	return joinCode, receiverURL.String()
}

func (s *Server) publicRelayHTTPURL(r *http.Request) string {
	if s.publicRelayURL != "" {
		return s.publicRelayURL
	}
	scheme := "http"
	if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

func (s *Server) resolveJoinCode(w http.ResponseWriter, r *http.Request) {
	setJoinCORS(w, r)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")

	code := r.PathValue("code")
	now := time.Now()
	s.joinMu.Lock()
	invite, found := s.joinInvites[code]
	if found && now.After(invite.expiresAt) {
		delete(s.joinInvites, code)
		found = false
	}
	s.joinMu.Unlock()
	if !found {
		http.Error(w, `{"error":"join_not_found"}`, http.StatusNotFound)
		return
	}
	if _, active := s.relaySessions.Get(invite.sessionID); !active {
		http.Error(w, `{"error":"join_not_found"}`, http.StatusNotFound)
		return
	}

	token, err := auth.Sign(s.jwtSecret, invite.sessionID, auth.RoleSubscriber, joinCodeTTL)
	if err != nil {
		http.Error(w, `{"error":"token_error"}`, http.StatusInternalServerError)
		return
	}
	response := map[string]any{
		"session_id":       invite.sessionID,
		"subscriber_token": token,
		"signal_url":       s.publicSignalURL(r),
	}
	if len(s.clientICEServers) > 0 {
		response["ice_servers"] = s.clientICEServers
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Server) publicSignalURL(r *http.Request) string {
	base := strings.TrimRight(s.publicRelayHTTPURL(r), "/")
	base = strings.Replace(base, "https://", "wss://", 1)
	base = strings.Replace(base, "http://", "ws://", 1)
	return base + "/v1/signal"
}

func setJoinCORS(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if len(allowedOrigins) == 0 {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		return
	}
	for _, allowed := range allowedOrigins {
		if origin == allowed {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			return
		}
	}
}
