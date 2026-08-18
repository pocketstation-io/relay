package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/pocketstation-io/relay/internal/session"
)

const packetLogMaxLimit = 1000
const sseKeepaliveInterval = 20 * time.Second

func (s *Server) roomLatency(w http.ResponseWriter, r *http.Request) {
	relaySession, found := s.relaySessions.Get(r.PathValue("id"))
	if !found {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(relaySession.GetLatencyStats())
}

func (s *Server) roomHealth(w http.ResponseWriter, r *http.Request) {
	relaySession, found := s.relaySessions.Get(r.PathValue("id"))
	if !found {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	threshold := time.Duration(session.DefaultMediaStallThresholdMs) * time.Millisecond
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(relaySession.BusHealthList(threshold))
}

func (s *Server) mediaDebug(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	relaySession, found := s.relaySessions.Get(sessionID)
	if !found {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	snapshot := struct {
		SessionID                  string                         `json:"session_id"`
		SourceClocks               []session.SourceClockSnapshot  `json:"source_clocks"`
		Downlinks                  []session.SubscriptionSnapshot `json:"downlinks"`
		SubscriptionEvictionsTotal uint64                         `json:"subscription_evictions_total"`
	}{
		SessionID:                  sessionID,
		SourceClocks:               relaySession.SourceClockSnapshots(),
		Downlinks:                  relaySession.SubscriptionSnapshots(),
		SubscriptionEvictionsTotal: relaySession.SubscriptionEvictionsTotal(),
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snapshot)
}

func (s *Server) packetLogHandler(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	relaySession, found := s.relaySessions.Get(sessionID)
	if !found {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	busID := r.URL.Query().Get("bus")
	if busID == "" {
		http.Error(w, "bus query parameter required", http.StatusBadRequest)
		return
	}
	limit := 100
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
			return
		}
		if parsed > packetLogMaxLimit {
			parsed = packetLogMaxLimit
		}
		limit = parsed
	}
	entries := relaySession.BusPacketLog(busID, limit)
	if entries == nil {
		http.Error(w, "bus not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(entries)
}

func (s *Server) sessionSSE(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	relaySession, found := s.relaySessions.Get(sessionID)
	if !found {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	flusher, supported := w.(http.Flusher)
	if !supported {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	packets, _, _ := relaySession.PacketStats()
	_, _ = fmt.Fprintf(
		w,
		"data: {\"session_id\":%q,\"source_active\":%v,\"subscription_count\":%d,\"packets_forwarded\":%d}\n\n",
		sessionID,
		relaySession.SourceActive(),
		relaySession.SubscriptionCount(),
		packets,
	)
	flusher.Flush()

	ticker := time.NewTicker(sseKeepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			_, _ = fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}
