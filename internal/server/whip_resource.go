package server

import (
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/pion/webrtc/v4"
)

func (s *Server) handleWHIPICE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	connectionID := r.PathValue("connID")
	value, found := s.whipConns.Load(connectionID)
	if !found {
		http.Error(w, "connection not found", http.StatusNotFound)
		return
	}
	connection := value.(*whipConn)

	fragment, err := io.ReadAll(io.LimitReader(r.Body, 8*1024))
	if err != nil {
		http.Error(w, "failed to read ICE fragment", http.StatusBadRequest)
		return
	}
	for _, line := range strings.Split(string(fragment), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "a=candidate:"):
			candidate := strings.TrimPrefix(line, "a=")
			if err := connection.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: candidate}); err != nil {
				slog.Warn("WHIP trickle ICE candidate rejected", "conn_id", connectionID, "error", err)
			}
		case line == "a=end-of-candidates":
			_ = connection.pc.AddICECandidate(webrtc.ICECandidateInit{})
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleWHIPDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	connectionID := r.PathValue("connID")
	value, found := s.whipConns.LoadAndDelete(connectionID)
	if !found {
		http.Error(w, "connection not found", http.StatusNotFound)
		return
	}
	connection := value.(*whipConn)
	_ = connection.pc.Close()
	if connection.room != nil {
		connection.room.RemoveSubscription(connectionID)
	}
	slog.Info("WHIP connection deleted", "conn_id", connectionID)
	w.WriteHeader(http.StatusOK)
}
