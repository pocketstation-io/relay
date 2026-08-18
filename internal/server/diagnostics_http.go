package server

import (
	"net/http"
	"time"
)

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) echo(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	for {
		var message struct {
			SendTimestampNs int64 `json:"send_timestamp_ns"`
		}
		if err := conn.ReadJSON(&message); err != nil {
			return
		}
		reply := struct {
			SendTimestampNs int64 `json:"send_timestamp_ns"`
			RecvTimestampNs int64 `json:"recv_timestamp_ns"`
		}{
			SendTimestampNs: message.SendTimestampNs,
			RecvTimestampNs: time.Now().UnixNano(),
		}
		if err := conn.WriteJSON(reply); err != nil {
			return
		}
	}
}

func (s *Server) metricsHandler(w http.ResponseWriter, _ *http.Request) {
	forwarded, dropped := s.relaySessions.PacketStats()
	s.Metrics.PacketsForwarded.Store(forwarded)
	s.Metrics.PacketsDropped.Store(dropped)
	s.Metrics.RoomsActive.Store(int64(s.relaySessions.RoomCount()))
	s.Metrics.HandshakeActive.Store(s.handshakeAdmission.Active())
	if s.webhookDispatcher != nil {
		s.Metrics.WebhookErrorsTotal.Store(s.webhookDispatcher.ErrorsTotal())
		s.Metrics.WebhookDroppedTotal.Store(s.webhookDispatcher.DroppedTotal())
	}
	s.Metrics.WritePrometheus(w)
}
