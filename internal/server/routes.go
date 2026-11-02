package server

import (
	"net/http"
	"strings"
)

func (s *Server) Handler() http.Handler {
	s.startControlStateSync()
	routes := http.NewServeMux()
	routes.HandleFunc("/healthz", s.healthz)
	routes.HandleFunc("GET /.well-known/pocketstation", s.serviceDiscovery)
	if s.authorityMode == "standalone" && s.accessService != nil {
		config := s.accessHandlerConfig
		config.RelayWHIPBaseURL = s.publicRelayURL
		config.PublicControlURL = s.publicRelayURL
		config.PublicReceiverURL = s.publicReceiverURL
		config.RelaySignalURL = strings.Replace(strings.Replace(s.publicRelayURL, "https://", "wss://", 1), "http://", "ws://", 1) + "/v1/signal"
		s.accessService.Register(routes, config)
	} else {
		for _, route := range []string{"GET /v1/join/{locator}", "POST /v1/join/{locator}", "POST /v1/join"} {
			routes.HandleFunc(route, func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, `{"error":"managed_session_service_required"}`, http.StatusConflict)
			})
		}
		routes.HandleFunc("POST /v1/sessions/{id}/invitations", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":"managed_session_service_required"}`, http.StatusConflict)
		})
		routes.HandleFunc("POST /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":"managed_session_service_required"}`, http.StatusConflict)
		})
	}
	routes.HandleFunc("GET /readyz", s.readyz)
	routes.HandleFunc("GET /v1/sessions/{id}/latency", s.roomLatency)
	routes.HandleFunc("GET /v1/sessions/{id}/health", s.roomHealth)
	routes.HandleFunc("GET /v1/sessions/{id}/packet-log", s.packetLogHandler)
	routes.HandleFunc("GET /v1/sessions/{id}/media-debug", s.mediaDebug)
	routes.HandleFunc("POST /v1/sessions/{id}/whip", s.handleWHIP)
	routes.HandleFunc("POST /v1/sessions/{id}/whep", s.handleWHEP)
	routes.HandleFunc("GET /v1/sessions/{id}/audio", s.audioIngress)
	routes.HandleFunc("PATCH /v1/connections/{connID}", s.handleWHIPICE)
	routes.HandleFunc("DELETE /v1/connections/{connID}", s.handleWHIPDelete)

	routes.HandleFunc("/v1/signal", s.signal)
	routes.HandleFunc("/v1/echo", s.echo)
	routes.HandleFunc("/metrics", s.metricsHandler)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.accessInitError != nil {
			http.Error(w, "Session service unavailable", http.StatusServiceUnavailable)
			return
		}
		setJoinCORS(w, r)
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type,Idempotency-Key")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		routes.ServeHTTP(w, r)
	})
}
