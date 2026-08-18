package server

import "net/http"

func (s *Server) Handler() http.Handler {
	routes := http.NewServeMux()
	routes.HandleFunc("/healthz", s.healthz)
	routes.HandleFunc("POST /v1/sessions", s.createRoom)
	routes.HandleFunc("GET /v1/join/{code}", s.resolveJoinCode)
	routes.HandleFunc("POST /v1/sessions/{id}/invitations", s.createJoinInvitation)
	routes.HandleFunc("GET /v1/sessions/{id}/latency", s.roomLatency)
	routes.HandleFunc("GET /v1/sessions/{id}/health", s.roomHealth)
	routes.HandleFunc("GET /v1/sessions/{id}/packet-log", s.packetLogHandler)
	routes.HandleFunc("GET /v1/sessions/{id}/events", s.sessionSSE)
	routes.HandleFunc("GET /v1/sessions/{id}/media-debug", s.mediaDebug)
	routes.HandleFunc("POST /v1/sessions/{id}/whip", s.handleWHIP)
	routes.HandleFunc("POST /v1/sessions/{id}/whep", s.handleWHEP)
	routes.HandleFunc("PATCH /v1/connections/{connID}", s.handleWHIPICE)
	routes.HandleFunc("DELETE /v1/connections/{connID}", s.handleWHIPDelete)

	routes.HandleFunc("/v1/rooms", s.createRoom)
	routes.HandleFunc("GET /v1/rooms/{id}/latency", s.roomLatency)
	routes.HandleFunc("/v1/channels", s.listChannels)
	routes.HandleFunc("/v1/signal", s.signal)
	routes.HandleFunc("/v1/echo", s.echo)
	routes.HandleFunc("/metrics", s.metricsHandler)
	return routes
}
