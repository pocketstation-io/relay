// Package server contains the relay HTTP + WebSocket server logic.
// cmd/relay-server/main.go is a thin entrypoint that calls New and Serve.
// test/integration imports this package to spin up an in-process server.
package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	pionIce "github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/callback"
	"github.com/pocketstation-io/relay/internal/graph"
	"github.com/pocketstation-io/relay/internal/metrics"
	"github.com/pocketstation-io/relay/internal/ratelimit"
	"github.com/pocketstation-io/relay/internal/signaling"
	"github.com/pocketstation-io/relay/internal/webhook"
)

// shutdownDrainTimeout is the maximum time Serve waits for in-flight HTTP
// connections to complete after receiving a shutdown signal.
const shutdownDrainTimeout = 5 * time.Second

// wsKeepAlivePingInterval is how often the relay sends a WebSocket ping to
// each connected peer. Browsers respond automatically with a pong.
// This prevents Fly.io's proxy and home NAT devices from silently dropping
// idle TCP connections after their inactivity timeout (~1 hour).
const wsKeepAlivePingInterval = 30 * time.Second

// wsKeepAliveTimeout is the maximum time the relay waits for a pong reply
// before treating the connection as dead and closing it.
const wsKeepAliveTimeout = 90 * time.Second

// defaultMaxRooms is the default room-count limit when RELAY_MAX_ROOMS is unset.
const defaultMaxRooms = 100

// defaultMaxListenersPerRoom is the default per-room listener limit when
// RELAY_MAX_LISTENERS_PER_ROOM is unset.
const defaultMaxListenersPerRoom = 50

// defaultMaxRoomsPerIPPerMinute is the default per-IP room-creation rate limit
// when MAX_ROOMS_PER_IP_PER_MINUTE is unset.
const defaultMaxRoomsPerIPPerMinute = 10

// Config holds the parameters for creating a Server.
type Config struct {
	// JWTSecret is the HMAC-SHA256 signing key used for session tokens.
	JWTSecret []byte
	// API is an optional *webrtc.API used instead of the default global API.
	// Provide a custom API in tests (e.g. with loopback ICE) so that Pion
	// does not need real network interfaces.
	API *webrtc.API
	// MaxRooms is the maximum number of concurrently active GraphRooms.
	// Zero means use defaultMaxRooms.
	MaxRooms int
	// MaxSubscribersPerRoom is the maximum number of subscribers in a single
	// GraphRoom. Zero means use defaultMaxListenersPerRoom.
	MaxSubscribersPerRoom int
	// MaxRoomsPerIPPerMinute is the maximum number of rooms a single IP may
	// create per minute. Zero means use defaultMaxRoomsPerIPPerMinute.
	// Set to -1 to disable per-IP rate limiting (tests, trusted environments).
	MaxRoomsPerIPPerMinute int
	// CallbackClient is an optional client for posting source/subscriber events
	// to api-server. Nil disables all outbound callbacks.
	CallbackClient *callback.Client
	// WebhookDispatcher is an optional dispatcher for posting relay lifecycle
	// events to an external HTTP endpoint. Nil disables all webhook delivery.
	WebhookDispatcher *webhook.Dispatcher
	// ICEServers overrides the ICE server list used by the relay's own Pion
	// PeerConnections. Falls back to stun.l.google.com:19302 when empty.
	ICEServers []webrtc.ICEServer
	// ClientICEServers is the ICE server list returned to connecting clients.
	// Separate from ICEServers to avoid the relay self-STUNing via TURN.
	ClientICEServers []webrtc.ICEServer
	// ICETCPMux, when non-nil, enables ICE-TCP candidates.
	ICETCPMux pionIce.TCPMux
	// ICEUDPMux, when non-nil, forces all UDP ICE traffic through one socket.
	ICEUDPMux pionIce.UDPMux
	// RegistryConfig sets per-room inactivity timeout and source reconnect window.
	RegistryConfig graph.RegistryConfig
	// UseTURN, when true, sets use_turn=true in ICE_RESTART messages (RELAY-023).
	UseTURN bool
	// NAT1To1IPs is the list of public IP addresses to announce in ICE host
	// candidates (Fly.io / NAT deployments). Env: RELAY_PUBLIC_IPS.
	NAT1To1IPs []string
}

// Server is the top-level relay server.
type Server struct {
	sessions_         *graph.SessionRegistry // named sessions_ to avoid collision with sessions map below
	jwtSecret         []byte
	api               *webrtc.API
	Metrics           *metrics.Registry
	callbackClient    *callback.Client
	webhookDispatcher *webhook.Dispatcher
	iceServers        []webrtc.ICEServer // relay's own Pion PeerConnections
	clientICEServers  []webrtc.ICEServer // returned to clients in createRoom
	iceTCPMux         pionIce.TCPMux
	iceUDPMux         pionIce.UDPMux
	nat1to1IPs        []string

	maxRooms              int // set once at construction
	maxSubscribersPerRoom int // set once at construction

	// ipLimiter enforces per-IP room-creation rate limiting.
	// Nil when per-IP limiting is disabled (MaxRoomsPerIPPerMinute == -1).
	ipLimiter *ratelimit.IPLimiter

	// mu guards httpServer and active WebSocket session map.
	mu         sync.RWMutex
	httpServer *http.Server
	sessions   map[string]*session // active WebSocket sessions

	// codecHintStates and iceRestartStates are keyed by GraphRoom ID.
	// sync.Map for concurrent access from multiple subscriber RTCP goroutines.
	codecHintStates  sync.Map
	iceRestartStates sync.Map

	// whipConns maps WHIP/WHEP connection IDs to their live PeerConnections.
	// Keyed by the opaque connID returned in the Location header (RFC 9725).
	// sync.Map: concurrent PATCH/DELETE from multiple HTTP goroutines.
	whipConns sync.Map

	// useTURN is set once from Config.UseTURN; propagated to ICE_RESTART msgs.
	useTURN bool
}

// New creates a Server from cfg.
func New(cfg Config) *Server {
	maxRooms := cfg.MaxRooms
	if maxRooms <= 0 {
		maxRooms = defaultMaxRooms
	}
	maxSubs := cfg.MaxSubscribersPerRoom
	if maxSubs <= 0 {
		maxSubs = defaultMaxListenersPerRoom
	}

	var ipLim *ratelimit.IPLimiter
	if cfg.MaxRoomsPerIPPerMinute != -1 {
		maxPerIP := cfg.MaxRoomsPerIPPerMinute
		if maxPerIP <= 0 {
			maxPerIP = defaultMaxRoomsPerIPPerMinute
		}
		ipLim = ratelimit.New(int64(maxPerIP), time.Minute)
	}

	// Propagate MaxSubscriptions into the RegistryConfig so each GraphRoom
	// enforces the same ceiling.
	regCfg := cfg.RegistryConfig
	regCfg.MaxSubscriptions = maxSubs

	return &Server{
		sessions_:             graph.NewRegistryWithConfig(regCfg),
		jwtSecret:             cfg.JWTSecret,
		api:                   cfg.API,
		Metrics:               metrics.New(),
		callbackClient:        cfg.CallbackClient,
		webhookDispatcher:     cfg.WebhookDispatcher,
		iceServers:            cfg.ICEServers,
		clientICEServers:      cfg.ClientICEServers,
		iceTCPMux:             cfg.ICETCPMux,
		iceUDPMux:             cfg.ICEUDPMux,
		nat1to1IPs:            cfg.NAT1To1IPs,
		maxRooms:              maxRooms,
		maxSubscribersPerRoom: maxSubs,
		ipLimiter:             ipLim,
		sessions:              make(map[string]*session),
		useTURN:               cfg.UseTURN,
	}
}

// Handler returns an http.Handler for the relay server routes.
// This is the testable surface: tests can pass the handler to httptest.NewServer.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)

	// v3.0 session endpoints (canonical)
	mux.HandleFunc("POST /v1/sessions", s.createRoom)
	mux.HandleFunc("GET /v1/sessions/{id}/latency", s.roomLatency)
	mux.HandleFunc("GET /v1/sessions/{id}/events", s.sessionSSE)

	// WHIP (RFC 9725) — HTTP-based WebRTC ingest/egress, no WebSocket needed.
	// POST body: application/sdp offer. Response: 201 + application/sdp answer.
	// ?bus=voice|music|agent_voice|events selects the named AudioBus.
	mux.HandleFunc("POST /v1/sessions/{id}/whip", s.handleWHIP)
	mux.HandleFunc("POST /v1/sessions/{id}/whep", s.handleWHEP)
	mux.HandleFunc("PATCH /v1/connections/{connID}", s.handleWHIPICE)
	mux.HandleFunc("DELETE /v1/connections/{connID}", s.handleWHIPDelete)

	// v2.3 room endpoints — backward-compat aliases
	mux.HandleFunc("/v1/rooms", s.createRoom)
	mux.HandleFunc("GET /v1/rooms/{id}/latency", s.roomLatency)

	mux.HandleFunc("/v1/channels", s.listChannels)
	mux.HandleFunc("/v1/signal", s.signal)
	mux.HandleFunc("/v1/echo", s.echo)
	mux.HandleFunc("/metrics", s.metricsHandler)
	return mux
}

// Serve starts the HTTP server on addr and blocks until it stops.
// The caller is responsible for calling Shutdown to trigger a graceful drain.
// Returns the first non-nil error from ListenAndServe (http.ErrServerClosed on
// clean shutdown).
//
// ReadHeaderTimeout is set to 5 s to mitigate Slowloris attacks. WriteTimeout
// is intentionally left unset: WebSocket connections are long-lived and would
// be terminated mid-stream by a write deadline.
func (s *Server) Serve(addr string) error {
	hs := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	s.mu.Lock()
	s.httpServer = hs
	s.mu.Unlock()

	slog.Info("relay listening", "addr", addr)
	return hs.ListenAndServe()
}

// Shutdown drains the server with a 5-second deadline, closes all active
// WebSocket sessions, and closes all rooms. It is safe to call from a signal
// handler goroutine.
//
// WebSocket connections are hijacked from the HTTP server and therefore not
// tracked by http.Server.Shutdown. We close them explicitly so peers observe
// a clean connection close rather than a silent hang.
//
// Before closing each connection, Shutdown sends a RELAY_SHUTTING_DOWN error
// frame so clients can detect the restart and reconnect promptly.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	hs := s.httpServer
	// Snapshot the live sessions to close them without holding the lock.
	toClose := make([]*session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		toClose = append(toClose, sess)
	}
	s.mu.Unlock()

	// Notify and close active WebSocket sessions so their read loops unblock
	// and can deregister cleanly.
	shuttingDown := signaling.ServerMessage{
		Type:    signaling.TypeError,
		Code:    "RELAY_SHUTTING_DOWN",
		Message: "relay restarting, reconnect",
	}
	for _, sess := range toClose {
		_ = sess.send(shuttingDown)
		sess.closeConn()
	}

	if hs != nil {
		drainCtx, cancel := context.WithTimeout(ctx, shutdownDrainTimeout)
		defer cancel()
		if err := hs.Shutdown(drainCtx); err != nil {
			return err
		}
	}
	// Close all active rooms so forwardLoops exit and peer connections are
	// cleaned up before the process exits.
	s.sessions_.CloseAll()

	// Stop the per-IP rate limiter's background goroutine.
	if s.ipLimiter != nil {
		s.ipLimiter.Stop()
	}
	return nil
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}

// echo implements the /v1/echo WebSocket endpoint (RELAY-020).
//
// Clients send JSON messages of the form {"send_timestamp_ns": <int64>}.
// The relay echoes each message back with the relay's receive timestamp added:
// {"send_timestamp_ns": <int64>, "recv_timestamp_ns": <int64>}.
//
// This is used by the capture-to-cloud benchmark CLI (cmd/benchmark) to measure
// the relay-visible portion of end-to-end latency without audio pipeline overhead.
// The endpoint requires no authentication — it is a pure timing mirror.
func (s *Server) echo(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	for {
		var msg struct {
			SendTimestampNs int64 `json:"send_timestamp_ns"`
		}
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}
		reply := struct {
			SendTimestampNs int64 `json:"send_timestamp_ns"`
			RecvTimestampNs int64 `json:"recv_timestamp_ns"`
		}{
			SendTimestampNs: msg.SendTimestampNs,
			RecvTimestampNs: time.Now().UnixNano(),
		}
		if err := conn.WriteJSON(reply); err != nil {
			return
		}
	}
}

func (s *Server) metricsHandler(w http.ResponseWriter, _ *http.Request) {
	fwd, drop := s.sessions_.PacketStats()
	s.Metrics.PacketsForwarded.Store(fwd)
	s.Metrics.PacketsDropped.Store(drop)
	s.Metrics.RoomsActive.Store(int64(s.sessions_.RoomCount()))
	if s.webhookDispatcher != nil {
		s.Metrics.WebhookErrorsTotal.Store(s.webhookDispatcher.ErrorsTotal())
	}
	s.Metrics.WritePrometheus(w)
}

func (s *Server) createRoom(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Per-IP rate limit: reject when a single IP creates rooms too rapidly.
	if s.ipLimiter != nil {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr // fall back to raw address if no port
		}
		if !s.ipLimiter.Allow(ip) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "rate_limit_exceeded"})
			return
		}
	}

	// Rate limit: reject when the room count has reached the ceiling.
	if s.sessions_.RoomCount() >= s.maxRooms {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "room_limit_exceeded"})
		return
	}

	id := newID()
	s.sessions_.GetOrCreate(id)
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
	resp := map[string]any{
		"session_id":       id,
		"room_id":          id, // v2.3 wire compat
		"source_token":     sourceToken,
		"subscriber_token": subscriberToken,
		"listener_token":   subscriberToken, // v2.3 wire compat
		"qr_url":           "/listen?room=" + id,
	}
	// Include ICE server configuration when TURN is enabled (RELAY-023).
	// Clients pass this list to RTCPeerConnection so they reach the relay's
	// embedded TURN server without hardcoding any addresses. This list differs
	// from s.iceServers (used by the relay's own Pion peers) — the relay must
	// not self-STUN via its own TURN server.
	if len(s.clientICEServers) > 0 {
		resp["ice_servers"] = s.clientICEServers
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// allowedOrigins is read once at startup from ALLOWED_ORIGINS (comma-separated).
// Empty means dev mode: all origins are accepted.
var allowedOrigins = parseOrigins(os.Getenv("ALLOWED_ORIGINS"))

// upgrader upgrades HTTP connections to WebSocket.
// In production (ALLOWED_ORIGINS set), only requests whose Origin header matches
// one of the listed origins are accepted. In dev mode (ALLOWED_ORIGINS unset),
// all origins are accepted.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		if len(allowedOrigins) == 0 {
			return true // dev mode: allow all origins
		}
		origin := r.Header.Get("Origin")
		for _, allowed := range allowedOrigins {
			if origin == allowed {
				return true
			}
		}
		return false
	},
}

// parseOrigins splits a comma-separated origin list and trims whitespace.
// Returns nil when s is empty so the upgrader can detect dev mode via len check.
func parseOrigins(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func (s *Server) signal(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	sess := &session{
		id:   newID(),
		srv:  s,
		conn: conn,
		done: make(chan struct{}),
	}
	// Register session so Shutdown can close it.
	s.mu.Lock()
	s.sessions[sess.id] = sess
	s.mu.Unlock()

	s.Metrics.SessionsTotal.Add(1)
	defer func() {
		s.mu.Lock()
		delete(s.sessions, sess.id)
		s.mu.Unlock()
		sess.cleanup()
	}()
	sess.run()
}

// listChannels handles GET /v1/channels (spec §3.1, Phase 6).
// Returns a JSON array of public GraphRooms. Returns [] when none exist.
func (s *Server) listChannels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	channels := s.sessions_.ListPublic()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(channels)
}

// roomLatency handles GET /v1/rooms/{id}/latency.
// Returns the rolling P50 latency statistics for the GraphRoom as JSON.
// Returns 404 when the session does not exist.
func (s *Server) roomLatency(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	rm, ok := s.sessions_.Get(sessionID)
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	stats := rm.GetLatencyStats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

// sseKeepaliveInterval is how often the relay sends SSE keepalive comments.
// 20 seconds prevents proxy and load-balancer idle-connection timeouts.
const sseKeepaliveInterval = 20 * time.Second

// sessionSSE streams GraphRoom presence events as Server-Sent Events.
// GET /v1/sessions/{id}/events
//
// Wire format: `data: {"source_active":bool,"subscription_count":N,"bus_id":"voice"}\n\n`
// Keepalive: `: keepalive\n\n` every sseKeepaliveInterval.
//
// The initial event carries the current room state so the client is never
// blind after connect. Subsequent events fire on source or subscriber changes
// (push model: the relay's future Phase 2 callback path triggers these).
//
// For Phase 1 the SSE stream is polling-free: the initial state is sent once
// and keepalives keep the connection alive. Phase 2 wires room-state deltas.
func (s *Server) sessionSSE(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	rm, ok := s.sessions_.Get(sessionID)
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	pkts, _, _ := rm.PacketStats()
	fmt.Fprintf(w, "data: {\"session_id\":%q,\"source_active\":%v,\"subscription_count\":%d,\"packets_forwarded\":%d}\n\n",
		sessionID, rm.SourceActive(), rm.SubscriptionCount(), pkts)
	flusher.Flush()

	ticker := time.NewTicker(sseKeepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

// newID returns a random UUID v4-formatted identifier.
// Uses crypto/rand; panics if the OS PRNG is unavailable (should never happen).
func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits RFC 4122
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
