// Package server contains the relay HTTP + WebSocket server logic.
// cmd/relay-server/main.go is a thin entrypoint that calls New and Serve.
// test/integration imports this package to spin up an in-process server.
package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	pionIce "github.com/pion/ice/v4"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/callback"
	"github.com/pocketstation-io/relay/internal/metrics"
	"github.com/pocketstation-io/relay/internal/ratelimit"
	"github.com/pocketstation-io/relay/internal/room"
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
	// JWTSecret is the HMAC-SHA256 signing key used for room tokens.
	JWTSecret []byte
	// API is an optional *webrtc.API used instead of the default global API.
	// Provide a custom API in tests (e.g. with loopback ICE) so that Pion
	// does not need real network interfaces.
	API *webrtc.API
	// MaxRooms is the maximum number of concurrently active rooms.
	// Zero means use defaultMaxRooms.
	MaxRooms int
	// MaxListenersPerRoom is the maximum number of listeners in a single room.
	// Zero means use defaultMaxListenersPerRoom.
	MaxListenersPerRoom int
	// MaxRoomsPerIPPerMinute is the maximum number of rooms a single IP may
	// create per minute. Zero means use defaultMaxRoomsPerIPPerMinute.
	// Set to -1 to disable per-IP rate limiting (tests, trusted environments).
	MaxRoomsPerIPPerMinute int
	// CallbackClient is an optional client for posting source/listener events to
	// api-server. Nil disables all outbound callbacks.
	CallbackClient *callback.Client
	// WebhookDispatcher is an optional dispatcher for posting relay lifecycle
	// events (session_started, utterance_detected, session_ended) to an
	// external HTTP endpoint. Nil disables all webhook delivery.
	WebhookDispatcher *webhook.Dispatcher
	// ICEServers replaces the default STUN-only ICE server list. When non-empty,
	// these servers are used for all PeerConnections. The createRoom response
	// also returns this list as ice_servers so clients can configure their own
	// PeerConnections with the same TURN credentials. See RELAY-023.
	// When nil or empty, the relay falls back to stun.l.google.com:19302.
	ICEServers []webrtc.ICEServer
	// ICETCPMux, when non-nil, enables ICE-TCP candidates for PeerConnections
	// created by this server. Production: SetICETCPMux(tcpMux) on port 443.
	// Tests: leave nil; the loopback ICE API handles connectivity.
	ICETCPMux pionIce.TCPMux
	// RoomConfig sets per-room inactivity timeout and source reconnect window.
	// Zero values in RoomConfig use package defaults (30 min / 60 s).
	// Read from ROOM_EXPIRY_MINUTES and SOURCE_RECONNECT_WINDOW_SEC env vars.
	RoomConfig room.ManagerConfig
	// UseTURN, when true, sets use_turn=true in ICE_RESTART messages sent to
	// the source (spec §10.4). Set to true when the relay's embedded TURN
	// server is configured (TURN_PUBLIC_IP is set, RELAY-023).
	UseTURN bool
	// NAT1To1IPs is the list of public IP addresses to announce in ICE host
	// candidates. Set to the relay's public IP when deployed behind NAT (e.g.
	// Fly.io). Without this, Pion generates private/link-local candidates that
	// remote peers cannot reach. Env: RELAY_PUBLIC_IPS (comma-separated).
	NAT1To1IPs []string
}

// Server is the top-level relay server.
type Server struct {
	rooms             *room.Manager
	jwtSecret         []byte
	api               *webrtc.API
	Metrics           *metrics.Registry
	callbackClient    *callback.Client
	webhookDispatcher *webhook.Dispatcher
	iceServers        []webrtc.ICEServer
	iceTCPMux         pionIce.TCPMux
	nat1to1IPs        []string

	// maxRooms and maxListenersPerRoom are the rate-limiting ceilings.
	// Both are set once at construction and never written again.
	maxRooms            int
	maxListenersPerRoom int

	// ipLimiter enforces per-IP room-creation rate limiting.
	// Nil when per-IP limiting is disabled (MaxRoomsPerIPPerMinute == -1).
	ipLimiter *ratelimit.IPLimiter

	// mu guards httpServer and sessions so Serve/Shutdown/signal handlers can
	// run concurrently without data races.
	mu         sync.Mutex
	httpServer *http.Server
	// sessions tracks active WebSocket sessions. Each session registers itself
	// on creation and deregisters on cleanup. Shutdown closes all tracked
	// connections so hijacked WebSocket connections are not abandoned.
	sessions map[string]*session

	// codecHintStates holds per-room debounce state for CODEC_HINT emission
	// (D13, RELAY-021). Keys are room IDs; values are *codecHintState.
	// sync.Map is safe for concurrent access from multiple listener goroutines.
	codecHintStates sync.Map

	// iceRestartStates holds per-room state for ICE restart tracking (spec §10.4).
	// Keys are room IDs; values are *iceRestartState.
	// sync.Map is safe for concurrent access from multiple listener goroutines.
	iceRestartStates sync.Map

	// useTURN is true when the relay's embedded TURN server is configured.
	// Propagated into ICE_RESTART messages so the source knows to prefer TURN
	// relay candidates on the next ICE negotiation (spec §10.4, RELAY-023).
	// Set once at construction time from Config.UseTURN.
	useTURN bool
}

// New creates a Server from cfg.
func New(cfg Config) *Server {
	maxRooms := cfg.MaxRooms
	if maxRooms <= 0 {
		maxRooms = defaultMaxRooms
	}
	maxListeners := cfg.MaxListenersPerRoom
	if maxListeners <= 0 {
		maxListeners = defaultMaxListenersPerRoom
	}

	var ipLim *ratelimit.IPLimiter
	if cfg.MaxRoomsPerIPPerMinute != -1 {
		maxPerIP := cfg.MaxRoomsPerIPPerMinute
		if maxPerIP <= 0 {
			maxPerIP = defaultMaxRoomsPerIPPerMinute
		}
		ipLim = ratelimit.New(int64(maxPerIP), time.Minute)
	}

	return &Server{
		rooms:               room.NewManagerWithConfig(cfg.RoomConfig),
		jwtSecret:           cfg.JWTSecret,
		api:                 cfg.API,
		Metrics:             metrics.New(),
		callbackClient:      cfg.CallbackClient,
		webhookDispatcher:   cfg.WebhookDispatcher,
		iceServers:          cfg.ICEServers,
		iceTCPMux:           cfg.ICETCPMux,
		nat1to1IPs:          cfg.NAT1To1IPs,
		maxRooms:            maxRooms,
		maxListenersPerRoom: maxListeners,
		ipLimiter:           ipLim,
		sessions:            make(map[string]*session),
		useTURN:             cfg.UseTURN,
	}
}

// Handler returns an http.Handler for the relay server routes.
// This is the testable surface: tests can pass the handler to httptest.NewServer.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/v1/rooms", s.createRoom)
	mux.HandleFunc("/v1/rooms/", s.roomsSubrouter)
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
func (s *Server) Serve(addr string) error {
	hs := &http.Server{
		Addr:    addr,
		Handler: s.Handler(),
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
		sess.send(shuttingDown)
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
	s.rooms.CloseAll()

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
	fwd, drop := s.rooms.PacketStats()
	s.Metrics.PacketsForwarded.Store(fwd)
	s.Metrics.PacketsDropped.Store(drop)
	s.Metrics.RoomsActive.Store(int64(s.rooms.RoomCount()))
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
	if s.rooms.RoomCount() >= s.maxRooms {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "room_limit_exceeded"})
		return
	}

	id := newID()
	s.rooms.GetOrCreate(id)
	s.Metrics.RoomsActive.Add(1)
	slog.Info("room created", "room_id", id)
	sourceToken, err := auth.Sign(s.jwtSecret, id, auth.RoleSource, 2*time.Hour)
	if err != nil {
		http.Error(w, "token error", http.StatusInternalServerError)
		return
	}
	listenerToken, err := auth.Sign(s.jwtSecret, id, auth.RoleListener, 2*time.Hour)
	if err != nil {
		http.Error(w, "token error", http.StatusInternalServerError)
		return
	}
	resp := map[string]any{
		"room_id":        id,
		"source_token":   sourceToken,
		"listener_token": listenerToken,
		"qr_url":         "/listen?room=" + id,
	}
	// Include ICE server configuration when TURN is enabled (RELAY-023).
	// Clients pass this list directly to RTCPeerConnection so they use the
	// relay's embedded TURN server without hardcoding any addresses.
	if len(s.iceServers) > 0 {
		resp["ice_servers"] = s.iceServers
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool { return true },
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

// session represents one WebSocket peer connection.
// The wmu mutex serialises WebSocket writes, which may come from the read
// loop goroutine (SDP answer, error messages) and from Pion's ICE goroutine
// (candidate notifications) concurrently.
type session struct {
	id  string
	srv *Server

	wmu  sync.Mutex
	conn *websocket.Conn

	pc         *webrtc.PeerConnection
	rm         *room.Room
	role       auth.Role
	pendingICE []string // ICE candidates received before PUBLISH/SUBSCRIBE
}

func (s *session) run() {
	// WebSocket keepalive: browsers respond automatically to server pings with
	// a pong. Without this, Fly.io's proxy and home NAT devices will silently
	// drop idle connections after roughly 1 hour.
	_ = s.conn.SetReadDeadline(time.Now().Add(wsKeepAliveTimeout))
	s.conn.SetPongHandler(func(string) error {
		return s.conn.SetReadDeadline(time.Now().Add(wsKeepAliveTimeout))
	})

	done := make(chan struct{})
	defer close(done)
	go func() {
		ticker := time.NewTicker(wsKeepAlivePingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.wmu.Lock()
				err := s.conn.WriteMessage(websocket.PingMessage, nil)
				s.wmu.Unlock()
				if err != nil {
					return
				}
			case <-done:
				return
			}
		}
	}()

	for {
		var msg signaling.ClientMessage
		if err := s.conn.ReadJSON(&msg); err != nil {
			return
		}
		// Any received message resets the read deadline.
		_ = s.conn.SetReadDeadline(time.Now().Add(wsKeepAliveTimeout))
		switch msg.Type {
		case signaling.TypePublish, signaling.TypeSubscribe:
			s.handleJoin(msg)
		case signaling.TypeIce:
			s.handleICE(msg)
		case signaling.TypeKeyExchange:
			s.handleKeyExchange(msg)
		case signaling.TypeLatencyReport:
			s.handleLatencyReport(msg)
		case signaling.TypeLeave:
			return
		default:
			s.sendError("unknown_type", "unknown message type: "+string(msg.Type))
		}
	}
}

func (s *session) cleanup() {
	if s.pc != nil {
		_ = s.pc.Close()
	}
	if s.rm != nil {
		switch s.role {
		case auth.RoleListener:
			s.rm.RemoveListener(s.id)
			s.srv.Metrics.ListenerCount.Add(-1)
			if s.srv.callbackClient != nil {
				go s.srv.callbackClient.PushListenerLeave(s.rm.ID)
			}
		case auth.RoleSource:
			// Notify api-server that the source is no longer active.
			// The room's forwardLoop will clear rm.source asynchronously;
			// we push the inactive state eagerly on session teardown.
			if s.srv.callbackClient != nil {
				go s.srv.callbackClient.PushSourceActive(s.rm.ID, false)
			}
		}
		s.srv.webhookDispatcher.Send(webhook.Event{
			Type:      webhook.EventSessionEnded,
			RoomID:    s.rm.ID,
			SessionID: s.id,
		})
	}
	slog.Info("session cleaned up", "session_id", s.id)
}

// closeConn sends a WebSocket close frame and closes the underlying connection.
// Called by Shutdown to unblock the session's read loop so the session goroutine
// can deregister and exit cleanly.
func (s *session) closeConn() {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	// Best-effort: send a close frame, then close the connection.
	_ = s.conn.WriteMessage(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutdown"),
	)
	_ = s.conn.Close()
}

// handleJoin processes a PUBLISH or SUBSCRIBE message.
// It verifies the JWT, creates a Pion PeerConnection, performs the SDP
// offer/answer exchange, and wires up ICE candidate forwarding.
func (s *session) handleJoin(msg signaling.ClientMessage) {
	if s.pc != nil {
		s.sendError("already_joined", "session has already joined a room")
		return
	}

	claims, err := auth.Verify(s.srv.jwtSecret, msg.Token)
	if err != nil {
		slog.Warn("bad token", "session_id", s.id, "error", err)
		s.sendError("bad_token", err.Error())
		return
	}

	if msg.Type == signaling.TypePublish && claims.Role != auth.RoleSource {
		s.sendError("role_mismatch", "PUBLISH requires a source token")
		return
	}
	if msg.Type == signaling.TypeSubscribe && claims.Role != auth.RoleListener {
		s.sendError("role_mismatch", "SUBSCRIBE requires a listener token")
		return
	}

	rm := s.srv.rooms.GetOrCreate(claims.RoomID)
	s.rm = rm
	s.role = claims.Role

	// Mark room public when the PUBLISH message carries "public": true.
	// Written only on the first PUBLISH; subsequent PUBLISH calls on an existing
	// room (ICE restart) do not clear a previously set public flag.
	if msg.Type == signaling.TypePublish && msg.Public {
		rm.Public = true
	}

	slog.Info("session joined", "session_id", s.id, "room_id", claims.RoomID, "role", string(claims.Role))

	pc, err := s.newPeerConnection()
	if err != nil {
		s.sendError("pc_error", "failed to create peer connection")
		return
	}
	s.pc = pc

	switch msg.Type {
	case signaling.TypePublish:
		// When the source's track arrives (after ICE connects), set it on the room.
		// Pass pc.Close as the closer so that a subsequent SetSource call (ICE
		// restart) closes this PeerConnection, causing ReadRTP to error and the
		// old forwardLoop to exit before the new one starts.
		// Notify api-server of source join in a goroutine so the audio path is
		// never blocked. Best-effort: errors are logged inside PushSourceActive.
		roomIDForCallback := claims.RoomID
		sessionIDForWebhook := s.id
		pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			rm.SetSource(&trackSource{track: track}, func() { _ = pc.Close() })
			if s.srv.callbackClient != nil {
				go s.srv.callbackClient.PushSourceActive(roomIDForCallback, true)
			}
			s.srv.webhookDispatcher.Send(webhook.Event{
				Type:      webhook.EventSessionStarted,
				RoomID:    roomIDForCallback,
				SessionID: sessionIDForWebhook,
			})
		})

	case signaling.TypeSubscribe:
		// Rate limit: reject if this room is already at listener capacity.
		if rm.ListenerCount() >= s.srv.maxListenersPerRoom {
			s.sendError("listener_limit_exceeded", "room has reached maximum listener count")
			return
		}

		audioTrack, err := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
			"audio", "pocketstation",
		)
		if err != nil {
			s.sendError("track_error", "failed to create audio track")
			return
		}
		sender, addErr := pc.AddTrack(audioTrack)
		if addErr != nil {
			s.sendError("track_error", "failed to add audio track to peer connection")
			return
		}
		if err := rm.AddListener(s.id, audioTrack); err != nil {
			s.sendError("listener_limit_exceeded", err.Error())
			return
		}
		s.srv.Metrics.ListenerCount.Add(1)

		// Late-join key delivery (RELAY-014): if a KEY_EXCHANGE was received
		// before this listener joined, forward the stored key immediately so
		// this listener can decrypt from the first packet.
		if existingKey := rm.GetKey(); existingKey != nil {
			s.send(signaling.ServerMessage{
				Type:      signaling.TypeKeyExchange,
				SFrameKey: base64.StdEncoding.EncodeToString(existingKey),
			})
		}

		// D13: read RTCP RR from this listener and forward CODEC_HINT to source (RELAY-021).
		// Also track sustained high loss and emit ICE_RESTART when needed (spec §10.4).
		hintState := s.srv.roomCodecHintState(claims.RoomID)
		restartState := s.srv.roomICERestartState(claims.RoomID)
		s.srv.startRTCPReader(sender, claims.RoomID, hintState, restartState)
	}

	offer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  msg.SDPOffer,
	}
	if err := pc.SetRemoteDescription(offer); err != nil {
		s.sendError("sdp_error", "failed to set remote description")
		return
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		s.sendError("sdp_error", "failed to create answer")
		return
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		s.sendError("sdp_error", "failed to set local description")
		return
	}

	s.send(signaling.ServerMessage{
		Type:      signaling.TypeSDPAnswer,
		SDPAnswer: answer.SDP,
	})
	s.send(signaling.ServerMessage{
		Type:          signaling.TypeRoomState,
		SourceActive:  rm.SourceActive(),
		ListenerCount: rm.ListenerCount(),
		Codec:         "opus",
	})

	// Apply any ICE candidates that arrived before this PUBLISH/SUBSCRIBE
	// was processed (browser ICE gathering can race the signaling message).
	for _, c := range s.pendingICE {
		_ = s.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: c})
	}
	s.pendingICE = nil
}

func (s *session) handleICE(msg signaling.ClientMessage) {
	if s.pc == nil {
		// PUBLISH/SUBSCRIBE has not been processed yet. Queue the candidate
		// so it can be applied once the peer connection is created.
		s.pendingICE = append(s.pendingICE, msg.Candidate)
		return
	}
	if err := s.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: msg.Candidate}); err != nil {
		s.sendError("ice_error", err.Error())
	}
}

// handleKeyExchange forwards an SFrame KEY_EXCHANGE message from the source to
// all listeners in the room (ADR-014). The relay does NOT read the key material:
// it copies SFrameKey verbatim into a ServerMessage and sends it to every
// current listener session. This preserves the SFrame guarantee that the relay
// is never in possession of plaintext audio.
//
// Invariant: only the source role may send KEY_EXCHANGE. Listener-sourced
// KEY_EXCHANGE messages are rejected with a role_mismatch error.
func (s *session) handleKeyExchange(msg signaling.ClientMessage) {
	if s.role != auth.RoleSource {
		s.sendError("role_mismatch", "KEY_EXCHANGE requires a source token")
		return
	}
	if s.rm == nil {
		s.sendError("not_joined", "join a room before sending KEY_EXCHANGE")
		return
	}

	forward := signaling.ServerMessage{
		Type:      signaling.TypeKeyExchange,
		SFrameKey: msg.SFrameKey,
	}

	s.srv.mu.Lock()
	sessions := make([]*session, 0, len(s.srv.sessions))
	for _, sess := range s.srv.sessions {
		sessions = append(sessions, sess)
	}
	s.srv.mu.Unlock()

	// Deliver to all current listener sessions in this room.
	for _, sess := range sessions {
		if sess.id != s.id && sess.rm != nil && sess.rm.ID == s.rm.ID &&
			sess.role == auth.RoleListener {
			sess.send(forward)
		}
	}

	// Persist the key so late-joining listeners receive it immediately on
	// SUBSCRIBE (ADR-014). Decode from base64 to raw bytes for storage;
	// re-encode on delivery so the wire format is always base64.
	if msg.SFrameKey != "" {
		keyBytes, err := base64.StdEncoding.DecodeString(msg.SFrameKey)
		if err == nil {
			s.rm.SetKey(keyBytes)
		}
	}
}

// handleLatencyReport processes a LATENCY_REPORT message sent by a client.
// The report is recorded into the room's rolling latency window.
// Silently discarded when the session has not joined a room or the report
// payload is absent, to protect the hot path from malformed messages.
func (s *session) handleLatencyReport(msg signaling.ClientMessage) {
	if s.rm == nil {
		s.sendError("not_joined", "join a room before sending LATENCY_REPORT")
		return
	}
	rpt := msg.LatencyReport
	if rpt == nil {
		s.sendError("bad_request", "LATENCY_REPORT requires a latency_report payload")
		return
	}
	s.rm.RecordLatency(
		rpt.CaptureMs,
		rpt.EncodeMs,
		rpt.RelayRttMs,
		rpt.JitterBufferMs,
		rpt.DecodeMs,
		rpt.PacketLossPct,
	)
	// A positive capture_ms indicates that the source is actively capturing
	// audio (i.e. an utterance is in progress). Emit utterance_detected so
	// voice-agent integrations can react without inspecting RTP.
	if rpt.CaptureMs > 0 {
		s.srv.webhookDispatcher.Send(webhook.Event{
			Type:      webhook.EventUtteranceDetected,
			RoomID:    s.rm.ID,
			SessionID: s.id,
		})
	}
}

// listChannels handles GET /v1/channels (spec §3.1, Phase 6).
// Returns a JSON array of public broadcast rooms. Returns [] when none exist.
func (s *Server) listChannels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	channels := s.rooms.ListPublic()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(channels)
}

// roomsSubrouter dispatches sub-paths under /v1/rooms/.
// Currently supports GET /v1/rooms/{id}/latency (spec §13.4).
func (s *Server) roomsSubrouter(w http.ResponseWriter, r *http.Request) {
	// Pattern: /v1/rooms/{id}/latency
	const prefix = "/v1/rooms/"
	tail := r.URL.Path[len(prefix):]
	// tail is "{id}/latency" or "{id}/..."
	slashIdx := -1
	for i, c := range tail {
		if c == '/' {
			slashIdx = i
			break
		}
	}
	if slashIdx < 0 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	roomID := tail[:slashIdx]
	subpath := tail[slashIdx+1:]

	switch subpath {
	case "latency":
		s.roomLatency(w, r, roomID)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// roomLatency handles GET /v1/rooms/{id}/latency.
// Returns the rolling P50 latency statistics for the specified room as JSON.
// Returns 404 when the room does not exist.
func (s *Server) roomLatency(w http.ResponseWriter, r *http.Request, roomID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rm, ok := s.rooms.Get(roomID)
	if !ok {
		http.Error(w, "room not found", http.StatusNotFound)
		return
	}
	stats := rm.GetLatencyStats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

// newPeerConnection creates a Pion PeerConnection and wires up ICE candidate
// forwarding to the WebSocket peer.
//
// ICE server list: uses s.srv.iceServers when set (STUN + embedded TURN per
// RELAY-023). Falls back to stun.l.google.com when no servers are configured
// (dev mode / tests).
//
// ICE-TCP mux: when s.srv.iceTCPMux is non-nil, TCP ICE candidates are added
// to the SettingEngine. Tests inject a nil mux via the loopback API path, so
// TCP binding is never attempted in CI. Production sets this from main.go.
//
// If s.srv.api is non-nil (e.g. in tests), it uses the injected API directly
// and the SettingEngine customisation for ICE-TCP is skipped (the test API
// already has loopback settings applied).
func (s *session) newPeerConnection() (*webrtc.PeerConnection, error) {
	iceServers := s.srv.iceServers
	if len(iceServers) == 0 {
		iceServers = []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		}
	}
	pcCfg := webrtc.Configuration{ICEServers: iceServers}

	var (
		pc  *webrtc.PeerConnection
		err error
	)
	if s.srv.api != nil {
		// Test path: use the injected loopback API as-is.
		pc, err = s.srv.api.NewPeerConnection(pcCfg)
	} else if s.srv.iceTCPMux != nil || len(s.srv.nat1to1IPs) > 0 {
		// Production path: build a SettingEngine when either ICE-TCP mux or
		// NAT1To1 IP overrides are configured. Both may be set independently:
		// NAT1To1IPs alone is used when RELAY_PUBLIC_IPS is set without
		// ICE_TCP_PORT (e.g. E2E tests forcing loopback candidates).
		se := webrtc.SettingEngine{}
		if s.srv.iceTCPMux != nil {
			se.SetICETCPMux(s.srv.iceTCPMux)
		}
		if len(s.srv.nat1to1IPs) > 0 {
			se.SetNAT1To1IPs(s.srv.nat1to1IPs, webrtc.ICECandidateTypeHost)
		}
		api := webrtc.NewAPI(webrtc.WithSettingEngine(se))
		pc, err = api.NewPeerConnection(pcCfg)
	} else {
		pc, err = webrtc.NewPeerConnection(pcCfg)
	}
	if err != nil {
		return nil, err
	}
	// Forward locally gathered ICE candidates to the client.
	// OnICECandidate fires from Pion's ICE goroutine, so send holds wmu.
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		s.send(signaling.ServerMessage{
			Type:      signaling.TypeIce,
			Candidate: c.ToJSON().Candidate,
		})
	})
	return pc, nil
}

func (s *session) send(msg signaling.ServerMessage) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_ = s.conn.WriteJSON(msg)
}

func (s *session) sendError(code, message string) {
	s.send(signaling.ServerMessage{Type: signaling.TypeError, Code: code, Message: message})
}

// trackSource adapts *webrtc.TrackRemote to room.Source.
// TrackRemote.ReadRTP returns (pkt, interceptor.Attributes, error); we drop
// the attributes because room.Source does not need them.
type trackSource struct {
	track *webrtc.TrackRemote
}

func (t *trackSource) ReadRTP() (*rtp.Packet, error) {
	pkt, _, err := t.track.ReadRTP()
	return pkt, err
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
