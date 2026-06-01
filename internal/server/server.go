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
	"github.com/pocketstation-io/relay/internal/room"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// shutdownDrainTimeout is the maximum time Serve waits for in-flight HTTP
// connections to complete after receiving a shutdown signal.
const shutdownDrainTimeout = 5 * time.Second

// defaultMaxRooms is the default room-count limit when RELAY_MAX_ROOMS is unset.
const defaultMaxRooms = 100

// defaultMaxListenersPerRoom is the default per-room listener limit when
// RELAY_MAX_LISTENERS_PER_ROOM is unset.
const defaultMaxListenersPerRoom = 50

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
	// CallbackClient is an optional client for posting source/listener events to
	// api-server. Nil disables all outbound callbacks.
	CallbackClient *callback.Client
	// ICEServers replaces the default STUN-only ICE server list. When non-empty,
	// these servers are used for all PeerConnections. The createRoom response
	// also returns this list as ice_servers so clients can configure their own
	// PeerConnections with the same TURN credentials. See ADR-023.
	// When nil or empty, the relay falls back to stun.l.google.com:19302.
	ICEServers []webrtc.ICEServer
	// ICETCPMux, when non-nil, enables ICE-TCP candidates for PeerConnections
	// created by this server. Production: SetICETCPMux(tcpMux) on port 443.
	// Tests: leave nil; the loopback ICE API handles connectivity.
	ICETCPMux pionIce.TCPMux
}

// Server is the top-level relay server.
type Server struct {
	rooms          *room.Manager
	jwtSecret      []byte
	api            *webrtc.API
	Metrics        *metrics.Registry
	callbackClient *callback.Client
	iceServers     []webrtc.ICEServer
	iceTCPMux      pionIce.TCPMux

	// maxRooms and maxListenersPerRoom are the rate-limiting ceilings.
	// Both are set once at construction and never written again.
	maxRooms            int
	maxListenersPerRoom int

	// mu guards httpServer and sessions so Serve/Shutdown/signal handlers can
	// run concurrently without data races.
	mu         sync.Mutex
	httpServer *http.Server
	// sessions tracks active WebSocket sessions. Each session registers itself
	// on creation and deregisters on cleanup. Shutdown closes all tracked
	// connections so hijacked WebSocket connections are not abandoned.
	sessions map[string]*session
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
	return &Server{
		rooms:               room.NewManager(),
		jwtSecret:           cfg.JWTSecret,
		api:                 cfg.API,
		Metrics:             metrics.New(),
		callbackClient:      cfg.CallbackClient,
		iceServers:          cfg.ICEServers,
		iceTCPMux:           cfg.ICETCPMux,
		maxRooms:            maxRooms,
		maxListenersPerRoom: maxListeners,
		sessions:            make(map[string]*session),
	}
}

// Handler returns an http.Handler for the relay server routes.
// This is the testable surface: tests can pass the handler to httptest.NewServer.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/v1/rooms", s.createRoom)
	mux.HandleFunc("/v1/signal", s.signal)
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
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	hs := s.httpServer
	// Snapshot the live sessions to close them without holding the lock.
	toClose := make([]*session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		toClose = append(toClose, sess)
	}
	s.mu.Unlock()

	// Close active WebSocket sessions first so their read loops unblock and
	// can deregister cleanly.
	for _, sess := range toClose {
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
	return nil
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
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
	sourceToken, err := auth.Sign(s.jwtSecret, id, auth.RoleSource, 15*time.Minute)
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
	// Include ICE server configuration when TURN is enabled (ADR-023).
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

	pc   *webrtc.PeerConnection
	rm   *room.Room
	role auth.Role
}

func (s *session) run() {
	for {
		var msg signaling.ClientMessage
		if err := s.conn.ReadJSON(&msg); err != nil {
			return
		}
		switch msg.Type {
		case signaling.TypePublish, signaling.TypeSubscribe:
			s.handleJoin(msg)
		case signaling.TypeIce:
			s.handleICE(msg)
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
		pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			rm.SetSource(&trackSource{track: track}, func() { _ = pc.Close() })
			if s.srv.callbackClient != nil {
				go s.srv.callbackClient.PushSourceActive(roomIDForCallback, true)
			}
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
		if _, err := pc.AddTrack(audioTrack); err != nil {
			s.sendError("track_error", "failed to add audio track to peer connection")
			return
		}
		rm.AddListener(s.id, audioTrack)
		s.srv.Metrics.ListenerCount.Add(1)
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
}

func (s *session) handleICE(msg signaling.ClientMessage) {
	if s.pc == nil {
		s.sendError("not_joined", "join a room before sending ICE candidates")
		return
	}
	if err := s.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: msg.Candidate}); err != nil {
		s.sendError("ice_error", err.Error())
	}
}

// newPeerConnection creates a Pion PeerConnection and wires up ICE candidate
// forwarding to the WebSocket peer.
//
// ICE server list: uses s.srv.iceServers when set (STUN + embedded TURN per
// ADR-023). Falls back to stun.l.google.com when no servers are configured
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
	} else if s.srv.iceTCPMux != nil {
		// Production with ICE-TCP: build a SettingEngine to add the TCP mux.
		se := webrtc.SettingEngine{}
		se.SetICETCPMux(s.srv.iceTCPMux)
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
