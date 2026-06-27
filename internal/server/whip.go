// Package server — WHIP/WHEP HTTP signaling (RFC 9725).
//
// WHIP (WebRTC-HTTP Ingestion Protocol, RFC 9725, March 2025) and WHEP
// (WebRTC-HTTP Egress Protocol, stable draft) replace the proprietary
// PUBLISH/SUBSCRIBE WebSocket messages with standard HTTP:
//
//   Client → POST /v1/sessions/{id}/whip  Content-Type: application/sdp
//             Body: SDP offer
//   Server → 201 Created                  Content-Type: application/sdp
//             Location: /v1/connections/{connID}
//             Link: <stun:...>; rel="ice-server"
//             Body: SDP answer (all ICE candidates gathered)
//
// ICE trickle (client → server):
//   PATCH /v1/connections/{connID}  Content-Type: application/trickle-ice-sdpfrag
//   DELETE /v1/connections/{connID} → teardown
//
// Why this beats the custom WebSocket approach:
//   - OBS, ffmpeg, Cloudflare Stream, and every standards-compliant encoder
//     speak WHIP natively (RFC 9725).
//   - WHEP subscribers interop with Chrome's built-in WebRTC WHEP client.
//   - HTTP semantics: auth via Bearer token, standard error codes, cacheable.
//   - No persistent WebSocket needed for the handshake path.
//   - Stateless behind an HTTP load balancer.
//
// The existing /v1/signal WebSocket is kept for richer v3.0 events
// (CODEC_HINT, KEY_EXCHANGE, LATENCY_REPORT, named-bus routing).

package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/graph"
)

// whipConn holds the PeerConnection for a WHIP/WHEP HTTP connection.
// Keyed by connID in Server.whipConns.
type whipConn struct {
	pc      *webrtc.PeerConnection
	room    *graph.GraphRoom
	busID   graph.BusID
	connID  string
	created time.Time
}

// handleWHIP processes a WHIP ingest request (publisher → relay, RFC 9725).
// POST /v1/sessions/{id}/whip
func (s *Server) handleWHIP(w http.ResponseWriter, r *http.Request) {
	s.handleWHIPRequest(w, r, true)
}

// handleWHEP processes a WHEP egress request (relay → subscriber, WHEP draft).
// POST /v1/sessions/{id}/whep
func (s *Server) handleWHEP(w http.ResponseWriter, r *http.Request) {
	s.handleWHIPRequest(w, r, false)
}

func (s *Server) handleWHIPRequest(w http.ResponseWriter, r *http.Request, isPublish bool) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/sdp") {
		w.Header().Set("Accept", "application/sdp")
		http.Error(w, "Content-Type must be application/sdp", http.StatusUnsupportedMediaType)
		return
	}

	rawToken := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	claims, err := auth.Verify(s.jwtSecret, rawToken)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	wantRole := auth.RoleSubscriber
	if isPublish {
		wantRole = auth.RoleSource
	}
	if claims.Role != wantRole && !(claims.Role == auth.RoleListener && !isPublish) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	sessionID := r.PathValue("id")
	if sessionID == "" {
		sessionID = claims.EffectiveSessionID()
	}

	// Bus ID: URL query param > token claim > default.
	busID := r.URL.Query().Get("bus")
	if busID == "" {
		busID = claims.BusID
	}
	if busID == "" {
		if isPublish {
			busID = "voice"
		} else {
			busID = graph.BusMix
		}
	}

	offerBytes, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	if err != nil || len(offerBytes) == 0 {
		http.Error(w, "missing or unreadable SDP offer", http.StatusBadRequest)
		return
	}

	rm := s.sessions_.GetOrCreate(sessionID)

	pc, err := s.newWHIPPeerConnection()
	if err != nil {
		slog.Error("WHIP: failed to create peer connection", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	connID := newID()

	if isPublish {
		busCapture := busID
		pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			rm.SetSource(busCapture, busRoleFor(busCapture), &trackSource{track: track}, func() { _ = pc.Close() })
		})
	} else {
		listenerMime := webrtc.MimeTypeOpus
		if redEnabled() {
			listenerMime = redMimeType
		}
		audioTrack, tErr := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: listenerMime},
			"audio", "pocketstation",
		)
		if tErr != nil {
			_ = pc.Close()
			http.Error(w, "failed to create audio track", http.StatusInternalServerError)
			return
		}
		if _, aErr := pc.AddTrack(audioTrack); aErr != nil {
			_ = pc.Close()
			http.Error(w, "failed to add audio track", http.StatusInternalServerError)
			return
		}
		connCapture := connID
		pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
			switch state {
			case webrtc.PeerConnectionStateConnected:
				var sub graph.BusSubscription = audioTrack
				if redEnabled() {
					sub = newREDListener(audioTrack, opusPayloadType)
				}
				_ = rm.AddSubscription(connCapture, sub)
			case webrtc.PeerConnectionStateFailed,
				webrtc.PeerConnectionStateClosed,
				webrtc.PeerConnectionStateDisconnected:
				rm.RemoveSubscription(connCapture)
				s.whipConns.Delete(connCapture)
			}
		})
	}

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  string(offerBytes),
	}); err != nil {
		_ = pc.Close()
		http.Error(w, "invalid SDP offer: "+err.Error(), http.StatusBadRequest)
		return
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		_ = pc.Close()
		http.Error(w, "failed to create SDP answer", http.StatusInternalServerError)
		return
	}

	// Wait for all ICE candidates before responding (RFC 9725 §4.3 recommendation).
	// A 10-second deadline prevents indefinite blocking on unreachable STUN servers.
	gatherDone := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		_ = pc.Close()
		http.Error(w, "failed to set local description", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	select {
	case <-gatherDone:
	case <-ctx.Done():
		_ = pc.Close()
		http.Error(w, "ICE gathering timed out", http.StatusGatewayTimeout)
		return
	}

	finalDesc := pc.LocalDescription()

	s.whipConns.Store(connID, &whipConn{
		pc:      pc,
		room:    rm,
		busID:   busID,
		connID:  connID,
		created: time.Now(),
	})

	// RFC 9725 §4.6: Link headers advertise ICE server configuration.
	for _, srv := range s.clientICEServers {
		for _, u := range srv.URLs {
			linkVal := `<` + u + `>; rel="ice-server"`
			if srv.Username != "" {
				linkVal += `; username="` + srv.Username + `"`
			}
			w.Header().Add("Link", linkVal)
		}
	}

	w.Header().Set("Content-Type", "application/sdp")
	w.Header().Set("Location", "/v1/connections/"+connID)
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(finalDesc.SDP))

	slog.Info("WHIP connection created",
		"conn_id", connID,
		"session_id", sessionID,
		"bus_id", busID,
		"publish", isPublish,
	)
}

// handleWHIPICE processes a trickle-ICE PATCH (RFC 9725 §4.5).
// PATCH /v1/connections/{connID}
// Content-Type: application/trickle-ice-sdpfrag
func (s *Server) handleWHIPICE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	connID := r.PathValue("connID")
	v, ok := s.whipConns.Load(connID)
	if !ok {
		http.Error(w, "connection not found", http.StatusNotFound)
		return
	}
	conn := v.(*whipConn)

	fragBytes, err := io.ReadAll(io.LimitReader(r.Body, 8*1024))
	if err != nil {
		http.Error(w, "failed to read ICE fragment", http.StatusBadRequest)
		return
	}

	// Parse SDP fragment: extract a=candidate: lines per RFC 8839 §4.2.3.
	for _, line := range strings.Split(string(fragBytes), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "a=candidate:"):
			candidate := strings.TrimPrefix(line, "a=")
			if iErr := conn.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: candidate}); iErr != nil {
				slog.Warn("WHIP trickle: AddICECandidate", "conn_id", connID, "error", iErr)
			}
		case line == "a=end-of-candidates":
			_ = conn.pc.AddICECandidate(webrtc.ICECandidateInit{})
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleWHIPDelete tears down a WHIP/WHEP connection (RFC 9725 §4.7).
// DELETE /v1/connections/{connID}
func (s *Server) handleWHIPDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	connID := r.PathValue("connID")
	v, loaded := s.whipConns.LoadAndDelete(connID)
	if !loaded {
		http.Error(w, "connection not found", http.StatusNotFound)
		return
	}
	conn := v.(*whipConn)
	_ = conn.pc.Close()
	if conn.room != nil {
		conn.room.RemoveSubscription(connID)
	}
	slog.Info("WHIP connection deleted", "conn_id", connID)
	w.WriteHeader(http.StatusOK)
}

// newWHIPPeerConnection creates a PeerConnection for WHIP/WHEP connections.
// Unlike WebSocket sessions, WHIP PCs do not wire OnICECandidate to a WebSocket.
// ICE gathering completes synchronously before the HTTP response is returned.
func (s *Server) newWHIPPeerConnection() (*webrtc.PeerConnection, error) {
	iceServers := s.iceServers
	if len(iceServers) == 0 {
		iceServers = []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}}
	}
	pcCfg := webrtc.Configuration{ICEServers: iceServers}

	m, err := newMediaEngineWithAudioNACK()
	if err != nil {
		return nil, err
	}
	ir, err := newInterceptorRegistry(m)
	if err != nil {
		return nil, err
	}

	if s.iceUDPMux != nil || s.iceTCPMux != nil || len(s.nat1to1IPs) > 0 {
		se := webrtc.SettingEngine{}
		if s.iceUDPMux != nil {
			se.SetICEUDPMux(s.iceUDPMux)
		}
		if s.iceTCPMux != nil {
			se.SetICETCPMux(s.iceTCPMux)
		}
		if len(s.nat1to1IPs) > 0 {
			se.SetNAT1To1IPs(s.nat1to1IPs, webrtc.ICECandidateTypeHost)
		}
		api := webrtc.NewAPI(
			webrtc.WithSettingEngine(se),
			webrtc.WithMediaEngine(m),
			webrtc.WithInterceptorRegistry(ir),
		)
		return api.NewPeerConnection(pcCfg)
	}
	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(m),
		webrtc.WithInterceptorRegistry(ir),
	)
	return api.NewPeerConnection(pcCfg)
}
