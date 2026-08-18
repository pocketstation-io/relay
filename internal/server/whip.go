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
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/media/clocklineage"
	"github.com/pocketstation-io/relay/internal/media/downlink"
	"github.com/pocketstation-io/relay/internal/session"
)

// whipConn holds the PeerConnection for a WHIP/WHEP HTTP connection.
// Keyed by connID in Server.whipConns.
type whipConn struct {
	pc      *webrtc.PeerConnection
	room    *session.RelaySession
	busID   session.BusID
	connID  string
	created time.Time
}

type whipDirection uint8

const (
	whipIngress whipDirection = iota
	whepEgress
)

var errInvalidWHIPDirection = errors.New("invalid WHIP direction")

type whipPolicy struct {
	requiredRole   auth.Role
	compatibleRole auth.Role
	defaultBus     session.BusID
}

func (direction whipDirection) policy() (whipPolicy, error) {
	switch direction {
	case whipIngress:
		return whipPolicy{requiredRole: auth.RoleSource, defaultBus: "voice"}, nil
	case whepEgress:
		return whipPolicy{
			requiredRole:   auth.RoleSubscriber,
			compatibleRole: auth.RoleListener,
			defaultBus:     session.BusMix,
		}, nil
	default:
		return whipPolicy{}, errInvalidWHIPDirection
	}
}

func (direction whipDirection) String() string {
	switch direction {
	case whipIngress:
		return "ingress"
	case whepEgress:
		return "egress"
	default:
		return "unknown"
	}
}

// handleWHIP processes a WHIP ingest request (publisher → relay, RFC 9725).
// POST /v1/sessions/{id}/whip
func (s *Server) handleWHIP(w http.ResponseWriter, r *http.Request) {
	s.handleWHIPRequest(w, r, whipIngress)
}

// handleWHEP processes a WHEP egress request (relay → subscriber, WHEP draft).
// POST /v1/sessions/{id}/whep
func (s *Server) handleWHEP(w http.ResponseWriter, r *http.Request) {
	s.handleWHIPRequest(w, r, whepEgress)
}

func (s *Server) handleWHIPRequest(w http.ResponseWriter, r *http.Request, direction whipDirection) {
	if !s.handshakeAdmission.TryAcquire() {
		s.Metrics.HandshakeRejectedTotal.Add(1)
		http.Error(w, "relay handshake capacity exceeded", http.StatusServiceUnavailable)
		return
	}
	defer s.handshakeAdmission.Release()

	policy, err := direction.policy()
	if err != nil {
		http.Error(w, "invalid connection direction", http.StatusInternalServerError)
		return
	}
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

	if claims.Role != policy.requiredRole && claims.Role != policy.compatibleRole {
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
		busID = policy.defaultBus
	}

	offerBytes, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	if err != nil || len(offerBytes) == 0 {
		http.Error(w, "missing or unreadable SDP offer", http.StatusBadRequest)
		return
	}

	rm, _, accepted := s.relaySessions.GetOrCreateWithinLimit(sessionID, s.maxRooms)
	if !accepted {
		http.Error(w, "relay session limit exceeded", http.StatusTooManyRequests)
		return
	}

	pc, lineage, err := s.newWHIPPeerConnection()
	if err != nil {
		slog.Error("WHIP: failed to create peer connection", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	connID := newID()

	switch direction {
	case whipIngress:
		busCapture := busID
		pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
			var timeline *clocklineage.Timeline
			if lineage != nil {
				timeline = lineage.Remote(uint32(track.SSRC()))
			}
			go drainPublisherRTCP(receiver)
			if sourceErr := rm.SetSource(
				busCapture,
				busRoleFor(busCapture),
				&trackSource{track: track, timeline: timeline},
				func() { _ = pc.Close() },
			); sourceErr != nil {
				slog.Warn("WHIP source attachment rejected", "conn_id", connID, "error", sourceErr)
				_ = pc.Close()
			}
		})
	case whepEgress:
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
		sender, aErr := pc.AddTrack(audioTrack)
		if aErr != nil {
			_ = pc.Close()
			http.Error(w, "failed to add audio track", http.StatusInternalServerError)
			return
		}
		connCapture := connID
		busCapture := busID
		pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
			switch state {
			case webrtc.PeerConnectionStateConnected:
				var sub session.PacketWriter = audioTrack
				if redEnabled() {
					sub = newREDListener(audioTrack, opusPayloadType)
				}
				dl := downlink.NewForwardingDownlink(connCapture, sub, nil)
				if localDescription := pc.LocalDescription(); localDescription != nil {
					dl.ConfigureExtensions(localDescription.SDP)
				}
				if lineage != nil {
					parameters := sender.GetParameters()
					if len(parameters.Encodings) > 0 {
						dl.SetSenderTimeline(lineage.Local(uint32(parameters.Encodings[0].SSRC)))
					}
				}
				var onNACK downlink.NackCallback
				if !redEnabled() {
					onNACK = dl.HandleNACK
				}
				dl.SetFeedback(downlink.StartFeedbackReader(sender, dl.Stats(), nil, onNACK))
				if err := rm.AddBusSubscription(connCapture, busCapture, dl); err != nil {
					dl.StopForwarding()
				}
			case webrtc.PeerConnectionStateFailed,
				webrtc.PeerConnectionStateClosed,
				webrtc.PeerConnectionStateDisconnected:
				rm.RemoveSubscription(connCapture)
				s.whipConns.Delete(connCapture)
			}
		})
	default:
		_ = pc.Close()
		http.Error(w, "invalid connection direction", http.StatusInternalServerError)
		return
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
		"direction", direction,
	)
}
