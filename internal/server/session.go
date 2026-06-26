package server

import (
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/room"
	"github.com/pocketstation-io/relay/internal/signaling"
	"github.com/pocketstation-io/relay/internal/webhook"
)

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

	// done is closed by cleanup() to signal session teardown to goroutines such
	// as the pion_tx diagnostic poller that outlive the WebSocket read loop.
	done     chan struct{}
	doneOnce sync.Once

	// slotReserved is true between a successful TryReserveSlot call and the
	// corresponding ReleaseSlot call. ReleaseSlot is called exactly once:
	// either in the OnConnectionStateChange(Connected) callback (before
	// AddListener) or in cleanup when DTLS never completed.
	slotReserved atomic.Bool

	// listenerRegistered is set to true after AddListener succeeds, which is
	// deferred to PeerConnectionStateConnected. It guards cleanup so the
	// listener-count metric is not decremented for connections that failed
	// during ICE/DTLS before AddListener was ever called.
	listenerRegistered atomic.Bool
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
			s.sendError(signaling.ErrCodeUnknownType, "unknown message type: "+string(msg.Type))
		}
	}
}

func (s *session) cleanup() {
	// Signal all session-scoped goroutines (e.g. pion_tx diagnostic poller)
	// to stop before tearing down the peer connection.
	s.doneOnce.Do(func() { close(s.done) })

	if s.pc != nil {
		_ = s.pc.Close()
	}
	if s.rm != nil {
		switch s.role {
		case auth.RoleListener:
			s.rm.RemoveListener(s.id) // no-op if AddListener was never called
			// Release the slot reservation if DTLS never completed and the
			// OnConnectionStateChange callback did not already release it.
			if s.slotReserved.Swap(false) {
				s.rm.ReleaseSlot()
			}
			if s.listenerRegistered.Load() {
				s.srv.Metrics.ListenerCount.Add(-1)
				if s.srv.callbackClient != nil {
					go s.srv.callbackClient.PushListenerLeave(s.rm.ID)
				}
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
		s.sendError(signaling.ErrCodeAlreadyJoined, "session has already joined a room")
		return
	}

	claims, err := auth.Verify(s.srv.jwtSecret, msg.Token)
	if err != nil {
		slog.Warn("bad token", "session_id", s.id, "error", err)
		s.sendError(signaling.ErrCodeBadToken, err.Error())
		return
	}

	if msg.Type == signaling.TypePublish && claims.Role != auth.RoleSource {
		s.sendError(signaling.ErrCodeRoleMismatch, "PUBLISH requires a source token")
		return
	}
	if msg.Type == signaling.TypeSubscribe && claims.Role != auth.RoleListener {
		s.sendError(signaling.ErrCodeRoleMismatch, "SUBSCRIBE requires a listener token")
		return
	}

	rm := s.srv.rooms.GetOrCreate(claims.RoomID)
	s.rm = rm
	s.role = claims.Role

	// Mark room public when the PUBLISH message carries "public": true.
	// Written only on the first PUBLISH; subsequent PUBLISH calls on an existing
	// room (ICE restart) do not clear a previously set public flag.
	if msg.Type == signaling.TypePublish && msg.Public {
		rm.Public.Store(true)
	}

	slog.Info("session joined", "session_id", s.id, "room_id", claims.RoomID, "role", string(claims.Role))

	pc, err := s.newPeerConnection()
	if err != nil {
		s.sendError(signaling.ErrCodePCError, "failed to create peer connection")
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
		// Reserve a listener slot. TryReserveSlot counts both active and
		// in-flight (DTLS-pending) listeners, so concurrent SUBSCRIBE bursts
		// cannot bypass the per-room limit during the ICE/DTLS window.
		// The reservation is released exactly once: in the OnConnectionStateChange
		// callback (converted to an active entry) or in cleanup (DTLS failed).
		if !rm.TryReserveSlot(s.srv.maxListenersPerRoom) {
			s.sendError(signaling.ErrCodeListenerLimitExceeded, "room has reached maximum listener count")
			return
		}
		s.slotReserved.Store(true)

		// The listener track is audio/red when RED is enabled (the relay frames
		// RED on egress; see redListener) and plain Opus otherwise.
		listenerMime := webrtc.MimeTypeOpus
		if redEnabled() {
			listenerMime = redMimeType
		}
		audioTrack, err := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: listenerMime},
			"audio", "pocketstation",
		)
		if err != nil {
			s.sendError(signaling.ErrCodeTrackError, "failed to create audio track")
			return
		}
		sender, addErr := pc.AddTrack(audioTrack)
		if addErr != nil {
			s.sendError(signaling.ErrCodeTrackError, "failed to add audio track to peer connection")
			return
		}
		// Gate AddListener on DTLS-complete to eliminate srtpWriterFuture silent
		// drops. In pion v4, TrackLocalStaticRTP.writeRTP delegates to
		// srtpWriterFuture.WriteRTP, which returns (0, nil) — no error — when
		// the srtpReady channel has not fired yet (DTLS not complete). The
		// forwardLoop only increments PacketDropCount on non-nil errors, so every
		// packet written before DTLS completes is silently discarded. At 50 pkt/s
		// over a 2-10 s ICE+DTLS window that is 100-500 invisible dropped packets,
		// producing the ~47-49% packet loss observed in Chrome's WebRTC stats.
		//
		// Deferring AddListener to PeerConnectionStateConnected ensures that SRTP
		// is ready before the first WriteRTP call reaches this listener, eliminating
		// the silent-drop window entirely.
		pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
			slog.Info("subscriber pc state", "session_id", s.id, "state", state.String())
			if state != webrtc.PeerConnectionStateConnected {
				return
			}
			// Release the reservation exactly once. If cleanup already ran and
			// released it, we skip — the session is being torn down.
			if !s.slotReserved.Swap(false) {
				slog.Warn("subscriber connected but slot already released", "session_id", s.id)
				return
			}
			rm.ReleaseSlot()

			if s.listenerRegistered.Swap(true) {
				return // guard against spurious re-fires (e.g. ICE restart)
			}
			var listener room.Listener = audioTrack
			if redEnabled() {
				// Forward through the RED decorator so each Opus frame is sent
				// with the previous one as RFC 2198 redundancy (loss resilience).
				listener = newREDListener(audioTrack, opusPayloadType)
			}
			if err := rm.AddListener(s.id, listener); err != nil {
				// Two sessions raced through the reservation window; the active
				// count reached capacity before this one could finalize.
				s.listenerRegistered.Store(false)
				slog.Warn("AddListener post-connect failed", "session_id", s.id, "err", err)
				return
			}
			slog.Info("AddListener ok", "session_id", s.id)
			s.srv.Metrics.ListenerCount.Add(1)

			// Diagnostic: poll pion's OutboundRTPStreamStats every 2 seconds
			// to detect silent drops inside srtpWriterFuture. A "packets_sent"
			// counter that lags behind the room's forwarded-packet count means
			// WriteRTP is returning (0, nil) without actually writing to Chrome.
			// The goroutine stops on s.done (WebSocket closed) or when the PC
			// reaches a terminal state, whichever comes first.
			go func() {
				ticker := time.NewTicker(2 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-s.done:
						return
					case <-ticker.C:
					}
					cs := pc.ConnectionState()
					if cs == webrtc.PeerConnectionStateDisconnected ||
						cs == webrtc.PeerConnectionStateFailed ||
						cs == webrtc.PeerConnectionStateClosed {
						return
					}
					// NOTE: pion v4.0.0 GetStats() does not collect RTPSender
					// stats — OutboundRTPStreamStats is never present in the
					// report. The fwd_peer log in room.go's forwardLoop provides
					// the equivalent per-listener write count for diagnosing
					// srtpWriterFuture silent drops.
					foundOutbound := false
					for _, stat := range pc.GetStats() {
						if out, ok := stat.(webrtc.OutboundRTPStreamStats); ok {
							foundOutbound = true
							slog.Info("pion_tx",
								"session_id", s.id,
								"ssrc", out.SSRC,
								"packets_sent", out.PacketsSent,
								"bytes_sent", out.BytesSent,
								"room_total", rm.PacketCount.Load(),
							)
						}
					}
					if !foundOutbound {
						slog.Debug("pion_tx_no_stats",
							"session_id", s.id,
							"pc_state", cs.String(),
							"room_total", rm.PacketCount.Load(),
						)
					}
				}
			}()
		})

		// Late-join key delivery (RELAY-014): if a KEY_EXCHANGE was received
		// before this listener joined, forward the stored key immediately so
		// this listener can decrypt from the first packet. GetKey returns the
		// opaque base64 string the source sent, which is forwarded as-is.
		if existingKey := rm.GetKey(); existingKey != "" {
			_ = s.send(signaling.ServerMessage{
				Type:      signaling.TypeKeyExchange,
				SFrameKey: existingKey,
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
		s.sendError(signaling.ErrCodeSDPError, "failed to set remote description")
		return
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		s.sendError(signaling.ErrCodeSDPError, "failed to create answer")
		return
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		s.sendError(signaling.ErrCodeSDPError, "failed to set local description")
		return
	}

	_ = s.send(signaling.ServerMessage{
		Type:      signaling.TypeSDPAnswer,
		SDPAnswer: answer.SDP,
	})
	_ = s.send(signaling.ServerMessage{
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
		s.sendError(signaling.ErrCodeICEError, err.Error())
	}
}

// handleLatencyReport processes a LATENCY_REPORT message sent by a client.
// The report is recorded into the room's rolling latency window.
// Silently discarded when the session has not joined a room or the report
// payload is absent, to protect the hot path from malformed messages.
func (s *session) handleLatencyReport(msg signaling.ClientMessage) {
	if s.rm == nil {
		s.sendError(signaling.ErrCodeNotJoined, "join a room before sending LATENCY_REPORT")
		return
	}
	rpt := msg.LatencyReport
	if rpt == nil {
		s.sendError(signaling.ErrCodeBadRequest, "LATENCY_REPORT requires a latency_report payload")
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

// opusPayloadType is the RTP payload type the relay registers Opus under. 111 is
// the de-facto WebRTC default (matches pion's RegisterDefaultCodecs and Chrome).
const opusPayloadType = 111

// redMimeType / redPayloadType identify the RFC 2198 RED codec the relay offers
// on the listener leg when RED is enabled. 63 is the de-facto WebRTC RED payload
// type (matches Chrome). pion has no MimeTypeRED constant, so use the string.
const (
	redMimeType    = "audio/red"
	redPayloadType = 63
)

// opusStereoFmtp is the SDP fmtp line the relay advertises for Opus. Per RFC 7587,
// the relay (which forwards stereo Opus from the source to the browser as an SFU,
// without transcoding) must signal stereo on BOTH negotiated legs or libwebrtc
// silently downmixes music to mono:
//   - stereo=1        — the relay accepts/decodes stereo (receive direction)
//   - sprop-stereo=1  — the relay will send stereo (send direction)
//   - useinbandfec=1  — accept Opus in-band FEC (loss concealment)
//   - maxaveragebitrate — receive-side cap (128 kbps, the transparent music bar)
//
// These are orthogonal between offer and answer (RFC 7587 §6.1), so the relay
// declares its own; the source declares sprop-stereo and the browser declares
// stereo on their sides.
const opusStereoFmtp = "minptime=10;useinbandfec=1;stereo=1;sprop-stereo=1;maxaveragebitrate=131072"

// newMediaEngineWithAudioNACK returns an audio-only MediaEngine that registers a
// stereo-capable Opus (48 kHz, 2 channels) with the stereo fmtp above and NACK
// feedback. The relay is an Opus-only audio SFU, so registering just Opus (rather
// than RegisterDefaultCodecs) keeps negotiation minimal and lets the answer carry
// the stereo fmtp — RegisterDefaultCodecs would register a mono-fmtp Opus at the
// same payload type and cannot be amended. NACK lets the relay request
// retransmission of dropped audio packets (pion enables NACK only for video by
// default).
func newMediaEngineWithAudioNACK() (*webrtc.MediaEngine, error) {
	m := &webrtc.MediaEngine{}

	// When RED is enabled, register audio/red FIRST so it is preferred over plain
	// Opus. Its fmtp "<opusPT>/<opusPT>" declares it carries two levels of Opus
	// redundancy (RFC 2198 / RFC 7587). pion has no RED codec, so register it as a
	// raw codec; the relay frames RED itself (see redListener) and the browser
	// decodes it (Chrome supports opus+red by default). Opus stays registered as
	// the source-leg codec and the RED fallback.
	if redEnabled() {
		if err := m.RegisterCodec(webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:    redMimeType,
				ClockRate:   48000,
				Channels:    2,
				SDPFmtpLine: strconv.Itoa(opusPayloadType) + "/" + strconv.Itoa(opusPayloadType),
			},
			PayloadType: redPayloadType,
		}, webrtc.RTPCodecTypeAudio); err != nil {
			return nil, err
		}
	}

	if err := m.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeOpus,
			ClockRate:    48000,
			Channels:     2,
			SDPFmtpLine:  opusStereoFmtp,
			RTCPFeedback: []webrtc.RTCPFeedback{{Type: "nack"}},
		},
		PayloadType: opusPayloadType,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, err
	}
	return m, nil
}

// newInterceptorRegistry builds an interceptor registry bound to the given
// MediaEngine with the default interceptors (NACK, RTCP reports, etc.) registered.
func newInterceptorRegistry(m *webrtc.MediaEngine) (*interceptor.Registry, error) {
	i := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, i); err != nil {
		return nil, err
	}
	return i, nil
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
//
// All non-test paths use a custom MediaEngine with audio NACK registered (FIX-8).
func (s *session) newPeerConnection() (*webrtc.PeerConnection, error) {
	iceServers := s.srv.iceServers
	if len(iceServers) == 0 {
		// Always fall back to an external STUN server when no ICE servers are
		// explicitly configured. Even with NAT1To1IPs set (Fly.io production),
		// the STUN request is necessary: it creates an outbound NAT mapping on
		// the host's shared IPv4, making the ephemeral UDP port reachable from
		// external peers. Skipping STUN leaves no NAT mapping and ICE fails.
		// Using stun.l.google.com (external) — not the embedded TURN server —
		// avoids the self-STUN loop that produces internal srflx candidates.
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
	} else if s.srv.iceTCPMux != nil || s.srv.iceUDPMux != nil || len(s.srv.nat1to1IPs) > 0 {
		// Production path: build a SettingEngine when an ICE-TCP mux, ICE-UDP
		// mux, or NAT1To1 IP override is configured. These may be set
		// independently: NAT1To1IPs alone is used when RELAY_PUBLIC_IPS is set
		// without ICE_TCP_PORT (e.g. E2E tests forcing loopback candidates).
		se := webrtc.SettingEngine{}
		if s.srv.iceUDPMux != nil {
			// Single shared UDP socket for all media. Prevents pion from
			// gathering multiple UDP host candidates (one per interface/NAT IP)
			// and egressing media from a socket the peer never consented to,
			// which dropped ~50% of RTP on multi-homed hosts.
			se.SetICEUDPMux(s.srv.iceUDPMux)
		}
		if s.srv.iceTCPMux != nil {
			// Offer ICE-TCP as a fallback candidate for NAT/firewall traversal,
			// but do NOT restrict the network type to TCP. Real-time Opus must
			// travel over UDP: TCP guarantees in-order delivery, so a single
			// delayed packet stalls every packet behind it (head-of-line
			// blocking). The browser's jitter buffer then conceals the resulting
			// gaps — the audible "radio glitch". Chrome already ranks UDP host
			// candidates above TCP, so leaving both available lets it nominate
			// the UDP pair when reachable and fall back to TCP only when UDP
			// cannot connect.
			se.SetICETCPMux(s.srv.iceTCPMux)
		}
		if len(s.srv.nat1to1IPs) > 0 {
			se.SetNAT1To1IPs(s.srv.nat1to1IPs, webrtc.ICECandidateTypeHost)
		}
		m, mErr := newMediaEngineWithAudioNACK()
		if mErr != nil {
			return nil, mErr
		}
		ir, irErr := newInterceptorRegistry(m)
		if irErr != nil {
			return nil, irErr
		}
		api := webrtc.NewAPI(
			webrtc.WithSettingEngine(se),
			webrtc.WithMediaEngine(m),
			webrtc.WithInterceptorRegistry(ir),
		)
		pc, err = api.NewPeerConnection(pcCfg)
	} else {
		// Default production path (no ICE-TCP mux, no NAT override).
		// Build a custom API with audio NACK so the default pion path also gets it.
		m, mErr := newMediaEngineWithAudioNACK()
		if mErr != nil {
			return nil, mErr
		}
		ir, irErr := newInterceptorRegistry(m)
		if irErr != nil {
			return nil, irErr
		}
		api := webrtc.NewAPI(
			webrtc.WithMediaEngine(m),
			webrtc.WithInterceptorRegistry(ir),
		)
		pc, err = api.NewPeerConnection(pcCfg)
	}
	if err != nil {
		return nil, err
	}
	// Forward locally gathered ICE candidates to the client.
	// OnICECandidate fires from Pion's ICE goroutine, so send holds wmu.
	// Write failures are logged by send(); the read loop detects the dead
	// connection and tears down the session naturally.
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		_ = s.send(signaling.ServerMessage{
			Type:      signaling.TypeIce,
			Candidate: c.ToJSON().Candidate,
		})
	})
	return pc, nil
}

func (s *session) send(msg signaling.ServerMessage) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	err := s.conn.WriteJSON(msg)
	if err != nil {
		slog.Warn("websocket write failed", "session_id", s.id, "error", err)
	}
	return err
}

func (s *session) sendError(code signaling.ErrorCode, message string) {
	// ServerMessage.Code is string for JSON marshaling; cast is safe and explicit.
	s.send(signaling.ServerMessage{Type: signaling.TypeError, Code: string(code), Message: message})
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
