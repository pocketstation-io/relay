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
	"github.com/pocketstation-io/relay/internal/graph"
	"github.com/pocketstation-io/relay/internal/signaling"
	"github.com/pocketstation-io/relay/internal/webhook"
)

// session represents one WebSocket peer connection.
// wmu serialises WebSocket writes from the read loop and Pion's ICE goroutine.
type session struct {
	id  string
	srv *Server

	wmu  sync.Mutex
	conn *websocket.Conn

	pc         *webrtc.PeerConnection
	room       *graph.GraphRoom
	role       auth.Role
	busID      graph.BusID  // bus this session publishes to or subscribes from
	pendingICE []string     // candidates received before PUBLISH/SUBSCRIBE

	// done is closed by cleanup() to signal teardown to scoped goroutines.
	done     chan struct{}
	doneOnce sync.Once

	// slotReserved and subscriptionRegistered guard the subscription lifecycle.
	// slotReserved: true between TryReserveSlot and ReleaseSlot.
	// subscriptionRegistered: true after AddSubscription succeeds (deferred to DTLS-complete).
	slotReserved             atomic.Bool
	subscriptionRegistered   atomic.Bool
}

func (s *session) run() {
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
	s.doneOnce.Do(func() { close(s.done) })

	if s.pc != nil {
		_ = s.pc.Close()
	}
	if s.room != nil {
		switch s.role {
		case auth.RoleSubscriber, auth.RoleListener:
			s.room.RemoveSubscription(s.id)
			if s.slotReserved.Swap(false) {
				s.room.ReleaseSlot()
			}
			if s.subscriptionRegistered.Load() {
				s.srv.Metrics.ListenerCount.Add(-1)
				if s.srv.callbackClient != nil {
					go s.srv.callbackClient.PushListenerLeave(s.room.ID)
				}
			}
		case auth.RoleSource:
			if s.srv.callbackClient != nil {
				go s.srv.callbackClient.PushSourceActive(s.room.ID, false)
			}
		}
		s.srv.webhookDispatcher.Send(webhook.Event{
			Type:      webhook.EventSessionEnded,
			RoomID:    s.room.ID,
			SessionID: s.id,
		})
	}
	slog.Info("session cleaned up", "session_id", s.id)
}

func (s *session) closeConn() {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_ = s.conn.WriteMessage(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutdown"),
	)
	_ = s.conn.Close()
}

// handleJoin processes PUBLISH or SUBSCRIBE. It verifies the JWT, creates a
// Pion PeerConnection, performs SDP exchange, and wires ICE candidate forwarding.
//
// PUBLISH: the session publishes a named bus (msg.BusID, default "voice").
// SUBSCRIBE: the session subscribes to relay.out("mix") or a named bus.
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

	// Accept both RoleListener (v2.3) and RoleSubscriber (v3.0) for SUBSCRIBE.
	if msg.Type == signaling.TypePublish && claims.Role != auth.RoleSource {
		s.sendError(signaling.ErrCodeRoleMismatch, "PUBLISH requires a source token")
		return
	}
	if msg.Type == signaling.TypeSubscribe &&
		claims.Role != auth.RoleSubscriber && claims.Role != auth.RoleListener {
		s.sendError(signaling.ErrCodeRoleMismatch, "SUBSCRIBE requires a subscriber token")
		return
	}

	sessionID := claims.EffectiveSessionID()
	if sessionID == "" {
		sessionID = msg.EffectiveSessionID()
	}
	rm := s.srv.sessions_.GetOrCreate(sessionID)
	s.room = rm
	s.role = claims.Role

	// Resolve the bus ID: prefer the message field, then the token claim,
	// then default to "voice" for PUBLISH and BusMix for SUBSCRIBE.
	busID := msg.BusID
	if busID == "" {
		busID = claims.BusID
	}
	if busID == "" {
		if msg.Type == signaling.TypePublish {
			busID = "voice"
		} else {
			busID = graph.BusMix
		}
	}
	s.busID = busID

	if msg.Type == signaling.TypePublish && msg.Public {
		rm.Public.Store(true)
	}
	if msg.GraphID != "" {
		rm.GraphID = msg.GraphID
	}

	slog.Info("session joined",
		"session_id", s.id,
		"graph_room_id", sessionID,
		"role", string(claims.Role),
		"bus_id", busID,
	)

	pc, err := s.newPeerConnection()
	if err != nil {
		s.sendError(signaling.ErrCodePCError, "failed to create peer connection")
		return
	}
	s.pc = pc

	switch msg.Type {
	case signaling.TypePublish:
		sessionIDForCallback := sessionID
		sessionIDForWebhook := s.id
		busIDForLoop := busID
		pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			rm.SetSource(busIDForLoop, busRoleFor(busIDForLoop), &trackSource{track: track}, func() { _ = pc.Close() })
			if s.srv.callbackClient != nil {
				go s.srv.callbackClient.PushSourceActive(sessionIDForCallback, true)
			}
			s.srv.webhookDispatcher.Send(webhook.Event{
				Type:      webhook.EventSessionStarted,
				RoomID:    sessionIDForCallback,
				SessionID: sessionIDForWebhook,
			})
		})

	case signaling.TypeSubscribe:
		if !rm.TryReserveSlot(s.srv.maxSubscribersPerRoom) {
			s.sendError(signaling.ErrCodeListenerLimitExceeded, "session has reached maximum subscriber count")
			return
		}
		s.slotReserved.Store(true)

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

		// Gate AddSubscription on DTLS-complete to eliminate srtpWriterFuture
		// silent drops (same fix as the original listener gate).
		pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
			slog.Info("subscriber pc state", "session_id", s.id, "state", state.String())
			if state != webrtc.PeerConnectionStateConnected {
				return
			}
			if !s.slotReserved.Swap(false) {
				slog.Warn("subscriber connected but slot already released", "session_id", s.id)
				return
			}
			rm.ReleaseSlot()

			if s.subscriptionRegistered.Swap(true) {
				return // guard against spurious re-fires
			}
			var sub graph.BusSubscription = audioTrack
			if redEnabled() {
				sub = newREDListener(audioTrack, opusPayloadType)
			}
			if err := rm.AddSubscription(s.id, sub); err != nil {
				s.subscriptionRegistered.Store(false)
				slog.Warn("AddSubscription post-connect failed", "session_id", s.id, "err", err)
				return
			}
			slog.Info("AddSubscription ok", "session_id", s.id)
			s.srv.Metrics.ListenerCount.Add(1)

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
					for _, stat := range pc.GetStats() {
						if out, ok := stat.(webrtc.OutboundRTPStreamStats); ok {
							pkts, _, _ := rm.PacketStats()
							slog.Info("pion_tx",
								"session_id", s.id,
								"ssrc", out.SSRC,
								"packets_sent", out.PacketsSent,
								"bytes_sent", out.BytesSent,
								"room_total", pkts,
							)
						}
					}
				}
			}()
		})

		if existingKey := rm.GetKey(); existingKey != "" {
			_ = s.send(signaling.ServerMessage{
				Type:      signaling.TypeKeyExchange,
				SFrameKey: existingKey,
			})
		}

		hintState    := s.srv.roomCodecHintState(sessionID)
		restartState := s.srv.roomICERestartState(sessionID)
		s.srv.startRTCPReader(sender, sessionID, hintState, restartState)
	}

	offer := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: msg.SDPOffer}
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
		Type:              signaling.TypeSDPAnswer,
		SDPAnswer:         answer.SDP,
		SessionID:         sessionID,
		BusID:             busID,
	})
	_ = s.send(signaling.ServerMessage{
		Type:              signaling.TypeRoomState,
		SourceActive:      rm.SourceActive(),
		SubscriptionCount: rm.SubscriptionCount(),
		Codec:             "opus",
		SessionID:         sessionID,
	})

	for _, c := range s.pendingICE {
		_ = s.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: c})
	}
	s.pendingICE = nil
}

func (s *session) handleICE(msg signaling.ClientMessage) {
	if s.pc == nil {
		s.pendingICE = append(s.pendingICE, msg.Candidate)
		return
	}
	if err := s.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: msg.Candidate}); err != nil {
		s.sendError(signaling.ErrCodeICEError, err.Error())
	}
}

func (s *session) handleLatencyReport(msg signaling.ClientMessage) {
	if s.room == nil {
		s.sendError(signaling.ErrCodeNotJoined, "join a session before sending LATENCY_REPORT")
		return
	}
	rpt := msg.LatencyReport
	if rpt == nil {
		s.sendError(signaling.ErrCodeBadRequest, "LATENCY_REPORT requires a latency_report payload")
		return
	}
	s.room.RecordLatency(rpt.CaptureMs, rpt.EncodeMs, rpt.RelayRttMs, rpt.JitterBufferMs, rpt.DecodeMs, rpt.PacketLossPct)
	if rpt.CaptureMs > 0 {
		s.srv.webhookDispatcher.Send(webhook.Event{
			Type:      webhook.EventUtteranceDetected,
			RoomID:    s.room.ID,
			SessionID: s.id,
		})
	}
}

// busRoleFor maps a well-known BusID to its BusRole. Unknown IDs default to BusRoleVoice.
func busRoleFor(id graph.BusID) graph.BusRole {
	switch id {
	case "music":
		return graph.BusRoleMusic
	case "agent_voice", "agent_output":
		return graph.BusRoleAgentOutput
	case "events":
		return graph.BusRoleEvents
	case "monitor":
		return graph.BusRoleMonitor
	default:
		return graph.BusRoleVoice
	}
}

// opusPayloadType is the RTP payload type the relay registers Opus under. 111 is
// the de-facto WebRTC default (matches pion's RegisterDefaultCodecs and Chrome).
const opusPayloadType = 111

// opusStereoFmtp is the SDP fmtp line the relay advertises for Opus.
// Stereo on both legs required or Chrome silently downmixes music to mono.
const opusStereoFmtp = "minptime=10;useinbandfec=1;stereo=1;sprop-stereo=1;maxaveragebitrate=131072"

// redMimeType / redPayloadType identify the RFC 2198 RED codec the relay offers
// on the subscriber leg when RED is enabled.
const (
	redMimeType    = "audio/red"
	redPayloadType = 63
)

// newMediaEngineWithAudioNACK returns an audio-only MediaEngine with stereo Opus
// + NACK. When RED is enabled, audio/red is registered first so it is preferred.
func newMediaEngineWithAudioNACK() (*webrtc.MediaEngine, error) {
	m := &webrtc.MediaEngine{}

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

func newInterceptorRegistry(m *webrtc.MediaEngine) (*interceptor.Registry, error) {
	i := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, i); err != nil {
		return nil, err
	}
	return i, nil
}

func (s *session) newPeerConnection() (*webrtc.PeerConnection, error) {
	iceServers := s.srv.iceServers
	if len(iceServers) == 0 {
		iceServers = []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}}
	}
	pcCfg := webrtc.Configuration{ICEServers: iceServers}

	var (
		pc  *webrtc.PeerConnection
		err error
	)
	if s.srv.api != nil {
		pc, err = s.srv.api.NewPeerConnection(pcCfg)
	} else if s.srv.iceTCPMux != nil || s.srv.iceUDPMux != nil || len(s.srv.nat1to1IPs) > 0 {
		se := webrtc.SettingEngine{}
		if s.srv.iceUDPMux != nil {
			se.SetICEUDPMux(s.srv.iceUDPMux)
		}
		if s.srv.iceTCPMux != nil {
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
	s.send(signaling.ServerMessage{Type: signaling.TypeError, Code: string(code), Message: message})
}

// trackSource adapts *webrtc.TrackRemote to graph.SourceSession.
type trackSource struct {
	track *webrtc.TrackRemote
}

func (t *trackSource) ReadRTP() (*rtp.Packet, error) {
	pkt, _, err := t.track.ReadRTP()
	return pkt, err
}
