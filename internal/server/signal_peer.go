package server

import (
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/nack"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/clocklineage"
	"github.com/pocketstation-io/relay/internal/downlink"
	"github.com/pocketstation-io/relay/internal/graph"
	"github.com/pocketstation-io/relay/internal/signaling"
	"github.com/pocketstation-io/relay/internal/webhook"
)

// signalPeer owns one WebSocket signaling and WebRTC peer connection.
// wmu serialises WebSocket writes from the read loop and Pion's ICE goroutine.
type signalPeer struct {
	id  string
	srv *Server

	wmu  sync.Mutex
	conn *websocket.Conn

	pc         *webrtc.PeerConnection
	lineage    *clocklineage.Registry
	room       *graph.RelaySession
	role       auth.Role
	busID      graph.BusID // bus this session publishes to or subscribes from
	pendingICE []string    // candidates received before PUBLISH/SUBSCRIBE

	// done is closed by cleanup() to signal teardown to scoped goroutines.
	done     chan struct{}
	doneOnce sync.Once

	// slotReserved and subscriptionRegistered guard the subscription lifecycle.
	// slotReserved: true between TryReserveSlot and ReleaseSlot.
	// subscriptionRegistered: true after AddSubscription succeeds (deferred to DTLS-complete).
	slotReserved           atomic.Bool
	subscriptionRegistered atomic.Bool
}

func (s *signalPeer) run() {
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
		case signaling.TypeSDPAnswer:
			s.handleSDPAnswer(msg)
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

func (s *signalPeer) cleanup() {
	s.doneOnce.Do(func() { close(s.done) })

	if s.pc != nil {
		_ = s.pc.Close()
	}
	if s.room != nil {
		switch s.role {
		case auth.RoleSubscriber, auth.RoleListener:
			if s.slotReserved.Swap(false) {
				s.room.ReleaseSlot()
			}
			if s.subscriptionRegistered.Load() {
				s.room.RemoveSubscription(s.id)
				s.srv.Metrics.ListenerCount.Add(-1)
				s.srv.broadcastSessionState(s.room)
				if s.srv.callbackClient != nil {
					go s.srv.callbackClient.PushSubscriberLeave(s.room.ID)
				}
			} else {
				s.room.RemoveSubscription(s.id)
			}
		case auth.RoleSource:
			if s.srv.callbackClient != nil {
				go s.srv.callbackClient.PushSourceActive(s.room.ID, false)
			}
			s.srv.broadcastSessionState(s.room)
		}
		s.srv.webhookDispatcher.Send(webhook.Event{
			Type:      webhook.EventSessionEnded,
			RoomID:    s.room.ID,
			SessionID: s.id,
		})
	}
	slog.Info("session cleaned up", "session_id", s.id)
}

func (s *signalPeer) closeConn() {
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
func (s *signalPeer) handleJoin(msg signaling.ClientMessage) {
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
	rm := s.srv.relaySessions.GetOrCreate(sessionID)
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
		"relay_session_id", sessionID,
		"role", string(claims.Role),
		"bus_id", busID,
	)

	pc, lineage, err := s.newPeerConnection()
	if err != nil {
		s.sendError(signaling.ErrCodePCError, "failed to create peer connection")
		return
	}
	s.pc = pc
	s.lineage = lineage
	var localDescriptionSDP string

	switch msg.Type {
	case signaling.TypePublish:
		if msg.SDPOffer == "" {
			if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
				Direction: webrtc.RTPTransceiverDirectionRecvonly,
			}); err != nil {
				s.sendError(signaling.ErrCodeTrackError, "failed to add publisher audio transceiver")
				return
			}
		}
		sessionIDForCallback := sessionID
		sessionIDForWebhook := s.id
		busIDForLoop := busID
		pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
			var timeline *clocklineage.Timeline
			if lineage != nil {
				timeline = lineage.Remote(uint32(track.SSRC()))
			}
			go drainPublisherRTCP(receiver)
			rm.SetSource(busIDForLoop, busRoleFor(busIDForLoop), &trackSource{track: track, timeline: timeline}, func() { _ = pc.Close() })
			if s.srv.callbackClient != nil {
				go s.srv.callbackClient.PushSourceActive(sessionIDForCallback, true)
			}
			s.srv.broadcastSessionState(rm)
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

		hintState := s.srv.roomCodecHintState(sessionID)
		restartState := s.srv.roomICERestartState(sessionID)

		// Gate AddDownlink on DTLS-complete to eliminate srtpWriterFuture
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

			// Create the Downlink first so the FeedbackReader writes RTCP metrics
			// (jitter, loss, nack) into the same SenderStats that Snapshot reads.
			dl := downlink.NewForwardingDownlink(s.id, sub, nil)
			dl.ConfigureExtensions(localDescriptionSDP)
			if lineage != nil {
				parameters := sender.GetParameters()
				if len(parameters.Encodings) > 0 {
					dl.SetSenderTimeline(lineage.Local(uint32(parameters.Encodings[0].SSRC)))
				}
			}

			onRecvReport := func(fractionLost float64) {
				observedRTT := false
				for _, stat := range pc.GetStats() {
					remote, ok := stat.(webrtc.RemoteInboundRTPStreamStats)
					if ok && remote.RoundTripTimeMeasurements > 0 {
						dl.ObserveRTT(time.Duration(remote.RoundTripTime * float64(time.Second)))
						observedRTT = true
						break
					}
				}
				if !observedRTT {
					rttUs := dl.Stats().RttLastUs.Load()
					if rttUs >= 0 {
						dl.ObserveRTT(time.Duration(rttUs) * time.Microsecond)
					}
				}
				hint := bitrateForLoss(fractionLost)
				s.srv.maybeEmitCodecHint(sessionID, hint, hintState)
				s.srv.maybeEmitICERestart(sessionID, fractionLost, restartState)
			}
			var onNACK downlink.NackCallback
			if !redEnabled() {
				onNACK = dl.HandleNACK
			}
			fr := downlink.StartFeedbackReader(sender, dl.Stats(), onRecvReport, onNACK)
			dl.SetFeedback(fr)

			if err := rm.AddDownlink(s.id, s.busID, dl); err != nil {
				s.subscriptionRegistered.Store(false)
				dl.StopPacer()
				// Do NOT call fr.Stop() here: ReadRTCP may be blocked, and Stop()
				// waits for the goroutine to exit. Calling Stop() before pc.Close()
				// can deadlock (the goroutine is stuck in ReadRTCP until the PC
				// closes). The goroutine will exit naturally when the PC is closed.
				slog.Warn("AddDownlink post-connect failed", "session_id", s.id, "err", err)
				return
			}
			slog.Info("AddDownlink ok", "session_id", s.id)
			s.srv.Metrics.ListenerCount.Add(1)
			s.srv.broadcastSessionState(rm)

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
	}

	if msg.SDPOffer == "" {
		offer, offerErr := pc.CreateOffer(nil)
		if offerErr != nil {
			s.sendError(signaling.ErrCodeSDPError, "failed to create offer")
			return
		}
		if setErr := pc.SetLocalDescription(offer); setErr != nil {
			s.sendError(signaling.ErrCodeSDPError, "failed to set local description")
			return
		}
		localDescriptionSDP = offer.SDP
		_ = s.send(signaling.ServerMessage{
			Type: signaling.TypeSDPOffer, SDPOffer: offer.SDP,
			SessionID: sessionID, BusID: busID,
		})
	} else {
		offer := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: msg.SDPOffer}
		if err := pc.SetRemoteDescription(offer); err != nil {
			s.sendError(signaling.ErrCodeSDPError, "failed to set remote description")
			return
		}
		answer, answerErr := pc.CreateAnswer(nil)
		if answerErr != nil {
			s.sendError(signaling.ErrCodeSDPError, "failed to create answer")
			return
		}
		if err := pc.SetLocalDescription(answer); err != nil {
			s.sendError(signaling.ErrCodeSDPError, "failed to set local description")
			return
		}
		localDescriptionSDP = answer.SDP
		_ = s.send(signaling.ServerMessage{
			Type: signaling.TypeSDPAnswer, SDPAnswer: answer.SDP,
			SessionID: sessionID, BusID: busID,
		})
	}
	_ = s.send(signaling.ServerMessage{
		Type:              signaling.TypeSessionState,
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

func (s *signalPeer) handleICE(msg signaling.ClientMessage) {
	if s.pc == nil || s.pc.RemoteDescription() == nil {
		s.pendingICE = append(s.pendingICE, msg.Candidate)
		return
	}
	if err := s.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: msg.Candidate}); err != nil {
		s.sendError(signaling.ErrCodeICEError, err.Error())
	}
}

func (s *signalPeer) handleSDPAnswer(msg signaling.ClientMessage) {
	if s.pc == nil || s.pc.LocalDescription() == nil || s.pc.RemoteDescription() != nil {
		s.sendError(signaling.ErrCodeSDPError, "SDP_ANSWER is not expected")
		return
	}
	answer := webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: msg.SDPAnswer}
	if err := s.pc.SetRemoteDescription(answer); err != nil {
		s.sendError(signaling.ErrCodeSDPError, "failed to set remote answer")
		return
	}
	for _, candidate := range s.pendingICE {
		_ = s.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: candidate})
	}
	s.pendingICE = nil
}

func (s *signalPeer) handleLatencyReport(msg signaling.ClientMessage) {
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
// In-band FEC remains enabled for the ordinary Opus path. Disabling it made the
// loss benchmark codec-inequivalent to the LiveKit control and removed the
// receiver's only recovery mechanism when RED is not negotiated.
const opusStereoFmtp = "minptime=10;useinbandfec=1;stereo=1;sprop-stereo=1;maxaveragebitrate=131072"

// redMimeType / redPayloadType identify the RFC 2198 RED codec the relay offers
// on the subscriber leg when RED is enabled.
const (
	redMimeType    = "audio/red"
	redPayloadType = 63
)

// NewMediaEngineWithAudioNACK returns an audio-only MediaEngine with stereo Opus
// + NACK. Opus remains first in SDP; Chrome treats RED as an associated repair
// codec and can reject audio NACK or decode silence when RED is offered first.
// Exported so that integration tests can build a client API with the same codec
// configuration as the server — ensuring SDP negotiation succeeds under RELAY_ENABLE_RED.
func NewMediaEngineWithAudioNACK() (*webrtc.MediaEngine, error) {
	m := &webrtc.MediaEngine{}
	for _, uri := range []string{
		"http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
		"http://www.webrtc.org/experiments/rtp-hdrext/abs-capture-time",
	} {
		if err := m.RegisterHeaderExtension(
			webrtc.RTPHeaderExtensionCapability{URI: uri},
			webrtc.RTPCodecTypeAudio,
		); err != nil {
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
	if redEnabled() {
		if err := m.RegisterCodec(webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:     redMimeType,
				ClockRate:    48000,
				Channels:     2,
				SDPFmtpLine:  strconv.Itoa(opusPayloadType) + "/" + strconv.Itoa(opusPayloadType),
				RTCPFeedback: []webrtc.RTCPFeedback{{Type: "nack"}},
			},
			PayloadType: redPayloadType,
		}, webrtc.RTPCodecTypeAudio); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// NewInterceptorRegistry builds the relay interceptor registry for m.
// Subscriber-hop NACK response is intentionally owned by downlink.Downlink;
// registering Pion's responder here would retransmit every requested packet
// twice. Pion's generator remains enabled for source-hop loss recovery.
func NewInterceptorRegistry(m *webrtc.MediaEngine) (*interceptor.Registry, error) {
	registry, _, err := newInterceptorRegistryWithClockLineage(m)
	return registry, err
}

func newInterceptorRegistryWithClockLineage(m *webrtc.MediaEngine) (*interceptor.Registry, *clocklineage.Registry, error) {
	i := &interceptor.Registry{}
	lineage := clocklineage.NewRegistry()
	i.Add(&clocklineage.InterceptorFactory{Registry: lineage})

	nackGenerator, err := nack.NewGeneratorInterceptor()
	if err != nil {
		return nil, nil, err
	}
	i.Add(nackGenerator)

	if err := webrtc.ConfigureRTCPReports(i); err != nil {
		return nil, nil, err
	}
	if err := webrtc.ConfigureSimulcastExtensionHeaders(m); err != nil {
		return nil, nil, err
	}
	if err := webrtc.ConfigureTWCCSender(m, i); err != nil {
		return nil, nil, err
	}
	if err := webrtc.ConfigureTWCCHeaderExtensionSender(m, i); err != nil {
		return nil, nil, err
	}
	return i, lineage, nil
}

func (s *signalPeer) newPeerConnection() (*webrtc.PeerConnection, *clocklineage.Registry, error) {
	iceServers := s.srv.iceServers
	if len(iceServers) == 0 {
		iceServers = []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}}
	}
	pcCfg := webrtc.Configuration{ICEServers: iceServers}

	var (
		pc      *webrtc.PeerConnection
		lineage *clocklineage.Registry
		err     error
	)
	if s.srv.settingEngine != nil {
		// Use the caller-supplied SettingEngine but always build a fresh
		// MediaEngine via NewMediaEngineWithAudioNACK so that RED codec
		// registration is never bypassed (fixes test isolation when RELAY_ENABLE_RED=1).
		m, mErr := NewMediaEngineWithAudioNACK()
		if mErr != nil {
			return nil, nil, mErr
		}
		ir, observedLineage, irErr := newInterceptorRegistryWithClockLineage(m)
		if irErr != nil {
			return nil, nil, irErr
		}
		lineage = observedLineage
		api := webrtc.NewAPI(
			webrtc.WithSettingEngine(*s.srv.settingEngine),
			webrtc.WithMediaEngine(m),
			webrtc.WithInterceptorRegistry(ir),
		)
		pc, err = api.NewPeerConnection(pcCfg)
	} else if s.srv.api != nil {
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
		m, mErr := NewMediaEngineWithAudioNACK()
		if mErr != nil {
			return nil, nil, mErr
		}
		ir, observedLineage, irErr := newInterceptorRegistryWithClockLineage(m)
		if irErr != nil {
			return nil, nil, irErr
		}
		lineage = observedLineage
		api := webrtc.NewAPI(
			webrtc.WithSettingEngine(se),
			webrtc.WithMediaEngine(m),
			webrtc.WithInterceptorRegistry(ir),
		)
		pc, err = api.NewPeerConnection(pcCfg)
	} else {
		m, mErr := NewMediaEngineWithAudioNACK()
		if mErr != nil {
			return nil, nil, mErr
		}
		ir, observedLineage, irErr := newInterceptorRegistryWithClockLineage(m)
		if irErr != nil {
			return nil, nil, irErr
		}
		lineage = observedLineage
		api := webrtc.NewAPI(
			webrtc.WithMediaEngine(m),
			webrtc.WithInterceptorRegistry(ir),
		)
		pc, err = api.NewPeerConnection(pcCfg)
	}
	if err != nil {
		return nil, nil, err
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
	return pc, lineage, nil
}

func (s *signalPeer) send(msg signaling.ServerMessage) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	err := s.conn.WriteJSON(msg)
	if err != nil {
		slog.Warn("websocket write failed", "session_id", s.id, "error", err)
	}
	return err
}

func (s *signalPeer) sendError(code signaling.ErrorCode, message string) {
	s.send(signaling.ServerMessage{Type: signaling.TypeError, Code: string(code), Message: message})
}

// trackSource adapts *webrtc.TrackRemote to graph.SourceSession.
type trackSource struct {
	track    *webrtc.TrackRemote
	timeline *clocklineage.Timeline
}

func (t *trackSource) ReadRTP() (*rtp.Packet, error) {
	pkt, _, err := t.track.ReadRTP()
	return pkt, err
}

func (t *trackSource) ClockLineage() *clocklineage.Timeline { return t.timeline }

// drainPublisherRTCP activates Pion's incoming RTCP interceptor chain. Sender
// Reports are retained by clocklineage.InterceptorFactory; other source-hop
// control packets remain available to Pion's configured interceptors.
func drainPublisherRTCP(receiver *webrtc.RTPReceiver) {
	for {
		if _, _, err := receiver.ReadRTCP(); err != nil {
			return
		}
	}
}
