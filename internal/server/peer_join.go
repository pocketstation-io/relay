package server

import (
	"log/slog"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/media/clocklineage"
	"github.com/pocketstation-io/relay/internal/media/downlink"
	"github.com/pocketstation-io/relay/internal/notifications/webhook"
	"github.com/pocketstation-io/relay/internal/session"
	"github.com/pocketstation-io/relay/internal/signaling"
)

func (peer *signalPeer) handleJoin(message signaling.ClientMessage) {
	if peer.pc != nil {
		peer.sendError(signaling.ErrCodeAlreadyJoined, "session has already joined a room")
		return
	}

	expectedRole := auth.RoleSubscriber
	if message.Type == signaling.TypePublish {
		expectedRole = auth.RoleSource
	}
	claims, err := peer.srv.verifyCapability(message.Token, expectedRole)
	if err != nil {
		slog.Warn("bad token", "session_id", peer.id, "error", err)
		peer.sendError(signaling.ErrCodeBadToken, err.Error())
		return
	}

	if message.Type == signaling.TypePublish && claims.Role != auth.RoleSource {
		peer.sendError(signaling.ErrCodeRoleMismatch, "PUBLISH requires a source token")
		return
	}
	if message.Type == signaling.TypeSubscribe && claims.Role != auth.RoleSubscriber {
		peer.sendError(signaling.ErrCodeRoleMismatch, "SUBSCRIBE requires a subscriber token")
		return
	}

	sessionID := claims.EffectiveSessionID()
	if sessionID == "" {
		sessionID = message.EffectiveSessionID()
	}
	relaySession, _, accepted := peer.srv.relaySessions.GetOrCreateWithinLimit(
		sessionID,
		peer.srv.maxRooms,
	)
	if !accepted {
		peer.sendError(signaling.ErrCodeRoomLimitExceeded, "relay has reached its RelaySession limit")
		return
	}
	peer.room = relaySession
	peer.srv.bindControlState(relaySession)
	peer.srv.queueControlState(relaySession)
	peer.role = claims.Role

	var publishBuses *publishBusPlan
	var busID session.BusID
	if message.Type == signaling.TypePublish {
		publishBuses, err = newPublishBusPlan(message, claims)
		if err != nil {
			peer.sendError(signaling.ErrCodeBadRequest, err.Error())
			return
		}
		busID = publishBuses.primaryBusID()
	} else {
		if len(message.PublishBuses) != 0 {
			peer.sendError(signaling.ErrCodeBadRequest, "publish_buses is valid only for PUBLISH")
			return
		}
		busID = message.BusID
		if busID == "" {
			busID = claims.BusID
		}
		if busID == "" {
			busID = session.BusMix
		}
	}
	peer.busID = busID

	if message.Type == signaling.TypePublish && message.Public {
		relaySession.Public.Store(true)
	}
	if message.GraphID != "" {
		relaySession.GraphID = message.GraphID
	}

	slog.Info("session joined",
		"session_id", peer.id,
		"relay_session_id", sessionID,
		"role", string(claims.Role),
		"bus_id", busID,
		"publish_bus_count", len(message.PublishBuses),
	)

	connection, lineage, err := peer.newPeerConnection()
	if err != nil {
		peer.sendError(signaling.ErrCodePCError, "failed to create peer connection")
		return
	}
	peer.pc = connection
	peer.lineage = lineage
	var localDescriptionSDP string

	switch message.Type {
	case signaling.TypePublish:
		if message.SDPOffer == "" {
			if _, err := connection.AddTransceiverFromKind(
				webrtc.RTPCodecTypeAudio,
				webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
			); err != nil {
				peer.sendError(signaling.ErrCodeTrackError, "failed to add publisher audio transceiver")
				return
			}
		}
		connection.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
			trackBusID, claimErr := publishBuses.claim(track.StreamID())
			if claimErr != nil {
				peer.sendError(signaling.ErrCodeTrackError, claimErr.Error())
				_ = connection.Close()
				return
			}
			var timeline *clocklineage.Timeline
			if lineage != nil {
				timeline = lineage.Remote(uint32(track.SSRC()))
			}
			go drainPublisherRTCP(receiver)
			if sourceErr := relaySession.SetSource(
				trackBusID,
				busRoleFor(trackBusID),
				&trackSource{track: track, timeline: timeline},
				func() { _ = connection.Close() },
			); sourceErr != nil {
				peer.sendError(signaling.ErrCodeTrackError, sourceErr.Error())
				_ = connection.Close()
				return
			}
			peer.srv.broadcastSessionState(relaySession)
			peer.srv.webhookDispatcher.Send(webhook.Event{
				Type:      webhook.EventSessionStarted,
				RoomID:    sessionID,
				SessionID: peer.id,
			})
		})

	case signaling.TypeSubscribe:
		if !relaySession.TryReserveSlot(peer.srv.maxSubscribersPerRoom) {
			peer.sendError(signaling.ErrCodeListenerLimitExceeded, "session has reached maximum subscriber count")
			return
		}
		peer.slotReserved.Store(true)

		listenerMime := webrtc.MimeTypeOpus
		if redEnabled() {
			listenerMime = redMimeType
		}
		audioTrack, err := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: listenerMime},
			"audio",
			"pocketstation",
		)
		if err != nil {
			peer.sendError(signaling.ErrCodeTrackError, "failed to create audio track")
			return
		}
		sender, err := connection.AddTrack(audioTrack)
		if err != nil {
			peer.sendError(signaling.ErrCodeTrackError, "failed to add audio track to peer connection")
			return
		}

		hintState := peer.srv.roomCodecHintState(sessionID)
		restartState := peer.srv.roomICERestartState(sessionID)
		connection.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
			slog.Info("subscriber pc state", "session_id", peer.id, "state", state.String())
			if state != webrtc.PeerConnectionStateConnected {
				return
			}
			if !peer.slotReserved.Swap(false) {
				slog.Warn("subscriber connected but slot already released", "session_id", peer.id)
				return
			}
			relaySession.ReleaseSlot()

			if peer.subscriptionRegistered.Swap(true) {
				return
			}
			var writer session.PacketWriter = audioTrack
			if redEnabled() {
				writer = newREDListener(audioTrack, opusPayloadType)
			}

			delivery := downlink.NewForwardingDownlink(peer.id, writer, nil)
			delivery.ConfigureExtensions(localDescriptionSDP)
			if lineage != nil {
				parameters := sender.GetParameters()
				if len(parameters.Encodings) > 0 {
					delivery.SetSenderTimeline(lineage.Local(uint32(parameters.Encodings[0].SSRC)))
				}
			}

			onReceiverReport := func(fractionLost float64) {
				observedRTT := false
				for _, stat := range connection.GetStats() {
					remote, found := stat.(webrtc.RemoteInboundRTPStreamStats)
					if found && remote.RoundTripTimeMeasurements > 0 {
						delivery.ObserveRTT(time.Duration(remote.RoundTripTime * float64(time.Second)))
						observedRTT = true
						break
					}
				}
				if !observedRTT {
					rttUs := delivery.Stats().RttLastUs.Load()
					if rttUs >= 0 {
						delivery.ObserveRTT(time.Duration(rttUs) * time.Microsecond)
					}
				}
				hint := bitrateForLoss(fractionLost)
				peer.srv.maybeEmitCodecHint(sessionID, hint, hintState)
				peer.srv.maybeEmitICERestart(sessionID, fractionLost, restartState)
			}
			var onNACK downlink.NackCallback
			if !redEnabled() {
				onNACK = delivery.HandleNACK
			}
			delivery.SetFeedback(downlink.StartFeedbackReader(
				sender,
				delivery.Stats(),
				onReceiverReport,
				onNACK,
			))

			if err := relaySession.AddBusSubscription(peer.id, peer.busID, delivery); err != nil {
				peer.subscriptionRegistered.Store(false)
				delivery.StopForwarding()
				slog.Warn("subscription registration failed", "session_id", peer.id, "err", err)
				return
			}
			slog.Info("subscription registered", "session_id", peer.id)
			peer.srv.Metrics.ListenerCount.Add(1)
			peer.srv.broadcastSessionState(relaySession)
			peer.observeOutboundRTP(connection, relaySession)
		})

		if existingKey := relaySession.GetKey(); existingKey != "" {
			_ = peer.send(signaling.ServerMessage{
				Type:      signaling.TypeKeyExchange,
				SFrameKey: existingKey,
			})
		}
	}

	if message.SDPOffer == "" {
		offer, err := connection.CreateOffer(nil)
		if err != nil {
			peer.sendError(signaling.ErrCodeSDPError, "failed to create offer")
			return
		}
		if err := connection.SetLocalDescription(offer); err != nil {
			peer.sendError(signaling.ErrCodeSDPError, "failed to set local description")
			return
		}
		localDescriptionSDP = offer.SDP
		_ = peer.send(signaling.ServerMessage{
			Type:      signaling.TypeSDPOffer,
			SDPOffer:  offer.SDP,
			SessionID: sessionID,
			BusID:     busID,
		})
	} else {
		offer := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: message.SDPOffer}
		if err := connection.SetRemoteDescription(offer); err != nil {
			peer.sendError(signaling.ErrCodeSDPError, "failed to set remote description")
			return
		}
		answer, err := connection.CreateAnswer(nil)
		if err != nil {
			peer.sendError(signaling.ErrCodeSDPError, "failed to create answer")
			return
		}
		if err := connection.SetLocalDescription(answer); err != nil {
			peer.sendError(signaling.ErrCodeSDPError, "failed to set local description")
			return
		}
		localDescriptionSDP = answer.SDP
		_ = peer.send(signaling.ServerMessage{
			Type:      signaling.TypeSDPAnswer,
			SDPAnswer: answer.SDP,
			SessionID: sessionID,
			BusID:     busID,
		})
	}
	_ = peer.send(signaling.ServerMessage{
		Type:              signaling.TypeSessionState,
		SourceActive:      relaySession.SourceActive(),
		SubscriptionCount: relaySession.SubscriptionCount(),
		Codec:             "opus",
		SessionID:         sessionID,
	})

	for _, candidate := range peer.pendingICE {
		_ = peer.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: candidate})
	}
	peer.pendingICE = nil
}

func (peer *signalPeer) observeOutboundRTP(
	connection *webrtc.PeerConnection,
	relaySession *session.RelaySession,
) {
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-peer.done:
				return
			case <-ticker.C:
			}
			state := connection.ConnectionState()
			if state == webrtc.PeerConnectionStateDisconnected ||
				state == webrtc.PeerConnectionStateFailed ||
				state == webrtc.PeerConnectionStateClosed {
				return
			}
			for _, stat := range connection.GetStats() {
				outbound, found := stat.(webrtc.OutboundRTPStreamStats)
				if !found {
					continue
				}
				packets, _, _ := relaySession.PacketStats()
				slog.Info("pion_tx",
					"session_id", peer.id,
					"ssrc", outbound.SSRC,
					"packets_sent", outbound.PacketsSent,
					"bytes_sent", outbound.BytesSent,
					"room_total", packets,
				)
			}
		}
	}()
}
