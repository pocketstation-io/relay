package main

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/signaling"
)

const (
	opusPayloadType      = 111
	opusSamplesPerPacket = 960
	rtpPacketCadence     = 20 * time.Millisecond
	iceConnectTimeout    = 30 * time.Second
)

var validOpusSilence = []byte{0xF8, 0xFF, 0xFE}

func run(relayBase, sessionID, busID, sourceToken, stunURL string, streamDuration time.Duration, logger *slog.Logger) error {
	websocketURL, err := relayWSURL(relayBase)
	if err != nil {
		return err
	}
	configuration := webrtc.Configuration{}
	if stunURL != "" {
		configuration.ICEServers = []webrtc.ICEServer{{URLs: []string{stunURL}}}
	}
	peerConnection, err := webrtc.NewPeerConnection(configuration)
	if err != nil {
		return fmt.Errorf("create peer connection: %w", err)
	}
	defer peerConnection.Close()

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "pocketstation-test-source",
	)
	if err != nil {
		return fmt.Errorf("create track: %w", err)
	}
	if _, err := peerConnection.AddTrack(audioTrack); err != nil {
		return fmt.Errorf("add track: %w", err)
	}
	offer, err := peerConnection.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("create offer: %w", err)
	}
	gatheringComplete := webrtc.GatheringCompletePromise(peerConnection)
	if err := peerConnection.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("set local description: %w", err)
	}
	<-gatheringComplete
	localDescription := peerConnection.LocalDescription()
	if localDescription == nil {
		return fmt.Errorf("local description unavailable after ICE gathering")
	}

	connection, _, err := websocket.DefaultDialer.Dial(websocketURL, nil)
	if err != nil {
		return fmt.Errorf("dial WebSocket: %w", err)
	}
	defer connection.Close()
	if err := connection.WriteJSON(signaling.ClientMessage{
		Type: signaling.TypePublish, Token: sourceToken, BusID: busID, SDPOffer: localDescription.SDP,
	}); err != nil {
		return fmt.Errorf("send PUBLISH: %w", err)
	}

	iceConnected := make(chan struct{})
	var iceConnectedOnce sync.Once
	peerConnection.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		logger.Info("ICE state", "state", state)
		if state == webrtc.ICEConnectionStateConnected {
			iceConnectedOnce.Do(func() { close(iceConnected) })
		}
	})

	outbound := make(chan signaling.ClientMessage, 32)
	go func() {
		for message := range outbound {
			if err := connection.WriteJSON(message); err != nil {
				logger.Error("WebSocket write error", "err", err)
				return
			}
		}
	}()
	peerConnection.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate != nil {
			outbound <- signaling.ClientMessage{Type: signaling.TypeIce, Candidate: candidate.ToJSON().Candidate}
		}
	})

	readDone := make(chan error, 1)
	go readRelayMessages(connection, peerConnection, logger, readDone)
	select {
	case <-iceConnected:
		logger.Info("ICE connected, starting RTP stream")
	case <-time.After(iceConnectTimeout):
		return fmt.Errorf("timed out waiting for ICE connection after %s", iceConnectTimeout)
	case err := <-readDone:
		return fmt.Errorf("WebSocket read loop exited before ICE connected: %w", err)
	}

	ticker := time.NewTicker(rtpPacketCadence)
	defer ticker.Stop()
	deadline := time.Now().Add(streamDuration)
	var sequence uint16
	var timestamp uint32
	for time.Now().Before(deadline) {
		select {
		case <-ticker.C:
			packet := &rtp.Packet{
				Header:  rtp.Header{Version: 2, PayloadType: opusPayloadType, SequenceNumber: sequence, Timestamp: timestamp, SSRC: 0x12345678},
				Payload: validOpusSilence,
			}
			if err := audioTrack.WriteRTP(packet); err != nil {
				logger.Warn("WriteRTP error", "err", err)
			}
			sequence++
			timestamp += opusSamplesPerPacket
		case err := <-readDone:
			return fmt.Errorf("WebSocket closed during stream: %w", err)
		}
	}
	outbound <- signaling.ClientMessage{Type: signaling.TypeLeave}
	close(outbound)
	return nil
}

func readRelayMessages(
	connection *websocket.Conn,
	peerConnection *webrtc.PeerConnection,
	logger *slog.Logger,
	done chan<- error,
) {
	for {
		var message signaling.ServerMessage
		if err := connection.ReadJSON(&message); err != nil {
			done <- err
			return
		}
		switch message.Type {
		case signaling.TypeSDPAnswer:
			if err := peerConnection.SetRemoteDescription(webrtc.SessionDescription{
				Type: webrtc.SDPTypeAnswer, SDP: message.SDPAnswer,
			}); err != nil {
				done <- err
				return
			}
		case signaling.TypeIce:
			if err := peerConnection.AddICECandidate(webrtc.ICECandidateInit{Candidate: message.Candidate}); err != nil {
				logger.Error("add ICE candidate failed", "err", err)
			}
		case signaling.TypeSessionState:
			logger.Info("RelaySession state", "source_active", message.SourceActive, "subscription_count", message.SubscriptionCount)
		case signaling.TypeError:
			logger.Error("relay error", "code", message.Code, "message", message.Message)
		}
	}
}
