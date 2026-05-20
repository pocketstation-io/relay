// fake-source is a synthetic RTP publisher for the PocketStation relay.
//
// It connects as a WebRTC source peer, negotiates an Opus audio track, and
// sends synthetic RTP packets at 20 ms cadence for the requested duration.
// The binary is used for local integration testing and load testing without
// requiring a real audio device.
//
// Usage:
//
//	fake-source [--relay http://localhost:8080] [--room ROOM_ID] [--token JWT] [--duration 5m]
//
// If --room and --token are both omitted the binary creates a room via POST
// /v1/rooms and prints the listener token to stdout before publishing.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/signaling"
)

const (
	// opusPayloadType is the dynamic payload type for Opus in this relay.
	opusPayloadType = 111
	// opusSamplesPerPacket is the number of samples in a 20 ms Opus frame at
	// 48 kHz (48000 * 0.020 = 960).
	opusSamplesPerPacket = 960
	// rtpPacketCadence is the wall-clock interval between synthetic packets.
	rtpPacketCadence = 20 * time.Millisecond
	// iceConnectTimeout is the maximum time we wait for ICE to reach Connected.
	iceConnectTimeout = 30 * time.Second
	// rtpPayloadByte is the fill value for synthetic Opus payloads.
	rtpPayloadByte = 0xAB
	// rtpPayloadSize is the size of each synthetic Opus payload in bytes.
	// 160 bytes is a plausible 20 ms Opus frame at low bitrate.
	rtpPayloadSize = 160
)

func main() {
	relayURL := flag.String("relay", "http://localhost:8080", "relay base URL (http/https)")
	roomID := flag.String("room", "", "room ID (omit to create a new room)")
	token := flag.String("token", "", "source JWT (omit to create a new room)")
	duration := flag.Duration("duration", 5*time.Minute, "how long to stream")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if (*roomID == "") != (*token == "") {
		logger.Error("--room and --token must both be provided, or both omitted")
		os.Exit(1)
	}

	var sourceToken string
	if *roomID == "" {
		var err error
		*roomID, sourceToken, err = createRoom(*relayURL, logger)
		if err != nil {
			logger.Error("failed to create room", "err", err)
			os.Exit(1)
		}
	} else {
		sourceToken = *token
	}

	logger.Info("publishing to room", "room_id", *roomID, "duration", *duration)

	if err := run(*relayURL, *roomID, sourceToken, *duration, logger); err != nil {
		logger.Error("publisher exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("clean shutdown")
}

// createRoom POSTs to /v1/rooms and returns the room_id and source_token.
func createRoom(relayBase string, logger *slog.Logger) (roomID, sourceToken string, err error) {
	resp, err := http.Post(relayBase+"/v1/rooms", "application/json", bytes.NewReader(nil))
	if err != nil {
		return "", "", fmt.Errorf("POST /v1/rooms: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}
	var payload struct {
		RoomID        string `json:"room_id"`
		SourceToken   string `json:"source_token"`
		ListenerToken string `json:"listener_token"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", "", fmt.Errorf("decode response: %w", err)
	}
	logger.Info("created room", "room_id", payload.RoomID, "listener_token", payload.ListenerToken)
	return payload.RoomID, payload.SourceToken, nil
}

// relayWSURL converts an http(s) URL to its ws(s) equivalent.
func relayWSURL(relayBase string) (string, error) {
	u, err := url.Parse(relayBase)
	if err != nil {
		return "", fmt.Errorf("parse relay URL: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/signal"
	return u.String(), nil
}

func run(relayBase, roomID, sourceToken string, streamDuration time.Duration, logger *slog.Logger) error {
	wsURL, err := relayWSURL(relayBase)
	if err != nil {
		return err
	}

	// --- WebRTC setup ---
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	})
	if err != nil {
		return fmt.Errorf("create peer connection: %w", err)
	}
	defer pc.Close()

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "pocketstation-fake",
	)
	if err != nil {
		return fmt.Errorf("create track: %w", err)
	}
	if _, err := pc.AddTrack(audioTrack); err != nil {
		return fmt.Errorf("add track: %w", err)
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("create offer: %w", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("set local description: %w", err)
	}

	// --- WebSocket connection ---
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial WebSocket: %w", err)
	}
	defer conn.Close()

	// Send PUBLISH with the SDP offer.
	publishMsg := signaling.ClientMessage{
		Type:     signaling.TypePublish,
		Token:    sourceToken,
		SDPOffer: offer.SDP,
	}
	if err := conn.WriteJSON(publishMsg); err != nil {
		return fmt.Errorf("send PUBLISH: %w", err)
	}
	logger.Info("sent PUBLISH, waiting for SDP_ANSWER")

	// iceConnected signals when ICE reaches the Connected state.
	iceConnected := make(chan struct{})
	var iceConnectedOnce struct {
		done bool
		ch   chan struct{}
	}
	iceConnectedOnce.ch = iceConnected

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		logger.Info("ICE state", "state", state)
		if state == webrtc.ICEConnectionStateConnected && !iceConnectedOnce.done {
			iceConnectedOnce.done = true
			close(iceConnectedOnce.ch)
		}
	})

	// Wire outgoing ICE candidates to the WebSocket.
	// wmu serialises writes from this goroutine and the read goroutine below.
	wsCh := make(chan signaling.ClientMessage, 32)
	go func() {
		for msg := range wsCh {
			if err := conn.WriteJSON(msg); err != nil {
				logger.Error("WebSocket write error", "err", err)
				return
			}
		}
	}()

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		wsCh <- signaling.ClientMessage{
			Type:      signaling.TypeIce,
			Candidate: c.ToJSON().Candidate,
		}
	})

	// Read loop: handle SDP_ANSWER and ICE from the relay.
	readDone := make(chan error, 1)
	go func() {
		for {
			var msg signaling.ServerMessage
			if err := conn.ReadJSON(&msg); err != nil {
				readDone <- err
				return
			}
			switch msg.Type {
			case signaling.TypeSDPAnswer:
				logger.Info("received SDP_ANSWER")
				answer := webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer,
					SDP:  msg.SDPAnswer,
				}
				if err := pc.SetRemoteDescription(answer); err != nil {
					logger.Error("set remote description failed", "err", err)
					readDone <- err
					return
				}
			case signaling.TypeIce:
				if err := pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: msg.Candidate}); err != nil {
					logger.Error("add ICE candidate failed", "err", err)
				}
			case signaling.TypeRoomState:
				logger.Info("room state", "source_active", msg.SourceActive, "listeners", msg.ListenerCount)
			case signaling.TypeError:
				logger.Error("relay error", "code", msg.Code, "message", msg.Message)
			}
		}
	}()

	// Wait for ICE connected.
	select {
	case <-iceConnected:
		logger.Info("ICE connected, starting RTP stream")
	case <-time.After(iceConnectTimeout):
		return fmt.Errorf("timed out waiting for ICE connection after %s", iceConnectTimeout)
	case err := <-readDone:
		return fmt.Errorf("WebSocket read loop exited before ICE connected: %w", err)
	}

	// --- RTP send loop ---
	payload := bytes.Repeat([]byte{rtpPayloadByte}, rtpPayloadSize)
	var seqNum uint16
	var timestamp uint32
	ticker := time.NewTicker(rtpPacketCadence)
	defer ticker.Stop()
	deadline := time.Now().Add(streamDuration)

	for time.Now().Before(deadline) {
		select {
		case <-ticker.C:
			pkt := &rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					PayloadType:    opusPayloadType,
					SequenceNumber: seqNum,
					Timestamp:      timestamp,
					SSRC:           0x12345678,
				},
				Payload: payload,
			}
			if err := audioTrack.WriteRTP(pkt); err != nil {
				logger.Warn("WriteRTP error", "err", err)
			}
			seqNum++
			timestamp += opusSamplesPerPacket
		case err := <-readDone:
			return fmt.Errorf("WebSocket closed during stream: %w", err)
		}
	}

	logger.Info("stream duration elapsed, sending LEAVE")

	// Signal end via wsCh so it is serialised with candidate messages.
	wsCh <- signaling.ClientMessage{Type: signaling.TypeLeave}
	close(wsCh)

	return nil
}
