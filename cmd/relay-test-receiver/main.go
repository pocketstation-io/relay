// relay-test-receiver verifies one real WebRTC subscriber attachment and RTP
// packet. It is a deterministic conformance fixture, not a browser claim.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/signaling"
)

func main() {
	relayURL := flag.String("relay", "http://127.0.0.1:4800", "Relay base URL")
	sessionID := flag.String("session", "", "RelaySession ID")
	busID := flag.String("bus", "application", "AudioBus to receive")
	token := flag.String("token", "", "subscriber capability")
	stunURL := flag.String("stun", "", "optional STUN URL for a remote-path test")
	timeout := flag.Duration("timeout", 15*time.Second, "connection and packet deadline")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if *sessionID == "" || *token == "" || *busID == "" {
		logger.Error("--session, --bus, and --token are required")
		os.Exit(2)
	}
	if err := receive(*relayURL, *sessionID, *busID, *token, *stunURL, *timeout, logger); err != nil {
		logger.Error("receiver conformance failed", "error", err)
		os.Exit(1)
	}
	logger.Info("received real RTP", "session_id", *sessionID, "bus_id", *busID)
}

func receive(relayBase, sessionID, busID, token, stunURL string, timeout time.Duration, logger *slog.Logger) error {
	endpoint, err := url.Parse(relayBase)
	if err != nil {
		return fmt.Errorf("parse Relay URL: %w", err)
	}
	switch endpoint.Scheme {
	case "http":
		endpoint.Scheme = "ws"
	case "https":
		endpoint.Scheme = "wss"
	default:
		return fmt.Errorf("unsupported Relay scheme %q", endpoint.Scheme)
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/v1/signal"

	configuration := webrtc.Configuration{}
	if stunURL != "" {
		configuration.ICEServers = []webrtc.ICEServer{{URLs: []string{stunURL}}}
	}
	peerConnection, err := webrtc.NewPeerConnection(configuration)
	if err != nil {
		return fmt.Errorf("create PeerConnection: %w", err)
	}
	defer peerConnection.Close()
	if _, err := peerConnection.AddTransceiverFromKind(
		webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
	); err != nil {
		return fmt.Errorf("add audio transceiver: %w", err)
	}

	connection, _, err := websocket.DefaultDialer.Dial(endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("dial signaling: %w", err)
	}
	defer connection.Close()

	outbound := make(chan signaling.ClientMessage, 32)
	writeError := make(chan error, 1)
	go func() {
		for message := range outbound {
			if writeErr := connection.WriteJSON(message); writeErr != nil {
				writeError <- writeErr
				return
			}
		}
	}()

	connected := make(chan struct{})
	var connectedOnce sync.Once
	peerConnection.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		logger.Info("ICE state", "state", state)
		if state == webrtc.ICEConnectionStateConnected {
			connectedOnce.Do(func() { close(connected) })
		}
	})
	packetReceived := make(chan struct{})
	var packetOnce sync.Once
	peerConnection.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if _, _, readErr := track.ReadRTP(); readErr == nil {
			packetOnce.Do(func() { close(packetReceived) })
		}
	})
	peerConnection.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate != nil {
			outbound <- signaling.ClientMessage{Type: signaling.TypeIce, Candidate: candidate.ToJSON().Candidate}
		}
	})

	offer, err := peerConnection.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("create offer: %w", err)
	}
	if err := peerConnection.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("set local description: %w", err)
	}
	outbound <- signaling.ClientMessage{
		Type: signaling.TypeSubscribe, Token: token, BusID: busID, SDPOffer: offer.SDP,
	}

	readError := make(chan error, 1)
	go func() {
		for {
			var message signaling.ServerMessage
			if readErr := connection.ReadJSON(&message); readErr != nil {
				readError <- readErr
				return
			}
			switch message.Type {
			case signaling.TypeSDPAnswer:
				if setErr := peerConnection.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: message.SDPAnswer}); setErr != nil {
					readError <- setErr
					return
				}
			case signaling.TypeIce:
				if iceErr := peerConnection.AddICECandidate(webrtc.ICECandidateInit{Candidate: message.Candidate}); iceErr != nil {
					readError <- iceErr
					return
				}
			case signaling.TypeError:
				readError <- fmt.Errorf("Relay error %s: %s", message.Code, message.Message)
				return
			}
		}
	}()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for connected == nil || packetReceived == nil {
		select {
		case <-connected:
			connected = nil
		case <-packetReceived:
			packetReceived = nil
		case err := <-readError:
			return fmt.Errorf("read signaling: %w", err)
		case err := <-writeError:
			return fmt.Errorf("write signaling: %w", err)
		case <-deadline.C:
			return fmt.Errorf("timed out after %s", timeout)
		}
	}
	outbound <- signaling.ClientMessage{Type: signaling.TypeLeave}
	return nil
}
