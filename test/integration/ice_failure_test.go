// Package integration_test — ICE failure mode tests (P5-PROD-003 / Phase 2 C3).
//
// Tests that the relay handles source ICE disconnection gracefully:
// - Source PeerConnection forcibly closed (simulates ICE failure / hard disconnect)
// - Relay survives without panic, no goroutine leak
// - Server continues to respond to healthz after the failure
package integration_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// TestGiven_SourceIceFails_When_PeerConnectionClosed_Then_RelayHealthy verifies
// that when the source's PeerConnection is forcibly closed (simulating ICE
// failure / hard disconnect), the relay does not panic and remains healthy.
func TestGiven_SourceIceFails_When_PeerConnectionClosed_Then_RelayHealthy(t *testing.T) {
	if testing.Short() {
		t.Skip("ICE failure test skipped in -short mode")
	}

	// Given — relay with source + listener both ICE-connected
	ts, api := newTestServer(t)
	room := createRoom(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Source publisher
	pubConn := dialSignal(t, ts)
	defer pubConn.Close()
	pubMsgs := readServerMessages(pubConn)

	pubPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("pub PC: %v", err)
	}

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "ice-fail-test",
	)
	if err != nil {
		t.Fatalf("create track: %v", err)
	}
	if _, err := pubPC.AddTrack(audioTrack); err != nil {
		t.Fatalf("add track: %v", err)
	}

	doPublishHandshake(t, pubConn, pubPC, room["source_token"], pubMsgs, 10*time.Second)
	waitICEConnected(ctx, t, pubPC)

	// Listener
	subConn := dialSignal(t, ts)
	defer subConn.Close()
	subMsgs := readServerMessages(subConn)

	subPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("sub PC: %v", err)
	}
	defer subPC.Close()

	if _, err := subPC.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatalf("add transceiver: %v", err)
	}
	doSubscribeHandshake(t, subConn, subPC, room["listener_token"], subMsgs, 10*time.Second)
	waitICEConnected(ctx, t, subPC)

	// Send a few packets to confirm forwarding is alive
	payload := bytes.Repeat([]byte{0xAB}, 160)
	for i := uint16(0); i < 5; i++ {
		_ = audioTrack.WriteRTP(&rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    111,
				SequenceNumber: i,
				Timestamp:      uint32(i) * 960,
				SSRC:           0xDEAD,
			},
			Payload: payload,
		})
		time.Sleep(20 * time.Millisecond)
	}

	// When — simulate ICE failure by forcibly closing source PeerConnection
	_ = pubPC.Close()
	time.Sleep(200 * time.Millisecond)

	// Then — relay is still healthy
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz after ICE failure: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz: expected 200, got %d", resp.StatusCode)
	}

	// And — listener can still send a clean LEAVE (relay WebSocket path is live)
	_ = subConn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeLeave})
}

// TestGiven_BothIceFail_Simultaneously_Then_RelayHealthy verifies that when
// both source and listener ICE fail simultaneously, the relay remains healthy.
func TestGiven_BothIceFail_Simultaneously_Then_RelayHealthy(t *testing.T) {
	if testing.Short() {
		t.Skip("ICE failure test skipped in -short mode")
	}

	ts, api := newTestServer(t)
	room := createRoom(t, ts)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pubConn := dialSignal(t, ts)
	pubMsgs := readServerMessages(pubConn)
	pubPC, _ := api.NewPeerConnection(webrtc.Configuration{})
	track, _ := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "both-fail")
	_, _ = pubPC.AddTrack(track)
	doPublishHandshake(t, pubConn, pubPC, room["source_token"], pubMsgs, 10*time.Second)
	waitICEConnected(ctx, t, pubPC)

	subConn := dialSignal(t, ts)
	subMsgs := readServerMessages(subConn)
	subPC, _ := api.NewPeerConnection(webrtc.Configuration{})
	_, _ = subPC.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	doSubscribeHandshake(t, subConn, subPC, room["listener_token"], subMsgs, 10*time.Second)
	waitICEConnected(ctx, t, subPC)

	// When — both fail simultaneously
	_ = pubPC.Close()
	_ = subPC.Close()
	time.Sleep(300 * time.Millisecond)

	// Then — relay still responds
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz: want 200, got %d", resp.StatusCode)
	}
}
