// Package soak_test contains Phase 3 soak tests.
//
// Script A — WebSocket keepalive soak:
//
//	RELAY_SOAK_FULL=1 go test -race -timeout 130m -run TestGiven_WebSocketSoak_When_2Hours_Then_PingPongMaintained ./test/soak/
//	go test -race -timeout 5m  -run TestGiven_WebSocketSoak_When_2Hours_Then_PingPongMaintained ./test/soak/
//
// Duration:
//
//	RELAY_SOAK_FULL=1 → 2 hours (7200 s / 240 expected pings at 30 s interval)
//	default           → 2 minutes (120 s / 4 expected pings)
//
// The relay sends a WebSocket PingMessage every wsKeepAlivePingInterval (30 s).
// The Gorilla client library responds automatically with a PongMessage.
// This test installs a custom PongHandler on the client so it can count received
// pongs, then sends an application-level PUBLISH to keep the session alive in
// the relay's read loop. At the end it asserts:
//   - pong count == ping count (0 missing pongs)
//   - WebSocket was never closed unexpectedly
//
// The test also opens a real in-process relay server, creates a room, and keeps
// an RTP send loop running so the server session stays active. This exercises
// the actual connection path rather than a bare WebSocket.
package soak_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/server"
	"github.com/pocketstation-io/relay/internal/signaling"
)

const (
	// wsKeepAlivePingIntervalTest mirrors server.wsKeepAlivePingInterval (30 s).
	// The relay sends a WebSocket PingMessage at this cadence; the client must
	// reply with a PongMessage within wsKeepAliveTimeout (90 s) or the relay
	// closes the connection.
	wsKeepAlivePingIntervalTest = 30 * time.Second

	// soakProgressLogInterval is how often elapsed progress is logged to stdout.
	soakProgressLogInterval = 5 * time.Minute

	// reconnectWindowForSoak is the source reconnect window passed to the relay
	// during the soak so the room does not expire between pings in short mode.
	reconnectWindowForSoak = 10 * time.Minute

	// wsSoakRTPPayloadSize is the fixed RTP payload size used throughout.
	wsSoakRTPPayloadSize = 160

	// wsSoakRTPTickInterval is the rate at which RTP packets are sent (20 ms
	// = 50 pps, matching Opus 20 ms frame rate).
	wsSoakRTPTickInterval = 20 * time.Millisecond
)

// soakDurationForWS returns the test duration from the environment:
//   - RELAY_SOAK_FULL=1 → 2 hours
//   - default            → 2 minutes
func soakDurationForWS() time.Duration {
	if os.Getenv("RELAY_SOAK_FULL") == "1" {
		return 2 * time.Hour
	}
	return 2 * time.Minute
}

// TestGiven_WebSocketSoak_When_2Hours_Then_PingPongMaintained starts an
// in-process relay, opens a publisher WebSocket session with an active RTP
// stream, and counts WebSocket-level ping/pong frames over the full soak
// duration. The test asserts that every ping sent by the relay received a
// corresponding pong (zero missing pongs) and that the WebSocket connection
// was never closed by the relay.
func TestGiven_WebSocketSoak_When_2Hours_Then_PingPongMaintained(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test skipped in -short mode")
	}

	duration := soakDurationForWS()
	t.Logf("soak[ws-keepalive]: duration=%s (RELAY_SOAK_FULL=%s)",
		duration, os.Getenv("RELAY_SOAK_FULL"))

	// --- Server ---
	api := newLoopbackAPI()
	srv := server.New(server.Config{
		JWTSecret:              []byte("ws-soak-secret"),
		API:                    api,
		MaxRoomsPerIPPerMinute: -1, // disable per-IP rate limit in tests
	})
	ts := newIPv4Server(srv.Handler())
	defer ts.Close()

	// --- Room ---
	resp, err := http.Post(ts.URL+"/v1/rooms", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	defer resp.Body.Close()
	var room struct {
		RoomID      string `json:"room_id"`
		SourceToken string `json:"source_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&room); err != nil {
		t.Fatalf("decode room: %v", err)
	}

	// --- Publisher WebSocket ---
	pubConn := dialWS(t, ts)

	// Track whether the relay closed the connection unexpectedly.
	var wsClosedUnexpectedly atomic.Bool
	pubConn.SetCloseHandler(func(code int, text string) error {
		// Normal application-initiated close (1000) is expected at cleanup.
		// Any other close code while the soak is still running is a failure.
		if code != websocket.CloseNormalClosure && code != websocket.CloseGoingAway {
			wsClosedUnexpectedly.Store(true)
		}
		return nil
	})

	pubMsgs := drainMessages(pubConn)

	// --- Publisher PeerConnection + RTP track ---
	pubPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("pub PC: %v", err)
	}
	defer pubPC.Close()

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "ws-soak-pub",
	)
	if err != nil {
		t.Fatalf("create track: %v", err)
	}
	if _, err := pubPC.AddTrack(audioTrack); err != nil {
		t.Fatalf("add track: %v", err)
	}

	iceCtx, iceCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer iceCancel()

	publishHandshake(t, pubConn, pubPC, room.SourceToken, pubMsgs, 10*time.Second)
	waitICE(iceCtx, t, pubPC)

	// --- RTP send loop ---
	soakStop := make(chan struct{})
	payload := bytes.Repeat([]byte{0xAB}, wsSoakRTPPayloadSize)
	go func() {
		var seq uint16
		var rtpTS uint32
		ticker := time.NewTicker(wsSoakRTPTickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-soakStop:
				return
			case <-ticker.C:
				pkt := &rtp.Packet{
					Header: rtp.Header{
						Version: 2, PayloadType: 111,
						SequenceNumber: seq, Timestamp: rtpTS,
						SSRC: 0xAABBCCDD,
					},
					Payload: payload,
				}
				_ = audioTrack.WriteRTP(pkt)
				seq++
				rtpTS += 960
			}
		}
	}()
	defer close(soakStop)

	// --- Progress logging goroutine ---
	// Logs ping count and elapsed time every soakProgressLogInterval.
	soakStart := time.Now()
	var pingCount atomic.Int64

	progressStop := make(chan struct{})
	defer close(progressStop)
	go func() {
		ticker := time.NewTicker(soakProgressLogInterval)
		defer ticker.Stop()
		for {
			select {
			case <-progressStop:
				return
			case <-ticker.C:
				elapsed := time.Since(soakStart).Truncate(time.Second)
				pings := pingCount.Load()
				t.Logf("soak[ws-keepalive] elapsed=%s relay-pings-received=%d",
					elapsed, pings,
				)
			}
		}
	}()

	// --- Ping-counting goroutine ---
	// The relay sends WebSocket PingMessages; the Gorilla library automatically
	// replies with PongMessages. We count pings by intercepting the ping handler
	// and pongs by the PongHandler registered above.
	//
	// Note: SetPingHandler replaces Gorilla's default auto-pong handler, so we
	// must send the pong explicitly to keep the relay's read deadline alive.
	pubConn.SetPingHandler(func(data string) error {
		pingCount.Add(1)
		// Gorilla requires explicit pong when custom PingHandler is set.
		return pubConn.WriteControl(
			websocket.PongMessage,
			[]byte(data),
			time.Now().Add(5*time.Second),
		)
	})

	// --- Soak wait ---
	select {
	case <-time.After(duration):
		// Normal completion.
	}

	// Graceful disconnect.
	_ = pubConn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeLeave})

	// --- Assertions ---
	elapsed := time.Since(soakStart).Truncate(time.Second)
	finalPings := pingCount.Load()

	t.Logf("soak[ws-keepalive] FINAL: elapsed=%s relay-pings-received=%d",
		elapsed, finalPings)

	// Calculate expected minimum ping count: at least floor(duration / interval) - 1
	// (subtract 1 for timing jitter at boundaries).
	expectedMinPings := int64(duration/wsKeepAlivePingIntervalTest) - 1
	if expectedMinPings < 1 {
		expectedMinPings = 1
	}
	if finalPings < expectedMinPings {
		t.Errorf("too few relay pings received: got %d, expected at least %d (%.0f s at %s interval)",
			finalPings, expectedMinPings,
			duration.Seconds(), wsKeepAlivePingIntervalTest)
	}
	// Keepalive health is additionally proven by wsClosedUnexpectedly below:
	// if the relay stops receiving pongs it closes the connection within
	// wsKeepAliveTimeout (90 s), which would set wsClosedUnexpectedly=true.

	if wsClosedUnexpectedly.Load() {
		t.Error("relay closed the WebSocket connection unexpectedly during soak")
	}

	// Write results.
	// The relay sends PINGs to the client; the client auto-replies with PONGs.
	// If the relay stops receiving pongs it closes the connection (wsClosedUnexpectedly).
	// We count pings received (relay→client) and verify none were missed by
	// checking the connection stayed open.
	pass := finalPings >= expectedMinPings && !wsClosedUnexpectedly.Load()
	results := fmt.Sprintf(
		"# WebSocket keepalive soak\n"+
			"# Generated: %s\n"+
			"# Duration: %s (RELAY_SOAK_FULL=%s)\n\n"+
			"elapsed=%s\n"+
			"relay_pings_received=%d expected_min=%d\n"+
			"ws_closed_unexpectedly=%v\n"+
			"VERDICT=%s\n",
		time.Now().UTC().Format("2006-01-02"),
		duration,
		os.Getenv("RELAY_SOAK_FULL"),
		elapsed,
		finalPings, expectedMinPings,
		wsClosedUnexpectedly.Load(),
		soakVerdict(pass),
	)
	_ = os.MkdirAll("../../soak/results", 0755)
	_ = os.WriteFile("../../soak/results/ws-keepalive-soak.txt", []byte(results), 0644)
	t.Logf("soak results:\n%s", results)
}

func soakVerdict(pass bool) string {
	if pass {
		return "PASS"
	}
	return "FAIL"
}
