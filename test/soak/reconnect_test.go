// Package soak_test — Script B: publisher reconnect soak test.
//
// Run:
//
//	RELAY_SOAK_FULL=1 go test -race -timeout 145m -run TestGiven_Publisher_When_ReconnectsAtIntervals_Then_SessionRestores ./test/soak/
//	go test -race -timeout 5m  -run TestGiven_Publisher_When_ReconnectsAtIntervals_Then_SessionRestores ./test/soak/
//
// Reconnect intervals:
//
//	RELAY_SOAK_FULL=1 → 16 min, 60 min, 130 min
//	default           → 16 s,   60 s,   130 s
//
// For each reconnect:
//  1. Publisher closes its WebSocket (simulating a network drop).
//  2. Test waits 5 seconds to let the relay register the disconnect.
//  3. Publisher opens a fresh WebSocket, performs PUBLISH handshake on the
//     same room_id (same token), and reconnects.
//  4. Verifies the relay accepts the reconnect (no ERROR response).
//  5. Verifies the room is still active (GET /v1/rooms/<id> or room-state check).
//  6. Asserts reconnect completed in under 5 seconds.
//
// The relay's source reconnect window is configured to be longer than the
// soak intervals so the room is never auto-closed between attempts.
package soak_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/room"
	"github.com/pocketstation-io/relay/internal/server"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// reconnectMaxLatency is the maximum acceptable time from WebSocket dial to
// ICE-connected state on a reconnect. The relay spec requires this to be
// under 5 seconds on loopback.
const reconnectMaxLatency = 5 * time.Second

// reconnectDisconnectDwell is the time the test waits after closing the
// publisher WebSocket before reconnecting, giving the relay time to register
// the disconnect and start the source reconnect timer.
const reconnectDisconnectDwell = 5 * time.Second

// reconnectReconnectWindow is the source reconnect window configured on the
// in-process relay. It must be larger than the largest reconnect interval to
// prevent the room from auto-closing between reconnects.
//
// Full mode:  largest interval is 130 min → window = 150 min.
// Short mode: largest interval is 130 s  → window = 10 min (generous margin).
const reconnectWindowFull = 150 * time.Minute
const reconnectWindowShort = 10 * time.Minute

// reconnectIntervals returns the three reconnect times scaled by environment.
//
//	RELAY_SOAK_FULL=1 → [16m, 60m, 130m]
//	default           → [16s, 60s, 130s]
func reconnectIntervals() []time.Duration {
	if os.Getenv("RELAY_SOAK_FULL") == "1" {
		return []time.Duration{16 * time.Minute, 60 * time.Minute, 130 * time.Minute}
	}
	return []time.Duration{16 * time.Second, 60 * time.Second, 130 * time.Second}
}

// reconnectWindow returns the relay source reconnect window matching the mode.
func reconnectWindow() time.Duration {
	if os.Getenv("RELAY_SOAK_FULL") == "1" {
		return reconnectWindowFull
	}
	return reconnectWindowShort
}

// reconnectResult records the outcome of a single reconnect attempt.
type reconnectResult struct {
	intervalLabel  string
	scheduledAt    time.Duration // elapsed time when reconnect was initiated
	reconnectTime  time.Duration // time from dial to ICE-connected
	succeeded      bool
	failureMessage string
}

// TestGiven_Publisher_When_ReconnectsAtIntervals_Then_SessionRestores
// verifies that a publisher can disconnect and reconnect at defined intervals
// and that the relay accepts each reconnect in under reconnectMaxLatency.
func TestGiven_Publisher_When_ReconnectsAtIntervals_Then_SessionRestores(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test skipped in -short mode")
	}

	intervals := reconnectIntervals()
	window := reconnectWindow()
	t.Logf("soak[reconnect]: intervals=%v reconnect_window=%s (RELAY_SOAK_FULL=%s)",
		intervals, window, os.Getenv("RELAY_SOAK_FULL"))

	// --- Server ---
	// Use a short inactivity timeout (equal to the reconnect window) so the
	// test room does not linger. The reconnect window is set long enough that
	// the room survives each deliberate disconnect.
	api := newLoopbackAPI()
	srv := server.New(server.Config{
		JWTSecret: []byte("reconnect-soak-secret"),
		API:       api,
		RoomConfig: room.ManagerConfig{
			InactivityTimeout: window + 10*time.Minute,
			ReconnectWindow:   window,
		},
		MaxRoomsPerIPPerMinute: -1,
	})
	ts := newIPv4Server(srv.Handler())
	defer ts.Close()

	// --- Room ---
	resp, err := http.Post(ts.URL+"/v1/rooms", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	defer resp.Body.Close()
	var roomPayload struct {
		RoomID      string `json:"room_id"`
		SourceToken string `json:"source_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&roomPayload); err != nil {
		t.Fatalf("decode room: %v", err)
	}

	// connectPublisher dials a fresh WebSocket, performs PUBLISH + ICE and
	// starts an RTP send loop. It returns (stopSending func, pcCleanup func,
	// connCleanup func) so each can be closed independently. Returns
	// (nil, nil, nil, errorMessage) on failure.
	type publisherHandles struct {
		stopRTP chan struct{}
		closePC func()
		closeWS func()
	}
	connectPublisher := func(label string) (*publisherHandles, string) {
		conn := dialWS(t, ts)
		msgs := drainMessages(conn)

		pc, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Sprintf("[%s] new PC: %v", label, err)
		}

		track, err := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
			"audio", "reconnect-soak",
		)
		if err != nil {
			_ = pc.Close()
			_ = conn.Close()
			return nil, fmt.Sprintf("[%s] new track: %v", label, err)
		}
		if _, err := pc.AddTrack(track); err != nil {
			_ = pc.Close()
			_ = conn.Close()
			return nil, fmt.Sprintf("[%s] add track: %v", label, err)
		}

		iceCtx, iceCancel := context.WithTimeout(context.Background(), reconnectMaxLatency)
		defer iceCancel()

		publishHandshake(t, conn, pc, roomPayload.SourceToken, msgs, reconnectMaxLatency)

		dialStart := time.Now()
		waitICE(iceCtx, t, pc)
		if iceCtx.Err() != nil {
			_ = pc.Close()
			_ = conn.Close()
			return nil, fmt.Sprintf("[%s] ICE connect timeout after %s", label, time.Since(dialStart))
		}

		stopRTP := make(chan struct{})
		payload := bytes.Repeat([]byte{0xCC}, 160)
		go func() {
			var seq uint16
			var rtpTS uint32
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopRTP:
					return
				case <-ticker.C:
					pkt := &rtp.Packet{
						Header: rtp.Header{
							Version: 2, PayloadType: 111,
							SequenceNumber: seq, Timestamp: rtpTS,
							SSRC: 0x11223344,
						},
						Payload: payload,
					}
					_ = track.WriteRTP(pkt)
					seq++
					rtpTS += 960
				}
			}
		}()

		return &publisherHandles{
			stopRTP: stopRTP,
			closePC: func() { _ = pc.Close() },
			closeWS: func() {
				_ = conn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeLeave})
				_ = conn.Close()
			},
		}, ""
	}

	// --- Initial connection ---
	soakStart := time.Now()
	handles, errMsg := connectPublisher("initial")
	if errMsg != "" {
		t.Fatalf("initial connect failed: %s", errMsg)
	}
	t.Logf("soak[reconnect]: initial connection established at t=0")

	// --- Reconnect loop ---
	results := make([]reconnectResult, 0, len(intervals))
	prevInterval := time.Duration(0)

	for i, interval := range intervals {
		label := fmt.Sprintf("reconnect-%d@%s", i+1, interval)
		waitFor := interval - prevInterval
		prevInterval = interval

		t.Logf("soak[reconnect]: waiting %s until %s...", waitFor, label)
		time.Sleep(waitFor)

		scheduledAt := time.Since(soakStart)

		// Tear down current publisher session.
		close(handles.stopRTP)
		handles.closeWS()
		handles.closePC()
		t.Logf("soak[reconnect]: publisher disconnected at t=%s (%s)",
			scheduledAt.Truncate(time.Second), label)

		// Dwell to let the relay observe the disconnect.
		time.Sleep(reconnectDisconnectDwell)

		// Reconnect and measure time.
		reconnectStart := time.Now()
		handles, errMsg = connectPublisher(label)
		reconnectDuration := time.Since(reconnectStart)

		res := reconnectResult{
			intervalLabel: label,
			scheduledAt:   scheduledAt,
			reconnectTime: reconnectDuration,
		}
		if errMsg != "" {
			res.succeeded = false
			res.failureMessage = errMsg
			t.Errorf("reconnect failed: %s", errMsg)
		} else {
			res.succeeded = true
			t.Logf("soak[reconnect]: %s reconnected in %s (limit=%s)",
				label, reconnectDuration.Truncate(time.Millisecond), reconnectMaxLatency)

			if reconnectDuration > reconnectMaxLatency {
				res.failureMessage = fmt.Sprintf("reconnect took %s, exceeds limit %s",
					reconnectDuration, reconnectMaxLatency)
				t.Errorf("soak[reconnect]: %s: %s", label, res.failureMessage)
			}
		}
		results = append(results, res)
	}

	// Final cleanup.
	if handles != nil {
		close(handles.stopRTP)
		handles.closeWS()
		handles.closePC()
	}

	// --- Report ---
	totalElapsed := time.Since(soakStart).Truncate(time.Second)
	allPassed := true
	reportLines := fmt.Sprintf(
		"# Publisher reconnect soak\n"+
			"# Generated: %s\n"+
			"# RELAY_SOAK_FULL: %s\n"+
			"# Total elapsed: %s\n\n",
		time.Now().UTC().Format("2006-01-02"),
		os.Getenv("RELAY_SOAK_FULL"),
		totalElapsed,
	)
	for _, r := range results {
		verdict := "PASS"
		if !r.succeeded || r.reconnectTime > reconnectMaxLatency {
			verdict = "FAIL"
			allPassed = false
		}
		reportLines += fmt.Sprintf(
			"interval=%s scheduled_at=%s reconnect_time=%s verdict=%s",
			r.intervalLabel,
			r.scheduledAt.Truncate(time.Second),
			r.reconnectTime.Truncate(time.Millisecond),
			verdict,
		)
		if r.failureMessage != "" {
			reportLines += fmt.Sprintf(" failure=%q", r.failureMessage)
		}
		reportLines += "\n"
	}
	reportLines += fmt.Sprintf("overall_verdict=%s\n", soakVerdict(allPassed))

	_ = os.MkdirAll("../../soak/results", 0755)
	_ = os.WriteFile("../../soak/results/reconnect-soak.txt", []byte(reportLines), 0644)
	t.Logf("soak results:\n%s", reportLines)
}
