package server_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/server"
	"github.com/pocketstation-io/relay/internal/signaling"
)

const rateLimitJWTSecret = "rate-limit-test-secret"

// newRateLimitTestServer creates a test server with custom rate-limit ceilings.
func newRateLimitTestServer(t *testing.T, maxRooms, maxListeners int) *httptest.Server {
	t.Helper()
	se := webrtc.SettingEngine{}
	se.SetNAT1To1IPs([]string{"127.0.0.1"}, webrtc.ICECandidateTypeHost)
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))
	srv := server.New(server.Config{
		JWTSecret:           []byte(rateLimitJWTSecret),
		API:                 api,
		MaxRooms:            maxRooms,
		MaxListenersPerRoom: maxListeners,
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

// postRoom POSTs to /v1/rooms and returns the HTTP status and decoded body.
func postRoom(t *testing.T, ts *httptest.Server) (statusCode int, body map[string]string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/rooms", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("POST /v1/rooms: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(raw, &body)
	return resp.StatusCode, body
}

// dialRateLimitSignal dials the WebSocket signal endpoint.
func dialRateLimitSignal(t *testing.T, ts *httptest.Server) *websocket.Conn {
	t.Helper()
	u, _ := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/v1/signal"
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("dial WebSocket: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// TestGiven_MaxRoomsReached_When_CreateRoom_Then_429 verifies that once the
// room count reaches MaxRooms, POST /v1/rooms returns HTTP 429 with
// {"error":"room_limit_exceeded"}.
func TestGiven_MaxRoomsReached_When_CreateRoom_Then_429(t *testing.T) {
	// Given — a server with a limit of 2 rooms.
	const limit = 2
	ts := newRateLimitTestServer(t, limit, defaultMaxListenersForTest)

	// Fill up to the limit.
	for i := 0; i < limit; i++ {
		code, _ := postRoom(t, ts)
		if code != http.StatusOK {
			t.Fatalf("room %d: expected 200, got %d", i+1, code)
		}
	}

	// When — try to create one more room.
	code, body := postRoom(t, ts)

	// Then — 429 with structured error body.
	if code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", code)
	}
	if body["error"] != "room_limit_exceeded" {
		t.Errorf("expected error room_limit_exceeded, got %q", body["error"])
	}
}

// TestGiven_MaxListenersReached_When_Subscribe_Then_ErrorFrame verifies that
// when a room has reached MaxListenersPerRoom, a new SUBSCRIBE receives a
// WebSocket ERROR frame with code "listener_limit_exceeded".
func TestGiven_MaxListenersReached_When_Subscribe_Then_ErrorFrame(t *testing.T) {
	// Given — a server with a limit of 1 listener per room.
	const listenerLimit = 1
	ts := newRateLimitTestServer(t, defaultMaxRoomsForTest, listenerLimit)

	// Create a room.
	code, roomBody := postRoom(t, ts)
	if code != http.StatusOK {
		t.Fatalf("create room: expected 200, got %d", code)
	}
	listenerToken := roomBody["listener_token"]
	if listenerToken == "" {
		t.Fatal("no listener_token in room response")
	}

	// Send one SUBSCRIBE that fills the limit. We don't complete the full
	// WebRTC handshake — we just need the session registered in the room.
	// The listener is registered in handleJoin before the SDP exchange, so
	// we need to provide a valid SDP offer to reach that code path.
	//
	// Strategy: sign a listener token directly and send SUBSCRIBE with a
	// minimal offer. We verify only that the second SUBSCRIBE gets the error.
	conn1 := dialRateLimitSignal(t, ts)

	// Parse room_id from the token to sign a second listener token.
	claims, err := auth.Verify([]byte(rateLimitJWTSecret), listenerToken)
	if err != nil {
		t.Fatalf("verify listener token: %v", err)
	}
	roomID := claims.RoomID

	// Sign a second listener token for the same room.
	listenerToken2, err := auth.Sign([]byte(rateLimitJWTSecret), roomID, auth.RoleListener, time.Hour)
	if err != nil {
		t.Fatalf("sign second listener token: %v", err)
	}

	// First subscriber: send SUBSCRIBE without SDP — the server will error on
	// SDP validation but the listener count check happens after AddListener.
	// To reach the listener count check we must send a token with no SDP to
	// trigger "sdp_error" — but we need the limit check first.
	// The limit check is before the SDP/track setup, so sending only the token
	// is sufficient to verify the rejection path for the second subscriber.

	// First subscriber registers by sending a SUBSCRIBE with token (no SDP
	// means we get sdp_error, but AddListener is not called without a track).
	// We need a different approach: the listener count check in handleJoin is
	// BEFORE the SDP exchange, so we just need a valid token. If there is no
	// SDP, SetRemoteDescription will fail. But the listener limit check fires
	// before pc creation and SDP. So for the first subscriber we need to
	// actually reach AddListener.
	//
	// Simpler: send SUBSCRIBE for conn1 with no SDP. The server checks limit
	// (0 < 1, ok), then tries to set remote description with empty SDP and
	// returns sdp_error. The listener count stays 0. So conn1's subscribe
	// fails with sdp_error, not listener_limit_exceeded.
	//
	// Instead: directly add a listener token subscription that succeeds by
	// providing a minimal valid SDP offer via the Pion API.
	conn1.Close() // discard conn1, use the full SDP path below

	// Use a real Pion API to build a valid subscriber offer so the first
	// subscribe actually registers in the room.
	se := webrtc.SettingEngine{}
	se.SetNAT1To1IPs([]string{"127.0.0.1"}, webrtc.ICECandidateTypeHost)
	clientAPI := webrtc.NewAPI(webrtc.WithSettingEngine(se))

	subscribeWithRealOffer := func(token string) (serverMessages <-chan signaling.ServerMessage, conn *websocket.Conn) {
		t.Helper()
		c := dialRateLimitSignal(t, ts)
		pc, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			t.Fatalf("create subscriber PC: %v", err)
		}
		t.Cleanup(func() { _ = pc.Close() })
		if _, err := pc.AddTransceiverFromKind(
			webrtc.RTPCodecTypeAudio,
			webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
		); err != nil {
			t.Fatalf("add transceiver: %v", err)
		}
		offer, err := pc.CreateOffer(nil)
		if err != nil {
			t.Fatalf("create offer: %v", err)
		}
		if err := pc.SetLocalDescription(offer); err != nil {
			t.Fatalf("set local description: %v", err)
		}
		if err := c.WriteJSON(signaling.ClientMessage{
			Type:     signaling.TypeSubscribe,
			Token:    token,
			SDPOffer: offer.SDP,
		}); err != nil {
			t.Fatalf("send SUBSCRIBE: %v", err)
		}
		ch := make(chan signaling.ServerMessage, 16)
		go func() {
			defer close(ch)
			for {
				var msg signaling.ServerMessage
				if err := c.ReadJSON(&msg); err != nil {
					return
				}
				ch <- msg
			}
		}()
		return ch, c
	}

	// First subscriber: should succeed (limit not reached).
	msgs1, _ := subscribeWithRealOffer(listenerToken)

	// Wait for first subscriber to receive SDP_ANSWER (confirms it registered).
	deadline := time.After(5 * time.Second)
	firstOK := false
	for !firstOK {
		select {
		case msg, ok := <-msgs1:
			if !ok {
				t.Fatal("first subscriber connection closed before SDP_ANSWER")
			}
			if msg.Type == signaling.TypeSDPAnswer {
				firstOK = true
			}
			if msg.Type == signaling.TypeError {
				t.Fatalf("first subscriber got ERROR: %s — %s", msg.Code, msg.Message)
			}
		case <-deadline:
			t.Fatal("timed out waiting for first subscriber SDP_ANSWER")
		}
	}

	// When — second subscriber tries to join the same room (now at capacity).
	msgs2, _ := subscribeWithRealOffer(listenerToken2)

	// Then — second subscriber gets ERROR with code listener_limit_exceeded.
	deadline2 := time.After(3 * time.Second)
	for {
		select {
		case msg, ok := <-msgs2:
			if !ok {
				t.Fatal("second subscriber connection closed without ERROR frame")
			}
			if msg.Type == signaling.TypeError {
				if msg.Code != "listener_limit_exceeded" {
					t.Errorf("expected code listener_limit_exceeded, got %q", msg.Code)
				}
				return // pass
			}
		case <-deadline2:
			t.Fatal("timed out waiting for listener_limit_exceeded ERROR frame")
		}
	}
}

// defaultMaxRoomsForTest and defaultMaxListenersForTest are large values used
// when only one dimension is being rate-limited in a test.
const (
	defaultMaxRoomsForTest     = 100
	defaultMaxListenersForTest = 50
)
