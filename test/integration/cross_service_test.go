// Package integration_test — cross-service JWT contract tests.
//
// These tests prove that a token minted by api-server (HS256, Claims{RoomID,
// Role}) using the shared POCKETSTATION_JWT_SECRET is accepted by relay
// /v1/signal without modification. No api-server binary is required: we use
// relay's own auth.Sign (which encodes the identical Claims struct and signing
// method), because the contract under test is the token *format*, not which
// process mints it.
//
// Run the full suite (includes real Pion ICE):
//
//	go test -race -run TestGivenApiServer ./test/integration/
//
// Skip the ICE-dependent tests:
//
//	go test -race -short ./test/integration/
package integration_test

import (
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/server"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// testCrossServiceSecret is the shared HS256 secret used by both api-server
// and relay in these tests. It mirrors the value POCKETSTATION_JWT_SECRET
// would be set to in a real deployment.
const testCrossServiceSecret = "cross-service-test-secret"

// TestGivenApiServerTokenWhenUsedForRelaySignalThenAccepted verifies
// that a token minted with the same HS256 secret and Claims struct as
// api-server is accepted by relay /v1/signal and results in an SDP_ANSWER.
func TestGivenApiServerTokenWhenUsedForRelaySignalThenAccepted(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test uses real Pion ICE — skipped in -short mode")
	}

	// Given — relay server started with the shared cross-service secret.
	loopbackAPI := newLoopbackAPI()
	ts, clientAPI := newTestServerWithSecret(t, []byte(testCrossServiceSecret), loopbackAPI)

	// Given — a room exists so the relay knows the RoomID.
	room := createRoom(t, ts)
	roomID := room["room_id"]
	if roomID == "" {
		t.Fatal("no room_id in room response")
	}

	// Given — a source token minted the way api-server mints it:
	// HS256, Claims{RoomID, Role}, same secret.
	apiServerToken, err := auth.Sign([]byte(testCrossServiceSecret), roomID, auth.RoleSource, 15*time.Minute)
	if err != nil {
		t.Fatalf("auth.Sign (simulating api-server): %v", err)
	}

	// When — client dials /v1/signal and sends PUBLISH with the api-server token.
	conn := dialSignal(t, ts)
	msgs := readServerMessages(conn)

	pc, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create peer connection: %v", err)
	}
	defer pc.Close()

	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "pocketstation-cross-service",
	)
	if err != nil {
		t.Fatalf("create audio track: %v", err)
	}
	if _, err := pc.AddTrack(track); err != nil {
		t.Fatalf("add track: %v", err)
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer: %v", err)
	}

	// wmu serialises all WebSocket writes on conn. Register OnICECandidate
	// before SetLocalDescription so every candidate fired during gathering is
	// serialised with the PUBLISH write below.
	var wmu sync.Mutex
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		wmu.Lock()
		defer wmu.Unlock()
		_ = conn.WriteJSON(signaling.ClientMessage{
			Type:      signaling.TypeIce,
			Candidate: c.ToJSON().Candidate,
		})
	})

	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("set local description: %v", err)
	}

	// Hold wmu for the PUBLISH write so that it serialises with any
	// concurrent ICE candidate write on the same conn.
	func() {
		wmu.Lock()
		defer wmu.Unlock()
		if err := conn.WriteJSON(signaling.ClientMessage{
			Type:     signaling.TypePublish,
			Token:    apiServerToken,
			SDPOffer: offer.SDP,
		}); err != nil {
			t.Errorf("send PUBLISH: %v", err)
		}
	}()

	// Then — relay must return SDP_ANSWER without any ERROR frame.
	deadline := time.After(10 * time.Second)
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				t.Fatal("WebSocket closed before SDP_ANSWER")
			}
			switch msg.Type {
			case signaling.TypeSDPAnswer:
				// Token accepted and SDP exchange completed — test passes.
				return
			case signaling.TypeError:
				t.Fatalf("relay returned ERROR (want SDP_ANSWER): code=%s message=%s", msg.Code, msg.Message)
			}
		case <-deadline:
			t.Fatal("timed out waiting for SDP_ANSWER after 10s")
		}
	}
}

// TestGivenApiServerTokenWhenSecretMismatchThenBadToken verifies that a
// token signed with the wrong secret is rejected with code "bad_token" before
// any SDP processing occurs.
func TestGivenApiServerTokenWhenSecretMismatchThenBadToken(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test uses real Pion ICE — skipped in -short mode")
	}

	// Given — relay started with the correct shared secret.
	loopbackAPI := newLoopbackAPI()
	ts, clientAPI := newTestServerWithSecret(t, []byte(testCrossServiceSecret), loopbackAPI)

	room := createRoom(t, ts)
	roomID := room["room_id"]
	if roomID == "" {
		t.Fatal("no room_id in room response")
	}

	// Given — a token minted with the WRONG secret (simulates misconfigured
	// api-server or a token from a different deployment).
	wrongSecret := []byte("wrong-secret-not-the-shared-one")
	badToken, err := auth.Sign(wrongSecret, roomID, auth.RoleSource, 15*time.Minute)
	if err != nil {
		t.Fatalf("auth.Sign (wrong secret): %v", err)
	}

	// When — client sends PUBLISH with the bad token.
	conn := dialSignal(t, ts)
	msgs := readServerMessages(conn)

	pc, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create peer connection: %v", err)
	}
	defer pc.Close()

	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "pocketstation-cross-service-bad",
	)
	if err != nil {
		t.Fatalf("create audio track: %v", err)
	}
	if _, err := pc.AddTrack(track); err != nil {
		t.Fatalf("add track: %v", err)
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer: %v", err)
	}

	var wmu sync.Mutex
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		wmu.Lock()
		defer wmu.Unlock()
		_ = conn.WriteJSON(signaling.ClientMessage{
			Type:      signaling.TypeIce,
			Candidate: c.ToJSON().Candidate,
		})
	})

	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("set local description: %v", err)
	}

	func() {
		wmu.Lock()
		defer wmu.Unlock()
		if err := conn.WriteJSON(signaling.ClientMessage{
			Type:     signaling.TypePublish,
			Token:    badToken,
			SDPOffer: offer.SDP,
		}); err != nil {
			t.Errorf("send PUBLISH: %v", err)
		}
	}()

	// Then — relay must return ERROR with code "bad_token" within 2 seconds.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				t.Fatal("WebSocket closed before ERROR frame")
			}
			if msg.Type == signaling.TypeError {
				if msg.Code != "bad_token" {
					t.Errorf("ERROR code = %q, want \"bad_token\"", msg.Code)
				}
				// Received expected bad_token error — test passes.
				return
			}
			if msg.Type == signaling.TypeSDPAnswer {
				t.Fatal("relay accepted a token signed with the wrong secret (got SDP_ANSWER)")
			}
		case <-deadline:
			t.Fatal("timed out waiting for ERROR frame after 2s")
		}
	}
}

// newTestServerWithSecret creates an httptest.Server using the given JWT
// secret and Pion API. It complements the existing newTestServer helper which
// hard-codes testJWTSecret, allowing cross-service tests to use their own
// shared secret.
func newTestServerWithSecret(t *testing.T, secret []byte, api *webrtc.API) (*httptest.Server, *webrtc.API) {
	t.Helper()
	srv := server.New(server.Config{
		JWTSecret: secret,
		API:       api,
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, api
}
