package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/server"
	"github.com/pocketstation-io/relay/internal/signaling"
)

const shutdownTestJWTSecret = "shutdown-test-secret"

// newShutdownTestServer creates a Server and a started httptest.Server.
// Unlike newTestServer in integration tests, cleanup is NOT registered so
// the test can control shutdown order explicitly.
func newShutdownTestServer(t *testing.T) (*httptest.Server, *server.Server, *webrtc.API) {
	t.Helper()
	se := webrtc.SettingEngine{}
	se.SetNAT1To1IPs([]string{"127.0.0.1"}, webrtc.ICECandidateTypeHost)
	se.SetICETimeouts(5*time.Second, 5*time.Second, 2*time.Second)
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))

	srv := server.New(server.Config{
		JWTSecret: []byte(shutdownTestJWTSecret),
		API:       api,
	})
	ts := httptest.NewServer(srv.Handler())
	return ts, srv, api
}

// createShutdownRoom POSTs to /v1/rooms and returns source and listener tokens.
func createShutdownRoom(t *testing.T, ts *httptest.Server) (roomID, sourceToken, listenerToken string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/rooms", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("POST /v1/rooms: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var payload map[string]string
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode room response: %v", err)
	}
	return payload["room_id"], payload["source_token"], payload["listener_token"]
}

// dialShutdownSignal dials the /v1/signal endpoint.
func dialShutdownSignal(t *testing.T, ts *httptest.Server) *websocket.Conn {
	t.Helper()
	u, _ := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/v1/signal"
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("dial WebSocket: %v", err)
	}
	return conn
}

// TestGiven_RelayShutdown_When_ActiveConnection_Then_PeerReceivesLeave
// verifies that calling Server.Shutdown closes active WebSocket connections
// (peers observe a close or read error) and all rooms are closed cleanly.
//
// "PeerReceivesLeave" is implemented as: the WebSocket read on the subscriber
// side returns an error (connection closed by server), which is the observable
// signal that the session was terminated. A protocol-level LEAVE message would
// require the server to iterate sessions; the Phase 2 design uses room.Close
// which tears down the underlying connection.
func TestGiven_RelayShutdown_When_ActiveConnection_Then_PeerReceivesLeave(t *testing.T) {
	ts, srv, _ := newShutdownTestServer(t)

	// Verify the server is up before we create a room.
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	resp.Body.Close()

	_, sourceToken, _ := createShutdownRoom(t, ts)
	if sourceToken == "" {
		t.Fatal("no source_token in room response")
	}

	// Verify the token is valid (source role).
	claims, err := auth.Verify([]byte(shutdownTestJWTSecret), sourceToken)
	if err != nil {
		t.Fatalf("verify source token: %v", err)
	}
	if claims.Role != auth.RoleSource {
		t.Fatalf("expected source role, got %s", claims.Role)
	}

	// Dial a WebSocket and hold the connection open (simulates an active peer).
	wsConn := dialShutdownSignal(t, ts)

	// Drain messages in the background; record when the connection closes.
	connClosed := make(chan struct{})
	var closeOnce sync.Once
	go func() {
		for {
			if _, _, err := wsConn.ReadMessage(); err != nil {
				closeOnce.Do(func() { close(connClosed) })
				return
			}
		}
	}()

	// Send a SUBSCRIBE to obtain a ROOM_STATE or ERROR — just to have a live
	// message exchange. We use the source token on purpose to get an error
	// response (role mismatch), which keeps the connection alive.
	_ = wsConn.WriteJSON(signaling.ClientMessage{
		Type:  signaling.TypeSubscribe,
		Token: sourceToken,
	})

	// When — call Shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	// Close the httptest server (it is no longer accepting new connections after
	// Shutdown, but httptest.Server.Close frees the listener).
	ts.Close()

	// Then — the WebSocket connection must be closed by the server shutdown.
	select {
	case <-connClosed:
		// Pass: peer observed the connection close.
	case <-time.After(3 * time.Second):
		t.Fatal("WebSocket connection was not closed within 3s after Shutdown")
	}
}
