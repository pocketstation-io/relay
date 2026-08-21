package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/server"
)

// TestGivenTURNConfiguredWhenCreateRoomThenIceServersReturned verifies
// that POST /v1/sessions includes the ice_servers field when the server is
// configured with client ICE servers (RELAY-023).
func TestGivenTURNConfiguredWhenCreateRoomThenIceServersReturned(t *testing.T) {
	// Given — server with TURN client ICE servers configured.
	// ClientICEServers are returned to connecting clients; ICEServers is left
	// nil so the relay's own Pion does not self-STUN via the embedded TURN.
	turnServers := []webrtc.ICEServer{
		{URLs: []string{"stun:relay.example.com:3478"}},
		{
			URLs:           []string{"turn:relay.example.com:3478", "turns:relay.example.com:443"},
			Username:       "1234567890:test-room",
			Credential:     "hmac-test-password",
			CredentialType: webrtc.ICECredentialTypePassword,
		},
	}
	srv := server.New(server.Config{
		JWTSecret:        []byte("test-secret-0123456789abcdef012345"),
		ClientICEServers: turnServers,
	})

	// When — call handler directly via ResponseRecorder (no TCP binding needed)
	req := httptest.NewRequest(http.MethodPost, "/v1/sessions", bytes.NewReader(nil))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	resp := w.Result()
	defer resp.Body.Close()

	// Then
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := body["session_id"]; !ok {
		t.Error("response missing session_id")
	}
	if _, ok := body["source_token"]; !ok {
		t.Error("response missing source_token")
	}
	if _, ok := body["subscriber_token"]; !ok {
		t.Error("response missing subscriber_token")
	}
	rawICE, ok := body["ice_servers"]
	if !ok {
		t.Fatal("response missing ice_servers field (RELAY-023: TURN servers not returned)")
	}

	var iceList []map[string]any
	if err := json.Unmarshal(rawICE, &iceList); err != nil {
		t.Fatalf("ice_servers is not a JSON array: %v", err)
	}
	if len(iceList) != 2 {
		t.Errorf("expected 2 ICE server entries, got %d", len(iceList))
	}
}

// TestGivenNoTURNConfigWhenCreateRoomThenNoIceServersField verifies that
// the ice_servers field is absent when no ICE servers are configured, keeping
// the API backward-compatible with clients that do not read ice_servers.
func TestGivenNoTURNConfigWhenCreateRoomThenNoIceServersField(t *testing.T) {
	// Given — server with default STUN-only config (no ICEServers set)
	srv := server.New(server.Config{
		JWTSecret: []byte("test-secret-0123456789abcdef012345"),
	})

	// When
	req := httptest.NewRequest(http.MethodPost, "/v1/sessions", bytes.NewReader(nil))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	resp := w.Result()
	defer resp.Body.Close()

	// Then
	var body map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := body["ice_servers"]; ok {
		t.Error("ice_servers should not be present when no TURN is configured")
	}
}
