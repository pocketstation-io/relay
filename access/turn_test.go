package access

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pocketstation-io/relay/access/metrics"
	"github.com/pocketstation-io/relay/access/turn"
)

func TestGivenTURNConfigurationWhenSessionIsCreatedThenScopedICEServersAreReturned(t *testing.T) {
	store := NewSessionStore(sessionTestSecret)
	handler := NewHandlerWithTURN(store, metrics.New(), &TURNConfig{
		PublicIP:              "203.0.113.10",
		Secret:                []byte("abcdef0123456789abcdef0123456789"),
		UDPPort:               3478,
		TLSPort:               5349,
		CredentialTTLDuration: time.Hour,
	})
	mux := http.NewServeMux()
	handler.Register(mux)

	request := httptest.NewRequest(http.MethodPost, "/v1/sessions", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)

	var body struct {
		SessionID  string           `json:"session_id"`
		ICEServers []turn.ICEServer `json:"ice_servers"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.ICEServers) != 2 {
		t.Fatalf("ICE server count = %d, want 2", len(body.ICEServers))
	}
	turnServer := body.ICEServers[1]
	if !strings.HasSuffix(turnServer.Username, ":"+body.SessionID) {
		t.Errorf("TURN username %q must be scoped to session %q", turnServer.Username, body.SessionID)
	}
	if turnServer.Credential == "" {
		t.Error("TURN credential must be non-empty")
	}
	if len(turnServer.URLs) != 3 {
		t.Errorf("TURN URL count = %d, want 3", len(turnServer.URLs))
	}
}

func TestGivenNoTURNConfigurationWhenSessionIsCreatedThenICEServersAreAbsent(t *testing.T) {
	store := NewSessionStore(sessionTestSecret)
	mux := http.NewServeMux()
	NewHandler(store, metrics.New()).Register(mux)

	request := httptest.NewRequest(http.MethodPost, "/v1/sessions", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)

	var body map[string]any
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := body["ice_servers"]; ok {
		t.Error("ice_servers must be omitted without TURN configuration")
	}
}

func TestGivenSTUNConfigurationWhenSessionIsCreatedThenURLsHaveNoCredentials(t *testing.T) {
	store := NewSessionStore(sessionTestSecret)
	handler := NewHandlerWithConfig(store, metrics.New(), HandlerConfig{
		STUNURLs: []string{
			"stun:stun1.example:3478",
			"stun:stun2.example:3478",
		},
	})
	mux := http.NewServeMux()
	handler.Register(mux)

	request := httptest.NewRequest(http.MethodPost, "/v1/sessions", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)

	var body struct {
		ICEServers []turn.ICEServer `json:"ice_servers"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.ICEServers) != 1 {
		t.Fatalf("ICE server count = %d, want 1", len(body.ICEServers))
	}
	server := body.ICEServers[0]
	if len(server.URLs) != 2 || server.URLs[0] != "stun:stun1.example:3478" || server.URLs[1] != "stun:stun2.example:3478" {
		t.Fatalf("STUN URLs = %#v", server.URLs)
	}
	if server.Username != "" || server.Credential != "" {
		t.Fatalf("STUN server must not contain credentials: %#v", server)
	}
}

func TestGivenSTUNConfigurationWhenPublisherIsIssuedThenURLsHaveNoCredentials(t *testing.T) {
	store := NewSessionStore(sessionTestSecret)
	handler := NewHandlerWithConfig(store, metrics.New(), HandlerConfig{
		STUNURLs: []string{"stun:stun.example:3478"},
	})
	mux := http.NewServeMux()
	handler.Register(mux)

	created, err := store.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/sessions/"+created.ID()+"/publish",
		bytes.NewBufferString(`{"bus_id":"application"}`),
	)
	request.Header.Set("Authorization", "Bearer "+created.SourceToken())
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("publish status=%d body=%s", response.Code, response.Body.String())
	}

	var body publishSessionResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.ICEServers) != 1 {
		t.Fatalf("ICE server count = %d, want 1", len(body.ICEServers))
	}
	server := body.ICEServers[0]
	if len(server.URLs) != 1 || server.URLs[0] != "stun:stun.example:3478" || server.Username != "" || server.Credential != "" {
		t.Fatalf("publisher must receive the configured credential-free STUN server: %#v", server)
	}
}

func TestGivenStaticICEConfigurationWhenSessionAndGrantReturnThenCredentialsArePreserved(t *testing.T) {
	store := NewSessionStore(sessionTestSecret)
	config := HandlerConfig{ICEServers: []turn.ICEServer{{URLs: []string{"turn:example.test:3478"}, Username: "configured-user", Credential: "configured-password"}}}
	handler := NewHandlerWithConfig(store, metrics.New(), config)
	config.ICEServers[0].URLs[0] = "mutated"
	servers := handler.buildICEServers("session")
	if len(servers) != 1 || servers[0].URLs[0] != "turn:example.test:3478" || servers[0].Username != "configured-user" || servers[0].Credential != "configured-password" {
		t.Fatal("static ICE config changed")
	}
	servers[0].URLs[0] = "mutated-output"
	if handler.buildICEServers("session")[0].URLs[0] != "turn:example.test:3478" {
		t.Fatal("returned ICE slice aliases configuration")
	}
}
