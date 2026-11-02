package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGivenPublicControlPlaneWhenRequestedThenAdvertisesOnlyThatAddress(t *testing.T) {
	t.Setenv("RELAY_API_SERVER_URL", "http://internal-callback.local:4801")
	server := New(Config{
		AuthorityMode:         "control-plane",
		PublicControlPlaneURL: "https://api.example.com",
	})
	cleanupTestRelay(t, server)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/.well-known/pocketstation", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("cache policy = %q", response.Header().Get("Cache-Control"))
	}
	var discovery struct {
		SchemaVersion   int    `json:"schema_version"`
		Authority       string `json:"authority"`
		AuthorityURL    string `json:"authority_url"`
		ControlPlaneURL string `json:"control_plane_url"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &discovery); err != nil {
		t.Fatal(err)
	}
	if discovery.SchemaVersion != 1 || discovery.Authority != "relay" || discovery.AuthorityURL != "https://api.example.com" || discovery.ControlPlaneURL != "https://api.example.com" {
		t.Fatalf("unexpected discovery: %+v", discovery)
	}
	if strings.Contains(response.Body.String(), "internal-callback") {
		t.Fatal("internal callback address was advertised")
	}
}

func TestGivenMissingOrInvalidPublicAddressWhenRequestedThenFailsClosed(t *testing.T) {
	for _, address := range []string{"", "http://api.example.com", "https://user:secret@api.example.com", "https://api.example.com?token=secret", "file:///tmp/control", "http://10.0.0.1:4801"} {
		t.Run(address, func(t *testing.T) {
			t.Setenv("PUBLIC_CONTROL_PLANE_URL", "")
			server := New(Config{AuthorityMode: "control-plane", PublicControlPlaneURL: address})
			cleanupTestRelay(t, server)
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/.well-known/pocketstation", nil))
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", response.Code)
			}
			if strings.Contains(response.Body.String(), address) && address != "" {
				t.Fatal("invalid address leaked in response")
			}
		})
	}
}

func TestGivenStandaloneAuthorityWhenRequestedThenAdvertisesRelayAccess(t *testing.T) {
	server := New(Config{JWTSecret: joinTestSecret, AuthorityMode: "standalone", PublicRelayURL: "https://relay.example.com", PublicControlPlaneURL: "https://api.example.com"})
	cleanupTestRelay(t, server)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/.well-known/pocketstation", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d", response.Code)
	}
	var value map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	if value["authority"] != "relay" || value["authority_url"] != "https://relay.example.com" || strings.Contains(response.Body.String(), "api.example.com") {
		t.Fatalf("wrong standalone discovery: %s", response.Body.String())
	}
}

func TestGivenLoopbackDevelopmentAuthorityWhenRequestedThenAdvertisesIt(t *testing.T) {
	server := New(Config{AuthorityMode: "control-plane", PublicControlPlaneURL: "http://127.0.0.1:4801"})
	cleanupTestRelay(t, server)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/.well-known/pocketstation", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
}
