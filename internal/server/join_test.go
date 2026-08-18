package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/session"
)

type blockingJoinSource struct {
	released <-chan struct{}
}

func (source blockingJoinSource) ReadRTP() (*rtp.Packet, error) {
	<-source.released
	return nil, io.EOF
}

func TestGivenSessionWhenCreatedThenJoinURLContainsNoSessionOrToken(t *testing.T) {
	server := New(Config{
		JWTSecret:         []byte("join-test-secret"),
		PublicReceiverURL: "https://receiver.example",
	})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions", nil)
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("create session status = %d", recorder.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	joinURL, _ := body["join_url"].(string)
	joinCode, _ := body["join_code"].(string)
	parsed, err := url.Parse(joinURL)
	if err != nil {
		t.Fatalf("parse join URL: %v", err)
	}
	if joinCode == "" || parsed.Scheme != "https" || parsed.Host != "receiver.example" || parsed.Query().Get("join") != joinCode {
		t.Fatalf("unexpected join URL %q for code %q", joinURL, joinCode)
	}
	if parsed.Query().Get("relay") != "http://example.com" {
		t.Fatalf("join URL relay origin = %q", parsed.Query().Get("relay"))
	}
	if strings.Contains(joinURL, body["session_id"].(string)) || strings.Contains(joinURL, "token") {
		t.Fatalf("join URL leaks session or token: %s", joinURL)
	}
}

func TestGivenActivePublisherWhenSourceAuthorizesInvitationThenOpaqueURLIsReturned(t *testing.T) {
	server := New(Config{
		JWTSecret:         []byte("join-test-secret"),
		PublicReceiverURL: "https://receiver.example",
		PublicRelayURL:    "https://relay.example",
	})
	createRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(
		createRecorder,
		httptest.NewRequest(http.MethodPost, "/v1/sessions", nil),
	)
	var created map[string]any
	_ = json.Unmarshal(createRecorder.Body.Bytes(), &created)
	sessionID := created["session_id"].(string)
	relaySession, found := server.relaySessions.Get(sessionID)
	if !found {
		t.Fatal("created relay Session is missing")
	}
	released := make(chan struct{})
	relaySession.SetSource("application", session.BusRoleMusic, blockingJoinSource{released: released}, nil)
	defer close(released)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/sessions/"+sessionID+"/invitations",
		nil,
	)
	request.Header.Set("Authorization", "Bearer "+created["source_token"].(string))
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("create invitation status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var invitation joinInvitationResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &invitation); err != nil {
		t.Fatalf("decode invitation: %v", err)
	}
	parsed, err := url.Parse(invitation.JoinURL)
	if err != nil {
		t.Fatalf("parse invitation URL: %v", err)
	}
	if invitation.SessionID != sessionID || invitation.JoinCode == "" || parsed.Query().Get("join") != invitation.JoinCode {
		t.Fatalf("invalid invitation: %#v", invitation)
	}
	if parsed.Query().Get("relay") != "https://relay.example" {
		t.Fatalf("relay origin = %q", parsed.Query().Get("relay"))
	}
	if strings.Contains(invitation.JoinURL, sessionID) || strings.Contains(invitation.JoinURL, created["source_token"].(string)) {
		t.Fatalf("invitation URL leaks identity or token: %s", invitation.JoinURL)
	}
	if recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("invitation response must not be cached")
	}
}

func TestGivenInactivePublisherWhenInvitationRequestedThenConflict(t *testing.T) {
	server := New(Config{JWTSecret: []byte("join-test-secret")})
	createRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(
		createRecorder,
		httptest.NewRequest(http.MethodPost, "/v1/sessions", nil),
	)
	var created map[string]any
	_ = json.Unmarshal(createRecorder.Body.Bytes(), &created)
	sessionID := created["session_id"].(string)
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+sessionID+"/invitations", nil)
	request.Header.Set("Authorization", "Bearer "+created["source_token"].(string))
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", recorder.Code)
	}
}

func TestGivenValidJoinCodeWhenResolvedThenFreshSubscriberCredentialsReturned(t *testing.T) {
	server := New(Config{JWTSecret: []byte("join-test-secret")})
	createRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(
		createRecorder,
		httptest.NewRequest(http.MethodPost, "/v1/sessions", nil),
	)
	var created map[string]any
	_ = json.Unmarshal(createRecorder.Body.Bytes(), &created)

	joinRecorder := httptest.NewRecorder()
	joinRequest := httptest.NewRequest(
		http.MethodGet,
		"http://relay.example/v1/join/"+created["join_code"].(string),
		nil,
	)
	joinRequest.Header.Set("Origin", "https://receiver.example")
	server.Handler().ServeHTTP(joinRecorder, joinRequest)

	if joinRecorder.Code != http.StatusOK {
		t.Fatalf("resolve join status = %d body=%s", joinRecorder.Code, joinRecorder.Body.String())
	}
	var joined map[string]string
	_ = json.Unmarshal(joinRecorder.Body.Bytes(), &joined)
	if joined["session_id"] != created["session_id"] || joined["subscriber_token"] == "" {
		t.Fatalf("invalid join response: %#v", joined)
	}
	if joined["signal_url"] != "ws://relay.example/v1/signal" {
		t.Fatalf("signal_url = %q", joined["signal_url"])
	}
	if joinRecorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("join credentials must not be cached")
	}
}

func TestGivenUnknownJoinCodeWhenResolvedThenNotFound(t *testing.T) {
	server := New(Config{JWTSecret: []byte("join-test-secret")})
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/v1/join/unknown", nil),
	)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
}
