package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
	if joinCode == "" || joinURL != "https://receiver.example?join="+joinCode {
		t.Fatalf("unexpected join URL %q for code %q", joinURL, joinCode)
	}
	if strings.Contains(joinURL, body["session_id"].(string)) || strings.Contains(joinURL, "token") {
		t.Fatalf("join URL leaks session or token: %s", joinURL)
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
