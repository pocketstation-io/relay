package callback_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pocketstation-io/relay/internal/notifications/callback"
)

// TestGivenCallbackClientWhenPushSourceActiveThenPostSent verifies that
// PushSourceActive sends a POST with the expected JSON body to the correct path.
func TestGivenCallbackClientWhenPushSourceActiveThenPostSent(t *testing.T) {
	// Given
	var (
		capturedMethod string
		capturedPath   string
		capturedBody   map[string]any
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedMethod = r.Method
		capturedPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &capturedBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := callback.NewClient(srv.URL)
	const roomID = "test-room-001"

	// When
	c.PushSourceActive(roomID, true)

	// Then
	if capturedMethod != http.MethodPost {
		t.Errorf("want POST, got %s", capturedMethod)
	}
	wantPath := "/v1/internal/sessions/" + roomID + "/source-active"
	if capturedPath != wantPath {
		t.Errorf("want path %q, got %q", wantPath, capturedPath)
	}
	if active, _ := capturedBody["active"].(bool); !active {
		t.Errorf("want active=true in body, got %v", capturedBody)
	}
}

// TestGivenCallbackClientWhenPushSourceActiveFalseThenPostSentWithFalse
// verifies the inactive case sends active=false.
func TestGivenCallbackClientWhenPushSourceActiveFalseThenPostSentWithFalse(t *testing.T) {
	// Given
	var capturedBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &capturedBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := callback.NewClient(srv.URL)

	// When
	c.PushSourceActive("room-abc", false)

	// Then
	if active, _ := capturedBody["active"].(bool); active {
		t.Errorf("want active=false in body, got %v", capturedBody)
	}
}

// TestGivenCallbackClientWhenServerDownThenNoError verifies that a refused
// connection causes no panic and no returned error (best-effort semantics).
func TestGivenCallbackClientWhenServerDownThenNoError(t *testing.T) {
	// Given — point at a port with nothing listening
	c := callback.NewClient("http://127.0.0.1:1") // port 1 is reserved; always refused

	// When / Then — must not panic
	c.PushSourceActive("room-xyz", true)
	c.PushSubscriberActive("room-xyz")
	c.PushSubscriberLeave("room-xyz")
}

// TestGivenCallbackClientWhenBaseURLEmptyThenNoop verifies that an empty
// baseURL disables all callbacks without any network activity.
func TestGivenCallbackClientWhenBaseURLEmptyThenNoop(t *testing.T) {
	// Given
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := callback.NewClient("") // disabled

	// When
	c.PushSourceActive("room-noop", true)
	c.PushSubscriberActive("room-noop")
	c.PushSubscriberLeave("room-noop")

	// Then
	if called {
		t.Error("no HTTP call expected when baseURL is empty")
	}
}

func TestGivenCallbackClientWhenPushSubscriberActiveThenPostSent(t *testing.T) {
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	callback.NewClient(srv.URL).PushSubscriberActive("subscriber-room-008")

	wantPath := "/v1/internal/sessions/subscriber-room-008/subscriber-active"
	if capturedPath != wantPath {
		t.Errorf("want path %q, got %q", wantPath, capturedPath)
	}
}

// TestGivenCallbackClientWhenPushSubscriberLeaveThenPostSent verifies that
// PushSubscriberLeave sends a POST to the canonical Session path.
func TestGivenCallbackClientWhenPushSubscriberLeaveThenPostSent(t *testing.T) {
	// Given
	var (
		capturedMethod string
		capturedPath   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedMethod = r.Method
		capturedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := callback.NewClient(srv.URL)
	const roomID = "listener-room-007"

	// When
	c.PushSubscriberLeave(roomID)

	// Then
	if capturedMethod != http.MethodPost {
		t.Errorf("want POST, got %s", capturedMethod)
	}
	wantPath := "/v1/internal/sessions/" + roomID + "/subscriber-leave"
	if capturedPath != wantPath {
		t.Errorf("want path %q, got %q", wantPath, capturedPath)
	}
}

func TestGivenDeletedSessionWhenPushSubscriberLeaveThenCleanupIsIdempotent(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "session not found", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	callback.NewClient(srv.URL).PushSubscriberLeave("already-deleted")

	if strings.Contains(logs.String(), "unexpected status") {
		t.Fatalf("idempotent subscriber cleanup logged a warning: %s", logs.String())
	}
}
