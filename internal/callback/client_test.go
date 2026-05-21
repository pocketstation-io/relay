package callback_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pocketstation-io/relay/internal/callback"
)

// TestGiven_CallbackClient_When_PushSourceActive_Then_PostSent verifies that
// PushSourceActive sends a POST with the expected JSON body to the correct path.
func TestGiven_CallbackClient_When_PushSourceActive_Then_PostSent(t *testing.T) {
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
	wantPath := "/v1/internal/rooms/" + roomID + "/source-active"
	if capturedPath != wantPath {
		t.Errorf("want path %q, got %q", wantPath, capturedPath)
	}
	if active, _ := capturedBody["active"].(bool); !active {
		t.Errorf("want active=true in body, got %v", capturedBody)
	}
}

// TestGiven_CallbackClient_When_PushSourceActive_False_Then_PostSentWithFalse
// verifies the inactive case sends active=false.
func TestGiven_CallbackClient_When_PushSourceActive_False_Then_PostSentWithFalse(t *testing.T) {
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

// TestGiven_CallbackClient_When_ServerDown_Then_NoError verifies that a refused
// connection causes no panic and no returned error (best-effort semantics).
func TestGiven_CallbackClient_When_ServerDown_Then_NoError(t *testing.T) {
	// Given — point at a port with nothing listening
	c := callback.NewClient("http://127.0.0.1:1") // port 1 is reserved; always refused

	// When / Then — must not panic
	c.PushSourceActive("room-xyz", true)
	c.PushListenerLeave("room-xyz")
}

// TestGiven_CallbackClient_When_BaseURLEmpty_Then_Noop verifies that an empty
// baseURL disables all callbacks without any network activity.
func TestGiven_CallbackClient_When_BaseURLEmpty_Then_Noop(t *testing.T) {
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
	c.PushListenerLeave("room-noop")

	// Then
	if called {
		t.Error("no HTTP call expected when baseURL is empty")
	}
}

// TestGiven_CallbackClient_When_PushListenerLeave_Then_PostSent verifies that
// PushListenerLeave sends a POST to the correct path.
func TestGiven_CallbackClient_When_PushListenerLeave_Then_PostSent(t *testing.T) {
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
	c.PushListenerLeave(roomID)

	// Then
	if capturedMethod != http.MethodPost {
		t.Errorf("want POST, got %s", capturedMethod)
	}
	wantPath := "/v1/internal/rooms/" + roomID + "/listener-leave"
	if capturedPath != wantPath {
		t.Errorf("want path %q, got %q", wantPath, capturedPath)
	}
}
