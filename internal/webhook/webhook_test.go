package webhook_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pocketstation-io/relay/internal/webhook"
)

// TestDispatcherSendsEventToURL verifies that Send POSTs a well-formed JSON
// body to the configured endpoint.
func TestDispatcherSendsEventToURL(t *testing.T) {
	t.Parallel()

	var (
		mu      sync.Mutex
		gotBody []byte
	)
	received := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotBody = body
		mu.Unlock()
		close(received)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Given: a dispatcher pointed at the test server
	d := webhook.New(srv.URL)
	event := webhook.Event{
		Type:      webhook.EventSessionStarted,
		RoomID:    "room-abc",
		SessionID: "sess-001",
		Timestamp: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
	}

	// When: Send is called
	d.Send(event)

	// Then: the server receives the POST within 2 seconds
	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: webhook POST not received")
	}

	mu.Lock()
	defer mu.Unlock()
	var got webhook.Event
	if err := json.Unmarshal(gotBody, &got); err != nil {
		t.Fatalf("unmarshal error: %v (body: %s)", err, gotBody)
	}
	if got.Type != webhook.EventSessionStarted {
		t.Errorf("type: got %q, want %q", got.Type, webhook.EventSessionStarted)
	}
	if got.RoomID != "room-abc" {
		t.Errorf("room_id: got %q, want %q", got.RoomID, "room-abc")
	}
	if got.SessionID != "sess-001" {
		t.Errorf("session_id: got %q, want %q", got.SessionID, "sess-001")
	}
}

// TestDispatcherNoopsWhenNilURL verifies that Send does not panic when the
// dispatcher was created with an empty URL.
func TestDispatcherNoopsWhenNilURL(t *testing.T) {
	t.Parallel()

	// Given: a dispatcher with no URL
	d := webhook.New("")
	event := webhook.Event{
		Type:      webhook.EventSessionEnded,
		RoomID:    "room-xyz",
		SessionID: "sess-002",
	}

	// When: Send is called (should not panic or block)
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.Send(event)
	}()

	// Then: Send returns immediately
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout: Send on empty URL did not return promptly")
	}
}

// TestEventJSON_SessionStarted_Fields verifies that an Event with
// EventSessionStarted marshals to JSON with the required top-level fields.
func TestEventJSON_SessionStarted_Fields(t *testing.T) {
	t.Parallel()

	// Given: a session_started event
	ts := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	event := webhook.Event{
		Type:      webhook.EventSessionStarted,
		RoomID:    "room-def",
		SessionID: "sess-003",
		Timestamp: ts,
	}

	// When: marshalled to JSON
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	// Then: the required fields are present with correct values
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if m["type"] != string(webhook.EventSessionStarted) {
		t.Errorf("type: got %v, want %q", m["type"], webhook.EventSessionStarted)
	}
	if m["room_id"] != "room-def" {
		t.Errorf("room_id: got %v, want %q", m["room_id"], "room-def")
	}
	if m["session_id"] != "sess-003" {
		t.Errorf("session_id: got %v, want %q", m["session_id"], "sess-003")
	}
	rawTS, ok := m["timestamp"].(string)
	if !ok || rawTS == "" {
		t.Errorf("timestamp field missing or not a string: %v", m["timestamp"])
	}
}
