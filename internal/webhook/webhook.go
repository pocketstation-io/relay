// Package webhook provides a fire-and-forget HTTP POST dispatcher for relay
// lifecycle events (session_started, utterance_detected, session_ended).
// Events are sent asynchronously with a 5-second timeout; failures are logged
// at WARN level and never returned to the caller.
// When the dispatcher URL is empty all Send calls are no-ops.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// dispatchTimeout is the per-request deadline for webhook POST calls.
const dispatchTimeout = 5 * time.Second

// EventType identifies the relay lifecycle event.
type EventType string

const (
	// EventSessionStarted fires when a source publishes to a room.
	EventSessionStarted EventType = "session_started"
	// EventUtteranceDetected fires when a LATENCY_REPORT indicates active capture.
	EventUtteranceDetected EventType = "utterance_detected"
	// EventSessionEnded fires when a source or listener session disconnects.
	EventSessionEnded EventType = "session_ended"
)

// Event is the JSON payload posted to the webhook URL.
type Event struct {
	Type      EventType `json:"type"`
	RoomID    string    `json:"room_id"`
	SessionID string    `json:"session_id"`
	Timestamp time.Time `json:"timestamp"`
	Data      any       `json:"data,omitempty"`
}

// Dispatcher posts relay events to a configured HTTP endpoint.
// All calls are fire-and-forget: Send spawns a goroutine and returns
// immediately. The zero value is not valid; use New.
type Dispatcher struct {
	url    string
	client *http.Client
}

// New returns a Dispatcher that POSTs events to webhookURL.
// If webhookURL is empty, all Send calls are no-ops.
func New(webhookURL string) *Dispatcher {
	return &Dispatcher{
		url:    webhookURL,
		client: &http.Client{Timeout: dispatchTimeout},
	}
}

// Send posts the event asynchronously (fire-and-forget with dispatchTimeout).
// Never blocks the caller. Logs failures at WARN but does not propagate them.
// A no-op when the dispatcher URL is empty.
func (d *Dispatcher) Send(event Event) {
	if d == nil || d.url == "" {
		return
	}
	// Capture a copy so the goroutine does not race on caller mutations.
	e := event
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	go func() {
		body, err := json.Marshal(e)
		if err != nil {
			slog.Warn("webhook: marshal error", "event_type", e.Type, "error", err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(body))
		if err != nil {
			slog.Warn("webhook: build request error", "event_type", e.Type, "error", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := d.client.Do(req)
		if err != nil {
			slog.Warn("webhook: POST failed", "event_type", e.Type, "room_id", e.RoomID, "error", err)
			return
		}
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			slog.Warn("webhook: unexpected status", "event_type", e.Type, "room_id", e.RoomID, "status", resp.StatusCode)
		}
	}()
}
