// Package callback provides a best-effort HTTP client for notifying api-server
// of relay-side events (source connect/disconnect, listener leave).
//
// All public methods are fire-and-forget: they spawn no goroutines themselves
// (callers use "go c.Push…") and never return an error to the caller. If
// api-server is unreachable the error is logged at WARN level and dropped.
// If baseURL is empty the client is a no-op and no network calls are made.
package callback

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// httpTimeout is the maximum time a single callback POST may take.
// Kept short so a slow api-server does not delay session cleanup.
const httpTimeout = 5 * time.Second

// Client posts internal callback events to api-server.
// The zero value is not valid; use NewClient.
type Client struct {
	baseURL string
	http    http.Client
}

// NewClient returns a Client that posts to baseURL.
// If baseURL is empty the client is permanently disabled (all calls are no-ops).
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    http.Client{Timeout: httpTimeout},
	}
}

// sourceActiveBody is the JSON payload for the source-active callback.
type sourceActiveBody struct {
	Active bool `json:"active"`
}

// PushSourceActive posts {"active": active} to
// {baseURL}/v1/internal/rooms/{roomID}/source-active.
//
// Best-effort: errors are logged at WARN and discarded. Never blocks the
// audio path — callers must invoke this in a goroutine.
func (c *Client) PushSourceActive(roomID string, active bool) {
	if c.baseURL == "" {
		return
	}
	body, err := json.Marshal(sourceActiveBody{Active: active})
	if err != nil {
		// json.Marshal of a plain bool struct never fails in practice; guard anyway.
		slog.Warn("callback: marshal error", "event", "source_active", "error", err)
		return
	}
	url := fmt.Sprintf("%s/v1/internal/rooms/%s/source-active", c.baseURL, roomID)
	resp, err := c.http.Post(url, "application/json", bytes.NewReader(body)) //nolint:noctx
	if err != nil {
		slog.Warn("callback: POST failed", "event", "source_active", "room_id", roomID, "active", active, "error", err)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.Warn("callback: unexpected status", "event", "source_active", "room_id", roomID, "status", resp.StatusCode)
	}
}

// PushListenerLeave posts to
// {baseURL}/v1/internal/rooms/{roomID}/listener-leave.
//
// Best-effort: errors are logged at WARN and discarded. Never blocks the
// audio path — callers must invoke this in a goroutine.
func (c *Client) PushListenerLeave(roomID string) {
	if c.baseURL == "" {
		return
	}
	url := fmt.Sprintf("%s/v1/internal/rooms/%s/listener-leave", c.baseURL, roomID)
	resp, err := c.http.Post(url, "application/json", http.NoBody) //nolint:noctx
	if err != nil {
		slog.Warn("callback: POST failed", "event", "listener_leave", "room_id", roomID, "error", err)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.Warn("callback: unexpected status", "event", "listener_leave", "room_id", roomID, "status", resp.StatusCode)
	}
}
