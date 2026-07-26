// Package callback provides a best-effort HTTP client for notifying the
// control plane of relay-side source and subscriber lifecycle events.
//
// All public methods are fire-and-forget: they spawn no goroutines themselves
// (callers use "go c.Push…") and never return an error to the caller. If
// the control plane is unreachable the error is logged at WARN level and dropped.
// If baseURL is empty the client is a no-op and no network calls are made.
package callback

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// httpTimeout is the maximum time a single callback POST may take.
// Kept short so a slow api-server does not delay session cleanup.
const httpTimeout = 5 * time.Second

// Client posts internal callback events to the control plane.
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
// {baseURL}/v1/internal/sessions/{sessionID}/source-active.
//
// Best-effort: errors are logged at WARN and discarded. Never blocks the
// audio path — callers must invoke this in a goroutine.
func (c *Client) PushSourceActive(sessionID string, active bool) {
	if c.baseURL == "" {
		return
	}
	body, err := json.Marshal(sourceActiveBody{Active: active})
	if err != nil {
		// json.Marshal of a plain bool struct never fails in practice; guard anyway.
		slog.Warn("callback: marshal error", "event", "source_active", "error", err)
		return
	}
	url := fmt.Sprintf("%s/v1/internal/sessions/%s/source-active", c.baseURL, sessionID)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		slog.Warn("callback: build request error", "event", "source_active", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if secret := os.Getenv("INTERNAL_SECRET"); secret != "" {
		req.Header.Set("X-Internal-Secret", secret)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		slog.Warn("callback: POST failed", "event", "source_active", "session_id", sessionID, "active", active, "error", err)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.Warn("callback: unexpected status", "event", "source_active", "session_id", sessionID, "status", resp.StatusCode)
	}
}

// PushSubscriberLeave posts to
// {baseURL}/v1/internal/sessions/{sessionID}/subscriber-leave.
//
// Best-effort: errors are logged at WARN and discarded. Never blocks the
// audio path — callers must invoke this in a goroutine.
func (c *Client) PushSubscriberLeave(sessionID string) {
	if c.baseURL == "" {
		return
	}
	url := fmt.Sprintf("%s/v1/internal/sessions/%s/subscriber-leave", c.baseURL, sessionID)
	req, err := http.NewRequest(http.MethodPost, url, http.NoBody)
	if err != nil {
		slog.Warn("callback: build request error", "event", "subscriber_leave", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if secret := os.Getenv("INTERNAL_SECRET"); secret != "" {
		req.Header.Set("X-Internal-Secret", secret)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		slog.Warn("callback: POST failed", "event", "subscriber_leave", "session_id", sessionID, "error", err)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.Warn("callback: unexpected status", "event", "subscriber_leave", "session_id", sessionID, "status", resp.StatusCode)
	}
}
