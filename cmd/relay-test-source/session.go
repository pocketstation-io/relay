package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

func createSession(relayBase string, logger *slog.Logger) (sessionID, sourceToken string, err error) {
	response, err := http.Post(relayBase+"/v1/sessions", "application/json", bytes.NewReader(nil))
	if err != nil {
		return "", "", fmt.Errorf("POST /v1/sessions: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if err != nil {
		return "", "", fmt.Errorf("read response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("unexpected status %d: %s", response.StatusCode, body)
	}
	var payload struct {
		SessionID   string `json:"session_id"`
		SourceToken string `json:"source_token"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", "", fmt.Errorf("decode response: %w", err)
	}
	logger.Info("created standalone RelaySession", "session_id", payload.SessionID)
	return payload.SessionID, payload.SourceToken, nil
}
