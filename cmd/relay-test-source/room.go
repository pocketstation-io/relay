package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

func createRoom(relayBase string, logger *slog.Logger) (roomID, sourceToken string, err error) {
	response, err := http.Post(relayBase+"/v1/rooms", "application/json", bytes.NewReader(nil))
	if err != nil {
		return "", "", fmt.Errorf("POST /v1/rooms: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", "", fmt.Errorf("read response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("unexpected status %d: %s", response.StatusCode, body)
	}
	var payload struct {
		RoomID        string `json:"room_id"`
		SourceToken   string `json:"source_token"`
		ListenerToken string `json:"listener_token"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", "", fmt.Errorf("decode response: %w", err)
	}
	logger.Info("created room", "room_id", payload.RoomID, "listener_token", payload.ListenerToken)
	return payload.RoomID, payload.SourceToken, nil
}
