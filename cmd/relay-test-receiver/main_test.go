package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestGivenNoMediaWhenReceiverWaitsThenDeadlineRejectsSuccess(t *testing.T) {
	const timeout = 150 * time.Millisecond
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		for {
			if _, _, err := connection.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	started := time.Now()
	err := receive(server.URL, "session", "application", "fixture-token", "", timeout, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("receiver without SDP/media returned %v, want timeout", err)
	}
	if time.Since(started) < timeout {
		t.Fatal("receiver returned before its media deadline")
	}
}
