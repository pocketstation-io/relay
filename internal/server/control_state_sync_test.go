package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pocketstation-io/relay/internal/notifications/callback"
)

const controlStateSyncTestSecret = "0123456789abcdef0123456789abcdef"

func TestGivenDeletedControlPlaneSessionWhenStateIsPushedThenRelaySessionIsRemoved(t *testing.T) {
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "session not found", http.StatusNotFound)
	}))
	defer controlPlane.Close()

	client, err := callback.NewClient(controlPlane.URL, controlStateSyncTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	server := New(Config{CallbackClient: client})
	server.controlSyncContext = context.Background()
	relaySession := server.relaySessions.GetOrCreate("session-1")

	server.pushControlState(relaySession)

	if count := server.relaySessions.RoomCount(); count != 0 {
		t.Fatalf("RelaySession count = %d, want 0", count)
	}
}
