package server_test

import (
	"net"
	"testing"
	"time"

	"github.com/pocketstation-io/relay/internal/signaling"
)

func TestGivenLeaveWhenSignalPeerStopsThenWebSocketClosesPromptly(t *testing.T) {
	testServer, _, _ := newShutdownTestServer(t)
	defer testServer.Close()

	connection := dialShutdownSignal(t, testServer)
	defer connection.Close()
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := connection.WriteJSON(signaling.ClientMessage{Type: signaling.TypeLeave}); err != nil {
		t.Fatal(err)
	}

	_, _, err := connection.ReadMessage()
	if err == nil {
		t.Fatal("Relay kept the signaling WebSocket open after LEAVE")
	}
	if networkError, ok := err.(net.Error); ok && networkError.Timeout() {
		t.Fatal("Relay did not finish the signaling WebSocket close handshake")
	}
}
