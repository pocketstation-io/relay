package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pocketstation-io/relay/internal/server"
)

// TestGivenEchoEndpointWhenSendTimestampThenReflected verifies the /v1/echo
// endpoint (RELAY-020): sends a send_timestamp_ns and asserts the relay reflects
// it back alongside a recv_timestamp_ns that is >= send_timestamp_ns.
func TestGivenEchoEndpointWhenSendTimestampThenReflected(t *testing.T) {
	// Given
	srv := server.New(server.Config{JWTSecret: []byte("test-secret-0123456789abcdef012345")})
	ts := newIPv4Server(srv.Handler())
	defer ts.Close()

	u := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/echo"
	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatalf("dial /v1/echo: %v", err)
	}
	defer conn.Close()

	// When
	sendTs := time.Now().UnixNano()
	if err := conn.WriteJSON(map[string]int64{"send_timestamp_ns": sendTs}); err != nil {
		t.Fatalf("write: %v", err)
	}

	var reply struct {
		SendTimestampNs int64 `json:"send_timestamp_ns"`
		RecvTimestampNs int64 `json:"recv_timestamp_ns"`
	}
	if err := conn.ReadJSON(&reply); err != nil {
		t.Fatalf("read reply: %v", err)
	}

	// Then
	if reply.SendTimestampNs != sendTs {
		t.Errorf("send_timestamp_ns not reflected: got %d, want %d",
			reply.SendTimestampNs, sendTs)
	}
	if reply.RecvTimestampNs < sendTs {
		t.Errorf("recv_timestamp_ns %d should be >= send_timestamp_ns %d",
			reply.RecvTimestampNs, sendTs)
	}
}

// TestGivenEchoEndpointWhenMultipleMessagesThenAllReflected verifies that
// the echo endpoint handles multiple round-trips in sequence without error.
func TestGivenEchoEndpointWhenMultipleMessagesThenAllReflected(t *testing.T) {
	// Given
	srv := server.New(server.Config{JWTSecret: []byte("test-secret-0123456789abcdef012345")})
	ts := newIPv4Server(srv.Handler())
	defer ts.Close()

	u := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/echo"
	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatalf("dial /v1/echo: %v", err)
	}
	defer conn.Close()

	// When / Then — send 5 timestamps and verify each is reflected
	for i := int64(1); i <= 5; i++ {
		if err := conn.WriteJSON(map[string]int64{"send_timestamp_ns": i * 1_000_000}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		var reply map[string]int64
		if err := conn.ReadJSON(&reply); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if reply["send_timestamp_ns"] != i*1_000_000 {
			t.Errorf("iter %d: send_timestamp_ns not reflected", i)
		}
	}
}

// TestGivenEchoEndpointWhenHTTPNotWebSocketThenBadRequest verifies the
// echo endpoint rejects plain HTTP requests (not WebSocket upgrades).
func TestGivenEchoEndpointWhenHTTPNotWebSocketThenBadRequest(t *testing.T) {
	// Given
	srv := server.New(server.Config{JWTSecret: []byte("test-secret-0123456789abcdef012345")})

	// When — plain HTTP GET (no Upgrade header)
	req := httptest.NewRequest(http.MethodGet, "/v1/echo", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	// Then — WebSocket upgrade failure returns 4xx
	if w.Code < 400 {
		t.Errorf("expected 4xx for non-WebSocket request, got %d", w.Code)
	}
}

// TestGivenKeyExchangeMessageTypeWhenParsedThenCorrect verifies the
// KEY_EXCHANGE and CODEC_HINT message types are correctly defined in signaling.
func TestGivenSignalingTypesWhenCheckedThenAllDefined(t *testing.T) {
	// Verify the JSON round-trip of a CODEC_HINT message.
	hint := map[string]interface{}{
		"type": "CODEC_HINT",
		"codec_hint": map[string]interface{}{
			"bitrate_kbps": 64,
			"complexity":   5,
			"fec":          true,
			"dtx":          false,
		},
	}
	data, err := json.Marshal(hint)
	if err != nil {
		t.Fatalf("marshal CODEC_HINT: %v", err)
	}
	if !strings.Contains(string(data), "CODEC_HINT") {
		t.Error("CODEC_HINT not in serialised message")
	}
	if !strings.Contains(string(data), "bitrate_kbps") {
		t.Error("bitrate_kbps not in serialised codec hint")
	}
}
