package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pocketstation-io/relay/internal/session"
)

func TestGivenLatencySampleWhenCanonicalEndpointQueriedThenP50StatsReturned(t *testing.T) {
	relayServer := New(Config{JWTSecret: []byte("latency-test-secret-0123456789abcdef")})
	createRecorder := httptest.NewRecorder()
	relayServer.Handler().ServeHTTP(
		createRecorder,
		httptest.NewRequest(http.MethodPost, "/v1/sessions", nil),
	)
	if createRecorder.Code != http.StatusOK {
		t.Fatalf("create session status = %d, want %d", createRecorder.Code, http.StatusOK)
	}

	var created struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(createRecorder.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	relaySession, found := relayServer.relaySessions.Get(created.SessionID)
	if !found {
		t.Fatalf("created session %q not found", created.SessionID)
	}
	samples := []struct {
		captureMs      float64
		encodeMs       float64
		relayRttMs     float64
		jitterBufferMs float64
		decodeMs       float64
		packetLossPct  float64
	}{
		{10, 1, 20, 4, 2, 0},
		{12, 3, 24, 8, 4, 0.5},
		{14, 5, 28, 12, 6, 1},
		{16, 7, 32, 16, 8, 1.5},
		{18, 9, 36, 20, 10, 2},
	}
	for _, sample := range samples {
		relaySession.RecordLatency(
			sample.captureMs,
			sample.encodeMs,
			sample.relayRttMs,
			sample.jitterBufferMs,
			sample.decodeMs,
			sample.packetLossPct,
		)
	}

	latencyRecorder := httptest.NewRecorder()
	relayServer.Handler().ServeHTTP(
		latencyRecorder,
		httptest.NewRequest(
			http.MethodGet,
			"/v1/sessions/"+created.SessionID+"/latency",
			nil,
		),
	)
	if latencyRecorder.Code != http.StatusOK {
		t.Fatalf(
			"latency endpoint status = %d, want %d",
			latencyRecorder.Code,
			http.StatusOK,
		)
	}
	if got := latencyRecorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("latency endpoint content type = %q, want application/json", got)
	}

	var stats session.LatencyStats
	if err := json.Unmarshal(latencyRecorder.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode latency response: %v", err)
	}
	want := session.LatencyStats{
		CaptureP50Ms:      14,
		EncodeP50Ms:       5,
		RelayRttP50Ms:     28,
		JitterBufferP50Ms: 12,
		DecodeP50Ms:       6,
		PacketLossPct:     1,
		SampleCount:       5,
	}
	if stats != want {
		t.Fatalf("latency stats = %#v, want %#v", stats, want)
	}
}

func TestGivenUnknownSessionWhenLatencyEndpointQueriedThenNotFound(t *testing.T) {
	relayServer := New(Config{JWTSecret: []byte("latency-test-secret-0123456789abcdef")})
	recorder := httptest.NewRecorder()
	relayServer.Handler().ServeHTTP(
		recorder,
		httptest.NewRequest(
			http.MethodGet,
			"/v1/sessions/unknown/latency",
			nil,
		),
	)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("latency endpoint status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}
