package server

import (
	"context"
	"encoding/json"
	"github.com/pocketstation-io/relay/access"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pocketstation-io/relay/internal/session"
)

func TestGivenLatencySampleWhenCanonicalEndpointQueriedThenP50StatsReturned(t *testing.T) {
	relayServer := New(Config{JWTSecret: []byte("latency-test-secret-0123456789abcdef")})
	cleanupTestRelay(t, relayServer)
	createRecorder := httptest.NewRecorder()
	relayServer.Handler().ServeHTTP(
		createRecorder,
		httptest.NewRequest(http.MethodPost, "/v1/sessions", nil),
	)
	if createRecorder.Code != http.StatusCreated {
		t.Fatalf("create session status = %d, want %d", createRecorder.Code, http.StatusCreated)
	}

	var created struct {
		SessionID   string `json:"session_id"`
		SourceToken string `json:"source_token"`
	}
	if err := json.Unmarshal(createRecorder.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	relaySession := relayServer.relaySessions.GetOrCreate(created.SessionID)

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
	request := httptest.NewRequest(http.MethodGet, "/v1/sessions/"+created.SessionID+"/latency", nil)
	request.Header.Set("Authorization", "Bearer "+created.SourceToken)
	relayServer.Handler().ServeHTTP(latencyRecorder, request)

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

func TestGivenSessionDiagnosticsWhenCredentialMissingWrongOrMediaOnlyThenDenied(t *testing.T) {
	domain := testAccessService(t, joinTestSecret, access.Config{})
	created, err := domain.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	other, err := domain.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := domain.IssuePublisherTokenContext(context.Background(), created.ID(), "application")
	if err != nil {
		t.Fatal(err)
	}
	relay := New(Config{JWTSecret: joinTestSecret, AccessService: domain})
	cleanupTestRelay(t, relay)
	for _, endpoint := range []string{"latency", "health", "media-debug", "packet-log"} {
		for _, test := range []struct {
			name, token string
			status      int
		}{{"missing", "", 401}, {"wrong Session", other.SourceToken(), 403}, {"media only", publisher, 403}} {
			t.Run(endpoint+"/"+test.name, func(t *testing.T) {
				request := httptest.NewRequest(http.MethodGet, "/v1/sessions/"+created.ID()+"/"+endpoint, nil)
				if test.token != "" {
					request.Header.Set("Authorization", "Bearer "+test.token)
				}
				response := httptest.NewRecorder()
				relay.Handler().ServeHTTP(response, request)
				if response.Code != test.status {
					t.Fatalf("status=%d want%d", response.Code, test.status)
				}
			})
		}
	}
}

func TestGivenSubscriberCapabilityWhenObservingThenSessionMetadataAllowedButOtherBusPacketsDenied(t *testing.T) {
	domain := testAccessService(t, joinTestSecret, access.Config{})
	created, err := domain.Create("application", "microphone")
	if err != nil {
		t.Fatal(err)
	}
	subscriber, err := domain.IssueSubscriberTokenContext(context.Background(), created.ID(), "application")
	if err != nil {
		t.Fatal(err)
	}
	relay := New(Config{JWTSecret: joinTestSecret, AccessService: domain})
	cleanupTestRelay(t, relay)
	relay.relaySessions.GetOrCreate(created.ID())
	for _, endpoint := range []string{"latency", "health", "media-debug", "packet-log?bus=microphone"} {
		request := httptest.NewRequest(http.MethodGet, "/v1/sessions/"+created.ID()+"/"+endpoint, nil)
		request.Header.Set("Authorization", "Bearer "+subscriber)
		response := httptest.NewRecorder()
		relay.Handler().ServeHTTP(response, request)
		expected := http.StatusOK
		if endpoint == "packet-log?bus=microphone" {
			expected = http.StatusForbidden
		}
		if response.Code != expected {
			t.Fatalf("%s status=%d want%d", endpoint, response.Code, expected)
		}
	}
}
