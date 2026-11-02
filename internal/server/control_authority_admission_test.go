package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pocketstation-io/relay/access"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/notifications/callback"
	"github.com/pocketstation-io/relay/internal/signaling"
)

func newAuthorityAdmissionClient(t *testing.T, handler http.Handler) *callback.Client {
	t.Helper()
	controlPlane := httptest.NewServer(handler)
	t.Cleanup(controlPlane.Close)
	client, err := callback.NewClient(controlPlane.URL, controlStateSyncTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestGivenInactiveControlSessionWhenWebSocketPeerAttachesThenRelayRejectsBeforeAllocation(t *testing.T) {
	const sessionID = "inactive-session"
	client := newAuthorityAdmissionClient(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/v1/internal/sessions/"+sessionID+"/authority" {
			t.Fatalf("authority request = %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("X-PocketStation-Internal-Secret") != controlStateSyncTestSecret {
			t.Fatal("authority request is not authenticated")
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	server := New(Config{
		JWTSecret:      joinTestSecret,
		AuthorityMode:  "control-plane",
		CallbackClient: client,
	})
	cleanupTestRelay(t, server)
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	endpoint, err := url.Parse(httpServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	endpoint.Scheme = "ws"
	endpoint.Path = "/v1/signal"
	connection, _, err := websocket.DefaultDialer.Dial(endpoint.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	token, err := auth.SignSource(joinTestSecret, auth.ControlPlaneIssuer, sessionID, []string{"application"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.WriteJSON(signaling.ClientMessage{Type: signaling.TypePublish, Token: token}); err != nil {
		t.Fatal(err)
	}
	var response signaling.ServerMessage
	if err := connection.ReadJSON(&response); err != nil {
		t.Fatal(err)
	}
	if response.Type != signaling.TypeError || response.Code != string(signaling.ErrCodeSessionNotActive) {
		t.Fatalf("admission response = %#v, want session_not_active", response)
	}
	if _, found := server.relaySessions.Get(sessionID); found {
		t.Fatal("inactive authority record created a RelaySession")
	}
}

func TestGivenUnavailableControlAuthorityWhenWebSocketPeerAttachesThenRelayFailsClosed(t *testing.T) {
	const sessionID = "unavailable-authority-session"
	client := newAuthorityAdmissionClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	server := New(Config{
		JWTSecret:      joinTestSecret,
		AuthorityMode:  "control-plane",
		CallbackClient: client,
	})
	cleanupTestRelay(t, server)
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	endpoint, _ := url.Parse(httpServer.URL)
	endpoint.Scheme = "ws"
	endpoint.Path = "/v1/signal"
	connection, _, err := websocket.DefaultDialer.Dial(endpoint.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	token, err := auth.SignSource(joinTestSecret, auth.ControlPlaneIssuer, sessionID, []string{"application"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.WriteJSON(signaling.ClientMessage{Type: signaling.TypePublish, Token: token}); err != nil {
		t.Fatal(err)
	}
	var response signaling.ServerMessage
	if err := connection.ReadJSON(&response); err != nil {
		t.Fatal(err)
	}
	if response.Type != signaling.TypeError || response.Code != string(signaling.ErrCodeAuthorityUnavailable) {
		t.Fatalf("admission response = %#v, want authority_unavailable", response)
	}
	if _, found := server.relaySessions.Get(sessionID); found {
		t.Fatal("authority failure created a RelaySession")
	}
}

func TestGivenControlAuthorityTimeoutWhenWebSocketPeerAttachesThenRelayFailsClosedBounded(t *testing.T) {
	const sessionID = "authority-timeout-session"
	client := newAuthorityAdmissionClient(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	server := New(Config{
		JWTSecret:               joinTestSecret,
		AuthorityMode:           "control-plane",
		CallbackClient:          client,
		ControlAuthorityTimeout: 20 * time.Millisecond,
	})
	cleanupTestRelay(t, server)
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	endpoint, _ := url.Parse(httpServer.URL)
	endpoint.Scheme = "ws"
	endpoint.Path = "/v1/signal"
	connection, _, err := websocket.DefaultDialer.Dial(endpoint.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	token, err := auth.SignSource(joinTestSecret, auth.ControlPlaneIssuer, sessionID, []string{"application"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := connection.WriteJSON(signaling.ClientMessage{Type: signaling.TypePublish, Token: token}); err != nil {
		t.Fatal(err)
	}
	var response signaling.ServerMessage
	if err := connection.ReadJSON(&response); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("authority timeout took %s, want at most 500ms", elapsed)
	}
	if response.Type != signaling.TypeError || response.Code != string(signaling.ErrCodeAuthorityUnavailable) {
		t.Fatalf("admission response = %#v, want authority_unavailable", response)
	}
	if _, found := server.relaySessions.Get(sessionID); found {
		t.Fatal("authority timeout created a RelaySession")
	}
}

func TestGivenControlSessionAdmissionFailureWhenWHIPOrWHEPStartsThenRelayRejectsBeforeAllocation(t *testing.T) {
	for _, authorityStatus := range []struct {
		name       string
		statusCode int
		wantStatus int
	}{
		{name: "inactive", statusCode: http.StatusNotFound, wantStatus: http.StatusNotFound},
		{name: "database unavailable", statusCode: http.StatusServiceUnavailable, wantStatus: http.StatusServiceUnavailable},
	} {
		for _, transport := range []struct {
			name string
			path string
			role auth.Role
		}{
			{name: "WHIP", path: "whip", role: auth.RoleSource},
			{name: "WHEP", path: "whep", role: auth.RoleSubscriber},
		} {
			t.Run(authorityStatus.name+"/"+transport.name, func(t *testing.T) {
				const sessionID = "http-admission-session"
				client := newAuthorityAdmissionClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(authorityStatus.statusCode)
				}))
				server := New(Config{
					JWTSecret:      joinTestSecret,
					AuthorityMode:  "control-plane",
					CallbackClient: client,
				})
				cleanupTestRelay(t, server)
				var token string
				var err error
				if transport.role == auth.RoleSource {
					token, err = auth.SignSource(joinTestSecret, auth.ControlPlaneIssuer, sessionID, []string{"application"}, time.Minute)
				} else {
					token, err = auth.SignSubscriber(joinTestSecret, auth.ControlPlaneIssuer, sessionID, "application", time.Minute)
				}
				if err != nil {
					t.Fatal(err)
				}
				request := httptest.NewRequest(
					http.MethodPost,
					"/v1/sessions/"+sessionID+"/"+transport.path+"?bus=application",
					bytes.NewBufferString("authority rejection happens before SDP parsing"),
				)
				request.Header.Set("Authorization", "Bearer "+token)
				request.Header.Set("Content-Type", "application/sdp")
				response := httptest.NewRecorder()
				server.Handler().ServeHTTP(response, request)
				if response.Code != authorityStatus.wantStatus {
					t.Fatalf("%s status = %d, want %d; body=%s", transport.name, response.Code, authorityStatus.wantStatus, response.Body.String())
				}
				if _, found := server.relaySessions.Get(sessionID); found {
					t.Fatal("failed authority admission created a RelaySession")
				}
			})
		}
	}
}

func TestGivenStandaloneAuthorityWhenControlPlaneIsUnavailableThenAdmissionRemainsLocal(t *testing.T) {
	domain := testAccessService(t, joinTestSecret, access.Config{})
	created, err := domain.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	server := New(Config{JWTSecret: joinTestSecret, AuthorityMode: "standalone", AccessService: domain})
	cleanupTestRelay(t, server)
	if err := server.requireActiveControlSession(context.Background(), created.ID()); err != nil {
		t.Fatal(err)
	}
	if err := server.requireActiveControlSession(context.Background(), "phantom"); err == nil {
		t.Fatal("missing logical Session admitted")
	}
}

func TestGivenAuthorityTimeoutAboveMaximumWhenServerIsBuiltThenDeadlineIsClamped(t *testing.T) {
	server := New(Config{ControlAuthorityTimeout: 30 * time.Second})
	cleanupTestRelay(t, server)
	if server.controlAuthorityTimeout != defaultControlAuthorityTimeout {
		t.Fatalf(
			"control authority timeout = %s, want maximum %s",
			server.controlAuthorityTimeout,
			defaultControlAuthorityTimeout,
		)
	}
}
