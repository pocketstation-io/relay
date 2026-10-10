package server

import (
	"context"
	"github.com/pocketstation-io/relay/access"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/notifications/callback"
	"github.com/pocketstation-io/relay/internal/signaling"
)

func TestGivenSubscriberBusScopeWhenJoiningThenOnlyExactBusIsAccepted(t *testing.T) {
	for _, authority := range []struct {
		name   string
		issuer string
		mode   string
	}{
		{name: "standalone", issuer: auth.RelayIssuer, mode: "standalone"},
		{name: "control plane", issuer: auth.ControlPlaneIssuer, mode: "control-plane"},
	} {
		for _, test := range []struct {
			name      string
			requested string
			allowed   bool
		}{
			{name: "claim bus when omitted", allowed: true},
			{name: "explicit claim bus", requested: "application", allowed: true},
			{name: "different bus", requested: "microphone"},
			{name: "mix override", requested: "mix"},
		} {
			t.Run(authority.name+"/"+test.name, func(t *testing.T) {
				service := testAccessService(t, joinTestSecret, access.Config{})
				created, err := service.Create("application", "microphone")
				if err != nil {
					t.Fatal(err)
				}
				var controlClient *callback.Client
				if authority.mode == "control-plane" {
					controlClient = testManagedAccess(t, service)
				}
				server := New(Config{
					JWTSecret:      joinTestSecret,
					AuthorityMode:  authority.mode,
					CallbackClient: controlClient,
					AccessService:  service,
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
				if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				sessionID := created.ID()
				token, err := service.IssueSubscriberTokenContext(context.Background(), sessionID, "application")
				if err != nil {
					t.Fatal(err)
				}
				if err := connection.WriteJSON(signaling.ClientMessage{
					Type: signaling.TypeSubscribe, Token: token, BusID: test.requested,
				}); err != nil {
					t.Fatal(err)
				}
				var response signaling.ServerMessage
				if err := connection.ReadJSON(&response); err != nil {
					t.Fatal(err)
				}
				if test.allowed {
					if response.Type != signaling.TypeSDPOffer || response.BusID != "application" {
						t.Fatalf("authorized JOIN response = %#v, want application SDP offer", response)
					}
					return
				}
				if response.Type != signaling.TypeError || response.Code != string(signaling.ErrCodeBadRequest) {
					t.Fatalf("unauthorized JOIN response = %#v, want bad_request", response)
				}
				if _, found := server.relaySessions.Get(sessionID); found {
					t.Fatal("unauthorized JOIN created a RelaySession")
				}
			})
		}
	}
}
