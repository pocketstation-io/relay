package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/pocketstation-io/relay/access"
	"github.com/pocketstation-io/relay/internal/auth"
)

var joinTestSecret = []byte("join-test-secret-0123456789abcdef")

type joinInvitationResponse struct {
	JoinURL   string `json:"join_url"`
	JoinCode  string `json:"join_code"`
	SessionID string `json:"session_id"`
}

func newJoinTestServer(t *testing.T) (*Server, *access.Service) {
	service := testAccessService(t, joinTestSecret, access.Config{})
	server := New(Config{JWTSecret: joinTestSecret, AccessService: service, PublicReceiverURL: "https://receiver.example", PublicRelayURL: "https://relay.example"})
	cleanupTestRelay(t, server)
	return server, service
}

func createJoinTestSession(t *testing.T, server *Server) map[string]any {
	t.Helper()
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/sessions", nil))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("create Session status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var created map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	return created
}

func requestJoinInvitation(t *testing.T, server *Server, created map[string]any, body string) joinInvitationResponse {
	t.Helper()
	sessionID := created["session_id"].(string)
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+sessionID+"/invitations", bytes.NewBufferString(body))
	request.Header.Set("Authorization", "Bearer "+created["source_token"].(string))
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("create invitation status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var invitation joinInvitationResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &invitation); err != nil {
		t.Fatal(err)
	}
	return invitation
}

func TestGivenStandaloneSessionWhenCreatedThenReceiverInvitationIsNotPreissued(t *testing.T) {
	server, _ := newJoinTestServer(t)
	created := createJoinTestSession(t, server)
	if created["join_code"] != nil || created["join_url"] != nil {
		t.Fatalf("Session creation preissued invitation: %#v", created)
	}
}

func TestGivenControlPlaneAuthorityWhenRelayMutationIsRequestedThenItIsRejected(t *testing.T) {
	server := New(Config{
		JWTSecret:     joinTestSecret,
		AuthorityMode: "control-plane",
	})
	cleanupTestRelay(t, server)

	created := httptest.NewRecorder()
	server.Handler().ServeHTTP(created, httptest.NewRequest(http.MethodPost, "/v1/sessions", nil))
	if created.Code != http.StatusConflict {
		t.Fatalf("Relay Session creation status=%d, want 409", created.Code)
	}

	invitation := httptest.NewRecorder()
	server.Handler().ServeHTTP(invitation, httptest.NewRequest(http.MethodPost, "/v1/sessions/session-1/invitations", nil))
	if invitation.Code != http.StatusConflict {
		t.Fatalf("Relay invitation status=%d, want 409", invitation.Code)
	}

	resolved := httptest.NewRecorder()
	server.Handler().ServeHTTP(resolved, httptest.NewRequest(http.MethodPost, "/v1/join/code", nil))
	if resolved.Code != http.StatusConflict {
		t.Fatalf("Relay invitation resolution status=%d, want 409", resolved.Code)
	}
}

func TestGivenPartiallyReadySessionWhenInvitationIsRequestedThenOnlyActiveBusCanBeScoped(t *testing.T) {
	server, service := newJoinTestServer(t)
	created := createJoinTestSession(t, server)
	testReadyAccess(t, service, created["session_id"].(string), "application")

	mixRequest := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+created["session_id"].(string)+"/invitations", nil)
	mixRequest.Header.Set("Authorization", "Bearer "+created["source_token"].(string))
	mixResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(mixResponse, mixRequest)
	if mixResponse.Code != http.StatusConflict {
		t.Fatalf("one-bus mix invitation status=%d, want 409", mixResponse.Code)
	}

	invitation := requestJoinInvitation(t, server, created, `{"bus_id":"application"}`)
	parsed, err := url.Parse(invitation.JoinURL)
	if err != nil || parsed.Query().Get("join") != "" || parsed.Fragment != "join="+invitation.JoinCode || strings.Contains(invitation.JoinURL, created["session_id"].(string)) {
		t.Fatalf("unsafe invitation %#v err=%v", invitation, err)
	}
}

func TestGivenJoinCodeWhenRedeemedThenSubscriberCapabilityIsScopedAndSingleUse(t *testing.T) {
	server, service := newJoinTestServer(t)
	created := createJoinTestSession(t, server)
	testReadyAccess(t, service, created["session_id"].(string), "application", "microphone")
	invitation := requestJoinInvitation(t, server, created, `{"bus_id":"application"}`)

	resolve := func() *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/join", strings.NewReader(`{"join_code":"`+invitation.JoinCode+`"}`)))
		return response
	}
	first := resolve()
	if first.Code != http.StatusOK {
		t.Fatalf("first resolve status=%d body=%s", first.Code, first.Body.String())
	}
	var joined map[string]any
	_ = json.Unmarshal(first.Body.Bytes(), &joined)
	claims, err := auth.VerifyCapability(joinTestSecret, joined["subscriber_token"].(string), auth.RelayIssuer, auth.RoleSubscriber)
	if err != nil || claims.BusID != "application" {
		t.Fatalf("subscriber scope=%#v err=%v", claims, err)
	}
	if second := resolve(); second.Code != http.StatusNotFound {
		t.Fatalf("replayed invitation status=%d, want 404", second.Code)
	}
}
