package access

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	auth "github.com/pocketstation-io/relay/access/token"
)

func TestGivenReadableFormatsWhenJoiningThenOnlyMatchingOpaqueCapabilityAuthorizes(t *testing.T) {
	for _, format := range []InvitationVisibility{InvitationVisibilityPublic, InvitationVisibilityPrivate} {
		t.Run(string(format), func(t *testing.T) {
			store, session, mux := readyInvitationTestServer(t)
			ctx := context.Background()
			first, err := store.CreateInvitationContext(ctx, session.ID(), "application", format)
			if err != nil {
				t.Fatal(err)
			}
			other, err := store.CreateInvitationContext(ctx, session.ID(), "microphone", format)
			if err != nil {
				t.Fatal(err)
			}
			post := func(target string, body map[string]string, status int) *httptest.ResponseRecorder {
				t.Helper()
				encoded, err := json.Marshal(body)
				if err != nil {
					t.Fatal(err)
				}
				req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(encoded))
				req.RemoteAddr = "192.0.2.55:4000"
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, req)
				if response.Code != status {
					t.Fatalf("HTTP status=%d want=%d", response.Code, status)
				}
				return response
			}
			for _, target := range []string{"/v1/join/" + first.Alias, "/v1/invitations/" + first.Alias + "/redeem"} {
				for _, code := range []string{"", other.Code, session.ID(), "abcdefghijklmnopqrstuv"} {
					post(target, map[string]string{"join_code": code}, http.StatusNotFound)
				}
			}
			post("/v1/join", map[string]string{}, http.StatusNotFound)
			post("/v1/join/"+first.Alias, map[string]string{"secret": "abcdefghijklmnopqrstuv"}, http.StatusNotFound)
			post("/v1/join/"+first.Alias, map[string]string{"join_code": first.Code, "secret": other.Code}, http.StatusBadRequest)
			preview := httptest.NewRecorder()
			mux.ServeHTTP(preview, httptest.NewRequest(http.MethodGet, "/v1/invitations/"+first.Alias, nil))
			if preview.Code != http.StatusOK || bytes.Contains(preview.Body.Bytes(), []byte(first.Code)) {
				t.Fatal("preview failed or disclosed join capability")
			}
			response := post("/v1/join/"+first.Alias, map[string]string{"join_code": first.Code}, http.StatusOK)
			var resolved resolveInvitationResponse
			if err := json.Unmarshal(response.Body.Bytes(), &resolved); err != nil {
				t.Fatal(err)
			}
			claims, err := auth.Verify(sessionTestSecret, resolved.SubscriberToken)
			if err != nil || claims.SessionID != session.ID() || claims.BusID != "application" || claims.Role != auth.RoleSubscriber {
				t.Fatal("matching grant did not preserve exact Session/role/bus")
			}
			post("/v1/join", map[string]string{"join_code": first.Code}, http.StatusNotFound)
			// The original opaque handoff works without a readable name or secret policy.
			post("/v1/join", map[string]string{"join_code": other.Code}, http.StatusOK)
			post("/v1/join/"+other.Code, map[string]string{}, http.StatusNotFound)
		})
	}
}

func TestGivenDeprecatedCredentialFormsWhenUsedThenTheyMatchCanonicalOneUseGrant(t *testing.T) {
	store, session, mux := readyInvitationTestServer(t)
	for _, legacyBody := range []bool{false, true} {
		grant, err := store.CreateInvitation(session.ID(), "application")
		if err != nil {
			t.Fatal(err)
		}
		target := "/v1/join/" + grant.Code
		body := []byte(`{}`)
		if legacyBody {
			target = "/v1/invitations/" + grant.Alias + "/redeem"
			body, _ = json.Marshal(map[string]string{"secret": grant.Code})
		}
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body)))
		if response.Code != http.StatusOK {
			t.Fatalf("deprecated credential status=%d", response.Code)
		}
		if _, err := store.ResolveInvitation(grant.Code); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("deprecated form bypassed one-use semantics")
		}
	}
}
