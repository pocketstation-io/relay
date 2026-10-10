package access

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pocketstation-io/relay/access/storage"
	"github.com/pocketstation-io/relay/access/storage/memory"
	"github.com/pocketstation-io/relay/access/token"
)

func TestGivenIdleSessionWhenOwnerRenewalUsesAdvertisedExpiryThenAuthoritySurvives(t *testing.T) {
	for _, test := range []struct {
		name       string
		config     Config
		ownerTTL   time.Duration
		sessionTTL time.Duration
	}{
		{"short_session", Config{SessionTTL: 5 * time.Minute}, 5 * time.Minute, 5 * time.Minute},
		{"short_owner", Config{OwnerTokenTTL: time.Minute}, time.Minute, 2 * time.Hour},
		{"defaults", Config{}, 15 * time.Minute, 2 * time.Hour},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Fractional seconds exercise the same whole-second expiry as the wire JWT.
			base := time.Now().UTC().Truncate(time.Second).Add(350 * time.Millisecond)
			var elapsed atomic.Int64
			clock := func() time.Time { return base.Add(time.Duration(elapsed.Load())) }
			backend := memory.NewWithClock(clock)
			t.Cleanup(func() { _ = backend.Close() })
			service, err := NewService(sessionTestSecret, backend, test.config)
			if err != nil {
				t.Fatal(err)
			}
			session, err := service.Create("application", "microphone")
			if err != nil {
				t.Fatal(err)
			}
			credential := session.SourceToken()
			claims, err := token.VerifyProfile(sessionTestSecret, credential, token.Issuer, clock())
			if err != nil {
				t.Fatal(err)
			}
			if !claims.ExpiresAt.Time.Equal(base.Add(test.ownerTTL).Truncate(time.Second)) {
				t.Error("initial owner expiry exceeds the configured authority lifetime")
			}
			incarnation := claims.Incarnation
			mux := testMux(service, HandlerConfig{})
			for renewal := 0; renewal < 3; renewal++ {
				// This is the clients' half-expiry schedule, without wall-clock sleeps,
				// media callbacks, token issuance, or another activity refreshing the row.
				elapsed.Add(int64(claims.ExpiresAt.Time.Sub(clock()) / 2))
				request := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+session.ID()+"/renew", nil)
				request.Header.Set("Authorization", "Bearer "+credential)
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, request)
				if response.Code != http.StatusOK {
					t.Fatalf("idle renewal %d: status=%d", renewal, response.Code)
				}
				var replacement struct {
					SourceToken string    `json:"source_token"`
					ExpiresAt   time.Time `json:"expires_at"`
				}
				if err := json.Unmarshal(response.Body.Bytes(), &replacement); err != nil {
					t.Fatal(err)
				}
				claims, err = token.VerifyProfile(sessionTestSecret, replacement.SourceToken, token.Issuer, clock())
				if err != nil {
					t.Fatal(err)
				}
				if !replacement.ExpiresAt.Equal(claims.ExpiresAt.Time) ||
					!replacement.ExpiresAt.Equal(clock().Add(test.ownerTTL).Truncate(time.Second)) {
					t.Fatal("renewal response and owner JWT disagree with the effective expiry")
				}
				if !claims.CanControl() || claims.Role != token.RoleSource ||
					claims.SessionID != session.ID() || claims.Incarnation != incarnation ||
					!slices.Equal(claims.BusIDs, []string{"application", "microphone"}) {
					t.Fatal("renewal changed owner authority or bus scope")
				}
				if err := backend.View(context.Background(), func(tx storage.Tx) error {
					row, found, err := tx.Session(session.ID())
					if err != nil {
						return err
					}
					if !found || !row.ExpiresAt.Equal(clock().Add(test.sessionTTL)) {
						t.Error("renewal did not extend the persisted idle Session deadline")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				credential = replacement.SourceToken
			}
			if test.name == "short_session" && !clock().After(base.Add(test.sessionTTL)) {
				t.Fatal("test did not maintain authority beyond the original Session expiry")
			}
			// An otherwise valid longer-lived token cannot revive an expired row.
			lateToken, err := token.SignProfile(sessionTestSecret, token.Issuer, incarnation,
				token.Claims{SessionID: session.ID(), Role: token.RoleSource, BusIDs: session.RequiredBuses(), Control: true},
				2*test.sessionTTL, clock())
			if err != nil {
				t.Fatal(err)
			}
			elapsed.Add(int64(test.sessionTTL))
			if _, _, err := service.RenewOwner(context.Background(), session.ID(), lateToken); err == nil {
				t.Fatal("expired Session revived by a still-valid owner token")
			}
		})
	}
}

func TestGivenShortSessionWhenGranularMediaTokensIssuedThenTheirLifetimesStayIndependent(t *testing.T) {
	base := time.Now().UTC().Truncate(time.Second).Add(350 * time.Millisecond)
	backend := memory.NewWithClock(func() time.Time { return base })
	t.Cleanup(func() { _ = backend.Close() })
	service, err := NewService(sessionTestSecret, backend, Config{
		SessionTTL: 5 * time.Minute, PublisherTokenTTL: 17 * time.Minute, SubscriberTokenTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		role  token.Role
		ttl   time.Duration
		issue func(context.Context, string, string) (string, error)
	}{
		{token.RoleSource, 17 * time.Minute, service.IssuePublisherTokenContext},
		{token.RoleSubscriber, time.Hour, service.IssueSubscriberTokenContext},
	} {
		credential, err := test.issue(context.Background(), session.ID(), "application")
		if err != nil {
			t.Fatal(err)
		}
		claims, err := token.VerifyProfile(sessionTestSecret, credential, token.Issuer, base)
		if err != nil {
			t.Fatal(err)
		}
		if !claims.ExpiresAt.Time.Equal(base.Add(test.ttl).Truncate(time.Second)) ||
			claims.CanControl() || claims.Role != test.role || !claims.AllowsBus("application") || claims.AllowsBus("microphone") {
			t.Fatal("owner expiry policy changed granular media lifetime or scope")
		}
	}
}
