package server

import (
	"context"
	"github.com/pocketstation-io/relay/access"
	"testing"
	"time"

	"github.com/pocketstation-io/relay/internal/auth"
)

var capabilityTestSecret = []byte("capability-test-secret-0123456789abcdef")

func TestGivenManagedSessionWhenSubscriberCapabilityIsVerifiedThenStoredIssuerAndIncarnationAreRequired(t *testing.T) {
	service := testAccessService(t, capabilityTestSecret, access.Config{})
	created, err := service.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	server := New(Config{JWTSecret: capabilityTestSecret, AuthorityMode: "control-plane", CallbackClient: testManagedAccess(t, service)})
	cleanupTestRelay(t, server)
	issued, err := service.IssueSubscriberTokenContext(context.Background(), created.ID(), "application")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := server.verifyCapability(issued, auth.RoleSubscriber)
	if err != nil || claims.BusID != "application" {
		t.Fatalf("legitimate token rejected: %v", err)
	}
	for _, issuer := range []string{auth.ControlPlaneIssuer, auth.RelayIssuer} {
		forged, err := auth.SignSubscriber(capabilityTestSecret, issuer, created.ID(), "application", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = server.verifyCapability(forged, auth.RoleSubscriber); err == nil {
			t.Fatal("unbound legacy token accepted for current Session")
		}
	}
}

func TestGivenStandaloneSessionWhenSubscriberCapabilityIsVerifiedThenSessionMustExist(t *testing.T) {
	service := testAccessService(t, capabilityTestSecret, access.Config{})
	created, err := service.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	server := New(Config{JWTSecret: capabilityTestSecret, AccessService: service, AuthorityMode: "standalone"})
	cleanupTestRelay(t, server)
	issued, err := service.IssueSubscriberTokenContext(context.Background(), created.ID(), "application")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = server.verifyCapability(issued, auth.RoleSubscriber); err != nil {
		t.Fatalf("legitimate token rejected: %v", err)
	}
	phantom, err := auth.SignSubscriber(capabilityTestSecret, auth.RelayIssuer, "phantom-session", "application", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = server.verifyCapability(phantom, auth.RoleSubscriber); err == nil {
		t.Fatal("phantom Session accepted")
	}
}
