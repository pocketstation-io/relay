package server

import (
	"testing"
	"time"

	"github.com/pocketstation-io/relay/internal/auth"
)

var capabilityTestSecret = []byte("capability-test-secret-0123456789abcdef")

func TestGivenControlPlaneModeWhenSubscriberCapabilityIsVerifiedThenOnlyControlIssuerIsAccepted(t *testing.T) {
	server := New(Config{
		JWTSecret:         capabilityTestSecret,
		SourceTokenIssuer: auth.ControlPlaneIssuer,
		AuthorityMode:     "control-plane",
	})
	controlToken, err := auth.SignSubscriber(
		capabilityTestSecret,
		auth.ControlPlaneIssuer,
		"session-1",
		"application",
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := server.verifyCapability(controlToken, auth.RoleSubscriber)
	if err != nil || claims.BusID != "application" {
		t.Fatalf("control-plane capability rejected: claims=%#v err=%v", claims, err)
	}

	standaloneToken, err := auth.SignSubscriber(
		capabilityTestSecret,
		auth.RelayIssuer,
		"session-1",
		"application",
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.verifyCapability(standaloneToken, auth.RoleSubscriber); err == nil {
		t.Fatal("control-plane mode accepted a Relay-issued subscriber capability")
	}
}

func TestGivenStandaloneModeWhenSubscriberCapabilityIsVerifiedThenRelayIssuerIsAccepted(t *testing.T) {
	server := New(Config{
		JWTSecret:           capabilityTestSecret,
		SubscriberJWTSecret: capabilityTestSecret,
		SourceTokenIssuer:   auth.RelayIssuer,
		AuthorityMode:       "standalone",
	})
	token, err := auth.SignSubscriber(
		capabilityTestSecret,
		auth.RelayIssuer,
		"session-1",
		"mix",
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.verifyCapability(token, auth.RoleSubscriber); err != nil {
		t.Fatalf("standalone capability rejected: %v", err)
	}
}
