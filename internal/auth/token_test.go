package auth

import (
	"testing"
	"time"
)

var authTestSecret = []byte("0123456789abcdef0123456789abcdef")

func TestGivenControlPlaneSourceCapabilityWhenVerifiedThenBusScopeIsStrict(t *testing.T) {
	token, err := SignSource(authTestSecret, ControlPlaneIssuer, "session-1", []string{"application", "microphone"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := VerifyCapability(authTestSecret, token, ControlPlaneIssuer, RoleSource)
	if err != nil {
		t.Fatal(err)
	}
	if !claims.AllowsBus("application") || claims.AllowsBus("other") || claims.SessionID != "session-1" {
		t.Fatalf("unexpected claims %#v", claims)
	}
	if _, err := VerifyCapability(authTestSecret, token, RelayIssuer, RoleSource); err == nil {
		t.Fatal("control-plane token accepted under Relay issuer")
	}
	if _, err := VerifyCapability(authTestSecret, token, ControlPlaneIssuer, RoleSubscriber); err == nil {
		t.Fatal("source token accepted as subscriber")
	}
}

func TestGivenRelaySubscriberCapabilityWhenVerifiedThenBusScopeIsStrict(t *testing.T) {
	token, err := SignSubscriber(authTestSecret, RelayIssuer, "session-1", "application", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := VerifyCapability(authTestSecret, token, RelayIssuer, RoleSubscriber)
	if err != nil || !claims.AllowsBus("application") || claims.AllowsBus("mix") {
		t.Fatalf("claims=%#v err=%v", claims, err)
	}
}

func TestGivenWrongSecretOrMalformedTokenWhenVerifiedThenValidationFails(t *testing.T) {
	token, _ := SignSubscriber(authTestSecret, RelayIssuer, "session-1", "mix", time.Minute)
	if _, err := VerifyCapability([]byte("abcdef0123456789abcdef0123456789"), token, RelayIssuer, RoleSubscriber); err == nil {
		t.Fatal("wrong secret accepted")
	}
	if _, err := VerifyCapability(authTestSecret, "not-a-token", RelayIssuer, RoleSubscriber); err == nil {
		t.Fatal("malformed token accepted")
	}
}
