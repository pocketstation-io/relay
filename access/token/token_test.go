package token

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var testSecret = []byte("0123456789abcdef0123456789abcdef")

func TestGivenSourceCapabilityWhenVerifiedThenBusScopeRoundTrips(t *testing.T) {
	token, err := SignSource(testSecret, "session-1", []string{"application", "microphone"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := Verify(testSecret, token)
	if err != nil {
		t.Fatal(err)
	}
	if claims.Role != RoleSource || claims.SessionID != "session-1" || len(claims.BusIDs) != 2 {
		t.Fatalf("unexpected claims: %#v", claims)
	}
	if !claims.CanControl() {
		t.Fatal("Session-owner source capability lost control authority")
	}
}

func TestGivenPublisherCapabilityWhenVerifiedThenItIsMediaOnlyAndBusScoped(t *testing.T) {
	token, err := SignPublisher(testSecret, "session-1", "microphone", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := Verify(testSecret, token)
	if err != nil {
		t.Fatal(err)
	}
	if claims.CanControl() || claims.Role != RoleSource || len(claims.BusIDs) != 1 || claims.BusIDs[0] != "microphone" {
		t.Fatalf("unexpected publisher claims: %#v", claims)
	}
}

func TestGivenSubscriberCapabilityWhenBusIsMissingThenSigningFails(t *testing.T) {
	if _, err := SignSubscriber(testSecret, "session-1", "", time.Minute); err == nil {
		t.Fatal("empty subscriber scope accepted")
	}
	token, err := SignSubscriber(testSecret, "session-1", "application", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := Verify(testSecret, token)
	if err != nil || claims.BusID != "application" || claims.Role != RoleSubscriber {
		t.Fatalf("unexpected result: claims=%#v err=%v", claims, err)
	}
}

func TestGivenInvalidJWTProfilesWhenVerifiedThenTheyAreRejected(t *testing.T) {
	now := time.Now().UTC()
	base := Claims{
		SessionID: "session-1",
		BusID:     "mix",
		Role:      RoleSubscriber,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: Issuer, Subject: "relay-session:session-1:subscriber",
			Audience: jwt.ClaimStrings{Audience}, ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
			IssuedAt: jwt.NewNumericDate(now), ID: "test-id",
		},
	}
	for name, mutate := range map[string]func(*jwt.Token, *Claims){
		"issuer":   func(_ *jwt.Token, claims *Claims) { claims.Issuer = "other" },
		"audience": func(_ *jwt.Token, claims *Claims) { claims.Audience = jwt.ClaimStrings{"other"} },
		"type":     func(token *jwt.Token, _ *Claims) { token.Header["typ"] = "JWT" },
	} {
		t.Run(name, func(t *testing.T) {
			claims := base
			token := jwt.NewWithClaims(jwt.SigningMethodHS256, &claims)
			token.Header["typ"] = tokenTyp
			mutate(token, &claims)
			encoded, err := token.SignedString(testSecret)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Verify(testSecret, encoded); err == nil {
				t.Fatal("invalid token accepted")
			}
		})
	}
	unsigned := jwt.NewWithClaims(jwt.SigningMethodNone, &base)
	unsigned.Header["typ"] = tokenTyp
	encoded, err := unsigned.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(testSecret, encoded); err == nil {
		t.Fatal("alg=none token accepted")
	}
}
