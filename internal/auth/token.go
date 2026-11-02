// Package auth adapts media callers to Relay's single capability implementation.
package auth

import (
	"github.com/pocketstation-io/relay/access/token"
	"time"
)

const (
	ControlPlaneIssuer = token.LegacyIssuer
	RelayIssuer        = token.Issuer
	Audience           = token.Audience
)

type Role = token.Role

const (
	RoleSource     = token.RoleSource
	RoleSubscriber = token.RoleSubscriber
)

type Claims = token.Claims

var ErrInvalidCapability = token.ErrInvalidCapability

func Sign(secret []byte, id string, role Role, ttl time.Duration) (string, error) {
	if role == RoleSource {
		return SignSource(secret, RelayIssuer, id, []string{"voice"}, ttl)
	}
	if role == RoleSubscriber {
		return SignSubscriber(secret, RelayIssuer, id, "mix", ttl)
	}
	return "", ErrInvalidCapability
}
func SignBus(secret []byte, id, bus string, role Role, ttl time.Duration) (string, error) {
	if role == RoleSource {
		return SignSource(secret, RelayIssuer, id, []string{bus}, ttl)
	}
	if role == RoleSubscriber {
		return SignSubscriber(secret, RelayIssuer, id, bus, ttl)
	}
	return "", ErrInvalidCapability
}
func SignSource(secret []byte, issuer, id string, buses []string, ttl time.Duration) (string, error) {
	return token.SignProfile(secret, issuer, "", Claims{SessionID: id, BusIDs: buses, Role: RoleSource}, ttl, time.Now().UTC())
}
func SignSubscriber(secret []byte, issuer, id, bus string, ttl time.Duration) (string, error) {
	return token.SignProfile(secret, issuer, "", Claims{SessionID: id, BusID: bus, Role: RoleSubscriber}, ttl, time.Now().UTC())
}
func Verify(secret []byte, encoded string) (*Claims, error) { return token.Verify(secret, encoded) }
func VerifyCapability(secret []byte, encoded, issuer string, role Role) (*Claims, error) {
	claims, err := token.VerifyProfile(secret, encoded, issuer, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	if role != "" && claims.Role != role {
		return nil, ErrInvalidCapability
	}
	return claims, nil
}
