// Package token issues the capability tokens accepted by PocketStation Relay.
// Tokens identify one RelaySession and one explicit capability. They are not
// user sessions and must not be reused for unrelated HTTP services.
package token

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	Issuer              = "pocketstation-relay"
	LegacyIssuer        = "pocketstation-control-plane"
	Audience            = "pocketstation-relay"
	tokenTyp            = "pks-relay-capability+jwt"
	maxBusScopes        = 16
	maxIdentifierLength = 128
)

// Role identifies the operation authorized by a capability token.
type Role string

const (
	RoleSource     Role = "source"
	RoleSubscriber Role = "subscriber"
)

var ErrInvalidCapability = errors.New("invalid relay capability")

// Claims is the versioned Relay capability payload shared with Relay.
// Source tokens carry BusIDs. Subscriber tokens carry exactly one BusID;
// "mix" means the declared mixed receiver output.
type Claims struct {
	SessionID string   `json:"session_id"`
	BusID     string   `json:"bus_id,omitempty"`
	BusIDs    []string `json:"bus_ids,omitempty"`
	Role      Role     `json:"role"`
	// Control distinguishes the Session-owner source capability from a
	// media-only publisher capability. Relay accepts either source capability,
	// while Session access operations require Control=true.
	Control     bool   `json:"control,omitempty"`
	Incarnation string `json:"incarnation,omitempty"`
	jwt.RegisteredClaims
}

// SignSource issues the Session-owner source capability. It may publish to the
// declared buses and authorize Session owner operations.
func SignSource(secret []byte, sessionID string, busIDs []string, ttl time.Duration) (string, error) {
	if len(busIDs) == 0 || len(busIDs) > maxBusScopes || invalidIdentifiers(busIDs, 64) {
		return "", fmt.Errorf("%w: source capability requires at least one bus", ErrInvalidCapability)
	}
	return sign(secret, Claims{SessionID: sessionID, BusIDs: append([]string(nil), busIDs...), Role: RoleSource, Control: true}, ttl)
}

// SignPublisher issues a media-only source capability limited to exactly one
// AudioBus. It cannot authorize Session reads, mutation, invitations, or new
// Session credentials.
func SignPublisher(secret []byte, sessionID, busID string, ttl time.Duration) (string, error) {
	if !validIdentifier(busID, 64) {
		return "", fmt.Errorf("%w: publisher capability requires a bus", ErrInvalidCapability)
	}
	return sign(secret, Claims{SessionID: sessionID, BusIDs: []string{busID}, Role: RoleSource}, ttl)
}

// SignSubscriber issues a receiver capability limited to one bus or "mix".
func SignSubscriber(secret []byte, sessionID, busID string, ttl time.Duration) (string, error) {
	if !validIdentifier(busID, 64) {
		return "", fmt.Errorf("%w: subscriber capability requires a bus", ErrInvalidCapability)
	}
	return sign(secret, Claims{SessionID: sessionID, BusID: busID, Role: RoleSubscriber}, ttl)
}

func sign(secret []byte, claims Claims, ttl time.Duration) (string, error) {
	return SignProfile(secret, Issuer, "", claims, ttl, time.Now().UTC())
}

// SignProfile signs one configured Session profile; callers commit before exposing the result.
func SignProfile(secret []byte, issuer, incarnation string, claims Claims, ttl time.Duration, now time.Time) (string, error) {
	if issuer != Issuer && issuer != LegacyIssuer {
		return "", ErrInvalidCapability
	}
	claims.Incarnation = incarnation
	if err := validateScope(&claims); err != nil {
		return "", err
	}
	if len(secret) < 32 || !validIdentifier(claims.SessionID, maxIdentifierLength) || ttl <= 0 {
		return "", ErrInvalidCapability
	}
	jti, err := randomTokenID()
	if err != nil {
		return "", err
	}
	claims.RegisteredClaims = jwt.RegisteredClaims{
		Issuer:    issuer,
		Subject:   "relay-session:" + claims.SessionID + ":" + string(claims.Role),
		Audience:  jwt.ClaimStrings{Audience},
		ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		NotBefore: jwt.NewNumericDate(now.Add(-5 * time.Second)),
		IssuedAt:  jwt.NewNumericDate(now),
		ID:        jti,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["typ"] = tokenTyp
	return token.SignedString(secret)
}

// Verify accepts only the PocketStation HS256 capability profile and validates
// issuer, audience, time bounds, token type, role, Session, and bus scope.
func Verify(secret []byte, encoded string) (*Claims, error) {
	return VerifyProfile(secret, encoded, Issuer, time.Now().UTC())
}

// VerifyProfile accepts exactly the configured issuer, never a caller-selected fallback.
func VerifyProfile(secret []byte, encoded, issuer string, now time.Time) (*Claims, error) {
	if issuer != Issuer && issuer != LegacyIssuer {
		return nil, ErrInvalidCapability
	}
	if len(secret) < 32 || encoded == "" || len(encoded) > 16384 {
		return nil, ErrInvalidCapability
	}
	claims := &Claims{}
	parsed, err := jwt.ParseWithClaims(
		encoded,
		claims,
		func(token *jwt.Token) (any, error) {
			if token.Method != jwt.SigningMethodHS256 || token.Header["typ"] != tokenTyp {
				return nil, ErrInvalidCapability
			}
			return secret, nil
		},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(issuer),
		jwt.WithTimeFunc(func() time.Time { return now }),
		jwt.WithAudience(Audience),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(5*time.Second),
	)
	if err != nil || !parsed.Valid {
		return nil, ErrInvalidCapability
	}
	if !validIdentifier(claims.SessionID, maxIdentifierLength) || claims.ID == "" ||
		claims.IssuedAt == nil || claims.NotBefore == nil || claims.ExpiresAt == nil ||
		claims.Subject != "relay-session:"+claims.SessionID+":"+string(claims.Role) ||
		claims.ExpiresAt.Time.Before(claims.IssuedAt.Time) {
		return nil, ErrInvalidCapability
	}
	if err := validateScope(claims); err != nil {
		return nil, err
	}
	return claims, nil
}

// CanControl reports whether this is the Session-owner source capability.
func (claims *Claims) CanControl() bool {
	return claims.Role == RoleSource && claims.Control
}

func invalidIdentifiers(values []string, maximum int) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !validIdentifier(value, maximum) {
			return true
		}
		if _, found := seen[value]; found {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}

func validIdentifier(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func randomTokenID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func (claims *Claims) EffectiveSessionID() string { return claims.SessionID }
func (claims *Claims) AllowsBus(busID string) bool {
	if claims.Role == RoleSubscriber {
		return claims.BusID == busID
	}
	for _, bus := range claims.BusIDs {
		if bus == busID {
			return true
		}
	}
	return false
}

func validateScope(claims *Claims) error {
	switch claims.Role {
	case RoleSource:
		if claims.BusID != "" || len(claims.BusIDs) == 0 || len(claims.BusIDs) > maxBusScopes || invalidIdentifiers(claims.BusIDs, 64) {
			return ErrInvalidCapability
		}
	case RoleSubscriber:
		if !validIdentifier(claims.BusID, 64) || len(claims.BusIDs) != 0 || claims.Control {
			return ErrInvalidCapability
		}
	default:
		return ErrInvalidCapability
	}
	return nil
}
