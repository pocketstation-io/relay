// Package auth validates explicit Relay transport capabilities.
package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	ControlPlaneIssuer  = "pocketstation-control-plane"
	RelayIssuer         = "pocketstation-relay"
	Audience            = "pocketstation-relay"
	tokenTyp            = "pks-relay-capability+jwt"
	maxBusScopes        = 16
	maxIdentifierLength = 128
)

type Role string

const (
	RoleSource     Role = "source"
	RoleSubscriber Role = "subscriber"
)

var ErrInvalidCapability = errors.New("invalid relay capability")

type Claims struct {
	SessionID string   `json:"session_id"`
	BusID     string   `json:"bus_id,omitempty"`
	BusIDs    []string `json:"bus_ids,omitempty"`
	Role      Role     `json:"role"`
	jwt.RegisteredClaims
}

func (claims *Claims) EffectiveSessionID() string { return claims.SessionID }

// Sign is the standalone-service helper. Production control-plane source
// capabilities use SignSource with ControlPlaneIssuer.
func Sign(secret []byte, sessionID string, role Role, ttl time.Duration) (string, error) {
	switch role {
	case RoleSource:
		return SignSource(secret, RelayIssuer, sessionID, []string{"voice"}, ttl)
	case RoleSubscriber:
		return SignSubscriber(secret, RelayIssuer, sessionID, "mix", ttl)
	default:
		return "", ErrInvalidCapability
	}
}

func SignBus(secret []byte, sessionID, busID string, role Role, ttl time.Duration) (string, error) {
	switch role {
	case RoleSource:
		return SignSource(secret, RelayIssuer, sessionID, []string{busID}, ttl)
	case RoleSubscriber:
		return SignSubscriber(secret, RelayIssuer, sessionID, busID, ttl)
	default:
		return "", ErrInvalidCapability
	}
}

func SignSource(secret []byte, issuer, sessionID string, busIDs []string, ttl time.Duration) (string, error) {
	if len(busIDs) == 0 || len(busIDs) > maxBusScopes || invalidIdentifiers(busIDs, 64) {
		return "", ErrInvalidCapability
	}
	return sign(secret, issuer, Claims{SessionID: sessionID, BusIDs: append([]string(nil), busIDs...), Role: RoleSource}, ttl)
}

func SignSubscriber(secret []byte, issuer, sessionID, busID string, ttl time.Duration) (string, error) {
	if !validIdentifier(busID, 64) {
		return "", ErrInvalidCapability
	}
	return sign(secret, issuer, Claims{SessionID: sessionID, BusID: busID, Role: RoleSubscriber}, ttl)
}

func sign(secret []byte, issuer string, claims Claims, ttl time.Duration) (string, error) {
	if len(secret) < 32 || !validIdentifier(claims.SessionID, maxIdentifierLength) || (issuer != ControlPlaneIssuer && issuer != RelayIssuer) || ttl <= 0 {
		return "", ErrInvalidCapability
	}
	now := time.Now().UTC()
	jti, err := randomTokenID()
	if err != nil {
		return "", err
	}
	claims.RegisteredClaims = jwt.RegisteredClaims{
		Issuer: issuer, Subject: "relay-session:" + claims.SessionID + ":" + string(claims.Role),
		Audience: jwt.ClaimStrings{Audience}, ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		NotBefore: jwt.NewNumericDate(now.Add(-5 * time.Second)), IssuedAt: jwt.NewNumericDate(now), ID: jti,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["typ"] = tokenTyp
	return token.SignedString(secret)
}

func Verify(secret []byte, encoded string) (*Claims, error) {
	return verify(secret, encoded, RelayIssuer, "")
}

// VerifyCapability validates one mutually exclusive issuer+role profile.
func VerifyCapability(secret []byte, encoded, issuer string, role Role) (*Claims, error) {
	return verify(secret, encoded, issuer, role)
}

func verify(secret []byte, encoded, issuer string, expectedRole Role) (*Claims, error) {
	if len(secret) < 32 || encoded == "" {
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
		jwt.WithIssuer(issuer), jwt.WithAudience(Audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithLeeway(5*time.Second),
	)
	if err != nil || !parsed.Valid || !validIdentifier(claims.SessionID, maxIdentifierLength) || claims.ID == "" ||
		claims.IssuedAt == nil || claims.NotBefore == nil || claims.ExpiresAt == nil ||
		claims.Subject != "relay-session:"+claims.SessionID+":"+string(claims.Role) ||
		claims.ExpiresAt.Time.Before(claims.IssuedAt.Time) {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCapability, err)
	}
	if expectedRole != "" && claims.Role != expectedRole {
		return nil, ErrInvalidCapability
	}
	switch claims.Role {
	case RoleSource:
		if claims.BusID != "" || len(claims.BusIDs) == 0 || len(claims.BusIDs) > maxBusScopes || invalidIdentifiers(claims.BusIDs, 64) {
			return nil, ErrInvalidCapability
		}
	case RoleSubscriber:
		if !validIdentifier(claims.BusID, 64) || len(claims.BusIDs) != 0 {
			return nil, ErrInvalidCapability
		}
	default:
		return nil, ErrInvalidCapability
	}
	return claims, nil
}

func (claims *Claims) AllowsBus(busID string) bool {
	if claims.Role == RoleSubscriber {
		return claims.BusID == busID
	}
	for _, allowed := range claims.BusIDs {
		if allowed == busID {
			return true
		}
	}
	return false
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
