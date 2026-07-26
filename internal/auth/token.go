package auth

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Role identifies the capability of a token holder.
type Role string

const (
	RoleSource     Role = "source"
	RoleListener   Role = "listener" // v2.3 alias; v3.0 uses RoleSubscriber
	RoleSubscriber Role = "subscriber"
)

// Claims is the JWT payload for relay session tokens.
// SessionID (v3.0) and RoomID (v2.3 alias) both identify the target RelaySession.
// BusID identifies the named bus for PUBLISH tokens; empty means "any bus".
type Claims struct {
	SessionID string `json:"session_id,omitempty"` // v3.0
	RoomID    string `json:"room_id,omitempty"`    // v2.3 alias — used when SessionID absent
	BusID     string `json:"bus_id,omitempty"`     // named bus; "" = any / mix
	Role      Role   `json:"role"`
	jwt.RegisteredClaims
}

// EffectiveSessionID returns the session/room ID, preferring SessionID (v3.0)
// over RoomID (v2.3) for backward compatibility.
func (c *Claims) EffectiveSessionID() string {
	if c.SessionID != "" {
		return c.SessionID
	}
	return c.RoomID
}

// Sign mints a signed HS256 JWT for sessionID with the given role and ttl.
// busID may be empty (any bus / subscriber token).
func Sign(secret []byte, sessionID string, role Role, ttl time.Duration) (string, error) {
	return SignBus(secret, sessionID, "", role, ttl)
}

// SignBus mints a signed HS256 JWT scoped to a specific bus.
func SignBus(secret []byte, sessionID, busID string, role Role, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		SessionID: sessionID,
		RoomID:    sessionID, // keep v2.3 field populated for old clients
		BusID:     busID,
		Role:      role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
			IssuedAt:  jwt.NewNumericDate(now),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
}

// Verify parses and validates a relay JWT. Returns the Claims on success.
func Verify(secret []byte, token string) (*Claims, error) {
	parsed, err := jwt.ParseWithClaims(token, &Claims{}, func(t *jwt.Token) (any, error) {
		return secret, nil
	})
	if err != nil {
		return nil, err
	}
	claims, ok := parsed.Claims.(*Claims)
	if !ok || !parsed.Valid {
		return nil, jwt.ErrTokenInvalidClaims
	}
	return claims, nil
}
