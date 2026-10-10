package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/pocketstation-io/relay/internal/notifications/callback"

	"github.com/golang-jwt/jwt/v5"
	"github.com/pocketstation-io/relay/access/token"
	"github.com/pocketstation-io/relay/internal/auth"
)

var errAuthorityUnavailable = errors.New("Session service unavailable")

// Unverified data selects only the Session lookup. Its stored profile fixes the
// issuer and incarnation before the shared verifier can authorize anything.
func (s *Server) verifyCapability(encoded string, role auth.Role) (*auth.Claims, error) {
	if len(encoded) == 0 || len(encoded) > 8192 {
		return nil, auth.ErrInvalidCapability
	}
	untrusted := &auth.Claims{}
	if _, _, err := new(jwt.Parser).ParseUnverified(encoded, untrusted); err != nil {
		return nil, auth.ErrInvalidCapability
	}
	id := untrusted.SessionID
	if len(id) == 0 || len(id) > 128 {
		return nil, auth.ErrInvalidCapability
	}
	for _, ch := range id {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.') {
			return nil, auth.ErrInvalidCapability
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.controlAuthorityTimeout)
	defer cancel()
	metadata, err := s.authorityMetadata(ctx, id)
	if err != nil {
		if errors.Is(err, callback.ErrSessionNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", errAuthorityUnavailable, err)
	}
	claims, err := auth.VerifyCapability(s.jwtSecret, encoded, metadata.Issuer, role)
	if err != nil {
		return nil, err
	}
	if claims.SessionID != metadata.SessionID {
		return nil, auth.ErrInvalidCapability
	}
	if claims.Incarnation != metadata.Incarnation {
		if claims.Incarnation != "" || metadata.Issuer != token.LegacyIssuer || metadata.LegacyTokensBefore.IsZero() || claims.IssuedAt == nil || claims.IssuedAt.Time.After(metadata.LegacyTokensBefore) {
			return nil, auth.ErrInvalidCapability
		}
	}
	return claims, nil
}
