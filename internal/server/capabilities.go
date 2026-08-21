package server

import "github.com/pocketstation-io/relay/internal/auth"

// verifyCapability keeps credential authority explicit. In control-plane mode,
// the control plane signs both source and subscriber capabilities. Standalone
// Relay signs both itself. The two modes never silently accept each other's
// credentials.
func (s *Server) verifyCapability(encoded string, role auth.Role) (*auth.Claims, error) {
	secret := s.jwtSecret
	issuer := s.sourceTokenIssuer
	if s.authorityMode == "standalone" && role == auth.RoleSubscriber {
		secret = s.subscriberJWTSecret
		issuer = auth.RelayIssuer
	}
	return auth.VerifyCapability(secret, encoded, issuer, role)
}
