// Package turn implements HMAC-SHA1 long-term credential generation for the
// embedded TURN server per RFC 5766 §9.2 / RFC 8489 §9.2.
//
// Credential format (username): "<expiry_unix_seconds>:<session_id>"
// Password: base64( HMAC-SHA1( sharedSecret, username ) )
//
// The relay's embedded TURN server (pion/turn) validates credentials using the
// same algorithm. Compositions inject TURN_SHARED_SECRET as the dedicated
// TURN credential key; it is separate from the Session capability signing key.
//
// Per-user quotas and IP allow-listing are not implemented.
package turn

import (
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec // SHA-1 required by RFC 5766 §9.2; not used for password hashing
	"encoding/base64"
	"fmt"
	"time"
)

// ICEServer is the JSON representation of a STUN or TURN server returned in
// POST /v1/sessions responses. Matches the RTCIceServer WebRTC API shape so
// clients can pass it directly to RTCPeerConnection.
type ICEServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

// Credentials generates a time-limited TURN username/password pair for the
// given sessionID using secret as the HMAC key.
//
// ttlDuration is the credential validity window. The expiry is embedded in the
// username so the server can reject stale credentials without state.
func Credentials(secret []byte, sessionID string, ttlDuration time.Duration) (username, password string) {
	expiry := time.Now().Add(ttlDuration).Unix()
	username = fmt.Sprintf("%d:%s", expiry, sessionID)
	password = computePassword(secret, username)
	return
}

// computePassword returns base64( HMAC-SHA1( secret, username ) ).
func computePassword(secret []byte, username string) string {
	mac := hmac.New(sha1.New, secret) //nolint:gosec
	mac.Write([]byte(username))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
