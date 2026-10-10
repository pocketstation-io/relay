package turn_test

import (
	"strings"
	"testing"
	"time"

	"github.com/pocketstation-io/relay/access/turn"
)

var testSecret = []byte("test-turn-secret-control-plane")

func TestGivenValidSecretWhenCredentialsAreGeneratedThenUsernameContainsSessionID(t *testing.T) {
	sessionID := "session-abc"

	username, password := turn.Credentials(testSecret, sessionID, time.Hour)

	if username == "" || password == "" {
		t.Fatal("expected non-empty username and password")
	}
	if !strings.HasSuffix(username, ":"+sessionID) {
		t.Errorf("username %q must end with session ID %q", username, sessionID)
	}
}

func TestGivenDifferentSessionsWhenCredentialsAreGeneratedThenPasswordsDiffer(t *testing.T) {
	_, firstPassword := turn.Credentials(testSecret, "session-aaa", time.Hour)
	_, secondPassword := turn.Credentials(testSecret, "session-bbb", time.Hour)

	if firstPassword == "" || secondPassword == "" {
		t.Fatal("expected non-empty passwords")
	}
	if firstPassword == secondPassword {
		t.Error("different session scopes must produce different passwords")
	}
}

func TestGivenICEServerWhenFieldsAreSetThenWebRTCShapeIsPreserved(t *testing.T) {
	server := turn.ICEServer{
		URLs:       []string{"turn:relay.example.com:3478", "turns:relay.example.com:443"},
		Username:   "1234567890:session-test",
		Credential: "hmac-value",
	}

	if len(server.URLs) != 2 {
		t.Errorf("URL count = %d, want 2", len(server.URLs))
	}
	if server.Username == "" || server.Credential == "" {
		t.Error("TURN server must have username and credential")
	}
}
