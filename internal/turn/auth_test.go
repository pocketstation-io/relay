package turn_test

import (
	"testing"
	"time"

	"github.com/pocketstation-io/relay/internal/turn"
)

var testSecret = []byte("test-turn-secret")

func TestGiven_ValidSecret_When_CredentialsGenerated_Then_PasswordValidates(t *testing.T) {
	// Given
	roomID := "room-abc-123"
	ttl := 1 * time.Hour

	// When
	username, password := turn.Credentials(testSecret, roomID, ttl)

	// Then
	if username == "" || password == "" {
		t.Fatal("expected non-empty username and password")
	}
	if !turn.Validate(testSecret, username, password) {
		t.Errorf("Validate returned false for just-generated credentials")
	}
}

func TestGiven_ExpiredCredential_When_Validate_Then_ReturnsFalse(t *testing.T) {
	// Given — negative TTL places expiry in the past
	username, password := turn.Credentials(testSecret, "room-xyz", -1*time.Second)

	// When / Then
	if turn.Validate(testSecret, username, password) {
		t.Error("Validate returned true for expired credential")
	}
}

func TestGiven_WrongSecret_When_Validate_Then_ReturnsFalse(t *testing.T) {
	// Given
	username, password := turn.Credentials(testSecret, "room-xyz", 1*time.Hour)
	wrongSecret := []byte("different-secret")

	// When / Then
	if turn.Validate(wrongSecret, username, password) {
		t.Error("Validate returned true for wrong secret")
	}
}

func TestGiven_TamperedPassword_When_Validate_Then_ReturnsFalse(t *testing.T) {
	// Given
	username, _ := turn.Credentials(testSecret, "room-xyz", 1*time.Hour)

	// When / Then
	if turn.Validate(testSecret, username, "tampered-password") {
		t.Error("Validate returned true for tampered password")
	}
}

func TestGiven_MalformedUsername_When_Validate_Then_ReturnsFalse(t *testing.T) {
	for _, username := range []string{"", "nocolon", "notseconds:room"} {
		if turn.Validate(testSecret, username, "any") {
			t.Errorf("Validate returned true for malformed username %q", username)
		}
	}
}

func TestGiven_SameRoomDifferentCalls_When_CredentialsGenerated_Then_PasswordsDiffer(t *testing.T) {
	// Given — two calls are far enough apart that expiry timestamps differ
	u1, p1 := turn.Credentials(testSecret, "room-xyz", 1*time.Hour)
	time.Sleep(1 * time.Second)
	u2, p2 := turn.Credentials(testSecret, "room-xyz", 1*time.Hour)

	// Then — both validate but produce different creds (timestamp differs)
	if !turn.Validate(testSecret, u1, p1) {
		t.Error("first credential did not validate")
	}
	if !turn.Validate(testSecret, u2, p2) {
		t.Error("second credential did not validate")
	}
	if p1 == p2 {
		t.Error("expected different passwords for different timestamps")
	}
}
