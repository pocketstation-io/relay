package auth

import (
	"testing"
	"time"
)

func TestSign_ThenVerify_MatchesClaims(t *testing.T) {
	// Given
	secret := []byte("test-secret")
	roomID := "room-abc"
	role := RoleSource
	// When
	token, err := Sign(secret, roomID, role, 5*time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	claims, err := Verify(secret, token)
	// Then
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.RoomID != roomID {
		t.Errorf("RoomID = %q, want %q", claims.RoomID, roomID)
	}
	if claims.Role != role {
		t.Errorf("Role = %q, want %q", claims.Role, role)
	}
}

func TestVerify_WrongSecret_ReturnsError(t *testing.T) {
	// Given
	token, _ := Sign([]byte("secret-a"), "room-1", RoleSource, time.Minute)
	// When
	_, err := Verify([]byte("secret-b"), token)
	// Then
	if err == nil {
		t.Error("expected error for wrong secret, got nil")
	}
}

func TestVerify_ExpiredToken_ReturnsError(t *testing.T) {
	// Given — sign with a negative TTL so the token is already expired
	secret := []byte("test-secret")
	token, _ := Sign(secret, "room-1", RoleSource, -time.Second)
	// When
	_, err := Verify(secret, token)
	// Then
	if err == nil {
		t.Error("expected error for expired token, got nil")
	}
}

func TestSign_ListenerRole_RoundTrips(t *testing.T) {
	// Given
	secret := []byte("test-secret")
	// When
	token, err := Sign(secret, "room-1", RoleListener, time.Hour)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	claims, err := Verify(secret, token)
	// Then
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.Role != RoleListener {
		t.Errorf("Role = %q, want %q", claims.Role, RoleListener)
	}
}

func TestVerify_MalformedToken_ReturnsError(t *testing.T) {
	// Given
	secret := []byte("test-secret")
	// When
	_, err := Verify(secret, "not.a.valid.jwt")
	// Then
	if err == nil {
		t.Error("expected error for malformed token, got nil")
	}
}

func TestVerify_EmptyToken_ReturnsError(t *testing.T) {
	// Given
	secret := []byte("test-secret")
	// When
	_, err := Verify(secret, "")
	// Then
	if err == nil {
		t.Error("expected error for empty token, got nil")
	}
}
