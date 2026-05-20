package auth

import (
	"testing"
	"time"
)

func BenchmarkVerify(b *testing.B) {
	secret := []byte("bench-secret")
	token, _ := Sign(secret, "bench-room", RoleSource, time.Hour)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = Verify(secret, token)
	}
}

func BenchmarkSign(b *testing.B) {
	secret := []byte("bench-secret")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = Sign(secret, "bench-room", RoleSource, time.Hour)
	}
}
