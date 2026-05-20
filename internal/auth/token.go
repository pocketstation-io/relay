package auth

import (
    "time"
    "github.com/golang-jwt/jwt/v5"
)

type Role string
const (
    RoleSource Role = "source"
    RoleListener Role = "listener"
)

type Claims struct {
    RoomID string `json:"room_id"`
    Role Role `json:"role"`
    jwt.RegisteredClaims
}

func Sign(secret []byte, roomID string, role Role, ttl time.Duration) (string, error) {
    claims := Claims{RoomID: roomID, Role: role, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(ttl)), IssuedAt: jwt.NewNumericDate(time.Now())}}
    return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
}

func Verify(secret []byte, token string) (*Claims, error) {
    parsed, err := jwt.ParseWithClaims(token, &Claims{}, func(token *jwt.Token) (interface{}, error) { return secret, nil })
    if err != nil { return nil, err }
    claims, ok := parsed.Claims.(*Claims)
    if !ok || !parsed.Valid { return nil, jwt.ErrTokenInvalidClaims }
    return claims, nil
}
