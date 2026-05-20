package main

import (
	"os"

	"github.com/pocketstation-io/relay/internal/server"
)

func main() {
	s := server.New(server.Config{
		JWTSecret: []byte(getenv("POCKETSTATION_JWT_SECRET", "dev-secret-change-me")),
	})
	s.Serve(":8080")
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
