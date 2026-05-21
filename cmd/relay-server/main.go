package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/pocketstation-io/relay/internal/callback"
	"github.com/pocketstation-io/relay/internal/server"
)

func main() {
	// RELAY_API_SERVER_URL is optional. When unset, source_active and
	// listener-leave callbacks are disabled; the relay operates normally.
	var cbClient *callback.Client
	if apiURL := os.Getenv("RELAY_API_SERVER_URL"); apiURL != "" {
		cbClient = callback.NewClient(apiURL)
		slog.Info("relay callback client enabled", "api_server_url", apiURL)
	}

	s := server.New(server.Config{
		JWTSecret:           []byte(getenv("POCKETSTATION_JWT_SECRET", "dev-secret-change-me")),
		MaxRooms:            getenvInt("RELAY_MAX_ROOMS", 0),
		MaxListenersPerRoom: getenvInt("RELAY_MAX_LISTENERS_PER_ROOM", 0),
		CallbackClient:      cbClient,
	})

	// Catch SIGTERM and SIGINT. The signal goroutine calls Shutdown which
	// drains in-flight HTTP connections (5s deadline) then closes all rooms.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		slog.Info("relay shutting down", "signal", sig.String())
		if err := s.Shutdown(context.Background()); err != nil {
			slog.Error("shutdown error", "error", err)
		}
	}()

	if err := s.Serve(":8080"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("relay serve error", "error", err)
		os.Exit(1)
	}
	slog.Info("relay stopped")
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// getenvInt returns the integer value of env var k, or d if unset or unparseable.
// A value of 0 for d means "use the server default".
func getenvInt(k string, d int) int {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		slog.Warn("invalid env var value, using default", "key", k, "value", v)
		return d
	}
	return n
}
