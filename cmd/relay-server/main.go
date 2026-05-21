package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/pocketstation-io/relay/internal/server"
)

func main() {
	s := server.New(server.Config{
		JWTSecret: []byte(getenv("POCKETSTATION_JWT_SECRET", "dev-secret-change-me")),
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
