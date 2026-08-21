// relay-test-source is a deterministic RTP publisher for Relay tests.
//
// It connects as a WebRTC source peer, negotiates an Opus audio track, and
// sends a fixed valid Opus packet at 20 ms cadence for the requested duration.
// The binary is a test fixture and never represents physical audio capture.
//
// Usage:
//
//	relay-test-source [--relay http://localhost:8080] [--session SESSION_ID] [--bus application] [--token JWT] [--duration 5m]
//
// If --session and --token are both omitted, the fixture asks a standalone
// Relay to create a Session. Control-plane mode requires explicit credentials.
package main

import (
	"flag"
	"log/slog"
	"os"
	"time"
)

func main() {
	relayURL := flag.String("relay", "http://localhost:8080", "relay base URL (http/https)")
	sessionID := flag.String("session", "", "RelaySession ID (omit only for standalone Relay)")
	busID := flag.String("bus", "application", "named AudioBus to publish")
	token := flag.String("token", "", "source capability (omit only for standalone Relay)")
	stunURL := flag.String("stun", "", "optional STUN URL for a remote-path test")
	duration := flag.Duration("duration", 5*time.Minute, "how long to stream")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if (*sessionID == "") != (*token == "") {
		logger.Error("--session and --token must both be provided, or both omitted")
		os.Exit(1)
	}

	var sourceToken string
	if *sessionID == "" {
		var err error
		*sessionID, sourceToken, err = createSession(*relayURL, logger)
		if err != nil {
			logger.Error("failed to create standalone RelaySession", "error", err)
			os.Exit(1)
		}
	} else {
		sourceToken = *token
	}

	logger.Info("publishing test audio", "session_id", *sessionID, "bus_id", *busID, "duration", *duration)

	if err := run(*relayURL, *sessionID, *busID, sourceToken, *stunURL, *duration, logger); err != nil {
		logger.Error("publisher exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("clean shutdown")
}
