// relay-test-source is a deterministic RTP publisher for Relay tests.
//
// It connects as a WebRTC source peer, negotiates an Opus audio track, and
// sends a fixed valid Opus packet at 20 ms cadence for the requested duration.
// The binary is a test fixture and never represents physical audio capture.
//
// Usage:
//
//	relay-test-source [--relay http://localhost:8080] [--room ROOM_ID] [--token JWT] [--duration 5m]
//
// If --room and --token are both omitted the binary creates a room via POST
// /v1/rooms and prints the listener token to stdout before publishing.
package main

import (
	"flag"
	"log/slog"
	"os"
	"time"
)

func main() {
	relayURL := flag.String("relay", "http://localhost:8080", "relay base URL (http/https)")
	roomID := flag.String("room", "", "room ID (omit to create a new room)")
	token := flag.String("token", "", "source JWT (omit to create a new room)")
	duration := flag.Duration("duration", 5*time.Minute, "how long to stream")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if (*roomID == "") != (*token == "") {
		logger.Error("--room and --token must both be provided, or both omitted")
		os.Exit(1)
	}

	var sourceToken string
	if *roomID == "" {
		var err error
		*roomID, sourceToken, err = createRoom(*relayURL, logger)
		if err != nil {
			logger.Error("failed to create room", "err", err)
			os.Exit(1)
		}
	} else {
		sourceToken = *token
	}

	logger.Info("publishing to room", "room_id", *roomID, "duration", *duration)

	if err := run(*relayURL, *roomID, sourceToken, *duration, logger); err != nil {
		logger.Error("publisher exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("clean shutdown")
}
