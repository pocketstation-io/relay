package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/callback"
	"github.com/pocketstation-io/relay/internal/room"
	"github.com/pocketstation-io/relay/internal/server"
	relayTurn "github.com/pocketstation-io/relay/internal/turn"
)

func main() {
	jwtSecret := []byte(getenv("POCKETSTATION_JWT_SECRET", "dev-secret-change-me"))

	var cbClient *callback.Client
	if apiURL := os.Getenv("RELAY_API_SERVER_URL"); apiURL != "" {
		cbClient = callback.NewClient(apiURL)
		slog.Info("relay callback client enabled", "api_server_url", apiURL)
	}

	// Build ICE server list and start embedded TURN if configured (ADR-023).
	// When TURN_PUBLIC_IP is unset the relay operates in STUN-only mode (dev).
	iceServers, turnSrv := setupTURN(jwtSecret)

	roomExpiryMin := getenvInt("ROOM_EXPIRY_MINUTES", 0)            // 0 → package default (30 min)
	reconnectWindowSec := getenvInt("SOURCE_RECONNECT_WINDOW_SEC", 0) // 0 → package default (60 s)
	maxListenersPerRoom := getenvInt("MAX_LISTENERS_PER_ROOM", 0)     // 0 → unlimited

	cfg := server.Config{
		JWTSecret:              jwtSecret,
		MaxRooms:               getenvInt("RELAY_MAX_ROOMS", 0),
		MaxListenersPerRoom:    getenvInt("RELAY_MAX_LISTENERS_PER_ROOM", 0),
		MaxRoomsPerIPPerMinute: getenvInt("MAX_ROOMS_PER_IP_PER_MINUTE", 0),
		CallbackClient:         cbClient,
		ICEServers:             iceServers,
		RoomConfig: room.ManagerConfig{
			InactivityTimeout: time.Duration(roomExpiryMin) * time.Minute,
			ReconnectWindow:   time.Duration(reconnectWindowSec) * time.Second,
			MaxListeners:      maxListenersPerRoom,
		},
	}

	// ICE-TCP mux: enabled when ICE_TCP_PORT is set.
	// This must be created before server.New so the mux is shared across all
	// PeerConnections. The mux owns the TCP listener for the lifetime of main.
	if tcpPort := getenvInt("ICE_TCP_PORT", 0); tcpPort > 0 {
		tcpAddr := net.JoinHostPort("0.0.0.0", strconv.Itoa(tcpPort))
		ln, err := net.Listen("tcp4", tcpAddr)
		if err != nil {
			slog.Error("failed to bind ICE-TCP listener", "addr", tcpAddr, "error", err)
			os.Exit(1)
		}
		cfg.ICETCPMux = webrtc.NewICETCPMux(nil, ln, 8)
		slog.Info("ICE-TCP mux started", "addr", tcpAddr)
	}

	s := server.New(cfg)

	const defaultShutdownGraceSec = 30
	shutdownGraceSec := getenvInt("SHUTDOWN_GRACE_PERIOD_SEC", defaultShutdownGraceSec)
	shutdownGrace := time.Duration(shutdownGraceSec) * time.Second

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		slog.Info("relay shutting down", "signal", sig.String(), "grace_period", shutdownGrace)
		turnSrv.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			slog.Error("shutdown error", "error", err)
		}
	}()

	if err := s.Serve(":8080"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("relay serve error", "error", err)
		os.Exit(1)
	}
	slog.Info("relay stopped")
}

// setupTURN starts the embedded TURN server when TURN_PUBLIC_IP is configured.
//
// Returns the ICE server list to include in createRoom responses and a
// *relayTurn.Server handle for shutdown. If TURN is not configured the ICE
// list is nil (server falls back to stun.l.google.com) and the returned
// server is a no-op.
//
// Environment variables:
//
//	TURN_PUBLIC_IP   — server's public IP (required to enable TURN)
//	TURN_UDP_PORT    — UDP TURN port (default 3478)
//	TURN_TLS_PORT    — TURNS/TLS port (default 0 = disabled; set to 443 or 5349)
//	TURN_REALM       — STUN realm (default "pocketstation.io")
func setupTURN(jwtSecret []byte) (iceServers []webrtc.ICEServer, srv *relayTurn.Server) {
	publicIPStr := os.Getenv("TURN_PUBLIC_IP")
	if publicIPStr == "" {
		slog.Info("TURN_PUBLIC_IP not set; relay running in STUN-only mode")
		return nil, new(relayTurn.Server) // no-op Stop()
	}

	publicIP := net.ParseIP(publicIPStr)
	if publicIP == nil {
		slog.Error("TURN_PUBLIC_IP is not a valid IP address", "value", publicIPStr)
		os.Exit(1)
	}

	udpPort := getenvInt("TURN_UDP_PORT", 3478)
	tlsPort := getenvInt("TURN_TLS_PORT", 0)
	realm := getenv("TURN_REALM", "pocketstation.io")

	srvCfg := relayTurn.ServerConfig{
		PublicIP: publicIP,
		Secret:   jwtSecret,
		UDPPort:  udpPort,
		TLSPort:  tlsPort,
		Realm:    realm,
	}

	var err error
	srv, err = relayTurn.Start(srvCfg)
	if err != nil {
		slog.Error("failed to start embedded TURN server", "error", err)
		os.Exit(1)
	}

	// Build TURN credential TTL: use relay's max token TTL (2h = listener token TTL).
	const credTTL = 2 * time.Hour
	turnHost := publicIPStr
	turnUser, turnPass := relayTurn.Credentials(jwtSecret, "relay", credTTL)

	iceServers = []webrtc.ICEServer{
		{URLs: []string{"stun:" + net.JoinHostPort(turnHost, strconv.Itoa(udpPort))}},
		{
			URLs:           turnURLs(turnHost, udpPort, tlsPort),
			Username:       turnUser,
			Credential:     turnPass,
			CredentialType: webrtc.ICECredentialTypePassword,
		},
	}

	slog.Info("embedded TURN started",
		"public_ip", publicIPStr,
		"udp_port", udpPort,
		"tls_port", tlsPort,
	)
	return iceServers, srv
}

// turnURLs builds the TURN URL list from configured ports.
// Includes TURNS (TLS) URL only when tlsPort > 0.
func turnURLs(host string, udpPort, tlsPort int) []string {
	urls := []string{
		"turn:" + net.JoinHostPort(host, strconv.Itoa(udpPort)),
		"turn:" + net.JoinHostPort(host, strconv.Itoa(udpPort)) + "?transport=tcp",
	}
	if tlsPort > 0 {
		urls = append(urls, "turns:"+net.JoinHostPort(host, strconv.Itoa(tlsPort)))
	}
	return urls
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
