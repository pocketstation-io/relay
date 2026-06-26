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
	"strings"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/callback"
	"github.com/pocketstation-io/relay/internal/room"
	"github.com/pocketstation-io/relay/internal/server"
	relayTurn "github.com/pocketstation-io/relay/internal/turn"
	"github.com/pocketstation-io/relay/internal/webhook"
)

func main() {
	jwtSecretStr := os.Getenv("POCKETSTATION_JWT_SECRET")
	if jwtSecretStr == "" {
		slog.Warn("POCKETSTATION_JWT_SECRET not set, using insecure default — set this env var in production")
		jwtSecretStr = "dev-secret-change-me"
	}
	jwtSecret := []byte(jwtSecretStr)

	var cbClient *callback.Client
	if apiURL := os.Getenv("RELAY_API_SERVER_URL"); apiURL != "" {
		cbClient = callback.NewClient(apiURL)
		slog.Info("relay callback client enabled", "api_server_url", apiURL)
	}

	var whDispatcher *webhook.Dispatcher
	if webhookURL := os.Getenv("WEBHOOK_URL"); webhookURL != "" {
		whDispatcher = webhook.New(webhookURL)
		slog.Info("relay webhook dispatcher enabled", "webhook_url", webhookURL)
	}

	// Build ICE server list and start embedded TURN if configured (ADR-023).
	// When TURN_PUBLIC_IP is unset the relay operates in STUN-only mode (dev).
	//
	// clientICEServers is what clients receive in createRoom so they can reach
	// the relay's TURN server. The relay's own Pion peers use ICEServers=nil
	// (falls back to stun.l.google.com when NAT1To1IPs is unset, or no STUN at
	// all when NAT1To1IPs is set) to avoid self-STUN via the embedded TURN.
	clientICEServers, turnSrv := setupTURN(jwtSecret)
	useTURN := len(clientICEServers) > 0

	roomExpiryMin := getenvInt("ROOM_EXPIRY_MINUTES", 0)              // 0 → package default (30 min)
	reconnectWindowSec := getenvInt("SOURCE_RECONNECT_WINDOW_SEC", 0) // 0 → package default (60 s)
	maxListenersPerRoom := getenvInt("MAX_LISTENERS_PER_ROOM", 0)     // 0 → unlimited

	cfg := server.Config{
		JWTSecret:              jwtSecret,
		MaxRooms:               getenvInt("RELAY_MAX_ROOMS", 0),
		MaxListenersPerRoom:    getenvInt("RELAY_MAX_LISTENERS_PER_ROOM", 0),
		MaxRoomsPerIPPerMinute: getenvInt("MAX_ROOMS_PER_IP_PER_MINUTE", 0),
		CallbackClient:         cbClient,
		WebhookDispatcher:      whDispatcher,
		ClientICEServers:       clientICEServers,
		UseTURN:                useTURN,
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
		cfg.ICETCPMux = webrtc.NewICETCPMux(nil, ln, 512)
		slog.Info("ICE-TCP mux started", "addr", tcpAddr)
	}

	// NAT1To1IPs: public IP(s) for ICE host candidates behind NAT (Fly.io).
	// Set RELAY_PUBLIC_IPS to the relay's public IP so remote peers receive
	// reachable ICE candidates. Multiple IPs are comma-separated.
	if publicIPs := os.Getenv("RELAY_PUBLIC_IPS"); publicIPs != "" {
		cfg.NAT1To1IPs = splitComma(publicIPs)
		slog.Info("NAT1To1 public IPs configured", "ips", cfg.NAT1To1IPs)
	}

	// ICE-UDP mux: force ALL UDP media through a single socket so the relay
	// gathers exactly one UDP host candidate. Without this, pion binds one
	// socket per local interface (and per NAT1To1 IP); media then egresses from
	// a socket the remote peer never consented to and is dropped.
	//
	// Bind the socket to the SPECIFIC advertised IP (RELAY_PUBLIC_IPS), not
	// 0.0.0.0. On a multi-homed host (e.g. a LAN IP plus a VM/bridge interface),
	// a 0.0.0.0 socket lets the kernel choose the egress interface per
	// destination, so media can leave from an interface that does not match the
	// advertised host candidate — the listener's nominated pair then receives
	// zero media. Pinning the socket to the advertised IP makes egress match the
	// candidate. Falls back to 0.0.0.0 when no public IP is configured.
	udpBindIP := net.IPv4zero
	if len(cfg.NAT1To1IPs) > 0 {
		if ip := net.ParseIP(cfg.NAT1To1IPs[0]); ip != nil {
			udpBindIP = ip
		}
	}
	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP:   udpBindIP,
		Port: getenvInt("ICE_UDP_PORT", 0),
	})
	if err != nil {
		slog.Error("failed to bind ICE-UDP socket", "error", err)
		os.Exit(1)
	}
	cfg.ICEUDPMux = webrtc.NewICEUDPMux(nil, udpConn)
	slog.Info("ICE-UDP mux started", "addr", udpConn.LocalAddr().String())

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

	httpAddr := ":" + strconv.Itoa(getenvInt("PORT", 4800))
	slog.Info("relay listening", "addr", httpAddr)
	if err := s.Serve(httpAddr); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
//	TURN_TCP_PORT    — plain TCP TURN port (default 3478; enables ?transport=tcp)
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
	tcpPort := getenvInt("TURN_TCP_PORT", 3478)
	tlsPort := getenvInt("TURN_TLS_PORT", 0)
	realm := getenv("TURN_REALM", "pocketstation.io")

	srvCfg := relayTurn.ServerConfig{
		PublicIP: publicIP,
		Secret:   jwtSecret,
		UDPPort:  udpPort,
		TCPPort:  tcpPort,
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
			URLs:           turnURLs(turnHost, udpPort, tcpPort, tlsPort),
			Username:       turnUser,
			Credential:     turnPass,
			CredentialType: webrtc.ICECredentialTypePassword,
		},
	}

	slog.Info("embedded TURN started",
		"public_ip", publicIPStr,
		"udp_port", udpPort,
		"tcp_port", tcpPort,
		"tls_port", tlsPort,
	)
	return iceServers, srv
}

// turnURLs builds the TURN URL list from configured ports.
// Includes TURN/TCP (?transport=tcp) only when tcpPort > 0 (a TCP listener is running).
// Includes TURNS (TLS) URL only when tlsPort > 0.
func turnURLs(host string, udpPort, tcpPort, tlsPort int) []string {
	urls := []string{
		"turn:" + net.JoinHostPort(host, strconv.Itoa(udpPort)),
	}
	if tcpPort > 0 {
		urls = append(urls, "turn:"+net.JoinHostPort(host, strconv.Itoa(tcpPort))+"?transport=tcp")
		slog.Info("TURN transport active", "transport", "tcp", "port", tcpPort)
	}
	if tlsPort > 0 {
		urls = append(urls, "turns:"+net.JoinHostPort(host, strconv.Itoa(tlsPort)))
		slog.Info("TURN transport active", "transport", "tls", "port", tlsPort)
	}
	slog.Info("TURN transport active", "transport", "udp", "port", udpPort)
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

// splitComma splits a comma-separated string, trimming whitespace, dropping empties.
func splitComma(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
