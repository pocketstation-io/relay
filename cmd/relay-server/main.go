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
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/notifications/callback"
	"github.com/pocketstation-io/relay/internal/notifications/webhook"
	"github.com/pocketstation-io/relay/internal/server"
	"github.com/pocketstation-io/relay/internal/session"
)

func main() {
	jwtSecret := []byte(os.Getenv("POCKETSTATION_JWT_SECRET"))
	if len(jwtSecret) < 32 {
		slog.Error("POCKETSTATION_JWT_SECRET must contain at least 32 bytes")
		os.Exit(1)
	}
	authorityMode := getenv("RELAY_AUTHORITY_MODE", "control-plane")
	if authorityMode != "control-plane" && authorityMode != "standalone" {
		slog.Error("RELAY_AUTHORITY_MODE must be control-plane or standalone")
		os.Exit(1)
	}
	subscriberSecret := []byte(os.Getenv("RELAY_INVITATION_SECRET"))
	if authorityMode == "standalone" && len(subscriberSecret) < 32 {
		slog.Error("RELAY_INVITATION_SECRET must contain at least 32 bytes in standalone authority mode")
		os.Exit(1)
	}
	sourceIssuer := auth.ControlPlaneIssuer
	if authorityMode == "standalone" {
		sourceIssuer = auth.RelayIssuer
	}

	var cbClient *callback.Client
	apiURL := os.Getenv("RELAY_API_SERVER_URL")
	if authorityMode == "control-plane" && apiURL == "" {
		slog.Error("RELAY_API_SERVER_URL is required in control-plane authority mode")
		os.Exit(1)
	}
	if apiURL != "" {
		internalSecret := os.Getenv("POCKETSTATION_INTERNAL_SECRET")
		var err error
		cbClient, err = callback.NewClient(apiURL, internalSecret)
		if err != nil {
			slog.Error("invalid control-plane callback configuration", "error", err)
			os.Exit(1)
		}
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
	turnSecret := []byte(os.Getenv("TURN_SHARED_SECRET"))
	if os.Getenv("TURN_PUBLIC_IP") != "" && len(turnSecret) < 32 {
		slog.Error("TURN_SHARED_SECRET must contain at least 32 bytes when TURN is enabled")
		os.Exit(1)
	}
	clientICEServers, turnSrv := setupTURN(turnSecret)
	useTURN := len(clientICEServers) > 0

	roomExpiryMin := getenvInt("ROOM_EXPIRY_MINUTES", 0)              // 0 → package default (30 min)
	reconnectWindowSec := getenvInt("SOURCE_RECONNECT_WINDOW_SEC", 0) // 0 → package default (60 s)

	cfg := server.Config{
		JWTSecret:                jwtSecret,
		SubscriberJWTSecret:      subscriberSecret,
		SourceTokenIssuer:        sourceIssuer,
		AuthorityMode:            authorityMode,
		MaxRooms:                 getenvInt("RELAY_MAX_ROOMS", 0),
		MaxSubscribersPerRoom:    getenvInt("RELAY_MAX_SUBSCRIBERS_PER_SESSION", 0),
		MaxRoomsPerIPPerMinute:   getenvInt("MAX_ROOMS_PER_IP_PER_MINUTE", 0),
		MaxConcurrentHandshakes:  getenvInt("RELAY_MAX_CONCURRENT_HANDSHAKES", 0),
		CallbackClient:           cbClient,
		WebhookDispatcher:        whDispatcher,
		ClientICEServers:         clientICEServers,
		UseTURN:                  useTURN,
		ControlReconcileInterval: time.Duration(getenvInt("CONTROL_RECONCILE_INTERVAL_SEC", 5)) * time.Second,
		RegistryConfig: session.RegistryConfig{
			InactivityTimeout: time.Duration(roomExpiryMin) * time.Minute,
			ReconnectWindow:   time.Duration(reconnectWindowSec) * time.Second,
			MaxBuses:          getenvInt("RELAY_MAX_BUSES_PER_SESSION", 0),
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
	// Special value "auto": resolve the app's public IP at startup (fly.io).
	if publicIPs := os.Getenv("RELAY_PUBLIC_IPS"); publicIPs != "" {
		if strings.EqualFold(publicIPs, "auto") {
			if ip, err := resolvePublicIP(); err == nil {
				cfg.NAT1To1IPs = []string{ip.String()}
				slog.Info("NAT1To1 public IP auto-detected", "ip", ip.String())
			} else {
				slog.Error("RELAY_PUBLIC_IPS=auto but detection failed", "error", err)
				os.Exit(1)
			}
		} else {
			cfg.NAT1To1IPs = splitComma(publicIPs)
			slog.Info("NAT1To1 public IPs configured", "ips", cfg.NAT1To1IPs)
		}
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
	//
	// On Fly.io, UDP must bind to the special fly-global-services address so
	// proxy replies retain the public source address. INADDR_ANY is not valid
	// for this path even though it accepts the local bind.
	udpBindIP := net.IPv4zero
	onFlyIO := os.Getenv("FLY_APP_NAME") != ""
	if len(cfg.NAT1To1IPs) > 0 && !onFlyIO {
		if ip := net.ParseIP(cfg.NAT1To1IPs[0]); ip != nil {
			udpBindIP = ip
		}
	}
	udpNetwork, udpAddress := iceUDPListenAddress(
		onFlyIO,
		udpBindIP,
		getenvInt("ICE_UDP_PORT", 0),
	)
	udpConn, err := net.ListenPacket(udpNetwork, udpAddress)
	if err != nil {
		slog.Error("failed to bind ICE-UDP socket", "addr", udpAddress, "error", err)
		os.Exit(1)
	}

	// Enlarge the kernel receive buffer to absorb burst arrivals without
	// packet drops. 2 MiB matches typical production SFU tuning (OpenAI,
	// LiveKit). The OS may cap this below the request; that is fine.
	const udpRecvBufBytes = 2 * 1024 * 1024
	if bufferedConn, ok := udpConn.(interface {
		SetReadBuffer(int) error
		SetWriteBuffer(int) error
	}); ok {
		if err := bufferedConn.SetReadBuffer(udpRecvBufBytes); err != nil {
			slog.Warn("failed to set UDP read buffer", "target_bytes", udpRecvBufBytes, "error", err)
		}
		if err := bufferedConn.SetWriteBuffer(udpRecvBufBytes); err != nil {
			slog.Warn("failed to set UDP write buffer", "target_bytes", udpRecvBufBytes, "error", err)
		}
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
