package turn

import (
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	pionTurn "github.com/pion/turn/v4"
)

// ServerConfig holds parameters for the embedded TURN server.
//
// Invariant: Secret must be the same value used by Credentials so that
// credentials issued by the relay server validate against this server.
//
// Ownership: Server owns the UDP PacketConn and TLS Listener; both are closed
// by Stop.
//
// Failure behavior: Start returns an error if the listeners cannot be bound.
// Stop is a no-op if Start was never called or already returned an error.
type ServerConfig struct {
	// PublicIP is the relay server's publicly reachable IP address.
	// Relay candidates in TURN ALLOCATE responses use this address.
	// Required.
	PublicIP net.IP

	// Secret is the HMAC key for authenticating TURN credentials.
	// Must match the relay's POCKETSTATION_JWT_SECRET.
	Secret []byte

	// UDPPort is the port for TURN/UDP (standard: 3478). Zero disables UDP.
	UDPPort int

	// TLSPort is the port for TURNS/TLS. Zero disables TLS TURN.
	TLSPort int

	// TLSConfig provides the TLS certificate for TURNS. Required when
	// TLSPort > 0.
	TLSConfig *tls.Config

	// Realm is the STUN realm string included in TURN error responses.
	// Defaults to "pocketstation.io".
	Realm string
}

// Server is the embedded TURN server.
type Server struct {
	inner *pionTurn.Server
}

// Start binds the configured listeners and starts the TURN server.
//
// TURN/UDP and TURNS/TLS listeners are started according to cfg. At least one
// of UDPPort or TLSPort must be non-zero.
//
// Returns an error if no listeners could be bound.
func Start(cfg ServerConfig) (*Server, error) {
	if cfg.PublicIP == nil {
		return nil, fmt.Errorf("turn.Start: PublicIP is required")
	}
	if cfg.UDPPort == 0 && cfg.TLSPort == 0 {
		return nil, fmt.Errorf("turn.Start: at least one of UDPPort or TLSPort must be set")
	}

	realm := cfg.Realm
	if realm == "" {
		realm = "pocketstation.io"
	}

	relayGen := &pionTurn.RelayAddressGeneratorStatic{
		RelayAddress: cfg.PublicIP,
		Address:      "0.0.0.0",
	}

	var packetConns []pionTurn.PacketConnConfig
	var listeners []pionTurn.ListenerConfig

	if cfg.UDPPort > 0 {
		udpAddr := fmt.Sprintf("0.0.0.0:%d", cfg.UDPPort)
		conn, err := net.ListenPacket("udp4", udpAddr)
		if err != nil {
			return nil, fmt.Errorf("turn.Start: bind UDP %s: %w", udpAddr, err)
		}
		packetConns = append(packetConns, pionTurn.PacketConnConfig{
			PacketConn:            conn,
			RelayAddressGenerator: relayGen,
		})
		slog.Info("turn UDP listener started", "addr", udpAddr)
	}

	if cfg.TLSPort > 0 {
		if cfg.TLSConfig == nil {
			return nil, fmt.Errorf("turn.Start: TLSConfig is required when TLSPort > 0")
		}
		tlsAddr := fmt.Sprintf("0.0.0.0:%d", cfg.TLSPort)
		ln, err := tls.Listen("tcp4", tlsAddr, cfg.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("turn.Start: bind TURNS %s: %w", tlsAddr, err)
		}
		listeners = append(listeners, pionTurn.ListenerConfig{
			Listener:              ln,
			RelayAddressGenerator: relayGen,
		})
		slog.Info("turn TLS listener started", "addr", tlsAddr)
	}

	secret := cfg.Secret
	srv, err := pionTurn.NewServer(pionTurn.ServerConfig{
		Realm: realm,
		AuthHandler: func(username, _ string, _ net.Addr) ([]byte, bool) {
			parts := strings.SplitN(username, ":", 2)
			if len(parts) != 2 {
				return nil, false
			}
			var expiry int64
			_, parseErr := fmt.Sscanf(parts[0], "%d", &expiry)
			if parseErr != nil || time.Now().Unix() > expiry {
				return nil, false
			}
			mac := hmac.New(sha1.New, secret) //nolint:gosec
			mac.Write([]byte(username))
			return mac.Sum(nil), true
		},
		PacketConnConfigs: packetConns,
		ListenerConfigs:   listeners,
	})
	if err != nil {
		return nil, fmt.Errorf("turn.Start: %w", err)
	}

	return &Server{inner: srv}, nil
}

// Stop shuts down the TURN server and releases all listeners.
func (s *Server) Stop() {
	if s == nil || s.inner == nil {
		return
	}
	if err := s.inner.Close(); err != nil {
		slog.Warn("turn server close error", "error", err)
	}
}
