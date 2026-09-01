package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	relayturn "github.com/pocketstation-io/relay/internal/turn"
)

const (
	defaultTURNPort       = 3478
	defaultRelayMinPort   = 49160
	defaultRelayMaxPort   = 49167
	minimumSecretByteSize = 32
)

func main() {
	config, err := loadConfig(os.Getenv)
	if err != nil {
		slog.Error("invalid TURN configuration", "error", err)
		os.Exit(1)
	}
	server, err := relayturn.Start(config)
	if err != nil {
		slog.Error("TURN startup failed", "error", err)
		os.Exit(1)
	}
	slog.Info("TURN server ready",
		"public_ip", config.PublicIP.String(),
		"udp_port", config.UDPPort,
		"tcp_port", config.TCPPort,
		"relay_min_port", config.RelayMinPort,
		"relay_max_port", config.RelayMaxPort,
	)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
	signal.Stop(shutdown)
	server.Stop()
	slog.Info("TURN server stopped")
}

func loadConfig(getenv func(string) string) (relayturn.ServerConfig, error) {
	publicIP := net.ParseIP(strings.TrimSpace(getenv("TURN_PUBLIC_IP")))
	if publicIP == nil {
		return relayturn.ServerConfig{}, errors.New("TURN_PUBLIC_IP must be a literal IP address")
	}
	secret := []byte(getenv("TURN_SHARED_SECRET"))
	if len(secret) < minimumSecretByteSize {
		return relayturn.ServerConfig{}, fmt.Errorf("TURN_SHARED_SECRET must contain at least %d bytes", minimumSecretByteSize)
	}
	udpPort, err := port(getenv, "TURN_UDP_PORT", defaultTURNPort)
	if err != nil {
		return relayturn.ServerConfig{}, err
	}
	tcpPort, err := port(getenv, "TURN_TCP_PORT", defaultTURNPort)
	if err != nil {
		return relayturn.ServerConfig{}, err
	}
	relayMinPort, err := port(getenv, "TURN_RELAY_MIN_PORT", defaultRelayMinPort)
	if err != nil {
		return relayturn.ServerConfig{}, err
	}
	relayMaxPort, err := port(getenv, "TURN_RELAY_MAX_PORT", defaultRelayMaxPort)
	if err != nil {
		return relayturn.ServerConfig{}, err
	}
	if relayMinPort > relayMaxPort {
		return relayturn.ServerConfig{}, errors.New("TURN relay port range must be ordered")
	}
	realm := strings.TrimSpace(getenv("TURN_REALM"))
	if realm == "" {
		realm = "pocketstation.io"
	}
	return relayturn.ServerConfig{
		PublicIP:     publicIP,
		Secret:       secret,
		UDPPort:      udpPort,
		TCPPort:      tcpPort,
		Realm:        realm,
		RelayMinPort: relayMinPort,
		RelayMaxPort: relayMaxPort,
	}, nil
}

func port(getenv func(string) string, name string, fallback int) (int, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 || parsed > 65535 {
		return 0, fmt.Errorf("%s must be a port from 1 through 65535", name)
	}
	return parsed, nil
}
