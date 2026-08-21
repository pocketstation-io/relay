package main

import (
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pion/webrtc/v4"
	relayTurn "github.com/pocketstation-io/relay/internal/turn"
)

func setupTURN(turnSecret []byte) (iceServers []webrtc.ICEServer, relay *relayTurn.Server) {
	publicIPString := os.Getenv("TURN_PUBLIC_IP")
	if publicIPString == "" {
		slog.Info("TURN_PUBLIC_IP not set; relay running in STUN-only mode")
		return nil, new(relayTurn.Server)
	}

	if strings.EqualFold(publicIPString, "auto") {
		ip, err := resolvePublicIP()
		if err != nil {
			slog.Error("TURN_PUBLIC_IP=auto but detection failed", "error", err)
			os.Exit(1)
		}
		publicIPString = ip.String()
		slog.Info("TURN public IP auto-detected", "ip", publicIPString)
	}

	publicIP := net.ParseIP(publicIPString)
	if publicIP == nil {
		slog.Error("TURN_PUBLIC_IP is not a valid IP address", "value", publicIPString)
		os.Exit(1)
	}

	udpPort := getenvInt("TURN_UDP_PORT", 3478)
	tcpPort := getenvInt("TURN_TCP_PORT", 3478)
	tlsPort := getenvInt("TURN_TLS_PORT", 0)
	realm := getenv("TURN_REALM", "pocketstation.io")

	var err error
	relay, err = relayTurn.Start(relayTurn.ServerConfig{
		PublicIP: publicIP,
		Secret:   turnSecret,
		UDPPort:  udpPort,
		TCPPort:  tcpPort,
		TLSPort:  tlsPort,
		Realm:    realm,
	})
	if err != nil {
		slog.Error("failed to start embedded TURN server", "error", err)
		os.Exit(1)
	}

	const credentialTTL = 2 * time.Hour
	username, password := relayTurn.Credentials(turnSecret, "relay", credentialTTL)
	iceServers = []webrtc.ICEServer{
		{URLs: []string{"stun:" + net.JoinHostPort(publicIPString, strconv.Itoa(udpPort))}},
		{
			URLs:           turnURLs(publicIPString, udpPort, tcpPort, tlsPort),
			Username:       username,
			Credential:     password,
			CredentialType: webrtc.ICECredentialTypePassword,
		},
	}

	slog.Info("embedded TURN started",
		"public_ip", publicIPString,
		"udp_port", udpPort,
		"tcp_port", tcpPort,
		"tls_port", tlsPort,
	)
	return iceServers, relay
}

func turnURLs(host string, udpPort, tcpPort, tlsPort int) []string {
	urls := []string{"turn:" + net.JoinHostPort(host, strconv.Itoa(udpPort))}
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
