package main

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

func resolvePublicIP() (net.IP, error) {
	if raw := os.Getenv("FLY_PUBLIC_IP"); raw != "" {
		if ip := net.ParseIP(raw); ip != nil && ip.To4() != nil {
			return ip.To4(), nil
		}
	}

	if appName := os.Getenv("FLY_APP_NAME"); appName != "" {
		hostname := appName + ".fly.dev"
		addresses, err := net.LookupIP(hostname)
		if err == nil {
			for _, address := range addresses {
				if ipv4 := address.To4(); ipv4 != nil {
					return ipv4, nil
				}
			}
		}
		slog.Warn("DNS lookup failed for fly.io hostname", "hostname", hostname, "error", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get("https://api.ipify.org")
	if err != nil {
		return nil, fmt.Errorf("ipify request failed: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("ipify read failed: %w", err)
	}
	if ip := net.ParseIP(strings.TrimSpace(string(body))); ip != nil && ip.To4() != nil {
		return ip.To4(), nil
	}
	return nil, fmt.Errorf("could not detect public IP from environment, DNS, or ipify")
}
