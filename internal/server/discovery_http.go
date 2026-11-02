package server

import (
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const maxPublicControlPlaneURLBytes = 2048

func (s *Server) serviceDiscovery(w http.ResponseWriter, r *http.Request) {
	address := s.publicRelayURL
	if s.authorityMode == "control-plane" {
		address = s.publicControlPlaneURL
	}
	if !validPublicControlPlaneURL(address) {
		http.Error(w, `{"error":"public_control_plane_unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	response := map[string]any{"schema_version": 1, "authority": "relay", "authority_url": address}
	if s.authorityMode == "control-plane" {
		response["control_plane_url"] = address
	}
	_ = json.NewEncoder(w).Encode(response)
}

func validPublicControlPlaneURL(address string) bool {
	if address == "" || len(address) > maxPublicControlPlaneURLBytes {
		return false
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Opaque != "" || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	if parsed.Scheme == "https" {
		return true
	}
	if parsed.Scheme != "http" {
		return false
	}
	host := parsed.Hostname()
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
