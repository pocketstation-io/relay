package main

import (
	"fmt"
	"net/url"
	"strings"
)

func relayWSURL(relayBase string) (string, error) {
	endpoint, err := url.Parse(relayBase)
	if err != nil {
		return "", fmt.Errorf("parse relay URL: %w", err)
	}
	switch strings.ToLower(endpoint.Scheme) {
	case "http":
		endpoint.Scheme = "ws"
	case "https":
		endpoint.Scheme = "wss"
	default:
		return "", fmt.Errorf("unsupported scheme %q", endpoint.Scheme)
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/v1/signal"
	return endpoint.String(), nil
}
