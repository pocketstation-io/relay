package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/pocketstation-io/relay/internal/ingress"
)

// proxyIngress shares only the explicitly configured TLS edge port. Existing
// HTTP health checks and direct TURN listeners keep their own listeners.
type proxyIngress struct {
	mux  *ingress.Listener
	http *http.Server
	done chan error
}

func openProxyIngress(configuredPort string, turnEnabled bool) (*proxyIngress, error) {
	if configuredPort == "" || configuredPort == "0" {
		return nil, nil
	}
	port, parseErr := strconv.Atoi(configuredPort)
	if parseErr != nil || port < 1 || port > 65535 || !turnEnabled {
		return nil, fmt.Errorf("RELAY_PROXY_INGRESS_PORT requires enabled embedded TURN and a port within 1..65535")
	}
	listener, err := net.Listen("tcp4", net.JoinHostPort("0.0.0.0", strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	return &proxyIngress{mux: ingress.New(listener)}, nil
}

func (p *proxyIngress) turnListener() net.Listener {
	if p == nil {
		return nil
	}
	return p.mux.TURN()
}

func (p *proxyIngress) serve(handler http.Handler) {
	if p == nil {
		return
	}
	p.http = &http.Server{
		Handler: handler, ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 * 1024,
	}
	p.done = make(chan error, 1)
	go func() { p.done <- p.http.Serve(p.mux.HTTP()) }()
}

func (p *proxyIngress) close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	var shutdownErr error
	if p.http != nil {
		shutdownErr = p.http.Shutdown(ctx)
	}
	_ = p.mux.Close()
	if p.done != nil {
		if serveErr := <-p.done; serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) && !errors.Is(serveErr, net.ErrClosed) {
			return serveErr
		}
	}
	return shutdownErr
}
