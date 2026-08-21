package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/pocketstation-io/relay/internal/signaling"
)

const shutdownDrainTimeout = 5 * time.Second

func (s *Server) Serve(address string) error {
	s.startControlStateSync()
	httpServer := &http.Server{
		Addr:              address,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}
	s.mu.Lock()
	s.httpServer = httpServer
	s.mu.Unlock()

	slog.Info("relay listening", "addr", address)
	return httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.stopControlStateSync()
	s.mu.Lock()
	httpServer := s.httpServer
	peers := make([]*signalPeer, 0, len(s.signalPeers))
	for _, peer := range s.signalPeers {
		peers = append(peers, peer)
	}
	s.mu.Unlock()

	message := signaling.ServerMessage{
		Type:    signaling.TypeError,
		Code:    "RELAY_SHUTTING_DOWN",
		Message: "relay restarting, reconnect",
	}
	for _, peer := range peers {
		_ = peer.send(message)
		peer.closeConn()
	}

	if httpServer != nil {
		drainContext, cancel := context.WithTimeout(ctx, shutdownDrainTimeout)
		defer cancel()
		if err := httpServer.Shutdown(drainContext); err != nil {
			return err
		}
	}
	s.relaySessions.CloseAll()
	if s.ipLimiter != nil {
		s.ipLimiter.Stop()
	}
	return nil
}
