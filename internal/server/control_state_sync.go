package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/pocketstation-io/relay/internal/session"
)

const defaultControlReconcileInterval = 5 * time.Second

func (server *Server) bindControlState(relaySession *session.RelaySession) {
	if server.callbackClient == nil || relaySession == nil {
		return
	}
	relaySession.SetStateObserver(func() { server.queueControlState(relaySession) })
}

func (server *Server) queueControlState(relaySession *session.RelaySession) {
	if server.callbackClient == nil || relaySession == nil {
		return
	}
	select {
	case server.controlStateChanges <- relaySession:
	default:
		server.Metrics.CallbackDroppedTotal.Add(1)
	}
}

func (server *Server) startControlStateSync() {
	if server.callbackClient == nil {
		return
	}
	server.controlSyncOnce.Do(func() {
		server.controlSyncContext, server.controlSyncCancel = context.WithCancel(context.Background())
		server.controlSyncWait.Add(1)
		go server.runControlStateSync()
	})
}

func (server *Server) runControlStateSync() {
	defer server.controlSyncWait.Done()
	ticker := time.NewTicker(server.controlReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-server.controlSyncContext.Done():
			return
		case relaySession := <-server.controlStateChanges:
			server.pushControlState(relaySession)
		case <-ticker.C:
			for _, relaySession := range server.relaySessions.All() {
				server.pushControlState(relaySession)
			}
		}
	}
}

func (server *Server) pushControlState(relaySession *session.RelaySession) {
	state := relaySession.ControlState(server.relayEpoch)
	if err := server.callbackClient.PushState(server.controlSyncContext, state); err != nil {
		slog.Warn("control-state synchronization failed",
			"relay_session_id", relaySession.ID,
			"relay_epoch", state.RelayEpoch,
			"relay_revision", state.Revision,
			"error", err,
		)
	}
}

func (server *Server) stopControlStateSync() {
	if server.controlSyncCancel != nil {
		server.controlSyncCancel()
		server.controlSyncWait.Wait()
	}
}
