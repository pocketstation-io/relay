package server

import (
	"context"
	"database/sql"
	"errors"
	"github.com/pocketstation-io/relay/access"
	"github.com/pocketstation-io/relay/access/storage"
	"log/slog"
	"os"
	"time"

	"github.com/pocketstation-io/relay/internal/notifications/callback"
	"github.com/pocketstation-io/relay/internal/session"
)

const defaultControlReconcileInterval = 5 * time.Second
const defaultControlAuthorityTimeout = 5 * time.Second

func (server *Server) requireActiveControlSession(parent context.Context, sessionID string) error {
	ctx, cancel := context.WithTimeout(parent, server.controlAuthorityTimeout)
	defer cancel()
	metadata, err := server.authorityMetadata(ctx, sessionID)
	if err != nil {
		return err
	}
	return server.ensureMediaWriter(ctx, metadata)
}

func (server *Server) bindControlState(relaySession *session.RelaySession) {
	if (server.callbackClient == nil && server.accessService == nil) || relaySession == nil {
		return
	}
	relaySession.SetStateObserver(func() { server.queueControlState(relaySession) })
}

func (server *Server) queueControlState(relaySession *session.RelaySession) {
	if (server.callbackClient == nil && server.accessService == nil) || relaySession == nil {
		return
	}
	select {
	case server.controlStateChanges <- relaySession:
	default:
		server.Metrics.CallbackDroppedTotal.Add(1)
	}
}

func (server *Server) startControlStateSync() {
	if server.callbackClient == nil && server.accessService == nil {
		return
	}
	server.controlSyncOnce.Do(func() {
		server.controlSyncContext, server.controlSyncCancel = context.WithCancel(context.Background())
		if server.accessService != nil {
			server.accessService.StartMaintenance(server.controlSyncContext)
		}
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
			server.pruneSessionFeedback()
			server.pruneMediaWriters(server.controlSyncContext)
			for _, relaySession := range server.relaySessions.All() {
				server.pushControlState(relaySession)
			}
		}
	}
}

func (server *Server) pushControlState(relaySession *session.RelaySession) {
	state := relaySession.ControlState(server.relayEpoch)
	ctx, cancel := context.WithTimeout(server.controlSyncContext, server.controlAuthorityTimeout)
	defer cancel()
	term, err := server.mediaWriterTerm(relaySession.ID)
	if err == nil {
		snapshot := storage.RelayState{ContractVersion: state.ContractVersion, SessionID: state.SessionID, RelayEpoch: state.RelayEpoch, Revision: state.Revision, ObservedAt: state.ObservedAt, WriterTerm: term}
		for _, bus := range state.Buses {
			snapshot.Buses = append(snapshot.Buses, storage.BusState{BusID: bus.BusID, Role: bus.Role, SourceActive: bus.SourceActive, SourceGeneration: bus.SourceGeneration})
		}
		for _, sub := range state.Subscriptions {
			snapshot.Subscriptions = append(snapshot.Subscriptions, storage.SubscriptionState{SubscriberID: sub.SubscriberID, BusID: sub.BusID})
		}
		if server.accessService != nil {
			_, _, err = server.accessService.ApplyRelayStateContext(ctx, relaySession.ID, snapshot)
		} else {
			err = server.callbackClient.PushAccessState(ctx, snapshot)
		}
	}
	if err != nil {
		if errors.Is(err, callback.ErrSessionNotFound) || errors.Is(err, sql.ErrNoRows) || errors.Is(err, os.ErrNotExist) || errors.Is(err, access.ErrWriterFenced) || errors.Is(err, callback.ErrWriterFenced) {
			server.relaySessions.Delete(relaySession.ID)
			server.forgetSessionFeedback(relaySession.ID)
			if errors.Is(err, callback.ErrSessionNotFound) || errors.Is(err, sql.ErrNoRows) || errors.Is(err, os.ErrNotExist) {
				server.mediaWriterMu.Lock()
				delete(server.mediaWriters, relaySession.ID)
				server.mediaWriterMu.Unlock()
			}
			slog.Info("relay session removed after control-plane deletion",
				"relay_session_id", relaySession.ID,
				"relay_epoch", state.RelayEpoch,
				"relay_revision", state.Revision,
			)
			return
		}
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
