package server

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/pocketstation-io/relay/access"
	"github.com/pocketstation-io/relay/internal/notifications/callback"
)

type mediaWriter struct {
	done     chan struct{}
	term     uint64
	expected uint64
	err      error
}

func (s *Server) authorityMetadata(ctx context.Context, id string) (access.AuthorityMetadata, error) {
	var result access.AuthorityMetadata
	var err error
	if s.authorityMode == "standalone" {
		if s.accessService == nil {
			return result, errors.New("Session service unavailable")
		}
		result, err = s.accessService.Metadata(ctx, id)
	} else {
		if s.callbackClient == nil {
			return result, errors.New("managed Session service unavailable")
		}
		result, err = s.callbackClient.Metadata(ctx, id)
	}
	if errors.Is(err, sql.ErrNoRows) {
		err = callback.ErrSessionNotFound
	}
	if errors.Is(err, callback.ErrSessionNotFound) {
		s.forgetSessionFeedback(id)
		s.mediaWriterMu.Lock()
		delete(s.mediaWriters, id)
		s.mediaWriterMu.Unlock()
	}
	return result, err
}
func (s *Server) ensureMediaWriter(ctx context.Context, metadata access.AuthorityMetadata) error {
	s.mediaWriterMu.Lock()
	writer, found := s.mediaWriters[metadata.SessionID]
	if found {
		select {
		case <-writer.done:
			if writer.err != nil && !errors.Is(writer.err, access.ErrWriterFenced) && !errors.Is(writer.err, callback.ErrWriterFenced) {
				writer = &mediaWriter{done: make(chan struct{}), expected: writer.expected}
				s.mediaWriters[metadata.SessionID] = writer
				found = false
			}
		default:
		}
	}

	if writer == nil {
		if len(s.mediaWriters) >= s.maxRooms {
			s.mediaWriterMu.Unlock()
			return errors.New("media writer capacity reached")
		}
		writer = &mediaWriter{done: make(chan struct{}), expected: metadata.MediaTerm}
		s.mediaWriters[metadata.SessionID] = writer
	}
	s.mediaWriterMu.Unlock()
	if found {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-writer.done:
		}
		if writer.err != nil {
			return writer.err
		}
		if metadata.MediaTerm != writer.term || metadata.RelayEpoch != s.relayEpoch {
			current, err := s.authorityMetadata(ctx, metadata.SessionID)
			if err != nil {
				return err
			}
			if current.MediaTerm != writer.term || current.RelayEpoch != s.relayEpoch {
				return callback.ErrWriterFenced
			}
		}
		return nil
	}
	if s.authorityMode == "standalone" {
		writer.term, writer.err = s.accessService.AcquireRelayWriter(ctx, metadata.SessionID, s.relayEpoch, writer.expected)
	} else {
		writer.term, writer.err = s.callbackClient.AcquireRelayWriter(ctx, metadata.SessionID, s.relayEpoch, writer.expected)
	}
	close(writer.done)
	return writer.err
}
func (s *Server) mediaWriterTerm(id string) (uint64, error) {
	s.mediaWriterMu.Lock()
	writer := s.mediaWriters[id]
	s.mediaWriterMu.Unlock()
	if writer == nil {
		return 0, errors.New("media placement not acquired")
	}
	select {
	case <-writer.done:
		return writer.term, writer.err
	default:
		return 0, errors.New("media placement pending")
	}
}
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.accessInitError != nil {
		http.Error(w, "Session service unavailable", 503)
		return
	}
	if s.accessService != nil {
		if err := s.accessService.Ready(r.Context()); err != nil {
			http.Error(w, "Session service unavailable", 503)
			return
		}
	}
	if s.authorityMode == "control-plane" {
		if s.callbackClient == nil {
			http.Error(w, "Session service unavailable", 503)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := s.callbackClient.Ready(ctx); err != nil {
			http.Error(w, "Session service unavailable", 503)
			return
		}
	}
	s.healthz(w, r)
}

// Only authoritative absence releases a placement tombstone. Dropping an idle
// or fenced entry alone would allow a displaced process to acquire it again.
func (s *Server) pruneMediaWriters(parent context.Context) {
	s.mediaWriterMu.Lock()
	ids := make([]string, 0, len(s.mediaWriters))
	for id := range s.mediaWriters {
		ids = append(ids, id)
	}
	s.mediaWriterMu.Unlock()
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		if _, live := s.relaySessions.Get(id); live {
			continue
		}
		_, _ = s.authorityMetadata(ctx, id)
	}
}
