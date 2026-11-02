package access

import (
	"context"
	"time"

	"github.com/pocketstation-io/relay/access/storage"
)

// AuthorityMetadata is sent only over the authenticated internal service path.
// The issuer and incarnation come from storage, never from an unverified JWT.
type AuthorityMetadata struct {
	SessionID          string    `json:"session_id"`
	Issuer             string    `json:"issuer"`
	Incarnation        string    `json:"incarnation"`
	LegacyTokensBefore time.Time `json:"legacy_tokens_before,omitempty"`
	MediaTerm          uint64    `json:"media_term"`
	RelayEpoch         string    `json:"relay_epoch"`
	RequiredBuses      []string  `json:"required_buses"`
}

func (s *Service) Ready(ctx context.Context) error {
	return s.view(ctx, func(tx storage.Tx) error {
		ns, err := tx.Namespace()
		if err != nil {
			return err
		}
		if ns.SchemaVersion != storage.SchemaVersion {
			return ErrWriterFenced
		}
		return s.checkWriter(tx)
	})
}
func (s *Service) Metadata(ctx context.Context, id string) (AuthorityMetadata, error) {
	var result AuthorityMetadata
	err := s.view(ctx, func(tx storage.Tx) error {
		if err := s.checkWriter(tx); err != nil {
			return err
		}
		r, err := s.active(tx, id)
		if err != nil {
			return err
		}
		result = AuthorityMetadata{SessionID: r.ID, Issuer: r.Issuer, Incarnation: r.Incarnation, LegacyTokensBefore: r.LegacyTokensBefore, MediaTerm: r.MediaTerm, RelayEpoch: r.Relay.RelayEpoch, RequiredBuses: append([]string(nil), r.RequiredBuses...)}
		return nil
	})
	return result, err
}

// AcquireRelayWriter uses compare-and-swap placement. A replaced media process
// cannot publish again using its old term. Retrying the same epoch is harmless.
func (s *Service) AcquireRelayWriter(ctx context.Context, id, epoch string, expected uint64) (uint64, error) {
	if !validIdentifier(epoch, 128) {
		return 0, ErrInvalidRelayState
	}
	var term uint64
	err := s.update(ctx, func(tx storage.Tx) error {
		r, err := s.active(tx, id)
		if err != nil {
			return err
		}
		if r.Relay.RelayEpoch == epoch && r.MediaTerm != 0 {
			term = r.MediaTerm
			return nil
		}
		if r.MediaTerm != expected {
			return ErrWriterFenced
		}
		r.MediaTerm++
		r.Relay = storage.RelayState{SessionID: id, RelayEpoch: epoch, WriterTerm: r.MediaTerm, ObservedAt: tx.Now()}
		r.StateRevision++
		r.WriterTerm = s.term
		term = r.MediaTerm
		return tx.PutSession(r)
	})
	return term, err
}
