package access

import (
	"context"
	"io"

	"github.com/pocketstation-io/relay/access/storage"
)

// WriteMetrics uses a bounded background request. HTTP callers should supply
// their request context through WriteMetricsContext and handle storage failures.
func (s *Service) WriteMetrics(w io.Writer) error {
	return s.WriteMetricsContext(context.Background(), w)
}

// WriteMetricsContext observes persisted live Sessions, including work created
// by another process. Storage failure returns before any healthy-looking output.
func (s *Service) WriteMetricsContext(ctx context.Context, w io.Writer) error {
	var active int64
	err := s.view(ctx, func(tx storage.Tx) error {
		if err := s.checkWriter(tx); err != nil {
			return err
		}
		rows, err := tx.Sessions(s.config.MaxSessions)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if row.Incarnation == s.incarnation && row.ExpiresAt.After(tx.Now()) {
				active++
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.registry.WriteMetrics(w, active)
	return nil
}
