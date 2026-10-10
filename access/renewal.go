package access

import (
	"context"
	"database/sql"
	"errors"
	"github.com/pocketstation-io/relay/access/storage"
	"github.com/pocketstation-io/relay/access/token"
	"net/http"
	"strings"
	"time"
)

// RenewOwner extends a live Session using its still-valid owner capability.
// It does not revive expired authority and never changes issuer, scope or ID.
func (s *Service) RenewOwner(ctx context.Context, id, credential string) (string, time.Time, error) {
	var result string
	var expires time.Time
	err := s.update(ctx, func(tx storage.Tx) error {
		r, err := s.active(tx, id)
		if err != nil {
			return err
		}
		c, err := s.authorizeRecord(r, credential, tx.Now())
		if err != nil {
			return err
		}
		if !c.CanControl() {
			return token.ErrInvalidCapability
		}
		result, err = s.sign(r, token.Claims{SessionID: id, Role: token.RoleSource, BusIDs: r.RequiredBuses, Control: true}, s.config.OwnerTokenTTL, tx.Now())
		if err != nil {
			return err
		}
		expires = tx.Now().Add(s.config.OwnerTokenTTL).Truncate(time.Second)
		r.LastActiveAt = tx.Now()
		r.ExpiresAt = tx.Now().Add(s.config.SessionTTL)
		return tx.PutSession(r)
	})
	if err != nil {
		return "", time.Time{}, err
	}
	return result, expires, nil
}
func (handler *Handler) renewOwner(w http.ResponseWriter, r *http.Request) {
	value, expires, err := handler.store.RenewOwner(r.Context(), r.PathValue("id"), strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if err != nil {
		if errors.Is(err, token.ErrInvalidCapability) || errors.Is(err, sql.ErrNoRows) {
			writeProblem(w, http.StatusNotFound, "session_not_found")
		} else {
			writeProblem(w, http.StatusServiceUnavailable, "session_authority_unavailable")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"source_token": value, "expires_at": expires})
}
