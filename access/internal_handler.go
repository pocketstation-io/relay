package access

import (
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"os"
)

const maxRelayStateBodyBytes = 256 * 1024

// InternalHandler accepts authenticated, complete Relay state snapshots.
type InternalHandler struct {
	store  *SessionStore
	secret []byte
}

func NewInternalHandler(store *SessionStore, secret []byte) *InternalHandler {
	return &InternalHandler{store: store, secret: append([]byte(nil), secret...)}
}

func (handler *InternalHandler) Register(mux *http.ServeMux) {
	mux.Handle("POST /v1/internal/sessions/{id}/relay-writer", handler.authenticate(http.HandlerFunc(handler.acquireRelayWriter)))
	mux.Handle("PUT /v1/internal/sessions/{id}/relay-state", handler.authenticate(http.HandlerFunc(handler.replaceRelayState)))
	mux.Handle("GET /v1/internal/sessions/{id}/authority", handler.authenticate(http.HandlerFunc(handler.getAuthorityState)))
	mux.Handle("HEAD /v1/internal/sessions/{id}/authority", handler.authenticate(http.HandlerFunc(handler.getAuthorityState)))
}

func (handler *InternalHandler) getAuthorityState(writer http.ResponseWriter, request *http.Request) {
	value, err := handler.store.Metadata(request.Context(), request.PathValue("id"))
	if errors.Is(err, sql.ErrNoRows) {
		writeProblem(writer, http.StatusNotFound, "session_not_found")
		return
	}
	if err != nil {
		writeProblem(writer, http.StatusServiceUnavailable, "session_authority_unavailable")
		return
	}
	if request.Method == http.MethodHead {
		writer.WriteHeader(http.StatusOK)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (handler *InternalHandler) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		provided := []byte(request.Header.Get("X-PocketStation-Internal-Secret"))
		if len(handler.secret) < 32 || len(provided) != len(handler.secret) || subtle.ConstantTimeCompare(provided, handler.secret) != 1 {
			writeProblem(writer, http.StatusUnauthorized, "internal_authentication_failed")
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func (handler *InternalHandler) replaceRelayState(writer http.ResponseWriter, request *http.Request) {
	id := request.PathValue("id")
	request.Body = http.MaxBytesReader(writer, request.Body, maxRelayStateBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var snapshot RelayStateSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_relay_state")
		return
	}
	if err := requireJSONEOF(decoder); err != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_relay_state")
		return
	}
	state, changed, err := handler.store.ApplyRelayStateContext(request.Context(), id, snapshot)
	if errors.Is(err, os.ErrNotExist) {
		writeProblem(writer, http.StatusNotFound, "session_not_found")
		return
	}
	if errors.Is(err, ErrWriterFenced) {
		writeProblem(writer, http.StatusConflict, "relay_writer_fenced")
		return
	}
	if err != nil {
		if !errors.Is(err, ErrInvalidRelayState) {
			writeProblem(writer, http.StatusServiceUnavailable, "session_authority_unavailable")
			return
		}
		writeProblem(writer, http.StatusBadRequest, "invalid_relay_state")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"session_id":     state.SessionID,
		"state_revision": state.StateRevision,
		"relay_epoch":    state.RelayEpoch,
		"relay_revision": state.RelayRevision,
		"changed":        changed,
	})
}

func (handler *InternalHandler) acquireRelayWriter(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	var input struct {
		RelayEpoch   string `json:"relay_epoch"`
		ExpectedTerm uint64 `json:"expected_term"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || requireJSONEOF(decoder) != nil {
		writeProblem(w, 400, "invalid_relay_writer")
		return
	}
	term, err := handler.store.AcquireRelayWriter(r.Context(), r.PathValue("id"), input.RelayEpoch, input.ExpectedTerm)
	if errors.Is(err, sql.ErrNoRows) {
		writeProblem(w, 404, "session_not_found")
		return
	}
	if errors.Is(err, ErrWriterFenced) {
		writeProblem(w, 409, "relay_writer_fenced")
		return
	}
	if err != nil {
		writeProblem(w, 503, "session_authority_unavailable")
		return
	}
	writeJSON(w, 200, map[string]uint64{"writer_term": term})
}
