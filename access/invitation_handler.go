package access

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pocketstation-io/relay/access/names"
	auth "github.com/pocketstation-io/relay/access/token"
	"github.com/pocketstation-io/relay/access/turn"
)

type createInvitationRequest struct {
	WordCount  json.RawMessage      `json:"word_count"`
	BusID      string               `json:"bus_id"`
	Visibility InvitationVisibility `json:"visibility"`
}

type createInvitationResponse struct {
	WordCount  int                  `json:"word_count"`
	JoinCode   string               `json:"join_code"`
	JoinURL    string               `json:"join_url,omitempty"`
	ShareAlias string               `json:"share_alias"`
	ShareURL   string               `json:"share_url,omitempty"`
	Visibility InvitationVisibility `json:"visibility"`
	ExpiresAt  time.Time            `json:"expires_at"`
}

type inspectInvitationResponse struct {
	WordCount  int                  `json:"word_count"`
	ShareAlias string               `json:"share_alias"`
	Visibility InvitationVisibility `json:"visibility"`
	ExpiresAt  time.Time            `json:"expires_at"`
}

type redeemInvitationRequest struct {
	JoinCode string `json:"join_code"`
	// Deprecated: use join_code. This accepts only that same opaque capability.
	Secret string `json:"secret"`
}

type resolveInvitationResponse struct {
	SessionID       string           `json:"session_id"`
	BusID           string           `json:"bus_id"`
	SubscriberToken string           `json:"subscriber_token"`
	SignalURL       string           `json:"signal_url,omitempty"`
	WHEPURL         string           `json:"whep_url,omitempty"`
	ICEServers      []turn.ICEServer `json:"ice_servers,omitempty"`
}

func (handler *Handler) createInvitation(writer http.ResponseWriter, request *http.Request) {
	sessionID := request.PathValue("id")
	if !handler.authorizeResponse(writer, request, sessionID, auth.RoleSource) {
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxPublicRequestBodyBytes)
	input := createInvitationRequest{BusID: "mix"}
	if request.ContentLength != 0 {
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil || requireJSONEOF(decoder) != nil {
			writeProblem(writer, http.StatusBadRequest, "invalid_invitation_request")
			return
		}
	}
	wordCount := 0
	if input.WordCount != nil {
		if err := json.Unmarshal(input.WordCount, &wordCount); err != nil || (wordCount < names.MinWordCount || wordCount > names.MaxWordCount) {
			writeProblem(writer, http.StatusBadRequest, "invalid_word_count")
			return
		}
	}
	created, err := handler.store.CreateInvitationWithOptionsContext(request.Context(), sessionID, input.BusID, InvitationOptions{Visibility: input.Visibility, WordCount: wordCount})
	switch {
	case errors.Is(err, os.ErrNotExist):
		writeProblem(writer, http.StatusNotFound, "session_not_found")
		return
	case errors.Is(err, ErrSessionNotReady):
		writeProblem(writer, http.StatusConflict, "session_not_ready")
		return
	case errors.Is(err, ErrInvitationCapacity):
		writeProblem(writer, http.StatusServiceUnavailable, "invitation_capacity_reached")
		return
	case errors.Is(err, ErrInvalidWordCount):
		writeProblem(writer, http.StatusBadRequest, "invalid_word_count")
		return
	case errors.Is(err, ErrInvalidInvitationVisibility):
		writeProblem(writer, http.StatusBadRequest, "invalid_invitation_visibility")
		return
	case err != nil:
		writeProblem(writer, http.StatusServiceUnavailable, "invitation_creation_failed")
		return
	}
	response := createInvitationResponse{
		WordCount:  created.WordCount,
		JoinCode:   created.Code,
		ShareAlias: created.Alias,
		Visibility: created.Visibility,
		ExpiresAt:  created.ExpiresAt,
	}
	response.JoinURL, response.ShareURL = handler.invitationURLs(created)
	writer.Header().Set("Cache-Control", "no-store")
	writeJSON(writer, http.StatusCreated, response)
}

func (handler *Handler) inspectInvitation(writer http.ResponseWriter, request *http.Request) {
	metadata, err := handler.store.InspectInvitationContext(request.Context(), request.PathValue("locator"))
	if errors.Is(err, os.ErrNotExist) {
		writeProblem(writer, http.StatusNotFound, "invitation_not_found")
		return
	}
	if err != nil {
		writeProblem(writer, http.StatusServiceUnavailable, "session_authority_unavailable")
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	if request.Method == http.MethodHead {
		writer.WriteHeader(http.StatusOK)
		return
	}
	writeJSON(writer, http.StatusOK, inspectInvitationResponse{
		WordCount:  metadata.WordCount,
		ShareAlias: metadata.Alias,
		Visibility: metadata.Visibility,
		ExpiresAt:  metadata.ExpiresAt,
	})
}

func (handler *Handler) redeemInvitation(writer http.ResponseWriter, request *http.Request) {
	if allowed, retryAfter := handler.resolveAdmission.Allow(handler.admissionIdentity(request), time.Now()); !allowed {
		writer.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Round(time.Second).Seconds())))
		writeProblem(writer, http.StatusTooManyRequests, "invitation_admission_exceeded")
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxPublicRequestBodyBytes)
	var input redeemInvitationRequest
	if request.ContentLength != 0 {
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil || requireJSONEOF(decoder) != nil {
			writeProblem(writer, http.StatusBadRequest, "invalid_invitation_request")
			return
		}
	}
	if input.Secret != "" {
		if input.JoinCode != "" && input.JoinCode != input.Secret {
			writeProblem(writer, http.StatusBadRequest, "conflicting_join_credential")
			return
		}
		input.JoinCode = input.Secret
	}
	locator := request.PathValue("locator")
	if locator == "" {
		locator = input.JoinCode
	}
	response, err := handler.store.redeem(request.Context(), locator, input.JoinCode, request.Header.Get("Idempotency-Key"), func(resolved ResolvedInvitation) ([]byte, error) {
		response := resolveInvitationResponse{SessionID: resolved.SessionID, BusID: resolved.BusID, SubscriberToken: resolved.SubscriberToken, SignalURL: handler.relaySignalURL}
		if handler.relayWHIPBaseURL != "" {
			response.WHEPURL = handler.relayWHIPBaseURL + "/v1/sessions/" + resolved.SessionID + "/whep"
		}
		response.ICEServers = handler.buildICEServers(resolved.SessionID)
		return json.Marshal(response)
	})
	if errors.Is(err, os.ErrNotExist) {
		writeProblem(writer, http.StatusNotFound, "invitation_not_found")
		return
	}
	if errors.Is(err, ErrRetryConflict) {
		writeProblem(writer, http.StatusConflict, "invitation_retry_conflict")
		return
	}
	if err != nil {
		writeProblem(writer, http.StatusServiceUnavailable, "session_authority_unavailable")
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(response)
}

func (handler *Handler) invitationURLs(created Invitation) (string, string) {
	if handler.publicReceiverURL == "" {
		return "", ""
	}
	receiver, err := url.Parse(handler.publicReceiverURL)
	if err != nil || receiver.Scheme == "" || receiver.Host == "" {
		return "", ""
	}
	receiver.RawQuery = ""
	receiver.Fragment = "join=" + url.QueryEscape(created.Code)
	receiver.Path = "/join"
	joinURL := receiver.String()
	receiver.Path = "/" + url.PathEscape(created.Alias)
	shareURL := receiver.String()
	return strings.TrimSpace(joinURL), strings.TrimSpace(shareURL)
}
