package access

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pocketstation-io/relay/access/admission"
	"github.com/pocketstation-io/relay/access/metrics"
	auth "github.com/pocketstation-io/relay/access/token"
	"github.com/pocketstation-io/relay/access/turn"
)

const (
	sseKeepaliveIntervalDuration = 20 * time.Second
	maxPublicRequestBodyBytes    = 4096
)

type TURNConfig struct {
	PublicIP              string
	Secret                []byte
	UDPPort               int
	TLSPort               int
	CredentialTTLDuration time.Duration
}

// HandlerConfig contains public endpoint configuration fixed at startup.
type HandlerConfig struct {
	RelayWHIPBaseURL  string
	RelaySignalURL    string
	PublicControlURL  string
	PublicReceiverURL string
	STUNURLs          []string
	ICEServers        []turn.ICEServer
	TURN              *TURNConfig
	CreateAdmission   *admission.IPLimiter
	ResolveAdmission  *admission.IPLimiter
	// TrustedClientIPHeader is read only when the operator explicitly names a
	// reverse-proxy header. PocketStation validates that its value is one IP.
	TrustedClientIPHeader string
}

type Handler struct {
	store                 *SessionStore
	metrics               *metrics.Registry
	relayWHIPBaseURL      string
	relaySignalURL        string
	publicControlURL      string
	publicReceiverURL     string
	stunURLs              []string
	iceServers            []turn.ICEServer
	turn                  *TURNConfig
	createAdmission       *admission.IPLimiter
	resolveAdmission      *admission.IPLimiter
	trustedClientIPHeader string
}

func NewHandler(store *SessionStore, registry *metrics.Registry) *Handler {
	return NewHandlerWithConfig(store, registry, HandlerConfig{})
}

func NewHandlerWithTURN(store *SessionStore, registry *metrics.Registry, config *TURNConfig) *Handler {
	return NewHandlerWithConfig(store, registry, HandlerConfig{TURN: config})
}

func NewHandlerWithConfig(store *SessionStore, registry *metrics.Registry, config HandlerConfig) *Handler {
	if config.TURN != nil && config.TURN.CredentialTTLDuration <= 0 {
		config.TURN.CredentialTTLDuration = 24 * time.Hour
	}
	return &Handler{
		store:                 store,
		metrics:               registry,
		relayWHIPBaseURL:      strings.TrimRight(config.RelayWHIPBaseURL, "/"),
		relaySignalURL:        strings.TrimRight(config.RelaySignalURL, "/"),
		publicControlURL:      strings.TrimRight(config.PublicControlURL, "/"),
		publicReceiverURL:     strings.TrimRight(config.PublicReceiverURL, "/"),
		stunURLs:              append([]string(nil), config.STUNURLs...),
		iceServers:            cloneICEServers(config.ICEServers),
		turn:                  config.TURN,
		createAdmission:       config.CreateAdmission,
		resolveAdmission:      config.ResolveAdmission,
		trustedClientIPHeader: strings.TrimSpace(config.TrustedClientIPHeader),
	}
}

func (handler *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/sessions", handler.createSession)
	mux.HandleFunc("GET /v1/sessions/{id}", handler.getSession)
	mux.HandleFunc("POST /v1/sessions/{id}/renew", handler.renewOwner)
	mux.HandleFunc("POST /v1/sessions/{id}/publish", handler.publishSession)
	mux.HandleFunc("POST /v1/sessions/{id}/subscribe", handler.subscribeSession)
	mux.HandleFunc("POST /v1/sessions/{id}/invitations", handler.createInvitation)
	mux.HandleFunc("GET /v1/invitations/{locator}", handler.inspectInvitation)
	mux.HandleFunc("HEAD /v1/invitations/{locator}", handler.inspectInvitation)
	mux.HandleFunc("POST /v1/invitations/{locator}/redeem", handler.redeemInvitation)
	mux.HandleFunc("POST /v1/join", handler.redeemInvitation)
	mux.HandleFunc("POST /v1/join/{locator}", handler.redeemInvitation)
	mux.HandleFunc("DELETE /v1/sessions/{id}", handler.deleteSession)
	mux.HandleFunc("GET /v1/sessions/{id}/events", handler.sseEvents)
}

type createSessionRequest struct {
	RequiredBuses []string `json:"required_buses"`
}

type createSessionResponse struct {
	SessionID     string           `json:"session_id"`
	RequiredBuses []string         `json:"required_buses"`
	SourceToken   string           `json:"source_token"`
	WHIPURL       string           `json:"whip_url,omitempty"`
	WHEPURL       string           `json:"whep_url,omitempty"`
	ICEServers    []turn.ICEServer `json:"ice_servers,omitempty"`
}

func (handler *Handler) createSession(writer http.ResponseWriter, request *http.Request) {
	if allowed, retryAfter := handler.createAdmission.Allow(handler.admissionIdentity(request), time.Now()); !allowed {
		writer.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Round(time.Second).Seconds())))
		writeProblem(writer, http.StatusTooManyRequests, "session_admission_exceeded")
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxPublicRequestBodyBytes)
	var input createSessionRequest
	if request.ContentLength != 0 {
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeProblem(writer, http.StatusBadRequest, "invalid_session_request")
			return
		}
		if err := requireJSONEOF(decoder); err != nil {
			writeProblem(writer, http.StatusBadRequest, "invalid_session_request")
			return
		}
	}
	created, err := handler.store.CreateContext(request.Context(), input.RequiredBuses...)
	if errors.Is(err, ErrSessionCapacity) {
		writeProblem(writer, http.StatusTooManyRequests, "session_capacity_reached")
		return
	}
	if err != nil {
		if !errors.Is(err, ErrInvalidRelayState) {
			writeProblem(writer, http.StatusServiceUnavailable, "session_authority_unavailable")
			return
		}
		writeProblem(writer, http.StatusBadRequest, "invalid_session_request")
		return
	}
	handler.metrics.SessionsCreated.Add(1)
	response := createSessionResponse{
		SessionID:     created.ID(),
		RequiredBuses: created.RequiredBuses(),
		SourceToken:   created.SourceToken(),
	}
	if handler.relayWHIPBaseURL != "" {
		response.WHIPURL = handler.relayWHIPBaseURL + "/v1/sessions/" + created.ID() + "/whip"
		response.WHEPURL = handler.relayWHIPBaseURL + "/v1/sessions/" + created.ID() + "/whep"
	}
	response.ICEServers = handler.buildICEServers(created.ID())
	writeJSON(writer, http.StatusCreated, response)
}

func (handler *Handler) admissionIdentity(request *http.Request) string {
	if handler.trustedClientIPHeader != "" {
		candidate := strings.TrimSpace(request.Header.Get(handler.trustedClientIPHeader))
		if ip := net.ParseIP(candidate); ip != nil {
			return ip.String()
		}
	}
	return request.RemoteAddr
}

type sessionResponse struct {
	State
	// SourceActive is a temporary compatibility field. It now means every
	// required bus is attached; Ready is the primary field.
	SourceActive bool `json:"source_active"`
}

func (handler *Handler) getSession(writer http.ResponseWriter, request *http.Request) {
	id := request.PathValue("id")
	if !handler.authorizeResponse(writer, request, id, auth.RoleSource, auth.RoleSubscriber) {
		return
	}
	value, err := handler.store.GetContext(request.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeProblem(writer, http.StatusNotFound, "session_not_found")
		return
	}
	if err != nil {
		writeProblem(writer, http.StatusServiceUnavailable, "session_authority_unavailable")
		return
	}
	state := value.State()
	writeJSON(writer, http.StatusOK, sessionResponse{State: state, SourceActive: state.Ready})
}

type subscribeSessionRequest struct {
	BusID string `json:"bus_id"`
}

type publishSessionResponse struct {
	SessionID      string           `json:"session_id"`
	BusID          string           `json:"bus_id"`
	PublisherToken string           `json:"publisher_token"`
	SignalURL      string           `json:"signal_url,omitempty"`
	ICEServers     []turn.ICEServer `json:"ice_servers,omitempty"`
}

func (handler *Handler) publishSession(writer http.ResponseWriter, request *http.Request) {
	id := request.PathValue("id")
	if !handler.authorizeResponse(writer, request, id, auth.RoleSource) {
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxPublicRequestBodyBytes)
	var input subscribeSessionRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || requireJSONEOF(decoder) != nil {
		writeProblem(writer, http.StatusBadRequest, "invalid_publisher_request")
		return
	}
	token, err := handler.store.IssuePublisherTokenContext(request.Context(), id, input.BusID)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, sql.ErrNoRows) {
		writeProblem(writer, http.StatusNotFound, "session_not_found")
		return
	}
	if err != nil {
		if !errors.Is(err, ErrInvalidRelayState) {
			writeProblem(writer, http.StatusServiceUnavailable, "session_authority_unavailable")
			return
		}
		writeProblem(writer, http.StatusBadRequest, "invalid_publisher_scope")
		return
	}
	response := publishSessionResponse{
		SessionID:      id,
		BusID:          input.BusID,
		PublisherToken: token,
		SignalURL:      handler.relaySignalURL,
	}
	response.ICEServers = handler.buildICEServers(id)
	writeJSON(writer, http.StatusOK, response)
}

type subscribeSessionResponse struct {
	SessionID       string `json:"session_id"`
	BusID           string `json:"bus_id"`
	SubscriberToken string `json:"subscriber_token"`
}

func (handler *Handler) subscribeSession(writer http.ResponseWriter, request *http.Request) {
	id := request.PathValue("id")
	if !handler.authorizeResponse(writer, request, id, auth.RoleSource) {
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxPublicRequestBodyBytes)
	input := subscribeSessionRequest{BusID: "mix"}
	if request.ContentLength != 0 {
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeProblem(writer, http.StatusBadRequest, "invalid_subscriber_request")
			return
		}
		if err := requireJSONEOF(decoder); err != nil {
			writeProblem(writer, http.StatusBadRequest, "invalid_subscriber_request")
			return
		}
	}
	token, err := handler.store.IssueSubscriberTokenContext(request.Context(), id, input.BusID)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, sql.ErrNoRows) {
		writeProblem(writer, http.StatusNotFound, "session_not_found")
		return
	}
	if err != nil {
		if !errors.Is(err, ErrInvalidRelayState) {
			writeProblem(writer, http.StatusServiceUnavailable, "session_authority_unavailable")
			return
		}
		writeProblem(writer, http.StatusBadRequest, "invalid_subscriber_scope")
		return
	}
	handler.metrics.SubscriptionRequests.Add(1)
	writeJSON(writer, http.StatusOK, subscribeSessionResponse{SessionID: id, BusID: input.BusID, SubscriberToken: token})
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func (handler *Handler) deleteSession(writer http.ResponseWriter, request *http.Request) {
	id := request.PathValue("id")
	if !handler.authorizeResponse(writer, request, id, auth.RoleSource) {
		return
	}
	deleted, err := handler.store.DeleteContext(request.Context(), id)
	if err != nil {
		writeProblem(writer, http.StatusServiceUnavailable, "session_revocation_failed")
		return
	}
	if !deleted {
		writeProblem(writer, http.StatusNotFound, "session_not_found")
		return
	}
	handler.metrics.SessionsDeleted.Add(1)
	writer.WriteHeader(http.StatusNoContent)
}

func (handler *Handler) sseEvents(writer http.ResponseWriter, request *http.Request) {
	id := request.PathValue("id")
	if !handler.authorizeResponse(writer, request, id, auth.RoleSource, auth.RoleSubscriber) {
		return
	}
	// Events are complete replacement snapshots, not deltas. A reconnecting
	// client may send Last-Event-ID; after validating it, the server always
	// returns the latest authoritative state before streaming newer revisions.
	if lastEventID := request.Header.Get("Last-Event-ID"); lastEventID != "" {
		if _, err := strconv.ParseUint(lastEventID, 10, 64); err != nil {
			writeProblem(writer, http.StatusBadRequest, "invalid_last_event_id")
			return
		}
	}
	state, eventBus, subscriptionID, eventChannel, err := handler.store.SubscribeEventsContext(request.Context(), id)
	if errors.Is(err, ErrSSECapacity) {
		writeProblem(writer, http.StatusTooManyRequests, "sse_capacity_reached")
		return
	}
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, os.ErrNotExist) {
		writeProblem(writer, http.StatusNotFound, "session_not_found")
		return
	}
	if err != nil {
		writeProblem(writer, http.StatusServiceUnavailable, "session_authority_unavailable")
		return
	}
	defer eventBus.Unsubscribe(subscriptionID)
	flusher, supported := writer.(http.Flusher)
	if !supported {
		writeProblem(writer, http.StatusInternalServerError, "streaming_not_supported")
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache, no-store")
	writer.Header().Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)
	if !writeStateEvent(writer, flusher, state) {
		return
	}
	ticker := time.NewTicker(sseKeepaliveIntervalDuration)
	defer ticker.Stop()
	for {
		select {
		case <-request.Context().Done():
			return
		case event, open := <-eventChannel:
			if !open || !writeEvent(writer, flusher, event) {
				return
			}
		case <-ticker.C:
			controller := http.NewResponseController(writer)
			_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := fmt.Fprint(writer, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
			_ = controller.SetWriteDeadline(time.Time{})
		}
	}
}

func (handler *Handler) authorizeResponse(writer http.ResponseWriter, request *http.Request, sessionID string, roles ...auth.Role) bool {
	raw := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
	if raw == "" {
		writeProblem(writer, http.StatusNotFound, "session_not_found")
		return false
	}
	claims, err := handler.store.Authorize(request.Context(), sessionID, raw)
	if err != nil {
		if errors.Is(err, auth.ErrInvalidCapability) || errors.Is(err, sql.ErrNoRows) || errors.Is(err, os.ErrNotExist) {
			writeProblem(writer, http.StatusNotFound, "session_not_found")
		} else {
			writeProblem(writer, http.StatusServiceUnavailable, "session_authority_unavailable")
		}
		return false
	}
	for _, role := range roles {
		if claims.Role == role && (role != auth.RoleSource || claims.CanControl()) {
			return true
		}
	}
	writeProblem(writer, http.StatusNotFound, "session_not_found")
	return false
}

func writeStateEvent(writer http.ResponseWriter, flusher http.Flusher, state State) bool {
	payload, err := json.Marshal(sessionResponse{State: state, SourceActive: state.Ready})
	if err != nil {
		return false
	}
	return writeEvent(writer, flusher, Event{Revision: state.StateRevision, Payload: string(payload)})
}

func writeEvent(writer http.ResponseWriter, flusher http.Flusher, event Event) bool {
	controller := http.NewResponseController(writer)
	_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintf(writer, "id: %d\nevent: session.state\ndata: %s\n\n", event.Revision, event.Payload); err != nil {
		return false
	}
	flusher.Flush()
	_ = controller.SetWriteDeadline(time.Time{})
	return true
}

func (handler *Handler) buildICEServers(sessionID string) []turn.ICEServer {
	servers := cloneICEServers(handler.iceServers)
	if len(handler.stunURLs) != 0 {
		servers = append(servers, turn.ICEServer{
			URLs: append([]string(nil), handler.stunURLs...),
		})
	}
	if handler.turn == nil {
		return servers
	}
	return append(servers, buildTURNICEServers(handler.turn, sessionID)...)
}

func buildTURNICEServers(config *TURNConfig, sessionID string) []turn.ICEServer {
	host := config.PublicIP
	udpPort := strconv.Itoa(config.UDPPort)
	username, password := turn.Credentials(config.Secret, sessionID, config.CredentialTTLDuration)
	urls := []string{
		"turn:" + net.JoinHostPort(host, udpPort) + "?transport=udp",
		"turn:" + net.JoinHostPort(host, udpPort) + "?transport=tcp",
	}
	if config.TLSPort > 0 {
		urls = append(urls, "turns:"+net.JoinHostPort(host, strconv.Itoa(config.TLSPort))+"?transport=tcp")
	}
	return []turn.ICEServer{
		{URLs: []string{"stun:" + net.JoinHostPort(host, udpPort)}},
		{URLs: urls, Username: username, Credential: password},
	}
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		slog.Warn("response write failed", "error", err)
	}
}

func writeProblem(writer http.ResponseWriter, status int, code string) {
	writeJSON(writer, status, map[string]string{"error": code})
}

func cloneICEServers(input []turn.ICEServer) []turn.ICEServer {
	result := append([]turn.ICEServer(nil), input...)
	for i := range result {
		result[i].URLs = append([]string(nil), result[i].URLs...)
	}
	return result
}
