package access

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/pocketstation-io/relay/access/storage"
	"github.com/pocketstation-io/relay/access/storage/memory"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pocketstation-io/relay/access/admission"
	"github.com/pocketstation-io/relay/access/metrics"
)

var internalTestSecret = []byte("abcdef0123456789abcdef0123456789")

func testMux(store *SessionStore, config HandlerConfig) *http.ServeMux {
	mux := http.NewServeMux()
	NewHandlerWithConfig(store, metrics.New(), config).Register(mux)
	NewInternalHandler(store, internalTestSecret).Register(mux)
	return mux
}

func TestGivenFlyProxyWhenCreatingSessionsThenAdmissionUsesValidatedClientIP(t *testing.T) {
	store := NewSessionStore(sessionTestSecret)
	mux := testMux(store, HandlerConfig{
		TrustedClientIPHeader: "Fly-Client-IP",
		CreateAdmission:       admission.NewIPLimiter(1, time.Minute, 8),
	})

	create := func(clientIP string) int {
		request := httptest.NewRequest(http.MethodPost, "/v1/sessions", nil)
		request.RemoteAddr = "192.0.2.10:443"
		request.Header.Set("Fly-Client-IP", clientIP)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		return response.Code
	}

	if status := create("198.51.100.1"); status != http.StatusCreated {
		t.Fatalf("first client status=%d", status)
	}
	if status := create("198.51.100.2"); status != http.StatusCreated {
		t.Fatalf("second client status=%d", status)
	}
	if status := create("198.51.100.1"); status != http.StatusTooManyRequests {
		t.Fatalf("repeated client status=%d", status)
	}
}

func TestGivenPublicLifecycleWhenAuthorizedThenScopedStateAndRelayURLsAreReturned(t *testing.T) {
	store := NewSessionStore(sessionTestSecret)
	mux := testMux(store, HandlerConfig{RelayWHIPBaseURL: "https://relay.example/"})
	create := httptest.NewRequest(http.MethodPost, "/v1/sessions", bytes.NewBufferString(`{"required_buses":["application","microphone"]}`))
	create.RemoteAddr = "192.0.2.1:443"
	created := httptest.NewRecorder()
	mux.ServeHTTP(created, create)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var credentials createSessionResponse
	if err := json.Unmarshal(created.Body.Bytes(), &credentials); err != nil {
		t.Fatal(err)
	}
	if credentials.WHIPURL != "https://relay.example/v1/sessions/"+credentials.SessionID+"/whip" {
		t.Fatalf("unexpected WHIP URL %q", credentials.WHIPURL)
	}

	unauthorized := httptest.NewRecorder()
	mux.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/v1/sessions/"+credentials.SessionID, nil))
	if unauthorized.Code != http.StatusNotFound {
		t.Fatalf("unauthorized GET status=%d", unauthorized.Code)
	}

	get := httptest.NewRequest(http.MethodGet, "/v1/sessions/"+credentials.SessionID, nil)
	get.Header.Set("Authorization", "Bearer "+credentials.SourceToken)
	got := httptest.NewRecorder()
	mux.ServeHTTP(got, get)
	if got.Code != http.StatusOK {
		t.Fatalf("authorized GET status=%d body=%s", got.Code, got.Body.String())
	}
	var state sessionResponse
	if err := json.Unmarshal(got.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if state.Ready || state.SourceActive || len(state.RequiredBuses) != 2 {
		t.Fatalf("unexpected initial state %#v", state)
	}

	subscribe := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+credentials.SessionID+"/subscribe", bytes.NewBufferString(`{"bus_id":"application"}`))
	subscribe.Header.Set("Authorization", "Bearer "+credentials.SourceToken)
	subscriber := httptest.NewRecorder()
	mux.ServeHTTP(subscriber, subscribe)
	if subscriber.Code != http.StatusOK {
		t.Fatalf("subscribe status=%d body=%s", subscriber.Code, subscriber.Body.String())
	}

	publish := httptest.NewRequest(http.MethodPost, "/v1/sessions/"+credentials.SessionID+"/publish", bytes.NewBufferString(`{"bus_id":"microphone"}`))
	publish.Header.Set("Authorization", "Bearer "+credentials.SourceToken)
	publisher := httptest.NewRecorder()
	mux.ServeHTTP(publisher, publish)
	if publisher.Code != http.StatusOK {
		t.Fatalf("publish credential status=%d body=%s", publisher.Code, publisher.Body.String())
	}
	var publisherCredentials publishSessionResponse
	if err := json.Unmarshal(publisher.Body.Bytes(), &publisherCredentials); err != nil {
		t.Fatal(err)
	}
	if publisherCredentials.BusID != "microphone" || publisherCredentials.PublisherToken == "" {
		t.Fatalf("unexpected publisher credentials %#v", publisherCredentials)
	}

	// A media-only publisher can reach Relay, but cannot exercise Session-owner
	// authority at the control plane.
	publisherGet := httptest.NewRequest(http.MethodGet, "/v1/sessions/"+credentials.SessionID, nil)
	publisherGet.Header.Set("Authorization", "Bearer "+publisherCredentials.PublisherToken)
	publisherGetResponse := httptest.NewRecorder()
	mux.ServeHTTP(publisherGetResponse, publisherGet)
	if publisherGetResponse.Code != http.StatusNotFound {
		t.Fatalf("publisher token authorized Session read status=%d", publisherGetResponse.Code)
	}
	publisherDelete := httptest.NewRequest(http.MethodDelete, "/v1/sessions/"+credentials.SessionID, nil)
	publisherDelete.Header.Set("Authorization", "Bearer "+publisherCredentials.PublisherToken)
	publisherDeleteResponse := httptest.NewRecorder()
	mux.ServeHTTP(publisherDeleteResponse, publisherDelete)
	if publisherDeleteResponse.Code != http.StatusNotFound {
		t.Fatalf("publisher token authorized Session deletion status=%d", publisherDeleteResponse.Code)
	}
}

func TestGivenInternalRelayStateWhenAuthenticatedThenReplacementIsIdempotent(t *testing.T) {
	store := NewSessionStore(sessionTestSecret)
	created, _ := store.Create()
	mux := testMux(store, HandlerConfig{})
	snapshot := relaySnapshot(created.ID(), "relay-epoch-1", 1, []BusState{
		{BusID: "application", Role: "application", SourceActive: true, SourceGeneration: 1},
		{BusID: "microphone", Role: "microphone", SourceActive: true, SourceGeneration: 1},
	})
	snapshot.WriterTerm, _ = store.AcquireRelayWriter(context.Background(), created.ID(), snapshot.RelayEpoch, 0)
	body, _ := json.Marshal(snapshot)
	path := "/v1/internal/sessions/" + created.ID() + "/relay-state"

	unauthorized := httptest.NewRecorder()
	mux.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPut, path, bytes.NewReader(body)))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("missing internal secret status=%d", unauthorized.Code)
	}

	apply := func() map[string]any {
		request := httptest.NewRequest(http.MethodPut, path, bytes.NewReader(body))
		request.Header.Set("X-PocketStation-Internal-Secret", string(internalTestSecret))
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("state apply status=%d body=%s", response.Code, response.Body.String())
		}
		var decoded map[string]any
		_ = json.Unmarshal(response.Body.Bytes(), &decoded)
		return decoded
	}
	if changed, _ := apply()["changed"].(bool); !changed {
		t.Fatal("first full snapshot did not change state")
	}
	if changed, _ := apply()["changed"].(bool); changed {
		t.Fatal("duplicate full snapshot changed state")
	}
	state, _ := store.Get(created.ID())
	if !state.State().Ready {
		t.Fatal("both required buses did not make Session ready")
	}
}

func TestGivenRelayAuthorityProbeWhenSessionIsActiveThenHeadReturns200WithoutBody(t *testing.T) {
	store := NewSessionStore(sessionTestSecret)
	created, err := store.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	mux := testMux(store, HandlerConfig{})
	request := httptest.NewRequest(http.MethodHead, "/v1/internal/sessions/"+created.ID()+"/authority", nil)
	request.Header.Set("X-PocketStation-Internal-Secret", string(internalTestSecret))
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || recorder.Body.Len() != 0 {
		t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestGivenReplacedMediaWriterWhenOldSnapshotArrivesThenHTTPSignalsFence(t *testing.T) {
	store := NewSessionStore(sessionTestSecret)
	created := readySession(t, store)
	meta, err := store.Metadata(context.Background(), created.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.AcquireRelayWriter(context.Background(), created.ID(), "new-media", meta.MediaTerm); err != nil {
		t.Fatal(err)
	}
	snapshot := relaySnapshot(created.ID(), meta.RelayEpoch, 10, []BusState{{BusID: "application", Role: "application", SourceActive: true, SourceGeneration: 1}})
	snapshot.WriterTerm = meta.MediaTerm
	body, _ := json.Marshal(snapshot)
	request := httptest.NewRequest(http.MethodPut, "/v1/internal/sessions/"+created.ID()+"/relay-state", bytes.NewReader(body))
	request.Header.Set("X-PocketStation-Internal-Secret", string(internalTestSecret))
	response := httptest.NewRecorder()
	testMux(store, HandlerConfig{}).ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("fence status=%d", response.Code)
	}
}

type unavailableViewStore struct {
	storage.Store
	unavailable bool
}

func (s *unavailableViewStore) View(ctx context.Context, fn func(storage.Tx) error) error {
	if s.unavailable {
		return errors.New("injected backend outage")
	}
	return s.Store.View(ctx, fn)
}
func TestGivenValidOwnerWhenAuthorityStorageIsUnavailableThenHTTPReportsTransientFailure(t *testing.T) {
	backend := &unavailableViewStore{Store: memory.New()}
	service, err := NewService(sessionTestSecret, backend, Config{})
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.Create()
	if err != nil {
		t.Fatal(err)
	}
	backend.unavailable = true
	request := httptest.NewRequest(http.MethodGet, "/v1/sessions/"+created.ID(), nil)
	request.Header.Set("Authorization", "Bearer "+created.SourceToken())
	response := httptest.NewRecorder()
	testMux(service, HandlerConfig{}).ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("backend outage status=%d", response.Code)
	}
}
