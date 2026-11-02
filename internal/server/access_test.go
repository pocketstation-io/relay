package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pocketstation-io/relay/access"
	"github.com/pocketstation-io/relay/access/storage/memory"
	"github.com/pocketstation-io/relay/internal/notifications/callback"
)

func testAccessService(t *testing.T, secret []byte, config access.Config) *access.Service {
	t.Helper()
	backend := memory.New()
	service, err := access.NewService(secret, backend, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	return service
}

func cleanupTestRelay(t *testing.T, server *Server) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
}

func TestGivenCachedMediaPlacementsWhenSessionEndsOrWriterMovesThenOnlyEndedEntryIsPruned(t *testing.T) {
	domain := testAccessService(t, joinTestSecret, access.Config{})
	ended, err := domain.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	live, err := domain.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	relay := New(Config{JWTSecret: joinTestSecret, AccessService: domain})
	cleanupTestRelay(t, relay)
	ctx := context.Background()
	for _, id := range []string{ended.ID(), live.ID()} {
		if err = relay.requireActiveControlSession(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = domain.DeleteContext(ctx, ended.ID()); err != nil {
		t.Fatal(err)
	}
	relay.pruneMediaWriters(ctx)
	relay.mediaWriterMu.Lock()
	_, endedPresent := relay.mediaWriters[ended.ID()]
	liveWriter, livePresent := relay.mediaWriters[live.ID()]
	relay.mediaWriterMu.Unlock()
	if endedPresent || !livePresent {
		t.Fatal("pruning did not distinguish ended and live Session")
	}
	if _, err = domain.AcquireRelayWriter(ctx, live.ID(), "replacement-media", liveWriter.term); err != nil {
		t.Fatal(err)
	}
	relay.pruneMediaWriters(ctx)
	if err = relay.requireActiveControlSession(ctx, live.ID()); !errors.Is(err, callback.ErrWriterFenced) {
		t.Fatalf("displaced writer readmitted: %v", err)
	}
	relay.mediaWriterMu.Lock()
	_, retained := relay.mediaWriters[live.ID()]
	relay.mediaWriterMu.Unlock()
	if !retained {
		t.Fatal("pruning forgot live placement fence")
	}
}

func testManagedAccess(t *testing.T, service *access.Service) *callback.Client {
	t.Helper()
	mux := http.NewServeMux()
	service.RegisterInternal(mux, []byte(controlStateSyncTestSecret))
	endpoint := httptest.NewServer(mux)
	t.Cleanup(endpoint.Close)
	client, err := callback.NewClient(endpoint.URL, controlStateSyncTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func testReadyAccess(t *testing.T, service *access.Service, id string, buses ...string) {
	t.Helper()
	metadata, err := service.Metadata(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	epoch := metadata.RelayEpoch
	if epoch == "" {
		epoch = "component-readiness"
	}
	term, err := service.AcquireRelayWriter(context.Background(), id, epoch, metadata.MediaTerm)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := access.RelayStateSnapshot{ContractVersion: 1, SessionID: id, RelayEpoch: epoch, WriterTerm: term, Revision: 1, ObservedAt: time.Now()}
	for _, bus := range buses {
		snapshot.Buses = append(snapshot.Buses, access.BusState{BusID: bus, Role: bus, SourceActive: true, SourceGeneration: 1})
	}
	if _, _, err = service.ApplyRelayStateContext(context.Background(), id, snapshot); err != nil {
		t.Fatal(err)
	}
}
