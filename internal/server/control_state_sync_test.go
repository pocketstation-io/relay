package server

import (
	"context"
	"github.com/pocketstation-io/relay/access"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/pocketstation-io/relay/internal/notifications/callback"
)

const controlStateSyncTestSecret = "0123456789abcdef0123456789abcdef"

func TestGivenDeletedControlPlaneSessionWhenStateIsPushedThenRelaySessionIsRemoved(t *testing.T) {
	domain := testAccessService(t, joinTestSecret, access.Config{})
	created, err := domain.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	domain.RegisterInternal(mux, []byte(controlStateSyncTestSecret))
	var deleted atomic.Bool
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if deleted.Load() {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	defer controlPlane.Close()

	client, err := callback.NewClient(controlPlane.URL, controlStateSyncTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	server := New(Config{JWTSecret: joinTestSecret, AuthorityMode: "control-plane", CallbackClient: client})
	cleanupTestRelay(t, server)
	server.controlSyncContext = context.Background()
	if err := server.requireActiveControlSession(context.Background(), created.ID()); err != nil {
		t.Fatal(err)
	}
	relaySession := server.relaySessions.GetOrCreate(created.ID())
	server.roomCodecHintState(created.ID())
	server.roomICERestartState(created.ID())
	deleted.Store(true)

	server.pushControlState(relaySession)

	if _, found := server.codecHintStates.Load(created.ID()); found {
		t.Fatal("deleted Session retains codec feedback")
	}
	if _, found := server.iceRestartStates.Load(created.ID()); found {
		t.Fatal("deleted Session retains ICE feedback")
	}
	if count := server.relaySessions.RoomCount(); count != 0 {
		t.Fatalf("RelaySession count = %d, want 0", count)
	}
}

func TestGivenLateFeedbackAfterSessionDeletionWhenSweptThenLiveSessionFeedbackIsPreserved(t *testing.T) {
	server := New(Config{JWTSecret: joinTestSecret})
	cleanupTestRelay(t, server)
	server.relaySessions.GetOrCreate("live-session")
	live := server.roomCodecHintState("live-session")
	server.roomICERestartState("live-session")
	for i := 0; i < 100; i++ {
		server.roomCodecHintState("deleted-session")
		server.roomICERestartState("deleted-session")
		server.pruneSessionFeedback()
		if _, found := server.codecHintStates.Load("deleted-session"); found {
			t.Fatal("orphan codec state")
		}
		if _, found := server.iceRestartStates.Load("deleted-session"); found {
			t.Fatal("orphan ICE state")
		}
	}
	if current, found := server.codecHintStates.Load("live-session"); !found || current != live {
		t.Fatal("live codec state changed")
	}
	if _, found := server.iceRestartStates.Load("live-session"); !found {
		t.Fatal("live ICE state removed")
	}
}
