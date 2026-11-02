package access

import (
	"context"
	"github.com/pocketstation-io/relay/access/storage/memory"
	"net/http"
	"testing"
	"time"
)

var sessionTestSecret = []byte("0123456789abcdef0123456789abcdef")

func NewSessionStore(secret []byte) *Service {
	s, e := NewService(secret, memory.New(), Config{})
	if e != nil {
		panic(e)
	}
	return s
}
func relaySnapshot(id, epoch string, revision uint64, buses []BusState) RelayStateSnapshot {
	return RelayStateSnapshot{ContractVersion: 1, SessionID: id, RelayEpoch: epoch, Revision: revision, ObservedAt: time.Now().UTC(), Buses: buses}
}
func readySession(t *testing.T, s *Service) Session {
	t.Helper()
	v, e := s.Create()
	if e != nil {
		t.Fatal(e)
	}
	term, e := s.AcquireRelayWriter(context.Background(), v.ID(), "test-media", 0)
	if e != nil {
		t.Fatal(e)
	}
	snapshot := relaySnapshot(v.ID(), "test-media", 1, []BusState{{BusID: "application", Role: "application", SourceActive: true, SourceGeneration: 1}, {BusID: "microphone", Role: "microphone", SourceActive: true, SourceGeneration: 1}})
	snapshot.WriterTerm = term
	if _, _, e = s.ApplyRelayStateContext(context.Background(), v.ID(), snapshot); e != nil {
		t.Fatal(e)
	}
	return v
}
func readyInvitationTestServer(t *testing.T) (*Service, Session, *http.ServeMux) {
	s := NewSessionStore(sessionTestSecret)
	v := readySession(t, s)
	return s, v, testMux(s, HandlerConfig{RelaySignalURL: "ws://127.0.0.1:4000/ws", PublicReceiverURL: "http://127.0.0.1:3000"})
}
func (s *Service) ResolveInvitation(code string) (ResolvedInvitation, error) {
	return s.ResolveInvitationContext(context.Background(), code, "")
}
