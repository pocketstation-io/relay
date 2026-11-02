package access

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/pocketstation-io/relay/access/storage"
	"github.com/pocketstation-io/relay/access/storage/memory"
	"github.com/pocketstation-io/relay/access/storage/sqlite"
	"github.com/pocketstation-io/relay/access/token"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func encodeResolved(r ResolvedInvitation) ([]byte, error) { return json.Marshal(r) }
func TestGivenCommittedRedemptionWhenRetriedThenOriginalCredentialAndRequestAreRequired(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var backend storage.Store = memory.New()
			if kind == "sqlite" {
				var e error
				backend, e = sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "access.db"))
				if e != nil {
					t.Fatal(e)
				}
			}
			defer backend.Close()
			s, e := NewService(sessionTestSecret, backend, Config{MaxReceipts: 1})
			if e != nil {
				t.Fatal(e)
			}
			v := readySession(t, s)
			ctx := context.Background()
			grant, e := s.CreateInvitation(v.ID(), "application")
			if e != nil {
				t.Fatal(e)
			}
			const key = "550e8400-e29b-41d4-a716-446655440000"
			first, e := s.redeem(ctx, grant.Alias, grant.Code, key, encodeResolved)
			if e != nil {
				t.Fatal(e)
			}
			retried, e := s.redeem(ctx, grant.Alias, grant.Code, key, encodeResolved)
			if e != nil || !bytes.Equal(first, retried) {
				t.Fatal("retry changed committed result")
			}
			for _, attempt := range [][3]string{{grant.Alias, v.ID(), key}, {grant.Code, grant.Code, key}, {grant.Alias, "", key}, {grant.Alias, grant.Code, "different-operation-key"}} {
				if _, e = s.redeem(ctx, attempt[0], attempt[1], attempt[2], encodeResolved); e == nil {
					t.Fatal("unauthorized retry accepted")
				}
			}
			second, e := s.CreateInvitation(v.ID(), "microphone")
			if e != nil {
				t.Fatal(e)
			}
			if _, e = s.redeem(ctx, second.Alias, second.Code, "second-operation-key", encodeResolved); !errors.Is(e, ErrReceiptCapacity) {
				t.Fatalf("receipt capacity: %v", e)
			}
			if _, e = s.InspectInvitationContext(ctx, second.Alias); e != nil {
				t.Fatal("capacity failure consumed grant")
			}
			// Reopening the domain retains a committed response but never restores live readiness.
			reopened, e := NewService(sessionTestSecret, backend, Config{MaxReceipts: 1})
			if e != nil {
				t.Fatal(e)
			}
			if current, e := reopened.GetContext(ctx, v.ID()); e != nil || current.State().Ready {
				t.Fatal("restart restored live readiness")
			}
			retried, e = reopened.redeem(ctx, grant.Alias, grant.Code, key, encodeResolved)
			if e != nil || !bytes.Equal(first, retried) {
				t.Fatal("restart lost committed receipt")
			}
			if _, e = reopened.DeleteContext(ctx, v.ID()); e != nil {
				t.Fatal(e)
			}
			if _, e = reopened.redeem(ctx, grant.Alias, grant.Code, key, encodeResolved); e == nil {
				t.Fatal("deleted Session retained retry authority")
			}
		})
	}
}
func TestGivenOneUseGrantWhenConcurrentConsumersRedeemThenExactlyOneCommits(t *testing.T) {
	s := NewSessionStore(sessionTestSecret)
	v := readySession(t, s)
	g, e := s.CreateInvitation(v.ID(), "application")
	if e != nil {
		t.Fatal(e)
	}
	var winners atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := s.ResolveInvitationContext(context.Background(), g.Alias, g.Code); e == nil {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("winners %d", winners.Load())
	}
}
func TestGivenFencedWritersWhenOldObservationsReturnThenReadinessCannotResurrect(t *testing.T) {
	s := NewSessionStore(sessionTestSecret)
	v := readySession(t, s)
	ctx := context.Background()
	first, e := s.Metadata(ctx, v.ID())
	if e != nil {
		t.Fatal(e)
	}
	term, e := s.AcquireRelayWriter(ctx, v.ID(), "replacement-media", first.MediaTerm)
	if e != nil {
		t.Fatal(e)
	}
	old := relaySnapshot(v.ID(), "test-media", 99, []BusState{{BusID: "application", Role: "application", SourceActive: true, SourceGeneration: 1}})
	old.WriterTerm = first.MediaTerm
	if _, _, e = s.ApplyRelayStateContext(ctx, v.ID(), old); !errors.Is(e, ErrWriterFenced) {
		t.Fatal("old media writer restored state")
	}
	old.RelayEpoch = "replacement-media"
	old.WriterTerm = term
	old.ObservedAt = time.Now().Add(365 * 24 * time.Hour)
	if _, _, e = s.ApplyRelayStateContext(ctx, v.ID(), old); e != nil {
		t.Fatal(e)
	}
	var observed time.Time
	s.storage.View(ctx, func(tx storage.Tx) error { r, _, e := tx.Session(v.ID()); observed = r.Relay.ObservedAt; return e })
	if observed.After(time.Now().Add(time.Second)) {
		t.Fatal("caller clock pinned presence in future")
	}
}
func TestGivenWriterLeaseWhenAnotherGroupTakesOverThenOldReadinessAndWritesFail(t *testing.T) {
	var elapsed atomic.Int64
	base := time.Now()
	backend := memory.NewWithClock(func() time.Time { return base.Add(time.Duration(elapsed.Load())) })
	s, e := NewService(sessionTestSecret, backend, Config{WriterID: "one", WriterLeaseDuration: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = NewService(sessionTestSecret, backend, Config{WriterID: "two"}); !errors.Is(e, ErrWriterFenced) {
		t.Fatal("live owner stolen")
	}
	elapsed.Store(int64(2 * time.Second))
	replacement, e := NewService(sessionTestSecret, backend, Config{WriterID: "two"})
	if e != nil {
		t.Fatal(e)
	}
	if e = replacement.Ready(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = s.Ready(context.Background()); !errors.Is(e, ErrWriterFenced) {
		t.Fatal("old owner ready")
	}
	if _, e = s.Create(); !errors.Is(e, ErrWriterFenced) {
		t.Fatal("old owner wrote")
	}
}
func TestGivenOwnerRenewalWhenCapabilityExpiresThenItCannotReviveAuthority(t *testing.T) {
	var elapsed atomic.Int64
	base := time.Now().UTC().Truncate(time.Second)
	backend := memory.NewWithClock(func() time.Time { return base.Add(time.Duration(elapsed.Load())) })
	s, e := NewService(sessionTestSecret, backend, Config{OwnerTokenTTL: time.Minute})
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.Create("application")
	if e != nil {
		t.Fatal(e)
	}
	elapsed.Store(int64(30 * time.Second))
	renewed, expiry, e := s.RenewOwner(context.Background(), v.ID(), v.SourceToken())
	if e != nil || !expiry.Equal(base.Add(90*time.Second)) {
		t.Fatal("renew failed")
	}
	claims, e := token.VerifyProfile(sessionTestSecret, renewed, token.Issuer, base.Add(30*time.Second))
	if e != nil || !claims.CanControl() || claims.SessionID != v.ID() {
		t.Fatal("renew changed scope")
	}
	elapsed.Store(int64(100 * time.Second))
	if _, _, e = s.RenewOwner(context.Background(), v.ID(), renewed); e == nil {
		t.Fatal("expired owner revived")
	}
}
func TestGivenReceiptExpiryWhenRetryArrivesThenConsumedGrantStaysConsumed(t *testing.T) {
	var elapsed atomic.Int64
	base := time.Now()
	backend := memory.NewWithClock(func() time.Time { return base.Add(time.Duration(elapsed.Load())) })
	s, e := NewService(sessionTestSecret, backend, Config{ReceiptTTL: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	v := readySession(t, s)
	g, e := s.CreateInvitation(v.ID(), "application")
	if e != nil {
		t.Fatal(e)
	}
	const key = "receipt-expiry-test"
	if _, e = s.redeem(context.Background(), g.Alias, g.Code, key, encodeResolved); e != nil {
		t.Fatal(e)
	}
	elapsed.Store(int64(2 * time.Second))
	if _, e = s.redeem(context.Background(), g.Alias, g.Code, key, encodeResolved); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("expired receipt: %v", e)
	}
}

func TestGivenStableMediaHeartbeatWhenPresenceExpiresThenEqualRevisionRestoresOnlyCurrentWriter(t *testing.T) {
	var elapsed atomic.Int64
	base := time.Now()
	backend := memory.NewWithClock(func() time.Time { return base.Add(time.Duration(elapsed.Load())) })
	s, e := NewService(sessionTestSecret, backend, Config{ObservationTTL: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	v := readySession(t, s)
	ctx := context.Background()
	meta, e := s.Metadata(ctx, v.ID())
	if e != nil {
		t.Fatal(e)
	}
	snapshot := relaySnapshot(v.ID(), meta.RelayEpoch, 1, []BusState{{BusID: "application", Role: "application", SourceActive: true, SourceGeneration: 1}, {BusID: "microphone", Role: "microphone", SourceActive: true, SourceGeneration: 1}})
	snapshot.WriterTerm = meta.MediaTerm
	elapsed.Store(int64(800 * time.Millisecond))
	_, changed, e := s.ApplyRelayStateContext(ctx, v.ID(), snapshot)
	if e != nil || changed {
		t.Fatalf("heartbeat false change: %v %v", changed, e)
	}
	elapsed.Store(int64(1500 * time.Millisecond))
	current, e := s.GetContext(ctx, v.ID())
	if e != nil || !current.State().Ready {
		t.Fatal("stable heartbeat failed to refresh")
	}
	elapsed.Store(int64(3 * time.Second))
	current, e = s.GetContext(ctx, v.ID())
	if e != nil || current.State().Ready {
		t.Fatal("observation did not expire")
	}
	revision := current.State().StateRevision
	restored, changed, e := s.ApplyRelayStateContext(ctx, v.ID(), snapshot)
	if e != nil || !changed || !restored.Ready || restored.StateRevision <= revision {
		t.Fatalf("equal revision did not restore current live writer: %+v %v", restored, e)
	}
}
func TestGivenExplicitBackupResetWhenOldCredentialsReturnThenTheyCannotRestoreAuthority(t *testing.T) {
	backend := memory.New()
	s, e := NewService(sessionTestSecret, backend, Config{})
	if e != nil {
		t.Fatal(e)
	}
	v := readySession(t, s)
	g, e := s.CreateInvitation(v.ID(), "application")
	if e != nil {
		t.Fatal(e)
	}
	if e = ResetRestoredStore(context.Background(), backend, 1000); e != nil {
		t.Fatal(e)
	}
	replacement, e := NewService(sessionTestSecret, backend, Config{})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = replacement.Authorize(context.Background(), v.ID(), v.SourceToken()); e == nil {
		t.Fatal("old owner survived reset")
	}
	if _, e = replacement.ResolveInvitationContext(context.Background(), g.Alias, g.Code); e == nil {
		t.Fatal("old grant survived reset")
	}
	if e = s.Ready(context.Background()); !errors.Is(e, ErrWriterFenced) {
		t.Fatal("old writer survived reset")
	}
}

type failingCommitStore struct {
	storage.Store
	fail bool
}

func (s *failingCommitStore) Update(ctx context.Context, fn func(storage.Tx) error) error {
	return s.Store.Update(ctx, func(tx storage.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		if s.fail {
			return errors.New("injected commit rejection")
		}
		return nil
	})
}
func TestGivenCommitFailureWhenGrantRedeemsThenNoCredentialEscapesAndGrantIsRetained(t *testing.T) {
	backend := &failingCommitStore{Store: memory.New()}
	s, e := NewService(sessionTestSecret, backend, Config{})
	if e != nil {
		t.Fatal(e)
	}
	v := readySession(t, s)
	g, e := s.CreateInvitation(v.ID(), "application")
	if e != nil {
		t.Fatal(e)
	}
	backend.fail = true
	response, e := s.redeem(context.Background(), g.Alias, g.Code, "commit-failure-operation", encodeResolved)
	if e == nil || len(response) != 0 {
		t.Fatal("credential exposed before commit")
	}
	backend.fail = false
	if _, e = s.InspectInvitationContext(context.Background(), g.Alias); e != nil {
		t.Fatal("failed commit consumed grant")
	}
	if _, e = s.redeem(context.Background(), g.Alias, g.Code, "commit-failure-operation", encodeResolved); e != nil {
		t.Fatal(e)
	}
}

type conflictStore struct {
	storage.Store
	calls   int
	collide bool
}
type conflictTx struct {
	storage.Tx
	owner *conflictStore
}

func (s *conflictStore) Update(ctx context.Context, fn func(storage.Tx) error) error {
	return s.Store.Update(ctx, func(tx storage.Tx) error { return fn(&conflictTx{Tx: tx, owner: s}) })
}
func (tx *conflictTx) PutGrant(r storage.GrantRecord) error {
	tx.owner.calls++
	if tx.owner.collide {
		return storage.ErrConflict
	}
	return tx.Tx.PutGrant(r)
}
func TestGivenNameCollisionsWhenRetryBudgetEndsThenAllocationFailsWithoutOverwriting(t *testing.T) {
	backend := &conflictStore{Store: memory.New()}
	s, e := NewService(sessionTestSecret, backend, Config{})
	if e != nil {
		t.Fatal(e)
	}
	v := readySession(t, s)
	first, e := s.CreateInvitation(v.ID(), "application")
	if e != nil {
		t.Fatal(e)
	}
	backend.calls = 0
	backend.collide = true
	if _, e = s.CreateInvitation(v.ID(), "microphone"); !errors.Is(e, ErrNameAllocationUnavailable) || backend.calls != 32 {
		t.Fatalf("unbounded collision handling: calls=%d err=%v", backend.calls, e)
	}
	if _, e = s.ResolveInvitationContext(context.Background(), first.Alias, first.Code); e != nil {
		t.Fatal("collision overwrote existing grant")
	}
}
