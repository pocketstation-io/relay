package storage_test

import (
	"context"
	"errors"
	"github.com/pocketstation-io/relay/access/storage"
	"github.com/pocketstation-io/relay/access/storage/memory"
	"github.com/pocketstation-io/relay/access/storage/sqlite"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGivenPublicAdaptersWhenTransactionsFailThenAtomicContractMatches(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var store storage.Store = memory.New()
			if kind == "sqlite" {
				var err error
				store, err = sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "access.db"))
				if err != nil {
					t.Fatal(err)
				}
			}
			defer store.Close()
			ctx := context.Background()
			session := storage.SessionRecord{ID: "session", ExpiresAt: time.Now().Add(time.Hour)}
			if err := store.Update(ctx, func(tx storage.Tx) error { return tx.PutSession(session) }); err != nil {
				t.Fatal(err)
			}
			rollback := errors.New("rollback")
			if err := store.Update(ctx, func(tx storage.Tx) error {
				if err := tx.DeleteSession(session.ID); err != nil {
					return err
				}
				return rollback
			}); !errors.Is(err, rollback) {
				t.Fatal(err)
			}
			if err := store.View(ctx, func(tx storage.Tx) error {
				_, ok, e := tx.Session(session.ID)
				if e != nil || !ok {
					t.Fatal("rollback lost Session")
				}
				return tx.DeleteSession(session.ID)
			}); !errors.Is(err, storage.ErrReadOnly) {
				t.Fatal(err)
			}
			grant := storage.GrantRecord{ID: "grant", SessionID: "missing", JoinCode: "code", Alias: "calm-fern", ExpiresAt: session.ExpiresAt}
			if err := store.Update(ctx, func(tx storage.Tx) error {
				if e := tx.PutGrant(grant); !errors.Is(e, storage.ErrConflict) {
					t.Fatalf("missing parent: %v", e)
				}
				grant.SessionID = session.ID
				grant.Alias = grant.JoinCode
				if e := tx.PutGrant(grant); !errors.Is(e, storage.ErrConflict) {
					t.Fatalf("equal locators: %v", e)
				}
				grant.Alias = "calm-fern"
				return tx.PutGrant(grant)
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.Update(ctx, func(tx storage.Tx) error {
				other := grant
				other.ID = "other"
				if e := tx.PutGrant(other); !errors.Is(e, storage.ErrConflict) {
					t.Fatalf("duplicate: %v", e)
				}
				return tx.PutReceipt(storage.Receipt{Key: "receipt", SessionID: session.ID, Response: []byte("result"), ExpiresAt: session.ExpiresAt})
			}); err != nil {
				t.Fatal(err)
			}
			huge := session
			huge.Codec = strings.Repeat("x", storage.MaxRecordBytes)
			if err := store.Update(ctx, func(tx storage.Tx) error { return tx.PutSession(huge) }); !errors.Is(err, storage.ErrCapacity) {
				t.Fatal(err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			called := false
			if err := store.Update(canceled, func(storage.Tx) error { called = true; return nil }); !errors.Is(err, context.Canceled) || called {
				t.Fatal("canceled callback ran")
			}
			if err := store.Update(ctx, func(tx storage.Tx) error { return tx.DeleteSession(session.ID) }); err != nil {
				t.Fatal(err)
			}
			if err := store.View(ctx, func(tx storage.Tx) error {
				counts, e := tx.Counts()
				if counts != (storage.Counts{}) {
					t.Fatalf("cascade %+v", counts)
				}
				return e
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGivenAdapterCallbackWhenItPanicsThenNoPartialMutationCommits(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			var s storage.Store = memory.New()
			if kind == "sqlite" {
				var err error
				s, err = sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "panic.db"))
				if err != nil {
					t.Fatal(err)
				}
			}
			defer s.Close()
			func() {
				defer func() {
					if recover() == nil {
						t.Error("panic swallowed")
					}
				}()
				_ = s.Update(context.Background(), func(tx storage.Tx) error {
					if err := tx.PutSession(storage.SessionRecord{ID: "aborted", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
						t.Fatal(err)
					}
					panic("deliberate fixture")
				})
			}()
			if err := s.View(context.Background(), func(tx storage.Tx) error {
				_, ok, e := tx.Session("aborted")
				if ok {
					t.Error("panic committed")
				}
				return e
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
