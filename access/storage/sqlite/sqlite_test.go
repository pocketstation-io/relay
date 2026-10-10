package sqlite

import (
	"context"
	"database/sql"
	"github.com/pocketstation-io/relay/access/storage"
	"path/filepath"
	"testing"
	"time"
)

func TestGivenDurableStoreWhenReopenedThenCommittedRecordsRemainAndFutureSchemaIsRejected(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "access.db")
	s, e := Open(ctx, path)
	if e != nil {
		t.Fatal(e)
	}
	e = s.Update(ctx, func(tx storage.Tx) error {
		return tx.PutSession(storage.SessionRecord{ID: "one", ExpiresAt: time.Now().Add(time.Hour)})
	})
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, e = Open(ctx, path)
	if e != nil {
		t.Fatal(e)
	}
	e = s.View(ctx, func(tx storage.Tx) error {
		_, ok, err := tx.Session("one")
		if !ok {
			t.Fatal("commit lost on reopen")
		}
		return err
	})
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	db, e := sql.Open("sqlite", path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec("PRAGMA user_version=99"); e != nil {
		t.Fatal(e)
	}
	db.Close()
	if future, e := Open(ctx, path); e == nil {
		future.Close()
		t.Fatal("future schema downgraded")
	}
}
