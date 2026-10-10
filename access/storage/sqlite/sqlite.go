// Package sqlite retains Relay access records on a local filesystem. WAL and
// FULL synchronization require a filesystem with SQLite's locking guarantees;
// this adapter is not a shared-file, multi-host failover service.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/pocketstation-io/relay/access/storage"
	_ "modernc.org/sqlite"
)

type Store struct{ database *sql.DB }

func Open(ctx context.Context, path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("SQLite access storage requires a file path")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(absolute, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	info, statErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil {
		return nil, statErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("SQLite access file must be readable only by its owner")
	}
	u := url.URL{Scheme: "file", Path: absolute}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "foreign_keys(ON)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var version int
	if err = db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err == nil && version > storage.SchemaVersion {
		err = errors.New("unsupported access storage schema")
	}
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err = db.ExecContext(ctx, "BEGIN IMMEDIATE;"+schema+"COMMIT;"); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{database: db}, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS access_namespace (id INTEGER PRIMARY KEY CHECK(id=1), payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS access_sessions (id TEXT PRIMARY KEY, expires_at INTEGER NOT NULL, payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS access_grants (id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES access_sessions(id) ON DELETE CASCADE, expires_at INTEGER NOT NULL, payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS access_locators (locator TEXT PRIMARY KEY, grant_id TEXT NOT NULL REFERENCES access_grants(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS access_receipts (id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES access_sessions(id) ON DELETE CASCADE, expires_at INTEGER NOT NULL, payload BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS access_grant_session ON access_grants(session_id);
CREATE INDEX IF NOT EXISTS access_receipt_session ON access_receipts(session_id);
PRAGMA user_version=1;`

type transactionMode uint8

const (
	readTransaction transactionMode = iota
	writeTransaction
)

func (s *Store) Update(ctx context.Context, fn func(storage.Tx) error) error {
	return s.run(ctx, fn, writeTransaction)
}
func (s *Store) View(ctx context.Context, fn func(storage.Tx) error) error {
	return s.run(ctx, fn, readTransaction)
}
func (s *Store) Close() error { return s.database.Close() }
func (s *Store) run(ctx context.Context, fn func(storage.Tx) error, mode transactionMode) error {
	conn, err := s.database.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	begin := "BEGIN"
	if mode == writeTransaction {
		begin = "BEGIN IMMEDIATE"
	}
	if _, err = conn.ExecContext(ctx, begin); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = conn.ExecContext(cleanup, "ROLLBACK")
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	t := &transaction{ctx: ctx, conn: conn, mode: mode, now: time.Now().UTC()}
	if err = fn(t); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	return err
}

type transaction struct {
	ctx  context.Context
	conn *sql.Conn
	mode transactionMode
	now  time.Time
}

func (t *transaction) Now() time.Time { return t.now }
func (t *transaction) write() error {
	if err := t.ctx.Err(); err != nil {
		return err
	}
	if t.mode != writeTransaction {
		return storage.ErrReadOnly
	}
	return nil
}
func (t *transaction) load(query string, target any, args ...any) (bool, error) {
	var data []byte
	err := t.conn.QueryRowContext(t.ctx, query, args...).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(data) > storage.MaxRecordBytes {
		return false, storage.ErrCapacity
	}
	return true, json.Unmarshal(data, target)
}
func (t *transaction) exec(query string, args ...any) error {
	if err := t.write(); err != nil {
		return err
	}
	_, err := t.conn.ExecContext(t.ctx, query, args...)
	return err
}
func (t *transaction) Session(id string) (storage.SessionRecord, bool, error) {
	var r storage.SessionRecord
	ok, err := t.load("SELECT payload FROM access_sessions WHERE id=?", &r, id)
	return r, ok, err
}
func (t *transaction) Sessions(limit int) ([]storage.SessionRecord, error) {
	if limit < 0 {
		return nil, storage.ErrCapacity
	}
	rows, err := t.conn.QueryContext(t.ctx, "SELECT payload FROM access_sessions ORDER BY id LIMIT ?", limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]storage.SessionRecord, 0)
	for rows.Next() {
		if len(result) >= limit {
			return nil, storage.ErrCapacity
		}
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		if len(data) > storage.MaxRecordBytes {
			return nil, storage.ErrCapacity
		}
		var r storage.SessionRecord
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
func (t *transaction) PutSession(r storage.SessionRecord) error {
	data, err := encode(r)
	if err != nil {
		return err
	}
	return t.exec("INSERT INTO access_sessions(id,expires_at,payload) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET expires_at=excluded.expires_at,payload=excluded.payload", r.ID, r.ExpiresAt.UnixNano(), data)
}
func (t *transaction) DeleteSession(id string) error {
	return t.exec("DELETE FROM access_sessions WHERE id=?", id)
}
func (t *transaction) Grant(locator string) (storage.GrantRecord, bool, error) {
	var r storage.GrantRecord
	ok, err := t.load("SELECT g.payload FROM access_grants g JOIN access_locators l ON l.grant_id=g.id WHERE l.locator=?", &r, locator)
	return r, ok, err
}
func (t *transaction) PutGrant(r storage.GrantRecord) error {
	if err := t.write(); err != nil {
		return err
	}
	if r.ID == "" || r.JoinCode == "" || r.Alias == "" || r.JoinCode == r.Alias {
		return storage.ErrConflict
	}
	if _, ok, err := t.Session(r.SessionID); err != nil {
		return err
	} else if !ok {
		return storage.ErrConflict
	}
	for _, locator := range []string{r.JoinCode, r.Alias} {
		_, exists, err := t.Grant(locator)
		if err != nil {
			return err
		}
		if exists {
			return storage.ErrConflict
		}
	}
	var existing storage.GrantRecord
	exists, err := t.load("SELECT payload FROM access_grants WHERE id=?", &existing, r.ID)
	if err != nil {
		return err
	}
	if exists {
		return storage.ErrConflict
	}
	data, err := encode(r)
	if err != nil {
		return err
	}
	if err = t.exec("INSERT INTO access_grants(id,session_id,expires_at,payload) VALUES(?,?,?,?)", r.ID, r.SessionID, r.ExpiresAt.UnixNano(), data); err != nil {
		return err
	}
	return t.exec("INSERT INTO access_locators(locator,grant_id) VALUES(?,?),(?,?)", r.JoinCode, r.ID, r.Alias, r.ID)
}
func (t *transaction) DeleteGrant(id string) error {
	return t.exec("DELETE FROM access_grants WHERE id=?", id)
}
func (t *transaction) Receipt(key string) (storage.Receipt, bool, error) {
	var r storage.Receipt
	ok, err := t.load("SELECT payload FROM access_receipts WHERE id=?", &r, key)
	return r, ok, err
}
func (t *transaction) PutReceipt(r storage.Receipt) error {
	if err := t.write(); err != nil {
		return err
	}
	if _, ok, err := t.Session(r.SessionID); err != nil {
		return err
	} else if !ok {
		return storage.ErrConflict
	}
	_, exists, err := t.Receipt(r.Key)
	if err != nil {
		return err
	}
	if exists {
		return storage.ErrConflict
	}
	data, err := encode(r)
	if err != nil {
		return err
	}
	return t.exec("INSERT INTO access_receipts(id,session_id,expires_at,payload) VALUES(?,?,?,?)", r.Key, r.SessionID, r.ExpiresAt.UnixNano(), data)
}
func (t *transaction) Counts() (storage.Counts, error) {
	var r storage.Counts
	err := t.conn.QueryRowContext(t.ctx, "SELECT (SELECT count(*) FROM access_sessions),(SELECT count(*) FROM access_grants),(SELECT count(*) FROM access_receipts)").Scan(&r.SessionCount, &r.GrantCount, &r.ReceiptCount)
	return r, err
}
func (t *transaction) PruneExpired() error {
	for _, table := range []string{"access_sessions", "access_grants", "access_receipts"} {
		if err := t.exec("DELETE FROM "+table+" WHERE expires_at<=?", t.now.UnixNano()); err != nil {
			return err
		}
	}
	return nil
}
func (t *transaction) Namespace() (storage.Namespace, error) {
	var r storage.Namespace
	_, err := t.load("SELECT payload FROM access_namespace WHERE id=1", &r)
	return r, err
}
func (t *transaction) PutNamespace(r storage.Namespace) error {
	data, err := encode(r)
	if err != nil {
		return err
	}
	return t.exec("INSERT INTO access_namespace(id,payload) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", data)
}
func encode(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(data) > storage.MaxRecordBytes {
		return nil, fmt.Errorf("%w: record exceeds 262144 bytes", storage.ErrCapacity)
	}
	return data, nil
}
