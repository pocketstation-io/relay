// Package memory provides process-local transactional access storage.
package memory

import (
	"context"
	"encoding/json"
	"maps"
	"sort"
	"time"

	"github.com/pocketstation-io/relay/access/storage"
)

type records struct {
	sessions  map[string]storage.SessionRecord
	grants    map[string]storage.GrantRecord
	locators  map[string]string
	receipts  map[string]storage.Receipt
	namespace storage.Namespace
}

type Store struct {
	gate   chan struct{}
	data   records
	clock  func() time.Time
	closed bool
}

func New() *Store { return NewWithClock(time.Now) }

// NewWithClock supports deterministic expiry qualification. Production uses New.
func NewWithClock(clock func() time.Time) *Store {
	if clock == nil {
		clock = time.Now
	}
	s := &Store{gate: make(chan struct{}, 1), clock: clock, data: records{
		sessions: make(map[string]storage.SessionRecord), grants: make(map[string]storage.GrantRecord),
		locators: make(map[string]string), receipts: make(map[string]storage.Receipt),
	}}
	s.gate <- struct{}{}
	return s
}

func (s *Store) Update(ctx context.Context, fn func(storage.Tx) error) error {
	return s.run(ctx, fn, writeTransaction)
}
func (s *Store) View(ctx context.Context, fn func(storage.Tx) error) error {
	return s.run(ctx, fn, readTransaction)
}

type transactionMode uint8

const (
	readTransaction transactionMode = iota
	writeTransaction
)

func (s *Store) run(ctx context.Context, fn func(storage.Tx) error, mode transactionMode) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.gate:
	}
	defer func() { s.gate <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return storage.ErrClosed
	}
	data := s.data
	if mode == writeTransaction {
		data.sessions = maps.Clone(data.sessions)
		data.grants = maps.Clone(data.grants)
		data.locators = maps.Clone(data.locators)
		data.receipts = maps.Clone(data.receipts)
	}
	tx := &transaction{ctx: ctx, data: &data, mode: mode, now: s.clock().UTC()}
	if err := fn(tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if mode == writeTransaction {
		s.data = data
	}
	return nil
}

func (s *Store) Close() error {
	<-s.gate
	defer func() { s.gate <- struct{}{} }()
	s.closed = true
	return nil
}

type transaction struct {
	ctx  context.Context
	data *records
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
func (t *transaction) Session(id string) (storage.SessionRecord, bool, error) {
	r, ok := t.data.sessions[id]
	return cloneSession(r), ok, t.ctx.Err()
}
func (t *transaction) Sessions(limit int) ([]storage.SessionRecord, error) {
	if limit < 0 {
		return nil, storage.ErrCapacity
	}
	keys := make([]string, 0, len(t.data.sessions))
	for k := range t.data.sessions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > limit {
		return nil, storage.ErrCapacity
	}
	result := make([]storage.SessionRecord, 0, len(keys))
	for _, k := range keys {
		result = append(result, cloneSession(t.data.sessions[k]))
	}
	return result, t.ctx.Err()
}
func (t *transaction) PutSession(r storage.SessionRecord) error {
	if err := bounded(r); err != nil {
		return err
	}
	if err := t.write(); err != nil {
		return err
	}
	t.data.sessions[r.ID] = cloneSession(r)
	return nil
}
func (t *transaction) DeleteSession(id string) error {
	if err := t.write(); err != nil {
		return err
	}
	delete(t.data.sessions, id)
	for k, r := range t.data.grants {
		if r.SessionID == id {
			_ = t.DeleteGrant(k)
		}
	}
	for k, r := range t.data.receipts {
		if r.SessionID == id {
			delete(t.data.receipts, k)
		}
	}
	return nil
}
func (t *transaction) Grant(locator string) (storage.GrantRecord, bool, error) {
	id, ok := t.data.locators[locator]
	if !ok {
		return storage.GrantRecord{}, false, t.ctx.Err()
	}
	r, ok := t.data.grants[id]
	return r, ok, t.ctx.Err()
}
func (t *transaction) PutGrant(r storage.GrantRecord) error {
	if err := bounded(r); err != nil {
		return err
	}
	if err := t.write(); err != nil {
		return err
	}
	if r.ID == "" || r.JoinCode == "" || r.Alias == "" || r.JoinCode == r.Alias {
		return storage.ErrConflict
	}
	if _, ok := t.data.sessions[r.SessionID]; !ok {
		return storage.ErrConflict
	}
	if _, ok := t.data.grants[r.ID]; ok {
		return storage.ErrConflict
	}
	if _, ok := t.data.locators[r.JoinCode]; ok {
		return storage.ErrConflict
	}
	if _, ok := t.data.locators[r.Alias]; ok {
		return storage.ErrConflict
	}
	t.data.grants[r.ID] = r
	t.data.locators[r.JoinCode] = r.ID
	t.data.locators[r.Alias] = r.ID
	return nil
}
func (t *transaction) DeleteGrant(id string) error {
	if err := t.write(); err != nil {
		return err
	}
	if r, ok := t.data.grants[id]; ok {
		delete(t.data.locators, r.JoinCode)
		delete(t.data.locators, r.Alias)
		delete(t.data.grants, id)
	}
	return nil
}
func (t *transaction) Receipt(key string) (storage.Receipt, bool, error) {
	r, ok := t.data.receipts[key]
	r.Response = append([]byte(nil), r.Response...)
	return r, ok, t.ctx.Err()
}
func (t *transaction) PutReceipt(r storage.Receipt) error {
	if err := bounded(r); err != nil {
		return err
	}
	if err := t.write(); err != nil {
		return err
	}
	if _, ok := t.data.sessions[r.SessionID]; !ok {
		return storage.ErrConflict
	}
	if _, ok := t.data.receipts[r.Key]; ok {
		return storage.ErrConflict
	}
	r.Response = append([]byte(nil), r.Response...)
	t.data.receipts[r.Key] = r
	return nil
}
func (t *transaction) Counts() (storage.Counts, error) {
	return storage.Counts{SessionCount: len(t.data.sessions), GrantCount: len(t.data.grants), ReceiptCount: len(t.data.receipts)}, t.ctx.Err()
}
func (t *transaction) PruneExpired() error {
	if err := t.write(); err != nil {
		return err
	}
	for k, r := range t.data.sessions {
		if !r.ExpiresAt.After(t.now) {
			_ = t.DeleteSession(k)
		}
	}
	for k, r := range t.data.grants {
		if !r.ExpiresAt.After(t.now) {
			_ = t.DeleteGrant(k)
		}
	}
	for k, r := range t.data.receipts {
		if !r.ExpiresAt.After(t.now) {
			delete(t.data.receipts, k)
		}
	}
	return nil
}
func (t *transaction) Namespace() (storage.Namespace, error) { return t.data.namespace, t.ctx.Err() }
func (t *transaction) PutNamespace(r storage.Namespace) error {
	if err := bounded(r); err != nil {
		return err
	}
	if err := t.write(); err != nil {
		return err
	}
	t.data.namespace = r
	return nil
}
func cloneSession(r storage.SessionRecord) storage.SessionRecord {
	r.RequiredBuses = append([]string(nil), r.RequiredBuses...)
	r.Relay.Buses = append([]storage.BusState(nil), r.Relay.Buses...)
	r.Relay.Subscriptions = append([]storage.SubscriptionState(nil), r.Relay.Subscriptions...)
	return r
}

func bounded(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > storage.MaxRecordBytes {
		return storage.ErrCapacity
	}
	return nil
}
