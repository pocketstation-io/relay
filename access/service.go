// Package access owns Relay Session declarations, capabilities and shared links.
// The same service runs inside standalone Relay or a managed control process.
package access

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"math"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/pocketstation-io/relay/access/metrics"
	"github.com/pocketstation-io/relay/access/storage"
	"github.com/pocketstation-io/relay/access/token"
)

var (
	ErrSessionCapacity    = errors.New("Session capacity reached")
	ErrInvitationCapacity = errors.New("invitation capacity reached")
	ErrSSECapacity        = errors.New("Session event subscription capacity reached")
	ErrSessionNotReady    = errors.New("Session is not ready")
	ErrWriterFenced       = errors.New("Session writer was replaced")
)

// Config fixes finite resource limits and the configured logical writer group.
// Replicas in one group share transactions; a different group may take over
// only after the persisted lease expires. A displaced instance stays fenced.
type Config struct {
	NamePolicy          NameAllocationPolicy
	MaxSessions         int
	MaxInvitations      int
	MaxReceipts         int
	MaxSSEPerSession    int
	SessionTTL          time.Duration
	OwnerTokenTTL       time.Duration // Maximum owner lifetime, bounded by SessionTTL.
	PublisherTokenTTL   time.Duration
	SubscriberTokenTTL  time.Duration
	ReceiptTTL          time.Duration
	ObservationTTL      time.Duration
	WriterLeaseDuration time.Duration
	WriterID            string
	OperationTimeout    time.Duration
}

type Service struct {
	storage      storage.Store
	secret       []byte
	config       Config
	term         uint64
	incarnation  string
	registry     *metrics.Registry
	mu           sync.Mutex
	sessionBuses map[string]*EventBus
}

// SessionStore remains an internal-source compatibility name for lifted HTTP
// adapters; new embeddings construct Service with an explicit storage adapter.
// Deprecated: use Service.
type SessionStore = Service

type Session struct {
	record      storage.SessionRecord
	sourceToken string
}

func NewService(secret []byte, backend storage.Store, config Config) (*Service, error) {
	if len(secret) < 32 || backend == nil {
		return nil, errors.New("Session access requires a signing key and storage")
	}
	if config.NamePolicy == "" {
		config.NamePolicy = AutoTwoThenThree
	}
	if _, valid := config.NamePolicy.fixedCount(); config.NamePolicy != AutoTwoThenThree && !valid {
		return nil, errors.New("invalid readable name allocation policy")
	}
	if config.MaxSessions <= 0 {
		config.MaxSessions = 1000
	}
	if config.MaxInvitations <= 0 {
		config.MaxInvitations = 4000
	}
	if config.MaxReceipts <= 0 {
		config.MaxReceipts = 4000
	}
	if config.MaxSSEPerSession <= 0 {
		config.MaxSSEPerSession = 16
	}
	if config.SessionTTL <= 0 {
		config.SessionTTL = 2 * time.Hour
	}
	if config.OwnerTokenTTL <= 0 {
		config.OwnerTokenTTL = 15 * time.Minute
	}
	// Clients renew before the advertised owner expiry, including while idle.
	if config.OwnerTokenTTL > config.SessionTTL {
		config.OwnerTokenTTL = config.SessionTTL
	}
	if config.PublisherTokenTTL <= 0 {
		config.PublisherTokenTTL = 15 * time.Minute
	}
	if config.SubscriberTokenTTL <= 0 {
		config.SubscriberTokenTTL = 2 * time.Hour
	}
	if config.ReceiptTTL <= 0 {
		config.ReceiptTTL = 2 * time.Minute
	}
	if config.ObservationTTL <= 0 {
		config.ObservationTTL = 15 * time.Second
	}
	if config.WriterLeaseDuration <= 0 {
		config.WriterLeaseDuration = 30 * time.Second
	}
	if config.OperationTimeout <= 0 || config.OperationTimeout > 30*time.Second {
		config.OperationTimeout = 5 * time.Second
	}
	if config.WriterID == "" {
		config.WriterID = "default"
	}
	s := &Service{storage: backend, secret: append([]byte(nil), secret...), config: config, registry: metrics.New(), sessionBuses: make(map[string]*EventBus)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := backend.Update(ctx, func(tx storage.Tx) error {
		ns, err := tx.Namespace()
		if err != nil {
			return err
		}
		if ns.SchemaVersion > storage.SchemaVersion {
			return errors.New("unsupported access storage schema")
		}
		if ns.Incarnation == "" {
			id, err := randomSessionID()
			if err != nil {
				return err
			}
			ns = storage.Namespace{SchemaVersion: storage.SchemaVersion, Incarnation: id, WriterTerm: 1, WriterID: config.WriterID}
		}
		if ns.WriterID == "" {
			ns.WriterID = config.WriterID
			if ns.WriterTerm == 0 {
				ns.WriterTerm = 1
			}
		}
		if ns.WriterID != config.WriterID {
			if ns.LeaseExpiresAt.After(tx.Now()) {
				return ErrWriterFenced
			}
			ns.WriterTerm++
			ns.WriterID = config.WriterID
		}
		ns.LeaseExpiresAt = tx.Now().Add(config.WriterLeaseDuration)
		if err := tx.PutNamespace(ns); err != nil {
			return err
		}
		rows, err := tx.Sessions(config.MaxSessions)
		if err != nil {
			return err
		}
		for _, r := range rows {
			if len(r.Relay.Buses) > 0 || len(r.Relay.Subscriptions) > 0 {
				r.Relay.Buses = nil
				r.Relay.Subscriptions = nil
				r.StateRevision++
				if err := tx.PutSession(r); err != nil {
					return err
				}
			}
		}
		s.term = ns.WriterTerm
		s.incarnation = ns.Incarnation
		return nil
	})
	return s, err
}

func (s *Service) Register(mux *http.ServeMux, config HandlerConfig) {
	NewHandlerWithConfig(s, s.registry, config).Register(mux)
}
func (s *Service) RegisterInternal(mux *http.ServeMux, secret []byte) {
	NewInternalHandler(s, secret).Register(mux)
}
func (s *Service) WriterTerm() uint64 { return s.term }

func (s *Service) update(ctx context.Context, fn func(storage.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.OperationTimeout)
	defer cancel()
	return s.storage.Update(ctx, func(tx storage.Tx) error {
		if err := s.checkWriter(tx); err != nil {
			return err
		}
		ns, err := tx.Namespace()
		if err != nil {
			return err
		}
		ns.LeaseExpiresAt = tx.Now().Add(s.config.WriterLeaseDuration)
		if err := tx.PutNamespace(ns); err != nil {
			return err
		}
		if err := tx.PruneExpired(); err != nil {
			return err
		}
		return fn(tx)
	})
}
func (s *Service) checkWriter(tx storage.Tx) error {
	ns, err := tx.Namespace()
	if err != nil {
		return err
	}
	if ns.WriterTerm != s.term || ns.WriterID != s.config.WriterID || ns.Incarnation != s.incarnation {
		return ErrWriterFenced
	}
	return nil
}
func (s *Service) active(tx storage.Tx, id string) (storage.SessionRecord, error) {
	r, ok, err := tx.Session(id)
	if err != nil {
		return r, err
	}
	if !ok || !r.ExpiresAt.After(tx.Now()) || r.Incarnation != s.incarnation {
		return r, sql.ErrNoRows
	}
	return r, nil
}

func (s *Service) Create(required ...string) (Session, error) {
	return s.CreateContext(context.Background(), required...)
}
func (s *Service) CreateContext(ctx context.Context, required ...string) (Session, error) {
	if len(required) == 0 {
		required = []string{"application", "microphone"}
	}
	required, err := validateRequiredBuses(required)
	if err != nil {
		return Session{}, err
	}
	var result Session
	err = s.update(ctx, func(tx storage.Tx) error {
		counts, err := tx.Counts()
		if err != nil {
			return err
		}
		if counts.SessionCount >= s.config.MaxSessions {
			return ErrSessionCapacity
		}
		id, err := randomSessionID()
		if err != nil {
			return err
		}
		r := storage.SessionRecord{SchemaVersion: storage.SchemaVersion, ID: id, Issuer: token.Issuer, Incarnation: s.incarnation, RequiredBuses: required, Codec: "opus", StateRevision: 1, WriterTerm: s.term, LastActiveAt: tx.Now(), ExpiresAt: tx.Now().Add(s.config.SessionTTL)}
		credential, err := s.sign(r, token.Claims{SessionID: id, Role: token.RoleSource, BusIDs: required, Control: true}, s.config.OwnerTokenTTL, tx.Now())
		if err != nil {
			return err
		}
		if err := tx.PutSession(r); err != nil {
			return err
		}
		result = Session{record: r, sourceToken: credential}
		return nil
	})
	if err != nil {
		return Session{}, err
	}
	return result, nil
}
func (s *Service) GetContext(ctx context.Context, id string) (Session, error) {
	var result Session
	err := s.update(ctx, func(tx storage.Tx) error {
		r, err := s.active(tx, id)
		if err != nil {
			return err
		}
		if r.WriterTerm != s.term || tx.Now().Sub(r.Relay.ObservedAt) > s.config.ObservationTTL {
			if len(r.Relay.Buses) > 0 || len(r.Relay.Subscriptions) > 0 {
				r.Relay.Buses = nil
				r.Relay.Subscriptions = nil
				r.StateRevision++
				if err := tx.PutSession(r); err != nil {
					return err
				}
			}
		}
		result.record = r
		return nil
	})
	return result, err
}
func (s *Service) Get(id string) (Session, bool) {
	r, err := s.GetContext(context.Background(), id)
	return r, err == nil
}
func (s *Service) DeleteContext(ctx context.Context, id string) (bool, error) {
	deleted := false
	err := s.update(ctx, func(tx storage.Tx) error {
		_, err := s.active(tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := tx.DeleteSession(id); err != nil {
			return err
		}
		deleted = true
		return nil
	})
	if deleted && err == nil {
		s.closeLocalEventBus(id)
	}
	return deleted, err
}
func (s *Service) Delete(id string) bool {
	ok, _ := s.DeleteContext(context.Background(), id)
	return ok
}
func (s *Service) sign(r storage.SessionRecord, claims token.Claims, ttl time.Duration, now time.Time) (string, error) {
	return token.SignProfile(s.secret, r.Issuer, r.Incarnation, claims, ttl, now)
}

func (s *Service) Authorize(ctx context.Context, id, encoded string) (*token.Claims, error) {
	var claims *token.Claims
	err := s.view(ctx, func(tx storage.Tx) error {
		if err := s.checkWriter(tx); err != nil {
			return err
		}
		r, err := s.active(tx, id)
		if err != nil {
			return err
		}
		c, err := s.authorizeRecord(r, encoded, tx.Now())
		if err != nil {
			return err
		}
		claims = c
		return nil
	})
	return claims, err
}

func (s *Service) IssuePublisherTokenContext(ctx context.Context, id, bus string) (string, error) {
	return s.issue(ctx, id, bus, token.RoleSource)
}
func (s *Service) IssueSubscriberTokenContext(ctx context.Context, id, bus string) (string, error) {
	return s.issue(ctx, id, bus, token.RoleSubscriber)
}
func (s *Service) issue(ctx context.Context, id, bus string, role token.Role) (string, error) {
	var result string
	err := s.update(ctx, func(tx storage.Tx) error {
		r, err := s.active(tx, id)
		if err != nil {
			return err
		}
		if bus != "mix" && !contains(r.RequiredBuses, bus) {
			return ErrInvalidRelayState
		}
		claims := token.Claims{SessionID: id, Role: role}
		ttl := s.config.SubscriberTokenTTL
		if role == token.RoleSource {
			if bus == "mix" {
				return ErrInvalidRelayState
			}
			claims.BusIDs = []string{bus}
			ttl = s.config.PublisherTokenTTL
		} else {
			claims.BusID = bus
		}
		result, err = s.sign(r, claims, ttl, tx.Now())
		if err != nil {
			return err
		}
		r.LastActiveAt = tx.Now()
		r.ExpiresAt = tx.Now().Add(s.config.SessionTTL)
		return tx.PutSession(r)
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

func (s *Service) ApplyRelayStateContext(ctx context.Context, id string, snapshot RelayStateSnapshot) (State, bool, error) {
	if snapshot.SessionID != id {
		return State{}, false, ErrInvalidRelayState
	}
	if err := validateRelaySnapshot(snapshot); err != nil {
		return State{}, false, err
	}
	snapshot = normalizeRelaySnapshot(snapshot)
	var result State
	changed := false
	err := s.update(ctx, func(tx storage.Tx) error {
		r, err := s.active(tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return os.ErrNotExist
		}
		if err != nil {
			return err
		}
		if snapshot.WriterTerm != r.MediaTerm || r.MediaTerm == 0 || snapshot.RelayEpoch != r.Relay.RelayEpoch {
			return ErrWriterFenced
		}
		if snapshot.RelayEpoch == r.Relay.RelayEpoch && snapshot.Revision < r.Relay.Revision {
			result = Session{record: r}.State()
			return nil
		}
		for _, bus := range snapshot.Buses {
			if !contains(r.RequiredBuses, bus.BusID) {
				return ErrInvalidRelayState
			}
		}
		changed = snapshot.Revision != r.Relay.Revision || !reflect.DeepEqual(snapshot.Buses, r.Relay.Buses) || !reflect.DeepEqual(snapshot.Subscriptions, r.Relay.Subscriptions)
		snapshot.ObservedAt = tx.Now()
		r.Relay = snapshot
		r.WriterTerm = s.term
		if changed {
			r.StateRevision++
		}
		active := len(snapshot.Subscriptions) > 0
		for _, bus := range snapshot.Buses {
			active = active || bus.SourceActive
		}
		if active {
			r.LastActiveAt = tx.Now()
			r.ExpiresAt = tx.Now().Add(s.config.SessionTTL)
		}
		if err := tx.PutSession(r); err != nil {
			return err
		}
		result = Session{record: r}.State()
		return nil
	})
	return result, changed, err
}

func (v Session) ID() string              { return v.record.ID }
func (v Session) SourceToken() string     { return v.sourceToken }
func (v Session) RequiredBuses() []string { return append([]string(nil), v.record.RequiredBuses...) }
func (v Session) Codec() string           { return v.record.Codec }
func (v Session) SubscriptionCount() int  { return len(v.record.Relay.Subscriptions) }
func (v Session) SourceActive() bool      { return v.State().Ready }
func (v Session) State() State {
	r := v.record
	state := State{SessionID: r.ID, StateRevision: r.StateRevision, RelayEpoch: r.Relay.RelayEpoch, RelayRevision: r.Relay.Revision, RequiredBuses: append([]string{}, r.RequiredBuses...), Buses: append([]BusState{}, r.Relay.Buses...), Subscriptions: append([]SubscriptionState{}, r.Relay.Subscriptions...), SubscriptionCount: len(r.Relay.Subscriptions), Codec: r.Codec, Ready: true}
	for _, required := range state.RequiredBuses {
		active := false
		for _, bus := range state.Buses {
			if bus.BusID == required && bus.SourceActive {
				active = true
			}
		}
		if !active {
			state.Ready = false
		}
	}
	return state
}
func contains(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}
func randomSessionID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	value[6] = (value[6] & 15) | 64
	value[8] = (value[8] & 63) | 128
	encoded := hex.EncodeToString(value)
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}
func ParseSessionExpiryDuration(getenv func(string) string) (time.Duration, error) {
	raw := getenv("POCKETSTATION_SESSION_TTL_SECONDS")
	if raw == "" {
		return 2 * time.Hour, nil
	}
	seconds, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || seconds <= 0 || seconds > math.MaxInt64/int64(time.Second) {
		return 0, errors.New("POCKETSTATION_SESSION_TTL_SECONDS must be positive and fit a time.Duration")
	}
	return time.Duration(seconds) * time.Second, nil
}

// ResetRestoredStore explicitly invalidates all access restored from an older
// backup. Ordinary Open cannot detect rollback without an external trust anchor.
func ResetRestoredStore(ctx context.Context, backend storage.Store, maxSessions int) error {
	return backend.Update(ctx, func(tx storage.Tx) error {
		rows, err := tx.Sessions(maxSessions)
		if err != nil {
			return err
		}
		for _, r := range rows {
			if err := tx.DeleteSession(r.ID); err != nil {
				return err
			}
		}
		id, err := randomSessionID()
		if err != nil {
			return err
		}
		return tx.PutNamespace(storage.Namespace{SchemaVersion: storage.SchemaVersion, Incarnation: id})
	})
}

func (s *Service) view(ctx context.Context, fn func(storage.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.OperationTimeout)
	defer cancel()
	return s.storage.View(ctx, fn)
}

func (s *Service) authorizeRecord(r storage.SessionRecord, encoded string, now time.Time) (*token.Claims, error) {
	c, err := token.VerifyProfile(s.secret, encoded, r.Issuer, now)
	if err != nil {
		return nil, err
	}
	if c.SessionID != r.ID {
		return nil, token.ErrInvalidCapability
	}
	if c.Incarnation != r.Incarnation {
		if c.Incarnation != "" || r.Issuer != token.LegacyIssuer || r.LegacyTokensBefore.IsZero() || c.IssuedAt.Time.After(r.LegacyTokensBefore) {
			return nil, token.ErrInvalidCapability
		}
	}
	return c, nil
}
