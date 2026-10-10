// Package storage lets applications supply transactional persistence for Relay
// Sessions and access grants. Adapters do not issue credentials or decide scope.
package storage

import (
	"context"
	"errors"
	"time"
)

const SchemaVersion = 1
const MaxRecordBytes = 262144

var (
	ErrConflict = errors.New("access storage conflict")
	ErrReadOnly = errors.New("access storage transaction is read-only")
	ErrClosed   = errors.New("access storage is closed")
	ErrCapacity = errors.New("access storage capacity reached")
)

// Store serializes Update across the entire configured namespace, including
// alias uniqueness and capacity. The callback runs once; an error, panic or
// cancellation rolls back every write. A successful return means commit was
// acknowledged. Adapters must not retry callbacks or retain Tx values.
// View supplies a consistent snapshot and rejects writes. This in-process
// callback API is not a remote database protocol.
type Store interface {
	Update(context.Context, func(Tx) error) error
	View(context.Context, func(Tx) error) error
	Close() error
}

// Tx reads and writes exported records within one Store callback. Reads return
// independent values. PutGrant is insert-only and requires an existing Session,
// a nonempty ID, and distinct nonempty code and alias locators. Locators are
// globally unique across Sessions. PutReceipt is insert-only and requires its
// Session to exist. These conflicts return ErrConflict without partial writes.
// Serialized records exceeding MaxRecordBytes return ErrCapacity.
// DeleteSession also removes its grants and receipts. Counts includes retained
// records; PruneExpired removes expired records before capacity is evaluated.
// A receipt remains reserved until its own ExpiresAt, unless its Session ends.
type Tx interface {
	Now() time.Time
	Session(string) (SessionRecord, bool, error)
	Sessions(limit int) ([]SessionRecord, error)
	PutSession(SessionRecord) error
	DeleteSession(string) error
	Grant(string) (GrantRecord, bool, error)
	PutGrant(GrantRecord) error
	DeleteGrant(string) error
	Receipt(string) (Receipt, bool, error)
	PutReceipt(Receipt) error
	Counts() (Counts, error)
	PruneExpired() error
	Namespace() (Namespace, error)
	PutNamespace(Namespace) error
}

type Counts struct {
	SessionCount int
	GrantCount   int
	ReceiptCount int
}

// Namespace persists the restoration incarnation and ordered writer fence.
// An adapter hosts one namespace; independent namespaces use separate stores.
type Namespace struct {
	SchemaVersion  int
	Incarnation    string
	WriterTerm     uint64
	WriterID       string
	LeaseExpiresAt time.Time
}

type BusState struct {
	BusID            string `json:"bus_id"`
	Role             string `json:"role"`
	SourceActive     bool   `json:"source_active"`
	SourceGeneration uint64 `json:"source_generation"`
}

type SubscriptionState struct {
	SubscriberID string `json:"subscriber_id"`
	BusID        string `json:"bus_id"`
}

// RelayState is an observation, never proof of restored live connections.
type RelayState struct {
	ContractVersion int                 `json:"contract_version"`
	SessionID       string              `json:"session_id"`
	RelayEpoch      string              `json:"relay_epoch"`
	Revision        uint64              `json:"revision"`
	WriterTerm      uint64              `json:"writer_term,omitempty"`
	ObservedAt      time.Time           `json:"observed_at"`
	Buses           []BusState          `json:"buses"`
	Subscriptions   []SubscriptionState `json:"subscriptions"`
}

type SessionRecord struct {
	SchemaVersion      int
	ID                 string
	Issuer             string
	Incarnation        string
	LegacyTokensBefore time.Time
	RequiredBuses      []string
	Codec              string
	StateRevision      uint64
	Relay              RelayState
	WriterTerm         uint64
	MediaTerm          uint64
	LastActiveAt       time.Time
	ExpiresAt          time.Time
}

type GrantRecord struct {
	SchemaVersion int
	ID            string
	JoinCode      string
	Alias         string
	GrammarName   string
	WordCount     int
	Visibility    string
	SessionID     string
	BusID         string
	CreatedAt     time.Time
	ExpiresAt     time.Time
}

// Receipt retains one committed result. Key alone never authenticates a retry;
// Relay checks both digests and rechecks Session/credential lifetime. Response
// contains credentials and requires the same storage protection as grants.
type Receipt struct {
	SchemaVersion    int
	Key              string
	Operation        string
	SessionID        string
	RequestDigest    [32]byte
	CredentialDigest [32]byte
	Response         []byte
	StatusCode       int
	CreatedAt        time.Time
	ExpiresAt        time.Time
}
