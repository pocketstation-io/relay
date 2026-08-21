package session

import (
	"sync"
	"time"
)

// SessionRegistry manages the set of active RelaySessions.
// (Named SessionRegistry, not Manager, per CODE_PROTOCOL LAW 18.)
type SessionRegistry struct {
	mu    sync.RWMutex
	rooms map[string]*RelaySession

	inactivityTimeout time.Duration
	reconnectWindow   time.Duration
	maxSubscriptions  int
	maxBuses          int
}

// RegistryConfig holds configurable parameters for the SessionRegistry.
type RegistryConfig struct {
	// InactivityTimeout: time a room waits for its first publisher before
	// auto-closing. Zero uses defaultInactivityTimeout (30 min).
	InactivityTimeout time.Duration
	// ReconnectWindow: time a room keeps subscriptions alive after the last
	// source disconnects. Zero uses defaultReconnectWindow (60 s).
	ReconnectWindow time.Duration
	// MaxSubscriptions: maximum subscribers per room. Zero means unlimited.
	MaxSubscriptions int
	// MaxBuses is the maximum number of named AudioBuses retained by one
	// RelaySession. Zero uses the finite package default.
	MaxBuses int
}

// NewRegistry returns an empty SessionRegistry with default timeouts.
func NewRegistry() *SessionRegistry { return NewRegistryWithConfig(RegistryConfig{}) }

// NewRegistryWithConfig returns a SessionRegistry configured with cfg.
func NewRegistryWithConfig(cfg RegistryConfig) *SessionRegistry {
	inactivity := cfg.InactivityTimeout
	if inactivity <= 0 {
		inactivity = defaultInactivityTimeout
	}
	reconnect := cfg.ReconnectWindow
	if reconnect <= 0 {
		reconnect = defaultReconnectWindow
	}
	registry := &SessionRegistry{
		rooms:             make(map[string]*RelaySession),
		inactivityTimeout: inactivity,
		reconnectWindow:   reconnect,
		maxSubscriptions:  cfg.MaxSubscriptions,
		maxBuses:          cfg.MaxBuses,
	}
	if registry.maxBuses <= 0 {
		registry.maxBuses = defaultMaxBuses
	}
	return registry
}

// GetOrCreate returns the RelaySession for id, creating it if absent.
func (reg *SessionRegistry) GetOrCreate(id string) *RelaySession {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	return reg.getOrCreateLocked(id)
}

// GetOrCreateWithinLimit atomically returns an existing RelaySession or
// creates one while the registry remains below maxSessions. A non-positive
// limit permits creation and is reserved for package-local tests.
func (reg *SessionRegistry) GetOrCreateWithinLimit(
	id string,
	maxSessions int,
) (relaySession *RelaySession, created bool, accepted bool) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if r := reg.rooms[id]; r != nil {
		return r, false, true
	}
	if maxSessions > 0 && len(reg.rooms) >= maxSessions {
		return nil, false, false
	}
	return reg.getOrCreateLocked(id), true, true
}

func (reg *SessionRegistry) getOrCreateLocked(id string) *RelaySession {
	if r := reg.rooms[id]; r != nil {
		return r
	}
	r := newWithTimeouts(id, reg.inactivityTimeout, reg.reconnectWindow)
	r.maxSubscriptions = reg.maxSubscriptions
	r.maxBuses = reg.maxBuses
	reg.rooms[id] = r
	return r
}

// Get returns the RelaySession for id and whether it was found.
func (reg *SessionRegistry) Get(id string) (*RelaySession, bool) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	r, ok := reg.rooms[id]
	return r, ok
}

// All returns a bounded snapshot of active RelaySession pointers for periodic
// control-state reconciliation.
func (reg *SessionRegistry) All() []*RelaySession {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	result := make([]*RelaySession, 0, len(reg.rooms))
	for _, relaySession := range reg.rooms {
		result = append(result, relaySession)
	}
	return result
}

// Delete closes and removes the RelaySession for id. No-op if absent.
func (reg *SessionRegistry) Delete(id string) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if r, ok := reg.rooms[id]; ok {
		r.Close()
		delete(reg.rooms, id)
	}
}

// RoomCount returns the number of active RelaySessions.
func (reg *SessionRegistry) RoomCount() int {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	return len(reg.rooms)
}

// PacketStats returns aggregate forwarded and dropped packet counts across
// all currently active RelaySessions.
func (reg *SessionRegistry) PacketStats() (forwarded, dropped uint64) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	for _, r := range reg.rooms {
		pkts, _, drop := r.PacketStats()
		forwarded += pkts
		dropped += drop
	}
	return
}

// CloseAll closes every active RelaySession and removes it from the registry.
func (reg *SessionRegistry) CloseAll() {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	for id, r := range reg.rooms {
		r.Close()
		delete(reg.rooms, id)
	}
}

// SessionSummary is a snapshot of a RelaySession's observable state.
// Used by GET /v1/channels (spec §3.1).
type SessionSummary struct {
	SessionID         string    `json:"session_id"`
	SubscriptionCount int       `json:"subscription_count"`
	SourceActive      bool      `json:"source_active"`
	CreatedAt         time.Time `json:"created_at"`
	Public            bool      `json:"public"`
}

// ListPublic returns a summary of every RelaySession created with Public==true.
// Returns a non-nil empty slice when no public rooms exist.
func (reg *SessionRegistry) ListPublic() []SessionSummary {
	reg.mu.RLock()
	defer reg.mu.RUnlock()

	result := make([]SessionSummary, 0)
	for _, r := range reg.rooms {
		if !r.Public.Load() {
			continue
		}
		result = append(result, SessionSummary{
			SessionID:         r.ID,
			SubscriptionCount: r.SubscriptionCount(),
			SourceActive:      r.SourceActive(),
			CreatedAt:         r.createdAt,
			Public:            true,
		})
	}
	return result
}
