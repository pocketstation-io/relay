package room

import (
    "errors"
    "sync"
    "sync/atomic"
    "github.com/pion/webrtc/v4"
)

var ErrNoSource = errors.New("room has no source")

type Room struct {
    ID string
    mu sync.RWMutex
    source *webrtc.TrackRemote
    listeners []*webrtc.TrackLocalStaticRTP
    PacketCount atomic.Uint64
    ByteCount atomic.Uint64
}

func New(id string) *Room { return &Room{ID: id} }

func (r *Room) SetSource(track *webrtc.TrackRemote) {
    r.mu.Lock(); r.source = track; r.mu.Unlock()
    go r.forwardLoop()
}

func (r *Room) AddListener(track *webrtc.TrackLocalStaticRTP) {
    r.mu.Lock(); r.listeners = append(r.listeners, track); r.mu.Unlock()
}

func (r *Room) ListenerCount() int { r.mu.RLock(); defer r.mu.RUnlock(); return len(r.listeners) }

func (r *Room) forwardLoop() {
    for {
        r.mu.RLock()
        source := r.source
        r.mu.RUnlock()
        if source == nil { return }
        pkt, _, err := source.ReadRTP()
        if err != nil { return }
        r.PacketCount.Add(1)
        r.ByteCount.Add(uint64(len(pkt.Payload)))
        r.mu.RLock()
        listeners := append([]*webrtc.TrackLocalStaticRTP(nil), r.listeners...)
        r.mu.RUnlock()
        for _, l := range listeners {
            // ADR-009: allocation/mutation profile must be measured before Phase 1 exit.
            _ = l.WriteRTP(pkt)
        }
    }
}

type Manager struct { mu sync.RWMutex; rooms map[string]*Room }
func NewManager() *Manager { return &Manager{rooms: map[string]*Room{}} }
func (m *Manager) GetOrCreate(id string) *Room { m.mu.Lock(); defer m.mu.Unlock(); if r:=m.rooms[id]; r!=nil { return r }; r:=New(id); m.rooms[id]=r; return r }
func (m *Manager) Get(id string) (*Room, bool) { m.mu.RLock(); defer m.mu.RUnlock(); r, ok := m.rooms[id]; return r, ok }
