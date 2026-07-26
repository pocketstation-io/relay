// Package graph implements the RelaySession — the core forwarding unit of the
// PocketStation relay (v3.0). A RelaySession owns a set of named AudioBuses,
// each carrying a distinct audio stream (voice, music, agent_voice, events…).
// A BusSubscription selects a single AudioBus, or the virtual BusMix to receive
// RTP from every active bus — enabling relay.out("mix") semantics from the
// holy-shit demo while still allowing per-bus subscriptions (spec §7).
//
//	graph.connect(duck.out("audio"),       relay.in_("music"))?;
//	graph.connect(agent.out("audio"),      relay.in_("agent_voice"))?;
//	graph.connect(relay.out("mix"),        browser.in_("audio"))?;
package graph

import (
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/clocklineage"
	"github.com/pocketstation-io/relay/internal/downlink"
)

// BusID names a forwarding lane within a RelaySession.
// Canonical values: "voice", "music", "agent_voice", "events".
// "mix" is a virtual value meaning all active buses (used on the subscriber side).
type BusID = string

// BusMix is the virtual BusID that subscribes to all active buses.
const BusMix BusID = "mix"

// BusRole classifies the audio content type of a bus.
// Determines the Opus codec profile, loss policy, and forwarding priority.
type BusRole uint8

const (
	BusRoleVoice       BusRole = iota // mono, Voip mode, VAD-gated; latency-rank 0
	BusRoleMusic                      // stereo, Audio mode, no DTX; latency-rank 1
	BusRoleAgentOutput                // TTS from a ModelNode; latency-rank 0
	BusRoleEvents                     // metadata / transcript fragments; latency-rank 2
	BusRoleMonitor                    // passive monitor mix; latency-rank 3
)

// LatencyRank returns the forwarding priority of this bus role.
// Lower values are higher priority (LAW 8 — classification as impl method).
func (r BusRole) LatencyRank() uint8 {
	switch r {
	case BusRoleVoice, BusRoleAgentOutput:
		return 0
	case BusRoleMusic:
		return 1
	case BusRoleEvents:
		return 2
	default:
		return 3
	}
}

// ReliabilityRank returns the loss tolerance rank of this bus role.
// Lower values tolerate less loss.
func (r BusRole) ReliabilityRank() uint8 {
	switch r {
	case BusRoleVoice, BusRoleAgentOutput:
		return 0
	case BusRoleMusic:
		return 1
	case BusRoleEvents:
		return 2
	default:
		return 3
	}
}

// String returns the canonical label for this bus role (LAW 8 — classification
// as an impl method; used in BusHealth JSON).
func (r BusRole) String() string {
	switch r {
	case BusRoleVoice:
		return "voice"
	case BusRoleMusic:
		return "music"
	case BusRoleAgentOutput:
		return "agent_output"
	case BusRoleEvents:
		return "events"
	case BusRoleMonitor:
		return "monitor"
	default:
		return "unknown"
	}
}

// defaultInactivityTimeout is the time a RelaySession with no active bus source
// will remain open before auto-closing. Configurable via ROOM_EXPIRY_MINUTES.
const defaultInactivityTimeout = 30 * time.Minute

// defaultReconnectWindow is the time a bus keeps subscribers alive after its
// source disconnects. Configurable via SOURCE_RECONNECT_WINDOW_SEC.
const defaultReconnectWindow = 60 * time.Second

// DefaultMediaStallThresholdMs is how long a bus with an attached source may go
// without forwarding an RTP packet before it is considered media-stalled.
// WebSocket/ICE can stay alive while media silently stops (Corrected Audit §6):
// at 50–100 pkt/s, ~2 s with zero packets is unambiguously a stall, not jitter.
const DefaultMediaStallThresholdMs = 2000

var (
	// ErrNoSource is returned when an operation requires a live source and none exists.
	ErrNoSource = errors.New("relay_session: bus has no source")
	// ErrRoomFull is returned by AddSubscription when the room is at capacity.
	ErrRoomFull = errors.New("relay_session: reached maximum subscription capacity")
)

// SourceSession is the readable side of a live audio stream on a bus.
// Using an interface decouples RelaySession from *webrtc.TrackRemote and allows
// the forward loop to be unit-tested without a live Pion setup.
type SourceSession interface {
	ReadRTP() (*rtp.Packet, error)
}

type clockLineageSource interface {
	ClockLineage() *clocklineage.Timeline
}

// BusSubscription is the write-side of an audio stream delivered to a subscriber.
// *webrtc.TrackLocalStaticRTP satisfies this interface directly.
type BusSubscription interface {
	WriteRTP(pkt *rtp.Packet) error
}

// subscriptionEntry pairs a subscriber ID with its Downlink and the BusID it
// selected. busID == BusMix means "all buses" (relay.out("mix") semantics);
// any other value receives only that bus's RTP (spec §7).
type subscriptionEntry struct {
	subscriberID string
	busID        BusID
	dl           *downlink.Downlink
}

// RelaySession owns the set of AudioBuses for one relay session.
// Each subscriber selects one bus, or BusMix to receive RTP from all buses
// (relay.out("mix") semantics). Buses are created lazily on first PUBLISH.
//
// Invariants:
//   - busesMu guards the buses map for writes; reads inside forwardLoop go
//     through the room's atomic subscription pointer (no room lock on hot path).
//   - subscriptions is copy-on-write behind subscriptionsMu.
//   - done is closed by Close and signals all bus forwardLoops to stop.
type RelaySession struct {
	ID      string
	GraphID string // graph template name from PUBLISH (e.g. "room-demo")

	busesMu sync.RWMutex
	buses   map[BusID]*AudioBus

	// subscriptionsMu and subscriptions are room-wide; every bus forwardLoop
	// writes here tagged with its BusID, and deliver filters per subscriber
	// (BusMix subscribers receive all buses on one track).
	subscriptionsMu sync.Mutex
	subscriptions   atomic.Pointer[[]*subscriptionEntry]

	Public    atomic.Bool
	createdAt time.Time

	keyMu      sync.RWMutex
	currentKey string

	latency *latencyStore

	// maxSubscriptions: 0 = unlimited.
	maxSubscriptions int
	pendingSlots     atomic.Int32

	// inactivity/reconnect configuration propagated to each AudioBus.
	inactivityTimeout time.Duration
	reconnectWindow   time.Duration

	// expiryTimer drives active-media-aware room expiry: it periodically closes
	// the room only when it is truly idle (Corrected Audit §6.4).
	expiryMu    sync.Mutex
	expiryTimer *time.Timer

	closeOnce sync.Once
	done      chan struct{}
}

// New returns an open RelaySession with default timeouts.
func New(id string) *RelaySession {
	return newWithTimeouts(id, defaultInactivityTimeout, defaultReconnectWindow)
}

func newWithTimeout(id string, timeout time.Duration) *RelaySession {
	return newWithTimeouts(id, timeout, defaultReconnectWindow)
}

func newWithTimeouts(id string, inactivityTimeout, reconnectWindow time.Duration) *RelaySession {
	r := &RelaySession{
		ID:                id,
		buses:             make(map[BusID]*AudioBus),
		done:              make(chan struct{}),
		inactivityTimeout: inactivityTimeout,
		reconnectWindow:   reconnectWindow,
		latency:           newLatencyStore(),
		createdAt:         time.Now().UTC(),
	}
	empty := make([]*subscriptionEntry, 0)
	r.subscriptions.Store(&empty)
	r.expiryMu.Lock()
	r.expiryTimer = time.AfterFunc(inactivityTimeout, r.checkExpiry)
	r.expiryMu.Unlock()
	return r
}

// checkExpiry closes the room if it is idle, otherwise reschedules itself.
// Runs on the expiry timer goroutine.
func (r *RelaySession) checkExpiry() {
	if r.isInactive() {
		r.Close()
		return
	}
	r.expiryMu.Lock()
	if r.expiryTimer != nil {
		r.expiryTimer.Reset(r.inactivityTimeout)
	}
	r.expiryMu.Unlock()
}

// isInactive reports whether the room may be expired: no live source, no
// subscribers, and no RTP forwarded within the inactivity window. A WebSocket
// staying open does not keep an otherwise-dead room alive (Corrected Audit §6.4).
func (r *RelaySession) isInactive() bool {
	if r.SourceActive() {
		return false
	}
	if r.SubscriptionCount() > 0 {
		return false
	}
	return !r.hasRecentRTP(r.inactivityTimeout)
}

// hasRecentRTP reports whether any bus forwarded an RTP packet within window.
func (r *RelaySession) hasRecentRTP(window time.Duration) bool {
	r.busesMu.RLock()
	defer r.busesMu.RUnlock()
	for _, b := range r.buses {
		if b.lastRTPAtNanos.Load() != 0 && b.LastRTPAge() < window {
			return true
		}
	}
	return false
}

// GetOrCreateBus returns the AudioBus named id, creating it with the given role
// if it does not yet exist. Safe to call concurrently from multiple PUBLISH goroutines.
func (r *RelaySession) GetOrCreateBus(id BusID, role BusRole) *AudioBus {
	r.busesMu.RLock()
	if b, ok := r.buses[id]; ok {
		r.busesMu.RUnlock()
		return b
	}
	r.busesMu.RUnlock()

	r.busesMu.Lock()
	defer r.busesMu.Unlock()
	if b, ok := r.buses[id]; ok {
		return b // created by a concurrent caller while we waited for the write lock
	}
	b := newAudioBus(id, role, r.ID, r.inactivityTimeout, r.reconnectWindow)
	r.buses[id] = b
	return b
}

// SetSource sets the SourceSession on the named bus (creating the bus if absent)
// and starts the forwarding loop. closer, if non-nil, is called when a new
// source replaces this one (ICE restart path).
func (r *RelaySession) SetSource(busID BusID, role BusRole, src SourceSession, closer func()) {
	bus := r.GetOrCreateBus(busID, role)
	// Per-bus scratch buffers captured in the closure. Each forwardLoop
	// goroutine is serial, so no lock needed. Avoids per-packet map/slice
	// allocation and GC pressure on the hot forwarding path.
	errCounts := make(map[string]int, 8)
	deadSubs := make([]string, 0, 8)

	bus.SetSource(src, closer, func(pkt *rtp.Packet, generation uint64) {
		captureTime, captureTimeKnown := bus.CaptureTime(pkt.Timestamp, time.Now())
		r.deliverWithSource(
			busID,
			pkt,
			captureTime,
			captureTimeKnown,
			downlink.SourceIdentity{BusID: string(busID), Generation: generation, SSRC: pkt.SSRC},
			errCounts,
			&deadSubs,
		)
	})
}

// deliver is the hot-path function passed to each AudioBus.forwardLoop. It
// writes pkt to every subscriber that selected this bus (entry.busID == busID)
// or the virtual mix (entry.busID == BusMix). A BusMix subscriber therefore
// receives RTP from all buses — byte-identical to the room-wide behavior.
//
// errCounts and deadSubs are pre-allocated per bus. A subscriber with
// negotiated packet mutation currently requires a deep RTP header clone; that
// clone allocates extension storage and remains a measured optimization target.
func (r *RelaySession) deliver(busID BusID, pkt *rtp.Packet, errCounts map[string]int, deadSubs *[]string) {
	r.deliverWithSource(busID, pkt, time.Time{}, false, downlink.SourceIdentity{}, errCounts, deadSubs)
}

func (r *RelaySession) deliverWithSource(
	busID BusID,
	pkt *rtp.Packet,
	captureTime time.Time,
	captureTimeKnown bool,
	source downlink.SourceIdentity,
	errCounts map[string]int,
	deadSubs *[]string,
) {
	const maxConsecutiveErrors = 5

	ls := *r.subscriptions.Load()

	*deadSubs = (*deadSubs)[:0]

	for _, e := range ls {
		if e.busID != busID && e.busID != BusMix {
			continue
		}
		out := pkt
		if e.dl.RequiresPacketCopy() {
			header := pkt.Header.Clone()
			out = &rtp.Packet{Header: header, Payload: pkt.Payload, PaddingSize: pkt.PaddingSize}
		}
		if wErr := e.dl.WriteRTPWithSource(out, captureTime, captureTimeKnown, source); wErr != nil {
			errCounts[e.subscriberID]++
			if errCounts[e.subscriberID] >= maxConsecutiveErrors {
				*deadSubs = append(*deadSubs, e.subscriberID)
			}
		} else {
			delete(errCounts, e.subscriberID)
		}
	}

	for _, id := range *deadSubs {
		r.RemoveSubscription(id)
		delete(errCounts, id)
		slog.Warn("evicted dead subscription", "relay_session_id", r.ID, "subscriber_id", id)
	}
}

// AddSubscription registers sub under subscriberID for the named busID. Pass
// BusMix to receive RTP from every bus (relay.out("mix") semantics); pass a
// concrete BusID (e.g. "voice") to receive only that bus (spec §7).
// Returns ErrRoomFull when r.maxSubscriptions > 0 and capacity is reached.
//
// sub is wrapped in a passthrough Downlink (no RTCP reader, no abs-send-time).
// Use AddDownlink when a full Downlink with RTCP stats is available.
func (r *RelaySession) AddSubscription(subscriberID string, busID BusID, sub BusSubscription) error {
	return r.AddDownlink(subscriberID, busID, downlink.NewPassthrough(subscriberID, sub))
}

// AddDownlink registers a full Downlink (with RTCP stats) for subscriberID.
// signal_peer.go uses this path for WebRTC subscribers with an RTPSender.
// Returns ErrRoomFull when r.maxSubscriptions > 0 and capacity is reached.
func (r *RelaySession) AddDownlink(subscriberID string, busID BusID, dl *downlink.Downlink) error {
	r.subscriptionsMu.Lock()
	defer r.subscriptionsMu.Unlock()

	old := *r.subscriptions.Load()
	if r.maxSubscriptions > 0 && len(old) >= r.maxSubscriptions {
		return ErrRoomFull
	}
	next := make([]*subscriptionEntry, len(old)+1)
	copy(next, old)
	next[len(old)] = &subscriptionEntry{subscriberID: subscriberID, busID: busID, dl: dl}
	r.subscriptions.Store(&next)
	return nil
}

// DownlinkSnapshots returns telemetry snapshots for all active subscribers.
// BusID is populated here from the subscriptionEntry because Downlink itself
// is bus-agnostic. Called from the HTTP handler goroutine; all reads are atomic.
func (r *RelaySession) DownlinkSnapshots() []downlink.DownlinkSnapshot {
	ls := *r.subscriptions.Load()
	snaps := make([]downlink.DownlinkSnapshot, 0, len(ls))
	for _, e := range ls {
		snap := e.dl.Snapshot()
		snap.BusID = string(e.busID)
		snaps = append(snaps, snap)
	}
	return snaps
}

// SourceClockSnapshots returns one bounded publisher clock snapshot per bus.
func (r *RelaySession) SourceClockSnapshots() []SourceClockSnapshot {
	r.busesMu.RLock()
	defer r.busesMu.RUnlock()
	snapshots := make([]SourceClockSnapshot, 0, len(r.buses))
	for _, bus := range r.buses {
		snapshots = append(snapshots, bus.SourceClockSnapshot())
	}
	return snapshots
}

// RemoveSubscription deregisters the subscription for subscriberID. No-op if absent.
func (r *RelaySession) RemoveSubscription(subscriberID string) {
	r.subscriptionsMu.Lock()
	old := *r.subscriptions.Load()
	next := make([]*subscriptionEntry, 0, len(old))
	var removed []*subscriptionEntry
	for _, e := range old {
		if e.subscriberID != subscriberID {
			next = append(next, e)
		} else {
			removed = append(removed, e)
		}
	}
	r.subscriptions.Store(&next)
	r.subscriptionsMu.Unlock()

	for _, e := range removed {
		e.dl.StopPacer()
	}
}

// TryReserveSlot atomically reserves a pending subscription slot.
// Returns false if active+pending subscriptions would reach or exceed max.
// max ≤ 0 means unlimited.
func (r *RelaySession) TryReserveSlot(max int) bool {
	if max <= 0 {
		r.pendingSlots.Add(1)
		return true
	}
	for {
		pending := r.pendingSlots.Load()
		active := int32(r.SubscriptionCount())
		if int(pending+active) >= max {
			return false
		}
		if r.pendingSlots.CompareAndSwap(pending, pending+1) {
			return true
		}
	}
}

// ReleaseSlot decrements the pending slot counter.
// Must be called exactly once for each successful TryReserveSlot call.
func (r *RelaySession) ReleaseSlot() { r.pendingSlots.Add(-1) }

// SubscriptionCount returns the current number of registered subscriptions.
func (r *RelaySession) SubscriptionCount() int { return len(*r.subscriptions.Load()) }

// SourceActive reports whether any bus currently has an active source.
func (r *RelaySession) SourceActive() bool {
	r.busesMu.RLock()
	defer r.busesMu.RUnlock()
	for _, b := range r.buses {
		if b.SourceActive() {
			return true
		}
	}
	return false
}

// BusSourceActive reports whether the named bus has an active source.
func (r *RelaySession) BusSourceActive(id BusID) bool {
	r.busesMu.RLock()
	b, ok := r.buses[id]
	r.busesMu.RUnlock()
	if !ok {
		return false
	}
	return b.SourceActive()
}

// PacketStats returns aggregate packet and byte counts across all buses.
func (r *RelaySession) PacketStats() (packets, bytes, dropped uint64) {
	r.busesMu.RLock()
	defer r.busesMu.RUnlock()
	for _, b := range r.buses {
		packets += b.PacketCount.Load()
		bytes += b.ByteCount.Load()
		dropped += b.PacketDropCount.Load()
	}
	return
}

// BusHealthList returns a media-plane health snapshot for every bus in the room.
// Returns a non-nil empty slice when the room has no buses yet.
func (r *RelaySession) BusHealthList(threshold time.Duration) []BusHealth {
	r.busesMu.RLock()
	defer r.busesMu.RUnlock()
	out := make([]BusHealth, 0, len(r.buses))
	for _, b := range r.buses {
		out = append(out, b.Health(threshold))
	}
	return out
}

// BusPacketLog returns the last limit per-packet relay timestamps for busID.
// Returns an empty slice when the bus exists but no packets have been forwarded.
// Returns nil when the bus does not exist in this session.
func (r *RelaySession) BusPacketLog(busID BusID, limit int) []PacketLogEntry {
	r.busesMu.RLock()
	b, ok := r.buses[busID]
	r.busesMu.RUnlock()
	if !ok {
		return nil
	}
	return b.packetLog.last(limit)
}

// AnyBusStalled reports whether any bus has a live source that has gone silent.
// This is the media-stall signal the room watchdog acts on (emit event / request
// ICE restart) — distinct from "no source", which is normal idleness.
func (r *RelaySession) AnyBusStalled(threshold time.Duration) bool {
	r.busesMu.RLock()
	defer r.busesMu.RUnlock()
	for _, b := range r.buses {
		if b.Stalled(threshold) {
			return true
		}
	}
	return false
}

// Close terminates the room and all its buses. Safe to call multiple times.
func (r *RelaySession) Close() {
	r.closeOnce.Do(func() {
		close(r.done)
		for _, e := range *r.subscriptions.Load() {
			e.dl.StopPacer()
		}
		r.expiryMu.Lock()
		if r.expiryTimer != nil {
			r.expiryTimer.Stop()
		}
		r.expiryMu.Unlock()
		r.busesMu.RLock()
		defer r.busesMu.RUnlock()
		for _, b := range r.buses {
			b.close()
		}
	})
}

// SetKey stores the SFrame room key received via KEY_EXCHANGE.
// The relay stores it opaquely and never decodes it to raw key material.
func (r *RelaySession) SetKey(keyBase64 string) {
	r.keyMu.Lock()
	defer r.keyMu.Unlock()
	r.currentKey = keyBase64
}

// GetKey returns the opaque base64 SFrame key, or "" if none has been received.
func (r *RelaySession) GetKey() string {
	r.keyMu.RLock()
	defer r.keyMu.RUnlock()
	return r.currentKey
}

// RecordLatency adds a latency sample to the room's rolling window.
func (r *RelaySession) RecordLatency(captureMs, encodeMs, relayRttMs, jitterBufferMs, decodeMs, packetLossPct float64) {
	r.latency.record(latencySample{
		captureMs:      captureMs,
		encodeMs:       encodeMs,
		relayRttMs:     relayRttMs,
		jitterBufferMs: jitterBufferMs,
		decodeMs:       decodeMs,
		packetLossPct:  packetLossPct,
	})
}

// GetLatencyStats returns the current rolling P50 latency statistics.
func (r *RelaySession) GetLatencyStats() LatencyStats { return r.latency.stats() }
