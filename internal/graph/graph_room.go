// Package graph implements the GraphRoom — the core forwarding unit of the
// PocketStation relay (v3.0). A GraphRoom owns a set of named AudioBuses,
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
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
)

// latencyWindowSizeCount is the rolling window depth for P50 percentile
// computation (spec §13.4).
const latencyWindowSizeCount = 100

// packetLogWindowSize is the number of per-packet entries retained per bus.
// At 50 pkt/s this covers 20 seconds of audio history.
const packetLogWindowSize = 1000

// LatencyStats holds aggregated per-segment latency percentiles for a
// GraphRoom. All duration fields are P50 medians over the last
// latencyWindowSizeCount reports received from source and subscriber clients.
type LatencyStats struct {
	CaptureP50Ms      float64 `json:"capture_p50_ms"`
	EncodeP50Ms       float64 `json:"encode_p50_ms"`
	RelayRttP50Ms     float64 `json:"relay_rtt_p50_ms"`
	JitterBufferP50Ms float64 `json:"jitter_buffer_p50_ms"`
	DecodeP50Ms       float64 `json:"decode_p50_ms"`
	PacketLossPct     float64 `json:"packet_loss_pct"`
	SampleCount       int     `json:"sample_count"`
}

type latencySample struct {
	captureMs      float64
	encodeMs       float64
	relayRttMs     float64
	jitterBufferMs float64
	decodeMs       float64
	packetLossPct  float64
}

// latencyStore accumulates latency samples in a fixed-size ring and computes
// rolling P50 percentiles. Safe for concurrent use.
type latencyStore struct {
	mu      sync.Mutex
	samples []latencySample // ring buffer; grows up to latencyWindowSizeCount
	head    int             // next write position
	full    bool            // true once the ring has wrapped at least once
}

func newLatencyStore() *latencyStore {
	return &latencyStore{samples: make([]latencySample, latencyWindowSizeCount)}
}

func (ls *latencyStore) record(s latencySample) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.samples[ls.head] = s
	ls.head = (ls.head + 1) % latencyWindowSizeCount
	if ls.head == 0 {
		ls.full = true
	}
}

func (ls *latencyStore) stats() LatencyStats {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	count := ls.head
	if ls.full {
		count = latencyWindowSizeCount
	}
	if count == 0 {
		return LatencyStats{}
	}

	capture := make([]float64, count)
	encode := make([]float64, count)
	rtt := make([]float64, count)
	jitter := make([]float64, count)
	decode := make([]float64, count)
	lossSum := 0.0
	for i := 0; i < count; i++ {
		s := ls.samples[i]
		capture[i] = s.captureMs
		encode[i] = s.encodeMs
		rtt[i] = s.relayRttMs
		jitter[i] = s.jitterBufferMs
		decode[i] = s.decodeMs
		lossSum += s.packetLossPct
	}
	return LatencyStats{
		CaptureP50Ms:      p50(capture),
		EncodeP50Ms:       p50(encode),
		RelayRttP50Ms:     p50(rtt),
		JitterBufferP50Ms: p50(jitter),
		DecodeP50Ms:       p50(decode),
		PacketLossPct:     lossSum / float64(count),
		SampleCount:       count,
	}
}

func p50(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sort.Float64s(v)
	mid := len(v) / 2
	if len(v)%2 == 0 {
		return (v[mid-1] + v[mid]) / 2
	}
	return v[mid]
}

// PacketLogEntry holds the relay-side timestamps for one forwarded RTP packet.
// RxTsNs is stamped immediately after ReadRTP returns (ingress).
// TxTsNs is stamped immediately after deliver completes (egress).
// TxTsNs - RxTsNs = relay-internal forwarding latency for that packet.
type PacketLogEntry struct {
	Seq    uint16 `json:"seq"`
	RxTsNs int64  `json:"rx_ts_ns"`
	TxTsNs int64  `json:"tx_ts_ns"`
}

// packetLogStore accumulates PacketLogEntry values in a fixed-size ring.
// Safe for concurrent use.
type packetLogStore struct {
	mu      sync.Mutex
	entries []PacketLogEntry
	head    int
	full    bool
}

func newPacketLogStore() *packetLogStore {
	return &packetLogStore{entries: make([]PacketLogEntry, packetLogWindowSize)}
}

func (pl *packetLogStore) record(e PacketLogEntry) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	pl.entries[pl.head] = e
	pl.head = (pl.head + 1) % packetLogWindowSize
	if pl.head == 0 {
		pl.full = true
	}
}

// last returns the most recent limit entries in chronological order.
// Returns an empty slice (not nil) when no entries exist yet.
func (pl *packetLogStore) last(limit int) []PacketLogEntry {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	count := pl.head
	if pl.full {
		count = packetLogWindowSize
	}
	if limit > count {
		limit = count
	}
	if limit == 0 {
		return []PacketLogEntry{}
	}

	out := make([]PacketLogEntry, limit)
	start := (pl.head - limit + packetLogWindowSize*2) % packetLogWindowSize
	for i := 0; i < limit; i++ {
		out[i] = pl.entries[(start+i)%packetLogWindowSize]
	}
	return out
}

// BusID names a forwarding lane within a GraphRoom.
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

// defaultInactivityTimeout is the time a GraphRoom with no active bus source
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
	ErrNoSource = errors.New("graph_room: bus has no source")
	// ErrRoomFull is returned by AddSubscription when the room is at capacity.
	ErrRoomFull = errors.New("graph_room: reached maximum subscription capacity")
)

// SourceSession is the readable side of a live audio stream on a bus.
// Using an interface decouples GraphRoom from *webrtc.TrackRemote and allows
// the forward loop to be unit-tested without a live Pion setup.
type SourceSession interface {
	ReadRTP() (*rtp.Packet, error)
}

// BusSubscription is the write-side of an audio stream delivered to a subscriber.
// *webrtc.TrackLocalStaticRTP satisfies this interface directly.
type BusSubscription interface {
	WriteRTP(pkt *rtp.Packet) error
}

// subscriptionEntry pairs a subscriber ID with its BusSubscription and the
// BusID it selected. busID == BusMix means "all buses" (relay.out("mix")
// semantics); any other value receives only that bus's RTP (spec §7).
type subscriptionEntry struct {
	subscriberID string
	busID        BusID
	sub          BusSubscription
}

// AudioBus is one named forwarding lane: one SourceSession → N BusSubscriptions.
//
// Each bus is independent: it has its own source, its own reconnect timer, and
// its own forwardLoop goroutine. Packets are written to the parent GraphRoom's
// subscription list, tagged with this bus's ID, so BusMix subscribers receive
// from all buses on a single track while per-bus subscribers receive only
// their selected bus (relay.out("mix") semantics for Phase 1; spec §7).
//
// Invariants:
//   - sourceMu guards source, sourceCloser, loopDone.
//   - sourceEverConnected is set to true by the first SetSource call.
//   - reconnectTimer fires if a source disconnects and no new PUBLISH arrives.
type AudioBus struct {
	ID      BusID
	Role    BusRole
	graphID string // back-reference to parent GraphRoom.ID for logging

	PacketCount     atomic.Uint64
	ByteCount       atomic.Uint64
	PacketDropCount atomic.Uint64

	// lastRTPAtNanos is the UnixNano timestamp of the most recently forwarded
	// RTP packet (0 = none yet). The media watchdog reads it to tell a live but
	// silent source from a healthy one — WebSocket liveness ≠ media liveness.
	lastRTPAtNanos atomic.Int64

	// packetLog is a fixed-size ring of per-packet relay timestamps used to
	// compute A3 (WebRTC→relay) and A4 (relay→subscriber) §11.3 budgets.
	packetLog *packetLogStore

	sourceMu     sync.Mutex
	source       SourceSession
	sourceCloser func()
	loopDone     chan struct{} // closed by forwardLoop on exit; nil if no loop running

	timerMu             sync.Mutex
	inactivityTimer     *time.Timer
	inactivityTimeout   time.Duration
	reconnectTimer      *time.Timer
	reconnectWindow     time.Duration
	sourceEverConnected atomic.Bool

	closeOnce sync.Once
	done      chan struct{}
}

func newAudioBus(id BusID, role BusRole, graphID string, inactivityTimeout, reconnectWindow time.Duration) *AudioBus {
	b := &AudioBus{
		ID:                id,
		Role:              role,
		graphID:           graphID,
		done:              make(chan struct{}),
		inactivityTimeout: inactivityTimeout,
		reconnectWindow:   reconnectWindow,
		packetLog:         newPacketLogStore(),
	}
	b.timerMu.Lock()
	b.inactivityTimer = time.AfterFunc(inactivityTimeout, func() { b.close() })
	b.timerMu.Unlock()
	return b
}

// SetSource sets the audio source and starts the forward loop.
// If a previous source is active (ICE restart), it is closed first.
// deliver is called for each received RTP packet to write to subscribers.
func (b *AudioBus) SetSource(src SourceSession, closer func(), deliver func(*rtp.Packet)) {
	newLoopDone := make(chan struct{})

	b.sourceMu.Lock()
	prevCloser := b.sourceCloser
	prevDone := b.loopDone
	b.source = src
	b.sourceCloser = closer
	b.loopDone = newLoopDone
	b.sourceMu.Unlock()

	if prevCloser != nil {
		prevCloser()
	}
	if prevDone != nil {
		<-prevDone
	}

	b.timerMu.Lock()
	b.inactivityTimer.Stop()
	if b.reconnectTimer != nil {
		b.reconnectTimer.Stop()
	}
	b.timerMu.Unlock()

	b.sourceEverConnected.Store(true)
	// Seed the watchdog clock so a freshly-attached source is not reported as
	// stalled before its first packet arrives.
	b.lastRTPAtNanos.Store(time.Now().UnixNano())
	go b.forwardLoop(src, newLoopDone, deliver)
}

// SourceActive reports whether a source is currently attached to this bus.
func (b *AudioBus) SourceActive() bool {
	b.sourceMu.Lock()
	defer b.sourceMu.Unlock()
	return b.source != nil
}

// LastRTPAge returns how long since this bus last forwarded an RTP packet.
// Returns the maximum duration if no packet has ever been forwarded.
func (b *AudioBus) LastRTPAge() time.Duration {
	last := b.lastRTPAtNanos.Load()
	if last == 0 {
		return time.Duration(math.MaxInt64)
	}
	return time.Since(time.Unix(0, last))
}

// Stalled reports whether the bus has an attached source but has not forwarded
// an RTP packet within threshold. A bus with no source is idle, not stalled —
// this is the media-plane liveness a WebSocket ping cannot detect (§6).
func (b *AudioBus) Stalled(threshold time.Duration) bool {
	return b.SourceActive() && b.LastRTPAge() > threshold
}

// BusHealth is a media-plane snapshot of one AudioBus, distinguishing a live
// source gone silent (Stalled) from a healthy or idle one (Corrected Audit §6).
type BusHealth struct {
	BusID        BusID  `json:"bus_id"`
	Role         string `json:"role"`
	SourceActive bool   `json:"source_active"`
	Stalled      bool   `json:"stalled"`
	LastRTPAgeMs int64  `json:"last_rtp_age_ms"` // -1 when no packet has ever flowed
	PacketCount  uint64 `json:"packet_count"`
	ByteCount    uint64 `json:"byte_count"`
	DropCount    uint64 `json:"drop_count"`
}

// Health snapshots this bus, marking it stalled if its source is live but no
// RTP has flowed within threshold.
func (b *AudioBus) Health(threshold time.Duration) BusHealth {
	age := b.LastRTPAge()
	ageMs := int64(-1)
	if age != time.Duration(math.MaxInt64) {
		ageMs = age.Milliseconds()
	}
	return BusHealth{
		BusID:        b.ID,
		Role:         b.Role.String(),
		SourceActive: b.SourceActive(),
		Stalled:      b.Stalled(threshold),
		LastRTPAgeMs: ageMs,
		PacketCount:  b.PacketCount.Load(),
		ByteCount:    b.ByteCount.Load(),
		DropCount:    b.PacketDropCount.Load(),
	}
}

func (b *AudioBus) close() {
	b.closeOnce.Do(func() {
		b.timerMu.Lock()
		b.inactivityTimer.Stop()
		if b.reconnectTimer != nil {
			b.reconnectTimer.Stop()
		}
		b.timerMu.Unlock()
		close(b.done)
	})
}

// forwardLoop reads RTP from src and calls deliver for each packet until src
// errors or the bus is closed. loopDone is closed on exit.
//
// LockOSThread pins this goroutine to a single OS thread for CPU cache
// locality on the hot forwarding path (same technique as OpenAI's Go relay).
func (b *AudioBus) forwardLoop(src SourceSession, loopDone chan struct{}, deliver func(*rtp.Packet)) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer close(loopDone)
	defer func() {
		b.sourceMu.Lock()
		if b.source == src {
			b.source = nil
			b.sourceCloser = nil
		}
		b.sourceMu.Unlock()

		select {
		case <-b.done:
		default:
			b.timerMu.Lock()
			b.reconnectTimer = time.AfterFunc(b.reconnectWindow, func() { b.close() })
			b.timerMu.Unlock()
		}
	}()

	for {
		pkt, err := src.ReadRTP()
		if err != nil {
			return
		}
		rxTs := time.Now().UnixNano() // A3/A4 measurement: ingress timestamp
		select {
		case <-b.done:
			return
		default:
		}

		b.PacketCount.Add(1)
		b.ByteCount.Add(uint64(len(pkt.Payload)))
		b.lastRTPAtNanos.Store(rxTs)

		// Normalise padding before forwarding (radio-glitch fix: pion strips
		// padding bytes but leaves Header.Padding=true; receivers then read
		// the last payload byte as padding length and discard the packet).
		pkt.Header.Padding = false
		pkt.PaddingSize = 0

		deliver(pkt)
		txTs := time.Now().UnixNano() // A3/A4 measurement: egress timestamp
		b.packetLog.record(PacketLogEntry{
			Seq:    pkt.Header.SequenceNumber,
			RxTsNs: rxTs,
			TxTsNs: txTs,
		})
	}
}

// GraphRoom owns the set of AudioBuses for one relay session.
// Each subscriber selects one bus, or BusMix to receive RTP from all buses
// (relay.out("mix") semantics). Buses are created lazily on first PUBLISH.
//
// Invariants:
//   - busesMu guards the buses map for writes; reads inside forwardLoop go
//     through the room's atomic subscription pointer (no room lock on hot path).
//   - subscriptions is copy-on-write behind subscriptionsMu.
//   - done is closed by Close and signals all bus forwardLoops to stop.
type GraphRoom struct {
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

// New returns an open GraphRoom with default timeouts.
func New(id string) *GraphRoom {
	return newWithTimeouts(id, defaultInactivityTimeout, defaultReconnectWindow)
}

func newWithTimeout(id string, timeout time.Duration) *GraphRoom {
	return newWithTimeouts(id, timeout, defaultReconnectWindow)
}

func newWithTimeouts(id string, inactivityTimeout, reconnectWindow time.Duration) *GraphRoom {
	r := &GraphRoom{
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
func (r *GraphRoom) checkExpiry() {
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
func (r *GraphRoom) isInactive() bool {
	if r.SourceActive() {
		return false
	}
	if r.SubscriptionCount() > 0 {
		return false
	}
	return !r.hasRecentRTP(r.inactivityTimeout)
}

// hasRecentRTP reports whether any bus forwarded an RTP packet within window.
func (r *GraphRoom) hasRecentRTP(window time.Duration) bool {
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
func (r *GraphRoom) GetOrCreateBus(id BusID, role BusRole) *AudioBus {
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
func (r *GraphRoom) SetSource(busID BusID, role BusRole, src SourceSession, closer func()) {
	bus := r.GetOrCreateBus(busID, role)
	// Per-bus scratch buffers captured in the closure. Each forwardLoop
	// goroutine is serial, so no lock needed. Avoids per-packet map/slice
	// allocation and GC pressure on the hot forwarding path.
	errCounts := make(map[string]int, 8)
	deadSubs := make([]string, 0, 8)

	bus.SetSource(src, closer, func(pkt *rtp.Packet) {
		r.deliver(busID, pkt, errCounts, &deadSubs)
	})
}

// deliver is the hot-path function passed to each AudioBus.forwardLoop. It
// writes pkt to every subscriber that selected this bus (entry.busID == busID)
// or the virtual mix (entry.busID == BusMix). A BusMix subscriber therefore
// receives RTP from all buses — byte-identical to the room-wide behavior.
//
// Allocation-free on the fast path: errCounts and deadSubs are pre-allocated
// per-bus in the SetSource closure and reused across calls.
func (r *GraphRoom) deliver(busID BusID, pkt *rtp.Packet, errCounts map[string]int, deadSubs *[]string) {
	const maxConsecutiveErrors = 5

	ls := *r.subscriptions.Load()

	for k := range errCounts {
		delete(errCounts, k)
	}
	*deadSubs = (*deadSubs)[:0]

	for _, e := range ls {
		if e.busID != busID && e.busID != BusMix {
			continue
		}
		if wErr := e.sub.WriteRTP(pkt); wErr != nil {
			errCounts[e.subscriberID]++
			if errCounts[e.subscriberID] >= maxConsecutiveErrors {
				*deadSubs = append(*deadSubs, e.subscriberID)
			}
		} else {
			errCounts[e.subscriberID] = 0
		}
	}

	for _, id := range *deadSubs {
		r.RemoveSubscription(id)
		slog.Warn("evicted dead subscription", "graph_room_id", r.ID, "subscriber_id", id)
	}
}

// AddSubscription registers sub under subscriberID for the named busID. Pass
// BusMix to receive RTP from every bus (relay.out("mix") semantics); pass a
// concrete BusID (e.g. "voice") to receive only that bus (spec §7).
// Returns ErrRoomFull when r.maxSubscriptions > 0 and capacity is reached.
func (r *GraphRoom) AddSubscription(subscriberID string, busID BusID, sub BusSubscription) error {
	r.subscriptionsMu.Lock()
	defer r.subscriptionsMu.Unlock()

	old := *r.subscriptions.Load()
	if r.maxSubscriptions > 0 && len(old) >= r.maxSubscriptions {
		return ErrRoomFull
	}
	next := make([]*subscriptionEntry, len(old)+1)
	copy(next, old)
	next[len(old)] = &subscriptionEntry{subscriberID: subscriberID, busID: busID, sub: sub}
	r.subscriptions.Store(&next)
	return nil
}

// RemoveSubscription deregisters the subscription for subscriberID. No-op if absent.
func (r *GraphRoom) RemoveSubscription(subscriberID string) {
	r.subscriptionsMu.Lock()
	defer r.subscriptionsMu.Unlock()

	old := *r.subscriptions.Load()
	next := make([]*subscriptionEntry, 0, len(old))
	for _, e := range old {
		if e.subscriberID != subscriberID {
			next = append(next, e)
		}
	}
	r.subscriptions.Store(&next)
}

// TryReserveSlot atomically reserves a pending subscription slot.
// Returns false if active+pending subscriptions would reach or exceed max.
// max ≤ 0 means unlimited.
func (r *GraphRoom) TryReserveSlot(max int) bool {
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
func (r *GraphRoom) ReleaseSlot() { r.pendingSlots.Add(-1) }

// SubscriptionCount returns the current number of registered subscriptions.
func (r *GraphRoom) SubscriptionCount() int { return len(*r.subscriptions.Load()) }

// SourceActive reports whether any bus currently has an active source.
func (r *GraphRoom) SourceActive() bool {
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
func (r *GraphRoom) BusSourceActive(id BusID) bool {
	r.busesMu.RLock()
	b, ok := r.buses[id]
	r.busesMu.RUnlock()
	if !ok {
		return false
	}
	return b.SourceActive()
}

// PacketStats returns aggregate packet and byte counts across all buses.
func (r *GraphRoom) PacketStats() (packets, bytes, dropped uint64) {
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
func (r *GraphRoom) BusHealthList(threshold time.Duration) []BusHealth {
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
func (r *GraphRoom) BusPacketLog(busID BusID, limit int) []PacketLogEntry {
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
func (r *GraphRoom) AnyBusStalled(threshold time.Duration) bool {
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
func (r *GraphRoom) Close() {
	r.closeOnce.Do(func() {
		close(r.done)
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
func (r *GraphRoom) SetKey(keyBase64 string) {
	r.keyMu.Lock()
	defer r.keyMu.Unlock()
	r.currentKey = keyBase64
}

// GetKey returns the opaque base64 SFrame key, or "" if none has been received.
func (r *GraphRoom) GetKey() string {
	r.keyMu.RLock()
	defer r.keyMu.RUnlock()
	return r.currentKey
}

// RecordLatency adds a latency sample to the room's rolling window.
func (r *GraphRoom) RecordLatency(captureMs, encodeMs, relayRttMs, jitterBufferMs, decodeMs, packetLossPct float64) {
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
func (r *GraphRoom) GetLatencyStats() LatencyStats { return r.latency.stats() }

// SessionRegistry manages the set of active GraphRooms.
// (Named SessionRegistry, not Manager, per CODE_PROTOCOL LAW 18.)
type SessionRegistry struct {
	mu    sync.RWMutex
	rooms map[string]*GraphRoom

	inactivityTimeout time.Duration
	reconnectWindow   time.Duration
	maxSubscriptions  int
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
	return &SessionRegistry{
		rooms:             make(map[string]*GraphRoom),
		inactivityTimeout: inactivity,
		reconnectWindow:   reconnect,
		maxSubscriptions:  cfg.MaxSubscriptions,
	}
}

// GetOrCreate returns the GraphRoom for id, creating it if absent.
func (reg *SessionRegistry) GetOrCreate(id string) *GraphRoom {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if r := reg.rooms[id]; r != nil {
		return r
	}
	r := newWithTimeouts(id, reg.inactivityTimeout, reg.reconnectWindow)
	r.maxSubscriptions = reg.maxSubscriptions
	reg.rooms[id] = r
	return r
}

// Get returns the GraphRoom for id and whether it was found.
func (reg *SessionRegistry) Get(id string) (*GraphRoom, bool) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	r, ok := reg.rooms[id]
	return r, ok
}

// Delete closes and removes the GraphRoom for id. No-op if absent.
func (reg *SessionRegistry) Delete(id string) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if r, ok := reg.rooms[id]; ok {
		r.Close()
		delete(reg.rooms, id)
	}
}

// RoomCount returns the number of active GraphRooms.
func (reg *SessionRegistry) RoomCount() int {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	return len(reg.rooms)
}

// PacketStats returns aggregate forwarded and dropped packet counts across
// all currently active GraphRooms.
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

// CloseAll closes every active GraphRoom and removes it from the registry.
func (reg *SessionRegistry) CloseAll() {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	for id, r := range reg.rooms {
		r.Close()
		delete(reg.rooms, id)
	}
}

// SessionSummary is a snapshot of a GraphRoom's observable state.
// Used by GET /v1/channels (spec §3.1).
type SessionSummary struct {
	SessionID         string    `json:"session_id"`
	SubscriptionCount int       `json:"subscription_count"`
	SourceActive      bool      `json:"source_active"`
	CreatedAt         time.Time `json:"created_at"`
	Public            bool      `json:"public"`
}

// ListPublic returns a summary of every GraphRoom created with Public==true.
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
