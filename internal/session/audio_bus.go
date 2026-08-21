package session

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/media/clocklineage"
)

// AudioBus is one named forwarding lane: one SourceSession → N BusSubscriptions.
//
// Each bus is independent: it has its own source, its own reconnect timer, and
// its own forwardLoop goroutine. Packets are written to the parent RelaySession's
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
	graphID string // back-reference to parent RelaySession.ID for logging

	PacketCount     atomic.Uint64
	ByteCount       atomic.Uint64
	PacketDropCount atomic.Uint64

	// lastRTPAtNanos is the UnixNano timestamp of the most recently forwarded
	// RTP packet (0 = none yet). The media watchdog reads it to tell a live but
	// silent source from a healthy one — WebSocket liveness ≠ media liveness.
	lastRTPAtNanos   atomic.Int64
	sourceTimeline   atomic.Pointer[clocklineage.Timeline]
	sourceGeneration atomic.Uint64

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
func (b *AudioBus) SetSource(
	src SourceSession,
	closer func(),
	deliver func(*rtp.Packet, uint64),
	onDetached func(),
) {
	newLoopDone := make(chan struct{})
	generation := b.sourceGeneration.Add(1)

	b.sourceMu.Lock()
	prevCloser := b.sourceCloser
	prevDone := b.loopDone
	b.source = src
	b.sourceCloser = closer
	b.loopDone = newLoopDone
	b.sourceMu.Unlock()
	var timeline *clocklineage.Timeline
	if source, ok := src.(clockLineageSource); ok {
		timeline = source.ClockLineage()
	}
	b.sourceTimeline.Store(timeline)

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
	go b.forwardLoop(src, generation, newLoopDone, deliver, onDetached)
}

// SourceActive reports whether a source is currently attached to this bus.
func (b *AudioBus) SourceActive() bool {
	b.sourceMu.Lock()
	defer b.sourceMu.Unlock()
	return b.source != nil
}

// CaptureTime maps an RTP timestamp onto the active publisher's NTP clock.
// The timeline pointer is replaced on every source attach, so reconnects cannot
// reuse the previous publisher's clock mapping.
func (b *AudioBus) CaptureTime(rtpTimestamp uint32, now time.Time) (time.Time, bool) {
	timeline := b.sourceTimeline.Load()
	if timeline == nil {
		return time.Time{}, false
	}
	return timeline.CaptureTime(rtpTimestamp, now, 48_000, 10*time.Second)
}

// SourceClockSnapshot is the bounded publisher Sender Report state exposed by
// the media-debug endpoint for one AudioBus.
type SourceClockSnapshot struct {
	BusID        BusID  `json:"bus_id"`
	Known        bool   `json:"known"`
	SSRC         uint32 `json:"ssrc"`
	NTPTime      uint64 `json:"ntp_time"`
	RTPTime      uint32 `json:"rtp_time"`
	ObservedAtNs int64  `json:"observed_at_ns"`
	ReportCount  uint64 `json:"report_count"`
}

func (b *AudioBus) SourceClockSnapshot() SourceClockSnapshot {
	result := SourceClockSnapshot{BusID: b.ID}
	timeline := b.sourceTimeline.Load()
	if timeline == nil {
		return result
	}
	snapshot, ok := timeline.Snapshot()
	result.Known = ok
	result.SSRC = snapshot.SSRC
	result.NTPTime = snapshot.NTPTime
	result.RTPTime = snapshot.RTPTime
	result.ObservedAtNs = snapshot.ObservedAtNs
	result.ReportCount = snapshot.ReportCount
	return result
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
func (b *AudioBus) forwardLoop(
	src SourceSession,
	generation uint64,
	loopDone chan struct{},
	deliver func(*rtp.Packet, uint64),
	onDetached func(),
) {
	defer close(loopDone)
	defer func() {
		b.sourceMu.Lock()
		if b.source == src {
			b.source = nil
			b.sourceCloser = nil
			b.sourceTimeline.Store(nil)
		}
		b.sourceMu.Unlock()
		if onDetached != nil {
			onDetached()
		}

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

		deliver(pkt, generation)
		txTs := time.Now().UnixNano() // A3/A4 measurement: egress timestamp
		b.packetLog.record(PacketLogEntry{
			Seq:          pkt.Header.SequenceNumber,
			RtpTsSamples: pkt.Header.Timestamp,
			PayloadType:  pkt.Header.PayloadType,
			Ssrc:         pkt.Header.SSRC,
			PayloadBytes: uint64(len(pkt.Payload)),
			Padding:      pkt.Header.Padding,
			PaddingBytes: pkt.PaddingSize,
			RxTsNs:       rxTs,
			TxTsNs:       txTs,
		})
	}
}

func (b *AudioBus) SourceGeneration() uint64 { return b.sourceGeneration.Load() }
