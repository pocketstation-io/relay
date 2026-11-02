package downlink

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/session"
)

const (
	defaultPacerCapacity    = 32
	defaultPacerMaxQueueAge = 120 * time.Millisecond
	defaultPacerInitialWait = 10 * time.Millisecond
	defaultPacerReorderWait = 60 * time.Millisecond
	// Locked Pion receives at most 1460 bytes; WSS Opus payloads are <=1275.
	// Oversized packets are dropped, never truncated or allocated a fallback.
	maxPacerPacketBytes = 1460
	// Both RFC8285 formats need at least two wire bytes per parsed entry.
	maxPacerHeaderExtensionsCount = maxPacerPacketBytes / 2
	// The worker can retain 33 pending packets while the queue holds 32 more.
	// Successful writes remain owned by the existing 512-entry repair cache.
	ownedPacerPacketCount = retransmissionCachePackets + 2*defaultPacerCapacity + 1
	pacerAdmissionClosed  = uint64(1) << 63
)

type packetWriter func(*rtp.Packet) error
type translatedPacketObserver func(*rtp.Packet, time.Time, bool)

// Pacer schedules packets for one subscriber. Enqueue must not block.
type Pacer interface {
	Enqueue(pkt *rtp.Packet) error
	EnqueueSource(pkt *rtp.Packet, source session.SourceIdentity, captureTime time.Time, captureTimeKnown bool) error
	HandleNACK(seq uint16)
	SetRTT(rtt time.Duration)
	Stop()
	Snapshot() PacerSnapshot
	RequiresPacketCopy() bool
}

// PacerSnapshot is a point-in-time view of bounded downlink worker state.
type PacerSnapshot struct {
	Mode                              string
	EnqueuedCount                     uint64
	SentCount                         uint64
	QueueFullDrops                    uint64
	StaleDrops                        uint64
	QueueDepth                        uint64
	QueuePeak                         uint64
	MaxQueueAgeNs                     uint64
	QueueAgeP95Ms                     float64
	LastSpacingNs                     uint64
	SpacingP50Ms                      float64
	SpacingP95Ms                      float64
	MaxSpacingNs                      uint64
	MaxTimerWaitNs                    uint64
	MaxTimerOversleepNs               uint64
	MaxWriterDurationNs               uint64
	WriterBlockedCount                uint64
	PaddingPacketCount                uint64
	SourcePaddingStrippedCount        uint64
	LatePacketDropCount               uint64
	LatePaddingDropCount              uint64
	LateMediaDropCount                uint64
	GapTimeoutCount                   uint64
	RecoveryPacketCount               uint64
	TimelineResets                    uint64
	NackQueueDrops                    uint64
	NackCacheHits                     uint64
	NackCacheMisses                   uint64
	NackThrottled                     uint64
	RetransmitSentCount               uint64
	RetransmitErrorCount              uint64
	OutputSequenceDiscontinuityCount  uint64
	OutputTimestampDiscontinuityCount uint64
	OutputMaxSequenceDelta            uint64
	OutputMaxTimestampDeltaSamples    uint64
}

type passThroughPacer struct {
	write packetWriter
}

func newPassThroughPacer(write packetWriter) Pacer {
	return &passThroughPacer{write: write}
}

func (p *passThroughPacer) Enqueue(pkt *rtp.Packet) error { return p.write(pkt) }
func (p *passThroughPacer) EnqueueSource(pkt *rtp.Packet, _ session.SourceIdentity, _ time.Time, _ bool) error {
	return p.write(pkt)
}
func (p *passThroughPacer) HandleNACK(uint16)    {}
func (p *passThroughPacer) SetRTT(time.Duration) {}
func (p *passThroughPacer) Stop()                {}
func (p *passThroughPacer) Snapshot() PacerSnapshot {
	return PacerSnapshot{Mode: "passthrough"}
}
func (p *passThroughPacer) RequiresPacketCopy() bool { return false }

type pacedPacket struct {
	packet           *rtp.Packet
	owned            *ownedPacerPacket
	enqueuedAt       time.Time
	extSeq           int64
	source           session.SourceIdentity
	captureTime      time.Time
	captureTimeKnown bool
}

// ownedPacerPacket owns media bytes until write/drop/cache eviction. The header
// is transferred from the caller: Session.Header.Clone already owns its CSRC
// and extension payloads. Only the borrowed media payload is copied here.
type ownedPacerPacket struct {
	claimed atomic.Bool
	packet  rtp.Packet
	payload [maxPacerPacketBytes]byte
}

func (packet *ownedPacerPacket) release() {
	packet.packet = rtp.Packet{}
	packet.claimed.Store(false)
}

func (item pacedPacket) release() {
	if item.owned != nil {
		item.owned.release()
	}
}

func withinPacerPacketBounds(packet *rtp.Packet) bool {
	if packet == nil || len(packet.CSRC) > 15 || len(packet.Extensions) > maxPacerHeaderExtensionsCount {
		return false
	}
	// Pion1.8.11 treats unknown profiles as one opaque extension and indexes
	// Extensions[0] inside MarshalSize. Reject the malformed empty form first.
	if packet.Extension && packet.ExtensionProfile != 0xbede && packet.ExtensionProfile != 0x1000 && len(packet.Extensions) == 0 {
		return false
	}
	return packet.MarshalSize() <= maxPacerPacketBytes
}

// AudioCadencePacer smooths clustered Opus arrivals according to their RTP
// timestamps. Its queue is bounded and enqueue is non-blocking. One worker owns
// the timer and writer, preserving packet order without locking the writer.
type AudioCadencePacer struct {
	write             packetWriter
	observeTranslated translatedPacketObserver
	paceMedia         bool
	reorderWait       time.Duration
	queue             chan pacedPacket
	nacks             chan uint16
	done              chan struct{}
	workerDone        chan struct{}
	stopOnce          sync.Once
	stopped           atomic.Bool
	enqueueState      atomic.Uint64
	nextOwnedSlot     atomic.Uint32
	ownedPackets      []ownedPacerPacket
	maxQueueAge       time.Duration

	enqueuedCount                     atomic.Uint64
	completedCount                    atomic.Uint64
	sentCount                         atomic.Uint64
	queueFullDrops                    atomic.Uint64
	staleDrops                        atomic.Uint64
	queuePeak                         atomic.Uint64
	maxQueueAgeNs                     atomic.Uint64
	queueAgeHist                      cadenceSpacingHistogram
	lastSpacingNs                     atomic.Uint64
	maxSpacingNs                      atomic.Uint64
	spacingHist                       cadenceSpacingHistogram
	maxTimerWaitNs                    atomic.Uint64
	maxTimerOversleepNs               atomic.Uint64
	maxWriterDurationNs               atomic.Uint64
	writerBlockedCount                atomic.Uint64
	paddingPacketCount                atomic.Uint64
	sourcePaddingStrippedCount        atomic.Uint64
	latePacketDropCount               atomic.Uint64
	latePaddingDropCount              atomic.Uint64
	lateMediaDropCount                atomic.Uint64
	gapTimeoutCount                   atomic.Uint64
	recoveryPacketCount               atomic.Uint64
	timelineResets                    atomic.Uint64
	nackQueueDrops                    atomic.Uint64
	nackCacheHits                     atomic.Uint64
	nackCacheMisses                   atomic.Uint64
	nackThrottled                     atomic.Uint64
	retransmitSentCount               atomic.Uint64
	retransmitErrorCount              atomic.Uint64
	outputSequenceDiscontinuityCount  atomic.Uint64
	outputTimestampDiscontinuityCount atomic.Uint64
	outputMaxSequenceDelta            atomic.Uint64
	outputMaxTimestampDeltaSamples    atomic.Uint64
	retransmissionCache               retransmissionCache
	writeScratch                      pacerWritePacket
}

func newAudioCadencePacer(write packetWriter) *AudioCadencePacer {
	return newAudioPacer(write, nil, true, defaultPacerReorderWait)
}

func newAudioForwardPacer(write packetWriter) *AudioCadencePacer {
	return newAudioPacer(write, nil, false, 0)
}

func newAudioForwardPacerWithObserver(write packetWriter, observer translatedPacketObserver) *AudioCadencePacer {
	return newAudioPacer(write, observer, false, 0)
}

func newAudioPacer(
	write packetWriter,
	observer translatedPacketObserver,
	paceMedia bool,
	reorderWait time.Duration,
) *AudioCadencePacer {
	p := &AudioCadencePacer{
		write:             write,
		observeTranslated: observer,
		paceMedia:         paceMedia,
		reorderWait:       reorderWait,
		queue:             make(chan pacedPacket, defaultPacerCapacity),
		nacks:             make(chan uint16, defaultPacerCapacity),
		done:              make(chan struct{}),
		workerDone:        make(chan struct{}),
		maxQueueAge:       defaultPacerMaxQueueAge,
		ownedPackets:      make([]ownedPacerPacket, ownedPacerPacketCount),
	}
	go p.run()
	return p
}

func (p *AudioCadencePacer) HandleNACK(seq uint16) {
	if p.stopped.Load() {
		return
	}
	select {
	case p.nacks <- seq:
	default:
		p.nackQueueDrops.Add(1)
	}
}

func (p *AudioCadencePacer) SetRTT(rtt time.Duration) {
	p.retransmissionCache.setRTT(rtt)
}

func (p *AudioCadencePacer) Enqueue(pkt *rtp.Packet) error {
	return p.EnqueueSource(pkt, session.SourceIdentity{}, time.Time{}, false)
}

func (p *AudioCadencePacer) EnqueueSource(
	pkt *rtp.Packet,
	source session.SourceIdentity,
	captureTime time.Time,
	captureTimeKnown bool,
) error {
	if !p.beginEnqueue() {
		return nil
	}
	defer p.enqueueState.Add(^uint64(0))
	return p.enqueueOwned(pkt, source, captureTime, captureTimeKnown)
}

// beginEnqueue admits concurrent AudioBus producers without a lock. Stop closes
// this same atomic gate and joins admitted producers before the final drain.
func (p *AudioCadencePacer) beginEnqueue() bool {
	for state := p.enqueueState.Load(); state&pacerAdmissionClosed == 0; state = p.enqueueState.Load() {
		if p.enqueueState.CompareAndSwap(state, state+1) {
			return true
		}
	}
	return false
}

// enqueueOwned receives a subscriber-owned header and a borrowed media payload.
// Header storage must not be mutated after admission; RequiresPacketCopy tells
// the Session to provide its existing deep Header.Clone before this boundary.
func (p *AudioCadencePacer) enqueueOwned(
	pkt *rtp.Packet,
	source session.SourceIdentity,
	captureTime time.Time,
	captureTimeKnown bool,
) error {
	if !withinPacerPacketBounds(pkt) {
		return errPacerPacketBounds
	}
	var owned *ownedPacerPacket
	startSlot := int(p.nextOwnedSlot.Add(1) % uint32(len(p.ownedPackets)))
	for offsetCount := range p.ownedPackets {
		candidate := &p.ownedPackets[(startSlot+offsetCount)%len(p.ownedPackets)]
		if candidate.claimed.CompareAndSwap(false, true) {
			owned = candidate
			break
		}
	}
	if owned == nil {
		p.queueFullDrops.Add(1)
		return nil
	}
	owned.packet = *pkt
	copy(owned.payload[:], pkt.Payload)
	owned.packet.Payload = owned.payload[:len(pkt.Payload)]
	owned.packet.Raw = nil // Deprecated input-wire view is never retained.
	item := pacedPacket{
		packet:           &owned.packet,
		owned:            owned,
		enqueuedAt:       time.Now(),
		source:           source,
		captureTime:      captureTime,
		captureTimeKnown: captureTimeKnown,
	}
	select {
	case p.queue <- item:
		enqueued := p.enqueuedCount.Add(1)
		depth := enqueued - min(enqueued, p.completedCount.Load())
		for peak := p.queuePeak.Load(); depth > peak; peak = p.queuePeak.Load() {
			if p.queuePeak.CompareAndSwap(peak, depth) {
				break
			}
		}
	default:
		p.queueFullDrops.Add(1)
		item.release()
	}
	return nil
}

func (p *AudioCadencePacer) Stop() {
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		p.enqueueState.Or(pacerAdmissionClosed)
		// Control-path join only: enqueue itself remains nonblocking.
		for p.enqueueState.Load()&^pacerAdmissionClosed != 0 {
			runtime.Gosched()
		}
		close(p.done)
		<-p.workerDone
		for {
			select {
			case item := <-p.queue:
				item.release()
				p.completedCount.Add(1)
			default:
				return
			}
		}
	})
}

func (p *AudioCadencePacer) Snapshot() PacerSnapshot {
	enqueued := p.enqueuedCount.Load()
	completed := p.completedCount.Load()
	return PacerSnapshot{
		Mode:                              p.mode(),
		EnqueuedCount:                     p.enqueuedCount.Load(),
		SentCount:                         p.sentCount.Load(),
		QueueFullDrops:                    p.queueFullDrops.Load(),
		StaleDrops:                        p.staleDrops.Load(),
		QueueDepth:                        enqueued - min(enqueued, completed),
		QueuePeak:                         p.queuePeak.Load(),
		MaxQueueAgeNs:                     p.maxQueueAgeNs.Load(),
		QueueAgeP95Ms:                     p.queueAgeHist.percentileMs(0.95),
		LastSpacingNs:                     p.lastSpacingNs.Load(),
		SpacingP50Ms:                      p.spacingHist.percentileMs(0.50),
		SpacingP95Ms:                      p.spacingHist.percentileMs(0.95),
		MaxSpacingNs:                      p.maxSpacingNs.Load(),
		MaxTimerWaitNs:                    p.maxTimerWaitNs.Load(),
		MaxTimerOversleepNs:               p.maxTimerOversleepNs.Load(),
		MaxWriterDurationNs:               p.maxWriterDurationNs.Load(),
		WriterBlockedCount:                p.writerBlockedCount.Load(),
		PaddingPacketCount:                p.paddingPacketCount.Load(),
		SourcePaddingStrippedCount:        p.sourcePaddingStrippedCount.Load(),
		LatePacketDropCount:               p.latePacketDropCount.Load(),
		LatePaddingDropCount:              p.latePaddingDropCount.Load(),
		LateMediaDropCount:                p.lateMediaDropCount.Load(),
		GapTimeoutCount:                   p.gapTimeoutCount.Load(),
		RecoveryPacketCount:               p.recoveryPacketCount.Load(),
		TimelineResets:                    p.timelineResets.Load(),
		NackQueueDrops:                    p.nackQueueDrops.Load(),
		NackCacheHits:                     p.nackCacheHits.Load(),
		NackCacheMisses:                   p.nackCacheMisses.Load(),
		NackThrottled:                     p.nackThrottled.Load(),
		RetransmitSentCount:               p.retransmitSentCount.Load(),
		RetransmitErrorCount:              p.retransmitErrorCount.Load(),
		OutputSequenceDiscontinuityCount:  p.outputSequenceDiscontinuityCount.Load(),
		OutputTimestampDiscontinuityCount: p.outputTimestampDiscontinuityCount.Load(),
		OutputMaxSequenceDelta:            p.outputMaxSequenceDelta.Load(),
		OutputMaxTimestampDeltaSamples:    p.outputMaxTimestampDeltaSamples.Load(),
	}
}

func (p *AudioCadencePacer) mode() string {
	if p.paceMedia {
		return "experimental-cadence"
	}
	return "forward"
}

func (*AudioCadencePacer) RequiresPacketCopy() bool { return true }
