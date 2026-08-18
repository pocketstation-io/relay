package downlink

import (
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
	enqueuedAt       time.Time
	extSeq           int64
	source           session.SourceIdentity
	captureTime      time.Time
	captureTimeKnown bool
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
	if p.stopped.Load() {
		return nil
	}
	item := pacedPacket{
		packet:           pkt,
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
	}
	return nil
}

func (p *AudioCadencePacer) Stop() {
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		close(p.done)
		<-p.workerDone
		for {
			select {
			case <-p.queue:
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
