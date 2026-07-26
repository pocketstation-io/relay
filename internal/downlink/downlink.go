// Package downlink wraps a BusSubscription with per-subscriber telemetry:
// write-duration tracking via a lock-free histogram, RTCP feedback, and
// RTP header extension patching.
//
// Downlink satisfies the graph.BusSubscription interface so it can be stored
// directly in subscriptionEntry without an extra adapter.
package downlink

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/clocklineage"
)

// writeDurBucketCount is the number of log-scale buckets in the write-duration histogram.
const writeDurBucketCount = 8

// writeDurBucketMaxNs defines the exclusive upper bound of each histogram bucket in nanoseconds.
// Bucket 7 is the overflow bucket (≥ 10 ms); its value is used as a display
// ceiling (100 ms) when computing percentiles.
var writeDurBucketMaxNs = [writeDurBucketCount]int64{
	10_000,      // bucket 0: < 10 μs
	50_000,      // bucket 1: < 50 μs
	100_000,     // bucket 2: < 100 μs
	500_000,     // bucket 3: < 500 μs
	1_000_000,   // bucket 4: < 1 ms
	5_000_000,   // bucket 5: < 5 ms
	10_000_000,  // bucket 6: < 10 ms
	100_000_000, // bucket 7: ≥ 10 ms (saturated; display ceiling = 100 ms)
}

// writeDurHistogram tracks write-duration samples using lock-free atomic bucket counters.
// The hot-path cost of observe() is one conditional scan + one atomic.Add: no mutex, no allocation.
// CODE_PROTOCOL LAW 11 (atomics on hot path) and LAW 15 (no lock/alloc in forwarding path) are met.
type writeDurHistogram struct {
	counts [writeDurBucketCount]atomic.Uint64
}

// observe records a write-duration sample. Hot-path safe.
func (h *writeDurHistogram) observe(durNs int64) {
	for i := 0; i < writeDurBucketCount-1; i++ {
		if durNs < writeDurBucketMaxNs[i] {
			h.counts[i].Add(1)
			return
		}
	}
	h.counts[writeDurBucketCount-1].Add(1)
}

// percentileMs returns the estimated p-th percentile write duration in milliseconds.
// p must be in [0.0, 1.0]. Returns 0.0 if no samples have been observed.
//
// Called only from Snapshot() (off hot path). No alloc, no lock.
// Resolution is the bucket boundary: e.g. P95 = 0.5 ms means "fell in the 500 μs bucket."
func (h *writeDurHistogram) percentileMs(p float64) float64 {
	var counts [writeDurBucketCount]uint64
	total := uint64(0)
	for i := range counts {
		counts[i] = h.counts[i].Load()
		total += counts[i]
	}
	if total == 0 {
		return 0.0
	}
	target := max(uint64(math.Ceil(float64(total)*p)), 1)
	var cum uint64
	for i, count := range counts {
		cum += count
		if cum >= target {
			return float64(writeDurBucketMaxNs[i]) / 1_000_000.0
		}
	}
	return float64(writeDurBucketMaxNs[writeDurBucketCount-1]) / 1_000_000.0
}

// Downlink wraps a BusSubscription with per-subscriber telemetry.
//
// WriteRTP may be called concurrently from multiple forwardLoop goroutines
// when the subscriber uses BusMix (receives from all buses). writeHist uses
// lock-free atomics; no mutex is acquired on the write path.
// Snapshot reads all stats atomically and is safe from any goroutine.
type Downlink struct {
	id             string
	inner          BusSubscription
	stats          SenderStats
	Extensions     ExtensionMapper
	feedback       *FeedbackReader
	pacer          Pacer
	writeHist      writeDurHistogram
	senderTimeline atomic.Pointer[clocklineage.Timeline]
}

// NewForwardingDownlink is the production downlink contract. It preserves
// ordered single-writer forwarding, reconnect-safe RTP translation, NACK
// recovery, and telemetry without applying a second RTP cadence schedule.
func NewForwardingDownlink(id string, inner BusSubscription, feedback *FeedbackReader) *Downlink {
	d := &Downlink{id: id, inner: inner, feedback: feedback}
	d.pacer = newAudioForwardPacerWithObserver(d.writeRTP, d.observeTranslatedPacket)
	d.stats.RttLastUs.Store(-1)
	return d
}

// NewPassthrough creates a minimal Downlink with no RTCP reader and no
// abs-send-time patching. Used where no RTPSender is available (WHIP ingest,
// unit tests).
func NewPassthrough(id string, inner BusSubscription) *Downlink {
	d := &Downlink{id: id, inner: inner}
	d.pacer = newPassThroughPacer(d.writeRTP)
	d.stats.RttLastUs.Store(-1)
	return d
}

// WriteRTP satisfies graph.BusSubscription.
//
// The inner.WriteRTP call is always lock-free on the hot path.
// Write duration is recorded in the lock-free histogram: one compare+atomic.Add.
func (d *Downlink) WriteRTP(pkt *rtp.Packet) error {
	return d.WriteRTPWithCaptureTime(pkt, time.Time{}, false)
}

// WriteRTPWithCaptureTime attaches genuine publisher clock lineage when it is
// known, then transfers ownership to the configured pacer.
func (d *Downlink) WriteRTPWithCaptureTime(pkt *rtp.Packet, captureTime time.Time, known bool) error {
	return d.WriteRTPWithSource(pkt, captureTime, known, SourceIdentity{})
}

// WriteRTPWithSource carries the publisher attachment identity into the
// single-writer downlink worker for reconnect-safe RTP translation.
func (d *Downlink) WriteRTPWithSource(
	pkt *rtp.Packet,
	captureTime time.Time,
	known bool,
	source SourceIdentity,
) error {
	if known && d.Extensions.PatchAbsCaptureTime(pkt, captureTime) {
		d.stats.AbsCapturePatched.Add(1)
	}
	return d.pacer.EnqueueSource(pkt, source, captureTime, known)
}

func (d *Downlink) observeTranslatedPacket(pkt *rtp.Packet, captureTime time.Time, known bool) {
	if !known {
		return
	}
	if timeline := d.senderTimeline.Load(); timeline != nil {
		timeline.ObserveCapture(pkt.Timestamp, captureTime, time.Now())
	}
}

// HandleNACK schedules a cached subscriber-hop packet for retransmission.
func (d *Downlink) HandleNACK(seq uint16) { d.pacer.HandleNACK(seq) }

// writeRTP applies negotiated per-subscriber extensions and writes one packet.
// It is invoked by the configured Pacer and remains allocation-free when no
// extension was negotiated.
func (d *Downlink) writeRTP(pkt *rtp.Packet) error {
	d.Extensions.PatchAbsSendTime(pkt)

	t0 := time.Now()
	err := d.inner.WriteRTP(pkt)
	d.writeHist.observe(time.Since(t0).Nanoseconds())

	if err != nil {
		d.stats.WriteErrCount.Add(1)
		return err
	}
	d.stats.PacketsSent.Add(1)
	d.stats.BytesSent.Add(uint64(pkt.MarshalSize()))
	return nil
}

// ConfigureExtensions records the header-extension IDs negotiated in the
// subscriber leg's local SDP answer. It must run before the downlink is exposed
// to the relay's forwarding loop.
func (d *Downlink) ConfigureExtensions(localSDP string) {
	d.Extensions.SetAbsSendTimeID(DiscoverAbsSendTimeID(localSDP))
	d.Extensions.SetAbsCaptureTimeID(DiscoverAbsCaptureTimeID(localSDP))
}

// RequiresPacketCopy reports whether WriteRTP mutates the packet header for
// subscriber-specific negotiated state.
func (d *Downlink) RequiresPacketCopy() bool {
	return d.pacer.RequiresPacketCopy() || d.Extensions.MutatesPackets()
}

// Stats returns a pointer to this Downlink's SenderStats so callers can
// direct a FeedbackReader to update the same counters that Snapshot reads.
func (d *Downlink) Stats() *SenderStats {
	return &d.stats
}

// ObserveRTT stores a valid sender-side RTT sample reported by Pion's remote
// inbound RTP stats. RTT is derived from RTCP SR/RR timing per RFC 3550.
func (d *Downlink) ObserveRTT(rtt time.Duration) {
	if rtt < 0 {
		return
	}
	d.stats.RttLastUs.Store(rtt.Microseconds())
	d.pacer.SetRTT(rtt)
}

// SetFeedback attaches a FeedbackReader after construction.
// Must be called before any concurrent WriteRTP calls.
func (d *Downlink) SetFeedback(fr *FeedbackReader) {
	d.feedback = fr
}

// SetSenderTimeline attaches Pion's outgoing Sender Report timeline for this
// subscriber. It must be called before the downlink is published to the graph.
func (d *Downlink) SetSenderTimeline(timeline *clocklineage.Timeline) {
	d.senderTimeline.Store(timeline)
}

// Stop shuts down the FeedbackReader goroutine.
// Safe to call even if no FeedbackReader was attached.
//
// Only call Stop() after the underlying peer connection has been closed,
// or after ReadRTCP() is guaranteed to return (e.g. due to an error).
// Calling Stop() with an open, idle PC will block until ReadRTCP returns.
func (d *Downlink) Stop() {
	d.pacer.Stop()
	if d.feedback != nil {
		d.feedback.Stop()
	}
}

// StopPacer terminates the serialized forwarding worker without waiting for
// RTCP. The feedback reader exits when its PeerConnection closes.
func (d *Downlink) StopPacer() {
	d.pacer.Stop()
}

// usToMs converts microseconds to milliseconds as float64.
// Returns -1.0 if the raw value is -1 (sentinel for "unknown").
func usToMs(us int64) float64 {
	if us == -1 {
		return -1.0
	}
	return float64(us) / 1_000.0
}

// Snapshot returns a point-in-time view of this Downlink's telemetry.
// BusID is left empty here; relay_session.DownlinkSnapshots() fills it from
// the subscriptionEntry so that Downlink itself stays bus-agnostic.
// All reads are atomic; safe to call from any goroutine.
func (d *Downlink) Snapshot() DownlinkSnapshot {
	pacer := d.pacer.Snapshot()
	snapshot := DownlinkSnapshot{
		SubscriberID:                      d.id,
		Mode:                              pacer.Mode,
		PacketsSent:                       d.stats.PacketsSent.Load(),
		BytesSent:                         d.stats.BytesSent.Load(),
		WriteErrCount:                     d.stats.WriteErrCount.Load(),
		WriteP50Ms:                        d.writeHist.percentileMs(0.50),
		WriteP95Ms:                        d.writeHist.percentileMs(0.95),
		ReceiverReportCount:               d.stats.ReceiverReportCount.Load(),
		ReceiverReportRttMs:               usToMs(d.stats.RttLastUs.Load()),
		ReceiverReportJitterMs:            usToMs(d.stats.JitterLastUs.Load()),
		FractionLostLast:                  uint8(d.stats.FractionLostLast.Load()),
		NackCount:                         d.stats.NackCount.Load(),
		TwccFeedbackCount:                 d.stats.TwccFeedbackCount.Load(),
		AbsCaptureTimeNegotiated:          d.Extensions.HasAbsCaptureTime(),
		AbsCaptureTimePatchedCount:        d.stats.AbsCapturePatched.Load(),
		PacerEnqueuedCount:                pacer.EnqueuedCount,
		PacerSentCount:                    pacer.SentCount,
		PacerQueueFullDrops:               pacer.QueueFullDrops,
		PacerStaleDrops:                   pacer.StaleDrops,
		PacerQueueDepth:                   pacer.QueueDepth,
		PacerQueuePeak:                    pacer.QueuePeak,
		PacerMaxQueueAgeMs:                float64(pacer.MaxQueueAgeNs) / float64(time.Millisecond),
		PacerQueueAgeP95Ms:                pacer.QueueAgeP95Ms,
		PacerLastSpacingMs:                float64(pacer.LastSpacingNs) / float64(time.Millisecond),
		PacerSpacingP50Ms:                 pacer.SpacingP50Ms,
		PacerSpacingP95Ms:                 pacer.SpacingP95Ms,
		PacerMaxSpacingMs:                 float64(pacer.MaxSpacingNs) / float64(time.Millisecond),
		PacerMaxTimerWaitMs:               float64(pacer.MaxTimerWaitNs) / float64(time.Millisecond),
		PacerMaxTimerOversleepMs:          float64(pacer.MaxTimerOversleepNs) / float64(time.Millisecond),
		PacerMaxWriterDurationMs:          float64(pacer.MaxWriterDurationNs) / float64(time.Millisecond),
		PacerWriterBlockedCount:           pacer.WriterBlockedCount,
		PacerPaddingPacketCount:           pacer.PaddingPacketCount,
		SourcePaddingStrippedCount:        pacer.SourcePaddingStrippedCount,
		PacerLatePacketDropCount:          pacer.LatePacketDropCount,
		PacerLatePaddingDropCount:         pacer.LatePaddingDropCount,
		PacerLateMediaDropCount:           pacer.LateMediaDropCount,
		PacerGapTimeoutCount:              pacer.GapTimeoutCount,
		PacerRecoveryPacketCount:          pacer.RecoveryPacketCount,
		PacerTimelineResets:               pacer.TimelineResets,
		NackQueueDrops:                    pacer.NackQueueDrops,
		NackCacheHits:                     pacer.NackCacheHits,
		NackCacheMisses:                   pacer.NackCacheMisses,
		NackThrottled:                     pacer.NackThrottled,
		RetransmitSentCount:               pacer.RetransmitSentCount,
		RetransmitErrorCount:              pacer.RetransmitErrorCount,
		OutputSequenceDiscontinuityCount:  pacer.OutputSequenceDiscontinuityCount,
		OutputTimestampDiscontinuityCount: pacer.OutputTimestampDiscontinuityCount,
		OutputMaxSequenceDelta:            pacer.OutputMaxSequenceDelta,
		OutputMaxTimestampDeltaSamples:    pacer.OutputMaxTimestampDeltaSamples,
	}
	if timeline := d.senderTimeline.Load(); timeline != nil {
		report, known := timeline.Snapshot()
		snapshot.SenderReportKnown = known
		snapshot.SenderReportSSRC = report.SSRC
		snapshot.SenderReportNTPTime = report.NTPTime
		snapshot.SenderReportRTPTime = report.RTPTime
		snapshot.SenderReportObservedAtNs = report.ObservedAtNs
		snapshot.SenderReportCount = report.ReportCount
		snapshot.SenderReportClockNormalized = report.Normalized
	}
	return snapshot
}
