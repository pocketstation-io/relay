package session

import (
	"errors"
	"time"

	"github.com/pion/rtp"
)

// ErrRoomFull is returned when a RelaySession reached its subscription capacity.
var ErrRoomFull = errors.New("relay_session: reached maximum subscription capacity")

type subscriptionEntry struct {
	subscriberID string
	busID        BusID
	subscription BusSubscription
}

// PacketWriter receives one RTP packet for a subscriber transport.
type PacketWriter interface {
	WriteRTP(pkt *rtp.Packet) error
}

// SourceIdentity identifies one publisher attachment. Generation changes on
// reconnect even when a publisher reuses its SSRC.
type SourceIdentity struct {
	BusID      string
	Generation uint64
	SSRC       uint32
}

// BusSubscription is the Session-owned delivery contract. Transport packages
// implement pacing, telemetry, and shutdown behind this boundary.
type BusSubscription interface {
	WriteRTPWithSource(
		pkt *rtp.Packet,
		captureTime time.Time,
		captureTimeKnown bool,
		source SourceIdentity,
	) error
	RequiresPacketCopy() bool
	StopForwarding()
	Snapshot() SubscriptionSnapshot
}

type directBusSubscription struct {
	id    string
	inner PacketWriter
}

func (subscription *directBusSubscription) WriteRTPWithSource(
	pkt *rtp.Packet,
	_ time.Time,
	_ bool,
	_ SourceIdentity,
) error {
	return subscription.inner.WriteRTP(pkt)
}

func (*directBusSubscription) RequiresPacketCopy() bool { return false }
func (*directBusSubscription) StopForwarding()          {}

func (subscription *directBusSubscription) Snapshot() SubscriptionSnapshot {
	return SubscriptionSnapshot{
		SubscriberID: subscription.id,
		Mode:         "direct",
	}
}

// SubscriptionSnapshot is a point-in-time view of one BusSubscription's telemetry.
// Serialises to JSON for the GET /v1/sessions/{id}/media-debug endpoint.
//
// All duration fields use milliseconds. Values of -1.0 indicate "not yet
// measured" (e.g. RTT before the first RTCP Receiver Report arrives).
//
// BusID is filled by RelaySession.SubscriptionSnapshots(), because the
// transport implementation is bus-agnostic.
type SubscriptionSnapshot struct {
	SubscriberID string `json:"subscriber_id"`
	BusID        string `json:"bus_id"`
	Mode         string `json:"downlink_mode"`

	PacketsSent   uint64 `json:"packets_sent"`
	BytesSent     uint64 `json:"bytes_sent"`
	WriteErrCount uint64 `json:"write_err_count"`

	// Write-duration estimates from the lock-free histogram. Zero means no
	// packet write has been observed yet.
	WriteP50Ms float64 `json:"write_p50_ms"`
	WriteP95Ms float64 `json:"write_p95_ms"`

	// RTCP Receiver Report fields. -1.0 = not yet received.
	ReceiverReportCount    uint64  `json:"receiver_report_count"`
	ReceiverReportRttMs    float64 `json:"receiver_report_rtt_ms"`
	ReceiverReportJitterMs float64 `json:"receiver_report_jitter_ms"`
	FractionLostLast       uint8   `json:"fraction_lost_last"` // raw RTCP 0-255

	// RTCP feedback counters.
	NackCount         uint64 `json:"nack_count"`
	TwccFeedbackCount uint64 `json:"twcc_feedback_count"`

	PacerEnqueuedCount                uint64  `json:"pacer_enqueued_count"`
	PacerSentCount                    uint64  `json:"pacer_sent_count"`
	PacerQueueFullDrops               uint64  `json:"pacer_queue_full_drops"`
	PacerStaleDrops                   uint64  `json:"pacer_stale_drops"`
	PacerQueueDepth                   uint64  `json:"pacer_queue_depth"`
	PacerQueuePeak                    uint64  `json:"pacer_queue_peak"`
	PacerMaxQueueAgeMs                float64 `json:"pacer_max_queue_age_ms"`
	PacerQueueAgeP95Ms                float64 `json:"pacer_queue_age_p95_ms"`
	PacerLastSpacingMs                float64 `json:"pacer_last_spacing_ms"`
	PacerSpacingP50Ms                 float64 `json:"pacer_spacing_p50_ms"`
	PacerSpacingP95Ms                 float64 `json:"pacer_spacing_p95_ms"`
	PacerMaxSpacingMs                 float64 `json:"pacer_max_spacing_ms"`
	PacerMaxTimerWaitMs               float64 `json:"pacer_max_timer_wait_ms"`
	PacerMaxTimerOversleepMs          float64 `json:"pacer_max_timer_oversleep_ms"`
	PacerMaxWriterDurationMs          float64 `json:"pacer_max_writer_duration_ms"`
	PacerWriterBlockedCount           uint64  `json:"pacer_writer_blocked_count"`
	PacerPaddingPacketCount           uint64  `json:"pacer_padding_packet_count"`
	SourcePaddingStrippedCount        uint64  `json:"source_padding_stripped_count"`
	PacerLatePacketDropCount          uint64  `json:"pacer_late_packet_drop_count"`
	PacerLatePaddingDropCount         uint64  `json:"pacer_late_padding_drop_count"`
	PacerLateMediaDropCount           uint64  `json:"pacer_late_media_drop_count"`
	PacerGapTimeoutCount              uint64  `json:"pacer_gap_timeout_count"`
	PacerRecoveryPacketCount          uint64  `json:"pacer_recovery_packet_count"`
	PacerTimelineResets               uint64  `json:"pacer_timeline_resets"`
	NackQueueDrops                    uint64  `json:"nack_queue_drops"`
	NackCacheHits                     uint64  `json:"nack_cache_hits"`
	NackCacheMisses                   uint64  `json:"nack_cache_misses"`
	NackThrottled                     uint64  `json:"nack_throttled"`
	RetransmitSentCount               uint64  `json:"retransmit_sent_count"`
	RetransmitErrorCount              uint64  `json:"retransmit_error_count"`
	OutputSequenceDiscontinuityCount  uint64  `json:"output_sequence_discontinuity_count"`
	OutputTimestampDiscontinuityCount uint64  `json:"output_timestamp_discontinuity_count"`
	OutputMaxSequenceDelta            uint64  `json:"output_max_sequence_delta"`
	OutputMaxTimestampDeltaSamples    uint64  `json:"output_max_timestamp_delta_samples"`

	// AbsCaptureTimeNegotiated is true when the abs-capture-time header extension
	// URI was present in the SDP answer. This reflects SDP negotiation only;
	// actual packet patching requires capture-time lineage from the CLI (future work).
	AbsCaptureTimeNegotiated   bool   `json:"abs_capture_time_negotiated"`
	AbsCaptureTimePatchedCount uint64 `json:"abs_capture_time_patched_count"`

	SenderReportKnown           bool   `json:"sender_report_known"`
	SenderReportSSRC            uint32 `json:"sender_report_ssrc"`
	SenderReportNTPTime         uint64 `json:"sender_report_ntp_time"`
	SenderReportRTPTime         uint32 `json:"sender_report_rtp_time"`
	SenderReportObservedAtNs    int64  `json:"sender_report_observed_at_ns"`
	SenderReportCount           uint64 `json:"sender_report_count"`
	SenderReportClockNormalized bool   `json:"sender_report_clock_normalized"`
}

func (relaySession *RelaySession) deliver(
	busID BusID,
	packet *rtp.Packet,
	errorCounts map[string]int,
	deadSubscriptions *[]string,
) {
	relaySession.deliverWithSource(
		busID,
		packet,
		time.Time{},
		false,
		SourceIdentity{},
		errorCounts,
		deadSubscriptions,
	)
}

func (relaySession *RelaySession) deliverWithSource(
	busID BusID,
	packet *rtp.Packet,
	captureTime time.Time,
	captureTimeKnown bool,
	source SourceIdentity,
	errorCounts map[string]int,
	deadSubscriptions *[]string,
) {
	const maxConsecutiveErrors = 5

	subscriptions := *relaySession.subscriptions.Load()
	*deadSubscriptions = (*deadSubscriptions)[:0]

	for _, entry := range subscriptions {
		if entry.busID != busID && entry.busID != BusMix {
			continue
		}
		outgoing := packet
		if entry.subscription.RequiresPacketCopy() {
			header := packet.Header.Clone()
			outgoing = &rtp.Packet{
				Header:      header,
				Payload:     packet.Payload,
				PaddingSize: packet.PaddingSize,
			}
		}
		if err := entry.subscription.WriteRTPWithSource(
			outgoing,
			captureTime,
			captureTimeKnown,
			source,
		); err != nil {
			errorCounts[entry.subscriberID]++
			if errorCounts[entry.subscriberID] >= maxConsecutiveErrors {
				*deadSubscriptions = append(*deadSubscriptions, entry.subscriberID)
			}
		} else {
			delete(errorCounts, entry.subscriberID)
		}
	}

	for _, subscriberID := range *deadSubscriptions {
		relaySession.RemoveSubscription(subscriberID)
		delete(errorCounts, subscriberID)
		relaySession.evictionsTotal.Add(1)
	}
}

func (relaySession *RelaySession) AddSubscription(
	subscriberID string,
	busID BusID,
	writer PacketWriter,
) error {
	return relaySession.AddBusSubscription(subscriberID, busID, &directBusSubscription{
		id:    subscriberID,
		inner: writer,
	})
}

func (relaySession *RelaySession) AddBusSubscription(
	subscriberID string,
	busID BusID,
	subscription BusSubscription,
) error {
	relaySession.subscriptionsMu.Lock()
	defer relaySession.subscriptionsMu.Unlock()

	current := *relaySession.subscriptions.Load()
	if relaySession.maxSubscriptions > 0 && len(current) >= relaySession.maxSubscriptions {
		return ErrRoomFull
	}
	next := make([]*subscriptionEntry, len(current)+1)
	copy(next, current)
	next[len(current)] = &subscriptionEntry{
		subscriberID: subscriberID,
		busID:        busID,
		subscription: subscription,
	}
	relaySession.subscriptions.Store(&next)
	relaySession.notifyStateChange()
	return nil
}

func (relaySession *RelaySession) SubscriptionSnapshots() []SubscriptionSnapshot {
	subscriptions := *relaySession.subscriptions.Load()
	snapshots := make([]SubscriptionSnapshot, 0, len(subscriptions))
	for _, entry := range subscriptions {
		snapshot := entry.subscription.Snapshot()
		snapshot.BusID = string(entry.busID)
		snapshots = append(snapshots, snapshot)
	}
	return snapshots
}

func (relaySession *RelaySession) RemoveSubscription(subscriberID string) {
	relaySession.subscriptionsMu.Lock()
	current := *relaySession.subscriptions.Load()
	next := make([]*subscriptionEntry, 0, len(current))
	removed := make([]*subscriptionEntry, 0, 1)
	for _, entry := range current {
		if entry.subscriberID == subscriberID {
			removed = append(removed, entry)
			continue
		}
		next = append(next, entry)
	}
	relaySession.subscriptions.Store(&next)
	relaySession.subscriptionsMu.Unlock()

	for _, entry := range removed {
		entry.subscription.StopForwarding()
	}
	if len(removed) > 0 {
		relaySession.notifyStateChange()
	}
}

func (relaySession *RelaySession) TryReserveSlot(maximum int) bool {
	if maximum <= 0 {
		relaySession.pendingSlots.Add(1)
		return true
	}
	for {
		pending := relaySession.pendingSlots.Load()
		active := int32(relaySession.SubscriptionCount())
		if int(pending+active) >= maximum {
			return false
		}
		if relaySession.pendingSlots.CompareAndSwap(pending, pending+1) {
			return true
		}
	}
}

func (relaySession *RelaySession) ReleaseSlot() { relaySession.pendingSlots.Add(-1) }

func (relaySession *RelaySession) SubscriptionCount() int {
	return len(*relaySession.subscriptions.Load())
}

func (relaySession *RelaySession) SubscriptionEvictionsTotal() uint64 {
	return relaySession.evictionsTotal.Load()
}
