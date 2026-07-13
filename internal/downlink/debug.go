package downlink

// DownlinkSnapshot is a point-in-time view of a Downlink's telemetry.
// Serialises to JSON for the GET /v1/sessions/{id}/media-debug endpoint.
//
// All duration fields use milliseconds. Values of -1.0 indicate "not yet
// measured" (e.g. RTT before the first RTCP Receiver Report arrives).
//
// BusID is filled by relay_session.DownlinkSnapshots(), not by Snapshot(),
// because Downlink itself is bus-agnostic.
type DownlinkSnapshot struct {
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

	PacerEnqueuedCount        uint64  `json:"pacer_enqueued_count"`
	PacerSentCount            uint64  `json:"pacer_sent_count"`
	PacerQueueFullDrops       uint64  `json:"pacer_queue_full_drops"`
	PacerStaleDrops           uint64  `json:"pacer_stale_drops"`
	PacerQueueDepth           uint64  `json:"pacer_queue_depth"`
	PacerQueuePeak            uint64  `json:"pacer_queue_peak"`
	PacerMaxQueueAgeMs        float64 `json:"pacer_max_queue_age_ms"`
	PacerQueueAgeP95Ms        float64 `json:"pacer_queue_age_p95_ms"`
	PacerLastSpacingMs        float64 `json:"pacer_last_spacing_ms"`
	PacerSpacingP50Ms         float64 `json:"pacer_spacing_p50_ms"`
	PacerSpacingP95Ms         float64 `json:"pacer_spacing_p95_ms"`
	PacerMaxSpacingMs         float64 `json:"pacer_max_spacing_ms"`
	PacerMaxTimerWaitMs       float64 `json:"pacer_max_timer_wait_ms"`
	PacerMaxTimerOversleepMs  float64 `json:"pacer_max_timer_oversleep_ms"`
	PacerMaxWriterDurationMs  float64 `json:"pacer_max_writer_duration_ms"`
	PacerWriterBlockedCount   uint64  `json:"pacer_writer_blocked_count"`
	PacerPaddingPacketCount   uint64  `json:"pacer_padding_packet_count"`
	PacerLatePacketDropCount  uint64  `json:"pacer_late_packet_drop_count"`
	PacerLatePaddingDropCount uint64  `json:"pacer_late_padding_drop_count"`
	PacerLateMediaDropCount   uint64  `json:"pacer_late_media_drop_count"`
	PacerGapTimeoutCount      uint64  `json:"pacer_gap_timeout_count"`
	PacerRecoveryPacketCount  uint64  `json:"pacer_recovery_packet_count"`
	PacerTimelineResets       uint64  `json:"pacer_timeline_resets"`
	NackQueueDrops            uint64  `json:"nack_queue_drops"`
	NackCacheHits             uint64  `json:"nack_cache_hits"`
	NackCacheMisses           uint64  `json:"nack_cache_misses"`
	NackThrottled             uint64  `json:"nack_throttled"`
	RetransmitSentCount       uint64  `json:"retransmit_sent_count"`
	RetransmitErrorCount      uint64  `json:"retransmit_error_count"`

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

// DebugSnapshot is the full response body for the media-debug endpoint.
type DebugSnapshot struct {
	SessionID string             `json:"session_id"`
	Downlinks []DownlinkSnapshot `json:"downlinks"`
}
