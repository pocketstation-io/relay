package downlink

import "sync/atomic"

// SenderStats holds per-subscriber delivery counters.
// All fields are accessed atomically so any goroutine may read a snapshot
// while the forwardLoop goroutine writes (LAW 11).
type SenderStats struct {
	PacketsSent         atomic.Uint64
	BytesSent           atomic.Uint64
	WriteErrCount       atomic.Uint64
	NackCount           atomic.Uint64
	TwccFeedbackCount   atomic.Uint64 // TransportLayerCC packets received
	ReceiverReportCount atomic.Uint64
	RttLastUs           atomic.Int64  // microseconds; -1 if unknown
	JitterLastUs        atomic.Int64  // microseconds
	FractionLostLast    atomic.Uint32 // raw RTCP fraction 0-255
	AbsCapturePatched   atomic.Uint64
	// Write-duration percentiles live in Downlink.writeHist (lock-free histogram).
	// They are computed on demand in Snapshot() and are not stored here.
}
