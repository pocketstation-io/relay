package session

import (
	"sort"
	"sync"
)

// latencyWindowSizeCount is the rolling window depth for P50 percentile
// computation (spec §13.4).
const latencyWindowSizeCount = 100

// LatencyStats holds aggregated per-segment latency percentiles for a
// RelaySession. All duration fields are P50 medians over the last
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
	samples []latencySample
	head    int
	full    bool
}

func newLatencyStore() *latencyStore {
	return &latencyStore{samples: make([]latencySample, latencyWindowSizeCount)}
}

func (ls *latencyStore) record(sample latencySample) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.samples[ls.head] = sample
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

	captureMs := make([]float64, count)
	encodeMs := make([]float64, count)
	relayRttMs := make([]float64, count)
	jitterBufferMs := make([]float64, count)
	decodeMs := make([]float64, count)
	packetLossPctSum := 0.0
	for index := 0; index < count; index++ {
		sample := ls.samples[index]
		captureMs[index] = sample.captureMs
		encodeMs[index] = sample.encodeMs
		relayRttMs[index] = sample.relayRttMs
		jitterBufferMs[index] = sample.jitterBufferMs
		decodeMs[index] = sample.decodeMs
		packetLossPctSum += sample.packetLossPct
	}
	return LatencyStats{
		CaptureP50Ms:      percentile50(captureMs),
		EncodeP50Ms:       percentile50(encodeMs),
		RelayRttP50Ms:     percentile50(relayRttMs),
		JitterBufferP50Ms: percentile50(jitterBufferMs),
		DecodeP50Ms:       percentile50(decodeMs),
		PacketLossPct:     packetLossPctSum / float64(count),
		SampleCount:       count,
	}
}

func percentile50(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	midpointIndex := len(values) / 2
	if len(values)%2 == 0 {
		return (values[midpointIndex-1] + values[midpointIndex]) / 2
	}
	return values[midpointIndex]
}
