package downlink

import (
	"math"
	"sync/atomic"
	"time"
)

var cadenceSpacingBucketMaxNs = [...]int64{
	1 * int64(time.Millisecond),
	5 * int64(time.Millisecond),
	10 * int64(time.Millisecond),
	15 * int64(time.Millisecond),
	18 * int64(time.Millisecond),
	22 * int64(time.Millisecond),
	25 * int64(time.Millisecond),
	30 * int64(time.Millisecond),
	40 * int64(time.Millisecond),
	50 * int64(time.Millisecond),
	60 * int64(time.Millisecond),
	70 * int64(time.Millisecond),
	80 * int64(time.Millisecond),
	100 * int64(time.Millisecond),
	150 * int64(time.Millisecond),
	250 * int64(time.Millisecond),
}

type cadenceSpacingHistogram struct {
	counts [len(cadenceSpacingBucketMaxNs)]atomic.Uint64
}

func (h *cadenceSpacingHistogram) observe(spacingNs int64) {
	for i := 0; i < len(cadenceSpacingBucketMaxNs)-1; i++ {
		if spacingNs < cadenceSpacingBucketMaxNs[i] {
			h.counts[i].Add(1)
			return
		}
	}
	h.counts[len(h.counts)-1].Add(1)
}

func (h *cadenceSpacingHistogram) percentileMs(percentile float64) float64 {
	var total uint64
	for i := range h.counts {
		total += h.counts[i].Load()
	}
	if total == 0 {
		return 0
	}
	target := max(uint64(math.Ceil(float64(total)*percentile)), 1)
	var cumulative uint64
	for i := range h.counts {
		cumulative += h.counts[i].Load()
		if cumulative >= target {
			return float64(cadenceSpacingBucketMaxNs[i]) / float64(time.Millisecond)
		}
	}
	return float64(cadenceSpacingBucketMaxNs[len(cadenceSpacingBucketMaxNs)-1]) / float64(time.Millisecond)
}
