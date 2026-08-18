package downlink

import "testing"

func TestGivenEvenHistogramWhenP50RequestedThenNearestRankIsUsed(t *testing.T) {
	var histogram writeDurHistogram
	for range 50 {
		histogram.observe(9_000)
	}
	for range 50 {
		histogram.observe(40_000)
	}

	if got := histogram.percentileMs(0.50); got != 0.01 {
		t.Fatalf("p50 = %v ms, want 0.01 ms", got)
	}
	if got := histogram.percentileMs(0.95); got != 0.05 {
		t.Fatalf("p95 = %v ms, want 0.05 ms", got)
	}
}
