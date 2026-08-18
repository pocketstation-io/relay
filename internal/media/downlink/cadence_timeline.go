package downlink

import "time"

const (
	audioClockRateHz                      = uint64(48_000)
	defaultPacerTargetDelay               = 10 * time.Millisecond
	defaultPacerMaxLead                   = 250 * time.Millisecond
	defaultPacerMaxLateness               = 40 * time.Millisecond
	defaultPacerResetSamples              = uint32(audioClockRateHz * 5)
	minimumCadenceSpacingNumeratorRatio   = int64(4)
	minimumCadenceSpacingDenominatorRatio = int64(5)
)

type cadenceTimeline struct {
	initialized    bool
	lastRtpTs      uint32
	lastDueAt      time.Time
	nominalSamples uint32
	maxLead        time.Duration
	maxLateness    time.Duration
	resetSamples   uint32
}

func (t *cadenceTimeline) reset() {
	t.initialized = false
	t.nominalSamples = 0
}

func newCadenceTimeline() cadenceTimeline {
	return cadenceTimeline{
		maxLead:      defaultPacerMaxLead,
		maxLateness:  defaultPacerMaxLateness,
		resetSamples: defaultPacerResetSamples,
	}
}

// dueAt maps RTP media time to a monotonic send time. uint32 subtraction
// intentionally handles timestamp wraparound. Large jumps and stale timelines
// re-anchor immediately instead of accumulating latency.
func (t *cadenceTimeline) dueAt(rtpTs uint32, arrival time.Time) (due time.Time, reset bool, advances bool) {
	if !t.initialized {
		t.initialized = true
		t.lastRtpTs = rtpTs
		t.lastDueAt = arrival.Add(defaultPacerTargetDelay)
		return t.lastDueAt, false, true
	}

	signedDeltaSamples := int32(rtpTs - t.lastRtpTs)
	if signedDeltaSamples <= 0 {
		return arrival, false, false
	}
	deltaSamples := uint32(signedDeltaSamples)
	if deltaSamples > t.resetSamples {
		t.lastRtpTs = rtpTs
		t.lastDueAt = arrival.Add(defaultPacerTargetDelay)
		return t.lastDueAt, true, true
	}
	if t.nominalSamples == 0 {
		t.nominalSamples = deltaSamples
	} else if deltaSamples > t.nominalSamples {
		// A missing ingress packet must remain an RTP sequence/timestamp gap, but
		// it must not create an equal wall-clock hole on the subscriber leg.
		deltaSamples = t.nominalSamples
	}

	due = t.lastDueAt.Add(time.Duration(uint64(deltaSamples) * uint64(time.Second) / audioClockRateHz))
	if due.Sub(arrival) > t.maxLead {
		t.lastRtpTs = rtpTs
		t.lastDueAt = arrival.Add(defaultPacerTargetDelay)
		return t.lastDueAt, true, true
	}
	if arrival.Sub(due) > t.maxLateness {
		t.lastRtpTs = rtpTs
		t.lastDueAt = arrival
		return arrival, true, true
	}
	t.lastRtpTs = rtpTs
	t.lastDueAt = due
	return due, false, true
}

func minimumCadenceSpacingNs(mediaSpacingNs time.Duration) time.Duration {
	if mediaSpacingNs <= 0 {
		return 0
	}
	// Bound catch-up after a late scheduler wake so the relay can recover
	// media time without turning that recovery into a subscriber-visible burst.
	return mediaSpacingNs * time.Duration(minimumCadenceSpacingNumeratorRatio) /
		time.Duration(minimumCadenceSpacingDenominatorRatio)
}
