package downlink

import (
	"time"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/session"
)

const defaultAudioFrameSamples = uint32(960)

// rtpForwardTranslator preserves source gaps within an attachment while
// rebasing reconnects into one continuous subscriber RTP sequence/timestamp
// space. One forward-pacer worker owns this state.
type rtpForwardTranslator struct {
	initialized         bool
	source              session.SourceIdentity
	sequenceOffset      uint16
	timestampOffset     uint32
	lastInputSequence   uint16
	lastInputTimestamp  uint32
	lastOutputSequence  uint16
	lastOutputTimestamp uint32
	nominalSamples      uint32
	lastArrival         time.Time
}

func (t *rtpForwardTranslator) translate(pkt *rtp.Packet, source session.SourceIdentity) (reset bool) {
	return t.translateAt(pkt, source, time.Time{})
}

func (t *rtpForwardTranslator) translateAt(pkt *rtp.Packet, source session.SourceIdentity, arrival time.Time) (reset bool) {
	if !t.initialized {
		t.initialized = true
		t.source = source
		t.lastInputSequence = pkt.SequenceNumber
		t.lastInputTimestamp = pkt.Timestamp
		t.lastOutputSequence = pkt.SequenceNumber
		t.lastOutputTimestamp = pkt.Timestamp
		t.nominalSamples = defaultAudioFrameSamples
		t.lastArrival = arrival
		return false
	}

	if source != t.source {
		t.source = source
		t.sequenceOffset = t.lastOutputSequence + 1 - pkt.SequenceNumber
		timestampStep := t.nominalSamples
		if timestampStep == 0 {
			timestampStep = defaultAudioFrameSamples
		}
		if !arrival.IsZero() && !t.lastArrival.IsZero() {
			elapsed := arrival.Sub(t.lastArrival)
			if elapsed > 0 {
				elapsedSamples := uint64(elapsed) * 48_000 / uint64(time.Second)
				quantizedSamples := (elapsedSamples + uint64(timestampStep)/2) / uint64(timestampStep) * uint64(timestampStep)
				if quantizedSamples > uint64(timestampStep) && quantizedSamples <= uint64(^uint32(0)) {
					timestampStep = uint32(quantizedSamples)
				}
			}
		}
		t.timestampOffset = t.lastOutputTimestamp + timestampStep - pkt.Timestamp
		pkt.SequenceNumber += t.sequenceOffset
		pkt.Timestamp += t.timestampOffset
		t.lastInputSequence = pkt.SequenceNumber - t.sequenceOffset
		t.lastInputTimestamp = pkt.Timestamp - t.timestampOffset
		t.lastOutputSequence = pkt.SequenceNumber
		t.lastOutputTimestamp = pkt.Timestamp
		t.lastArrival = arrival
		return true
	}

	inputSequence := pkt.SequenceNumber
	inputTimestamp := pkt.Timestamp
	pkt.SequenceNumber += t.sequenceOffset
	pkt.Timestamp += t.timestampOffset

	sequenceDelta := int16(inputSequence - t.lastInputSequence)
	if sequenceDelta > 0 {
		timestampDelta := int32(inputTimestamp - t.lastInputTimestamp)
		if timestampDelta > 0 {
			candidate := uint32(timestampDelta) / uint32(sequenceDelta)
			if candidate > 0 && candidate <= 4_800 {
				t.nominalSamples = candidate
			}
		}
		t.lastInputSequence = inputSequence
		t.lastInputTimestamp = inputTimestamp
		t.lastOutputSequence = pkt.SequenceNumber
		t.lastOutputTimestamp = pkt.Timestamp
		t.lastArrival = arrival
	}
	return false
}

type forwardDecision uint8

const (
	forwardMedia forwardDecision = iota
	dropPadding
	dropLate
)

// sequenceForwarder translates an ordered source-hop RTP stream into a
// contiguous subscriber-hop sequence space. One pacer worker owns it.
type sequenceForwarder struct {
	unwrapper          sequenceUnwrapper
	haveReleased       bool
	lastReleasedExtSeq int64
	haveOutput         bool
	lastOutputSeq      uint16
}

func (f *sequenceForwarder) unwrap(seq uint16) int64 {
	return f.unwrapper.unwrap(seq)
}

func (f *sequenceForwarder) release(pkt *rtp.Packet, extSeq int64) forwardDecision {
	if f.haveReleased && extSeq <= f.lastReleasedExtSeq {
		return dropLate
	}
	f.haveReleased = true
	f.lastReleasedExtSeq = extSeq
	if pkt.Padding {
		return dropPadding
	}
	if !f.haveOutput {
		f.lastOutputSeq = pkt.SequenceNumber
		f.haveOutput = true
	} else {
		f.lastOutputSeq++
	}
	pkt.SequenceNumber = f.lastOutputSeq
	return forwardMedia
}

func (f *sequenceForwarder) hasGap(nextExtSeq int64) bool {
	return f.haveReleased && nextExtSeq > f.lastReleasedExtSeq+1
}

type sequenceUnwrapper struct {
	initialized bool
	highestExt  int64
}

func (u *sequenceUnwrapper) unwrap(seq uint16) int64 {
	if !u.initialized {
		u.initialized = true
		u.highestExt = int64(seq)
		return u.highestExt
	}
	ext := u.highestExt + int64(int16(seq-uint16(u.highestExt)))
	if ext > u.highestExt {
		u.highestExt = ext
	}
	return ext
}
