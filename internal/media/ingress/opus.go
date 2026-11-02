// Package ingress validates the bounded pks-opus-v1 network boundary.
// It neither decodes audio nor maps a publisher monotonic clock to UTC.
package ingress

import (
	"encoding/binary"
	"errors"
	"time"
)

const (
	HeaderBytes     = 40
	MaxPayloadBytes = 1275
	MaxMessageBytes = HeaderBytes + MaxPayloadBytes
	FrameSamples    = 960
	FrameDuration   = 20 * time.Millisecond
	MaxSessionAge   = 24 * time.Hour
	MaxLate         = 250 * time.Millisecond
	MaxAhead        = 100 * time.Millisecond
)

var (
	ErrFrame      = errors.New("invalid pks-opus-v1 frame")
	ErrContinuity = errors.New("invalid pks-opus-v1 continuity")
	ErrFuture     = errors.New("pks-opus-v1 publisher exceeds realtime pacing")
)

// Frame borrows Payload until the next network read. OutputGeneration describes
// publisher-owned generated speech; it is not a Relay attachment generation.
type Frame struct {
	Sequence           uint64
	TimestampSamples   uint32
	CaptureTimestampNs uint64
	OutputGeneration   uint64
	Channels           uint8
	Payload            []byte
}

func Parse(message []byte) (Frame, error) {
	var frame Frame
	if len(message) <= HeaderBytes || len(message) > MaxMessageBytes ||
		string(message[:4]) != "PKSO" || message[4] != 1 ||
		(message[5] != 1 && message[5] != 2) || message[6] != 0 || message[7] != 0 ||
		binary.BigEndian.Uint16(message[20:22]) != FrameSamples ||
		int(binary.BigEndian.Uint16(message[22:24])) != len(message)-HeaderBytes {
		return frame, ErrFrame
	}
	payload := message[HeaderBytes:]
	// RFC 6716 section 3.1: TOC configuration fixes frame duration; packet
	// frame-count coding fixes aggregate duration. This checks framing/timing,
	// not decoded signal quality. The receiver remains the decoder authority.
	configuration := payload[0] >> 3
	samples := uint16(0)
	switch {
	case configuration < 12:
		samples = [...]uint16{480, 960, 1920, 2880}[configuration&3]
	case configuration < 16:
		samples = 480 << (configuration & 1)
	default:
		samples = 120 << (configuration & 3)
	}
	count := uint16(1)
	switch payload[0] & 3 {
	case 1, 2:
		count = 2
	case 3:
		if len(payload) < 2 {
			return frame, ErrFrame
		}
		count = uint16(payload[1] & 63)
	}
	if count == 0 || samples*count != FrameSamples {
		return frame, ErrFrame
	}
	frame = Frame{
		Sequence:           binary.BigEndian.Uint64(message[8:16]),
		TimestampSamples:   binary.BigEndian.Uint32(message[16:20]),
		CaptureTimestampNs: binary.BigEndian.Uint64(message[24:32]),
		OutputGeneration:   binary.BigEndian.Uint64(message[32:40]),
		Channels:           message[5], Payload: payload,
	}
	return frame, nil
}

// Timeline fences replay and excess pacing without a queue. Late TCP backlog
// is discarded against the first local arrival, preserving sequence gaps.
// The reader owns Timeline exclusively; no per-frame locking/allocation.
type Timeline struct {
	started        bool
	originNs       uint64
	originSequence uint64
	originAt       time.Time
	last           Frame
}

func (timeline *Timeline) Admit(frame Frame, now time.Time) (late bool, err error) {
	if !timeline.started {
		timeline.started = true
		timeline.originNs = frame.CaptureTimestampNs
		timeline.originSequence = frame.Sequence
		timeline.originAt = now
		timeline.last = frame
		return false, nil
	}
	last := timeline.last
	if frame.Channels != last.Channels || frame.Sequence <= last.Sequence ||
		frame.CaptureTimestampNs <= last.CaptureTimestampNs ||
		frame.CaptureTimestampNs-timeline.originNs > uint64(MaxSessionAge) ||
		frame.Sequence-timeline.originSequence > uint64(MaxSessionAge/FrameDuration) {
		return false, ErrContinuity
	}
	delta := frame.Sequence - last.Sequence
	if delta > 1500 || frame.TimestampSamples-last.TimestampSamples != uint32(delta*FrameSamples) {
		return false, ErrContinuity
	}
	// Pacing follows media samples; a sender cannot bypass it by inventing
	// capture timestamps. Publisher capture time remains monotonic metadata.
	elapsed := time.Duration(frame.Sequence-timeline.originSequence) * FrameDuration
	arrivalElapsed := now.Sub(timeline.originAt)
	if elapsed-arrivalElapsed > MaxAhead {
		return false, ErrFuture
	}
	timeline.last = frame
	return arrivalElapsed-elapsed > MaxLate, nil
}
