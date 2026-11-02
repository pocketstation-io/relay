package ingress

import (
	"encoding/binary"
	"testing"
	"time"
)

func testMessage(sequence uint64, timestamp uint32, captureNs uint64) []byte {
	message := make([]byte, HeaderBytes+3)
	copy(message, "PKSO")
	message[4], message[5] = 1, 1
	binary.BigEndian.PutUint64(message[8:16], sequence)
	binary.BigEndian.PutUint32(message[16:20], timestamp)
	binary.BigEndian.PutUint16(message[20:22], FrameSamples)
	binary.BigEndian.PutUint16(message[22:24], 3)
	binary.BigEndian.PutUint64(message[24:32], captureNs)
	copy(message[HeaderBytes:], []byte{0xf8, 0xff, 0xfe})
	return message
}

func TestGivenOpusIngressWhenMalformedThenRejected(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func([]byte) []byte
	}{
		{"truncated", func(b []byte) []byte { return b[:HeaderBytes] }},
		{"oversize", func(b []byte) []byte { return make([]byte, MaxMessageBytes+1) }},
		{"version", func(b []byte) []byte { b[4] = 2; return b }},
		{"channel", func(b []byte) []byte { b[5] = 3; return b }},
		{"reserved", func(b []byte) []byte { b[6] = 1; return b }},
		{"duration", func(b []byte) []byte { b[20] = 0; b[21] = 120; return b }},
		{"length", func(b []byte) []byte { b[23] = 4; return b }},
		{"toc duration", func(b []byte) []byte { b[40] = 0x80; return b }},
		{"zero frame count", func(b []byte) []byte { b[40] = 0xfb; b[41] = 0; return b }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Parse(test.change(testMessage(0, 0, 1))); err == nil {
				t.Fatal("malformed frame admitted")
			}
		})
	}
}

func TestGivenIngressContinuityWhenGapAndTimestampWrapThenAccepted(t *testing.T) {
	now := time.Now()
	var timeline Timeline
	first, _ := Parse(testMessage(65535, ^uint32(0)-959, 1))
	if late, err := timeline.Admit(first, now); late || err != nil {
		t.Fatalf("initial frame: %v %v", late, err)
	}
	second, _ := Parse(testMessage(65537, 960, 40_000_001))
	if late, err := timeline.Admit(second, now.Add(40*time.Millisecond)); late || err != nil {
		t.Fatalf("gap/wrap: %v %v", late, err)
	}
	if _, err := timeline.Admit(second, now.Add(60*time.Millisecond)); err != ErrContinuity {
		t.Fatalf("replay: %v", err)
	}
}

func TestGivenTCPBacklogWhenOldFramesArriveThenDropAndRecover(t *testing.T) {
	now := time.Now()
	var timeline Timeline
	first, _ := Parse(testMessage(0, 0, 1))
	_, _ = timeline.Admit(first, now)
	stale, _ := Parse(testMessage(1, 960, 20_000_001))
	if late, err := timeline.Admit(stale, now.Add(time.Second)); !late || err != nil {
		t.Fatalf("stale: %v %v", late, err)
	}
	fresh, _ := Parse(testMessage(50, 48000, 1_000_000_001))
	if late, err := timeline.Admit(fresh, now.Add(time.Second)); late || err != nil {
		t.Fatalf("recovery: %v %v", late, err)
	}
}

func TestGivenIngressTimelineWhenPublisherRunsAheadOrReplaysClockThenReject(t *testing.T) {
	for _, test := range []struct {
		name      string
		seq       uint64
		timestamp uint32
		capture   uint64
		want      error
	}{
		{"future", 10, 9600, 200_000_001, ErrFuture},
		{"sample mismatch", 1, 1920, 20_000_001, ErrContinuity},
		{"clock replay", 1, 960, 1, ErrContinuity},
		{"excess gap", 1501, 1501 * 960, 30_020_000_001, ErrContinuity},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now()
			var timeline Timeline
			first, _ := Parse(testMessage(0, 0, 1))
			_, _ = timeline.Admit(first, now)
			frame, _ := Parse(testMessage(test.seq, test.timestamp, test.capture))
			if _, err := timeline.Admit(frame, now); err != test.want {
				t.Fatalf("got %v want %v", err, test.want)
			}
		})
	}
}

func TestGivenSpoofedCaptureClockWhenMediaRunsAheadThenRejected(t *testing.T) {
	now := time.Now()
	var timeline Timeline
	first, _ := Parse(testMessage(0, 0, 1))
	_, _ = timeline.Admit(first, now)
	frame, _ := Parse(testMessage(10, 9600, 2))
	if _, err := timeline.Admit(frame, now); err != ErrFuture {
		t.Fatalf("spoofed capture clock admitted burst: %v", err)
	}
}

func TestGivenCaptureClockAdvancesWhenMediaBacklogRemainsThenFrameDropped(t *testing.T) {
	now := time.Now()
	var timeline Timeline
	first, _ := Parse(testMessage(0, 0, 1))
	_, _ = timeline.Admit(first, now)
	frame, _ := Parse(testMessage(1, 960, 1_000_000_001))
	if late, err := timeline.Admit(frame, now.Add(time.Second)); !late || err != nil {
		t.Fatalf("capture clock hid media backlog: late=%v err=%v", late, err)
	}
}

func TestGivenValidIngressWhenParsedAndAdmittedThenNoPerFrameAllocations(t *testing.T) {
	message := testMessage(0, 0, 1)
	now := time.Now()
	allocations := testing.AllocsPerRun(1000, func() {
		var timeline Timeline
		frame, err := Parse(message)
		if err != nil {
			panic(err)
		}
		_, err = timeline.Admit(frame, now)
		if err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations/frame=%f", allocations)
	}
}
