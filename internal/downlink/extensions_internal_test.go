package downlink

import (
	"testing"
	"time"
)

func TestGivenHalfSecondNTPTime_WhenAbsSendTimeEncoded_ThenUsesSixDotEighteenLayout(t *testing.T) {
	now := time.Unix(0, 500_000_000)
	seconds := ntpEpochOffset & 0x3F
	wantValue := uint32(seconds<<18 | 1<<17)
	want := [3]byte{byte(wantValue >> 16), byte(wantValue >> 8), byte(wantValue)}

	if got := encodeAbsSendTime(now); got != want {
		t.Fatalf("abs-send-time = %x, want %x", got, want)
	}
}
