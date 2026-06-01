package server

import (
	"testing"
	"time"
)

// Pure-function unit tests for the D13 codec-hint logic (ADR-021).
// Integration of maybeEmitCodecHint with live WebSocket sessions is covered
// by test/integration/echo_test.go which already exercises the signaling path.

func TestGiven_ZeroLoss_When_BitrateForLoss_Then_HighTier(t *testing.T) {
	hint := bitrateForLoss(0.0)
	if hint.BitRateKbps != bitrateHighKbps {
		t.Fatalf("want %d kbps, got %d", bitrateHighKbps, hint.BitRateKbps)
	}
	if hint.Fec || hint.Dtx {
		t.Fatal("want fec=false, dtx=false on clean link")
	}
}

func TestGiven_MediumLoss_When_BitrateForLoss_Then_MediumTierWithFEC(t *testing.T) {
	hint := bitrateForLoss(0.03) // 3% — between 2% and 5%
	if hint.BitRateKbps != bitrateMediumKbps {
		t.Fatalf("want %d kbps, got %d", bitrateMediumKbps, hint.BitRateKbps)
	}
	if !hint.Fec {
		t.Fatal("want fec=true at medium loss")
	}
	if hint.Dtx {
		t.Fatal("want dtx=false at medium loss")
	}
}

func TestGiven_HighLoss_When_BitrateForLoss_Then_LowTierWithFECAndDTX(t *testing.T) {
	hint := bitrateForLoss(0.10) // 10% — above 5%
	if hint.BitRateKbps != bitrateLowKbps {
		t.Fatalf("want %d kbps, got %d", bitrateLowKbps, hint.BitRateKbps)
	}
	if !hint.Fec {
		t.Fatal("want fec=true at high loss")
	}
	if !hint.Dtx {
		t.Fatal("want dtx=true at high loss")
	}
}

func TestGiven_LossAtMediumBoundary_When_BitrateForLoss_Then_HighTier(t *testing.T) {
	// Exactly 2% is NOT > threshold → still high tier.
	hint := bitrateForLoss(lossMediumThreshold)
	if hint.BitRateKbps != bitrateHighKbps {
		t.Fatalf("want %d kbps at exact lower boundary, got %d", bitrateHighKbps, hint.BitRateKbps)
	}
}

func TestGiven_LossJustAboveHighBoundary_When_BitrateForLoss_Then_LowTier(t *testing.T) {
	hint := bitrateForLoss(lossHighThreshold + 0.001)
	if hint.BitRateKbps != bitrateLowKbps {
		t.Fatalf("want %d kbps just above high threshold, got %d", bitrateLowKbps, hint.BitRateKbps)
	}
}

func TestGiven_FractionLostFromRTCP_When_Converted_Then_MapsCorrectly(t *testing.T) {
	// RTCP FractionLost field is uint8 on scale 0–255 (255 = 100% loss).
	cases := []struct {
		rtcpValue uint8
		wantKbps  int
	}{
		{0, bitrateHighKbps},   // 0/256 = 0%
		{5, bitrateHighKbps},   // 5/256 ≈ 2.0% — at boundary → high
		{8, bitrateMediumKbps}, // 8/256 ≈ 3.1% → medium
		{14, bitrateLowKbps},   // 14/256 ≈ 5.5% → low
		{255, bitrateLowKbps},  // 255/256 ≈ 100% → low
	}
	for _, c := range cases {
		frac := float64(c.rtcpValue) / 256.0
		hint := bitrateForLoss(frac)
		if hint.BitRateKbps != c.wantKbps {
			t.Errorf("rtcpValue=%d (frac=%.4f): want %d kbps, got %d",
				c.rtcpValue, frac, c.wantKbps, hint.BitRateKbps)
		}
	}
}

func TestGiven_NoSourceInRoom_When_MaybeEmitCodecHint_Then_NoPanic(t *testing.T) {
	srv := &Server{sessions: make(map[string]*session)}
	state := &codecHintState{}
	hint := bitrateForLoss(0.10)
	// Must not panic when no source session exists for the room.
	srv.maybeEmitCodecHint("nonexistent-room", hint, state)
}

func TestGiven_DebouncePeriodNotElapsed_When_MaybeEmitCodecHint_Then_StateUnchanged(t *testing.T) {
	srv := &Server{sessions: make(map[string]*session)}
	before := time.Now().Add(-1 * time.Millisecond) // lastSent is effectively "just now"
	state := &codecHintState{lastSent: before}
	hint := bitrateForLoss(0.10)

	srv.maybeEmitCodecHint("room-debounce", hint, state)

	// lastSent must not have advanced — the debounce check returned early.
	if !state.lastSent.Equal(before) {
		t.Fatal("debounce should have blocked emission; lastSent was updated unexpectedly")
	}
}

func TestGiven_RoomCodecHintState_When_CalledTwice_Then_SamePointer(t *testing.T) {
	srv := &Server{}
	s1 := srv.roomCodecHintState("room-x")
	s2 := srv.roomCodecHintState("room-x")
	if s1 != s2 {
		t.Fatal("LoadOrStore must return the same *codecHintState on repeated calls")
	}
}

func TestGiven_TwoRooms_When_RoomCodecHintState_Then_DifferentPointers(t *testing.T) {
	srv := &Server{}
	s1 := srv.roomCodecHintState("room-a")
	s2 := srv.roomCodecHintState("room-b")
	if s1 == s2 {
		t.Fatal("different rooms must have different *codecHintState instances")
	}
}
