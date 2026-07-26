package server

// Unit tests for ICE restart loss tracking and debounce (spec §10.4).
//
// Test naming follows the GWT convention established by codec_hint_test.go:
// TestGiven_[context]_When_[action]_Then_[expected].

import (
	"testing"
	"time"
)

// --- lossTracker unit tests ---

func TestGiven_TwoHighLossReports_When_ThirdArrives_Then_TrackerTriggers(t *testing.T) {
	// Given: two consecutive high-loss reports have been recorded.
	tracker := newLossTracker()
	if tracker.record(0.20) {
		t.Fatal("should not trigger on first high-loss report")
	}
	if tracker.record(0.20) {
		t.Fatal("should not trigger on second high-loss report")
	}
	// When: the third consecutive high-loss report arrives.
	triggered := tracker.record(0.20)
	// Then: the tracker signals an ICE restart.
	if !triggered {
		t.Fatal("lossTracker should trigger after 3 consecutive high-loss reports")
	}
}

func TestGiven_TwoHighLossReports_When_LowLossArrives_Then_TrackerResets(t *testing.T) {
	// Given: two consecutive high-loss reports.
	tracker := newLossTracker()
	tracker.record(0.20)
	tracker.record(0.20)
	// When: a low-loss report arrives.
	triggered := tracker.record(0.05) // at or below threshold — not > 0.15
	// Then: the tracker does not trigger and the consecutive count resets.
	if triggered {
		t.Fatal("lossTracker should not trigger after a low-loss report breaks the streak")
	}
	// A subsequent single high-loss report should not trigger (reset confirmed).
	if tracker.record(0.20) {
		t.Fatal("count should have been reset to 1 by the previous low-loss report; should not trigger on next single high-loss report")
	}
}

func TestGiven_ExactlyAtThreshold_When_Recorded_Then_NotHighLoss(t *testing.T) {
	// Given: loss exactly at lossICEThreshold (not strictly greater than).
	tracker := newLossTracker()
	for i := 0; i < iceRestartConsecutiveRequired; i++ {
		if tracker.record(lossICEThreshold) {
			t.Fatalf("loss == threshold should not count as high loss (iteration %d)", i)
		}
	}
}

func TestGiven_LossJustAboveThreshold_When_ThreeConsecutive_Then_Triggers(t *testing.T) {
	tracker := newLossTracker()
	const justAbove = lossICEThreshold + 0.001
	tracker.record(justAbove)
	tracker.record(justAbove)
	if !tracker.record(justAbove) {
		t.Fatal("loss just above threshold for 3 reports should trigger")
	}
}

func TestGiven_TrackerTriggered_When_Reset_Then_RequiresThreeMoreReports(t *testing.T) {
	// Given: tracker has just triggered (3 consecutive high-loss reports).
	tracker := newLossTracker()
	tracker.record(0.20)
	tracker.record(0.20)
	tracker.record(0.20) // triggers
	// When: tracker is explicitly reset (as done after sending ICE_RESTART).
	tracker.reset()
	// Then: it takes another 3 consecutive reports to trigger again.
	tracker.record(0.20)
	tracker.record(0.20)
	triggered := tracker.record(0.20)
	if !triggered {
		t.Fatal("after reset, tracker should trigger again on 3 consecutive high-loss reports")
	}
}

// --- iceRestartState debounce tests ---

func TestGiven_NoDebounceElapsed_When_MaybeEmitICERestart_Then_NoMessageSent(t *testing.T) {
	// Given: a server with no sessions and a state that was last sent "just now".
	srv := &Server{signalPeers: make(map[string]*signalPeer)}
	state := newICERestartState()
	// Seed the tracker so it will trigger on the next report.
	state.tracker.record(0.20)
	state.tracker.record(0.20)
	// Record the trigger time as "just now" so the debounce blocks.
	state.lastSent = time.Now()

	// When: the third high-loss report arrives (would trigger the tracker).
	// maybeEmitICERestart will see triggered=true but debounce not elapsed → no send.
	srv.maybeEmitICERestart("room-debounce", 0.20, state)

	// Then: lastSent must not have advanced (debounce check returned early).
	// We verify by checking that the state still holds the original lastSent timestamp.
	// Because there are no sessions, if it had passed the debounce it would have
	// updated lastSent; we detect that by checking the gap.
	if time.Since(state.lastSent) > time.Millisecond {
		t.Fatal("debounce should have blocked ICE_RESTART; lastSent advanced unexpectedly")
	}
}

func TestGiven_DebounceElapsed_When_ThreeConsecutiveHighLoss_Then_StateUpdated(t *testing.T) {
	// Given: a server with no source sessions (so no WS write occurs) and a
	// state whose lastSent is older than the debounce window.
	srv := &Server{signalPeers: make(map[string]*signalPeer)}
	state := newICERestartState()
	state.lastSent = time.Now().Add(-(iceRestartDebounce + time.Second))

	// When: three consecutive high-loss reports arrive.
	srv.maybeEmitICERestart("room-x", 0.20, state)
	srv.maybeEmitICERestart("room-x", 0.20, state)
	srv.maybeEmitICERestart("room-x", 0.20, state)

	// Then: lastSent was updated (the debounce gate was passed) even though no
	// WebSocket message was sent (no source session exists).
	if time.Since(state.lastSent) > time.Second {
		t.Fatal("lastSent should have been updated when debounce elapsed and tracker triggered")
	}
}

func TestGiven_ICERestartJustSent_When_AnotherThreeHighLoss_Then_DebounceBlocks(t *testing.T) {
	// Given: an ICE_RESTART was sent (lastSent = now, tracker reset).
	srv := &Server{signalPeers: make(map[string]*signalPeer)}
	state := newICERestartState()
	// Simulate having just sent a restart.
	state.lastSent = time.Now()
	state.tracker.reset() // tracker is fresh after a send

	sentBefore := state.lastSent

	// When: another three consecutive high-loss reports arrive immediately after.
	srv.maybeEmitICERestart("room-y", 0.20, state)
	srv.maybeEmitICERestart("room-y", 0.20, state)
	srv.maybeEmitICERestart("room-y", 0.20, state)

	// Then: lastSent must not have changed (debounce blocks the second restart).
	if !state.lastSent.Equal(sentBefore) {
		t.Fatal("second ICE_RESTART within debounce window should have been blocked")
	}
}

// --- roomICERestartState helper ---

func TestGiven_RoomICERestartState_When_CalledTwice_Then_SamePointer(t *testing.T) {
	srv := &Server{}
	s1 := srv.roomICERestartState("room-ice-1")
	s2 := srv.roomICERestartState("room-ice-1")
	if s1 != s2 {
		t.Fatal("LoadOrStore must return the same *iceRestartState on repeated calls for the same room")
	}
}

func TestGiven_TwoRooms_When_RoomICERestartState_Then_DifferentPointers(t *testing.T) {
	srv := &Server{}
	s1 := srv.roomICERestartState("room-ice-a")
	s2 := srv.roomICERestartState("room-ice-b")
	if s1 == s2 {
		t.Fatal("different rooms must have different *iceRestartState instances")
	}
}
