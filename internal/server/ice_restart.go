package server

// Spec §10.4 — Bandwidth-Adaptive Codec Control: ICE restart on sustained loss.
//
// When packet loss exceeds 15% for 3 consecutive RTCP Receiver Reports, the
// relay sends an ICE_RESTART message to the source WebSocket. The source calls
// RTCPeerConnection.restartIce() to renegotiate the ICE path. When the relay's
// embedded TURN server is enabled (TURN_PUBLIC_IP set), use_turn=true is
// included in the message to hint that the client should prefer TURN relay
// candidates on the next ICE negotiation.
//
// Rate limiting: at most one ICE_RESTART is sent per room per 30 seconds to
// prevent restart storms when loss is sustained.

import (
	"sync"
	"time"

	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// iceRestartDebounce is the minimum interval between ICE_RESTART messages sent
// to the source for a single room. Prevents restart storms under sustained loss.
const iceRestartDebounce = 30 * time.Second

// lossICEThreshold is the packet-loss fraction above which a report is counted
// as a "high-loss" sample for ICE restart purposes (spec §10.4: 15%).
const lossICEThreshold = 0.15

// iceRestartConsecutiveRequired is the number of consecutive high-loss RTCP
// reports required before an ICE_RESTART is triggered (spec §10.4: 3).
const iceRestartConsecutiveRequired = 3

// lossTracker counts consecutive RTCP reports where the loss fraction exceeds
// lossICEThreshold. When the count reaches iceRestartConsecutiveRequired,
// record returns true to signal that an ICE restart should be triggered.
// The tracker resets the consecutive count when a low-loss report is received.
// It is not safe for concurrent use; the caller is responsible for
// synchronisation (iceRestartState.mu covers it when used via iceRestartState).
type lossTracker struct {
	consecutiveHighLoss int
	threshold           float64
}

// record updates the tracker with the given loss rate. Returns true when the
// ICE restart threshold has been reached (consecutiveHighLoss ≥ 3).
// Resets the consecutive count to zero when lossRate ≤ threshold.
func (t *lossTracker) record(lossRate float64) bool {
	if lossRate > t.threshold {
		t.consecutiveHighLoss++
	} else {
		t.consecutiveHighLoss = 0
	}
	return t.consecutiveHighLoss >= iceRestartConsecutiveRequired
}

// reset clears the consecutive high-loss counter. Called after an ICE_RESTART
// is sent so the tracker starts fresh for the next ICE negotiation cycle.
func (t *lossTracker) reset() {
	t.consecutiveHighLoss = 0
}

// newLossTracker returns a lossTracker initialised with lossICEThreshold.
func newLossTracker() *lossTracker {
	return &lossTracker{threshold: lossICEThreshold}
}

// iceRestartState holds per-room state for ICE restart tracking.
// Stored in Server.iceRestartStates keyed by room ID.
// A single iceRestartState is shared across all listener RTCP goroutines in
// a room; mu serialises both tracker updates and debounce checks.
type iceRestartState struct {
	mu       sync.Mutex
	tracker  *lossTracker
	lastSent time.Time
}

// newICERestartState returns an initialised iceRestartState.
func newICERestartState() *iceRestartState {
	return &iceRestartState{tracker: newLossTracker()}
}

// roomICERestartState returns the shared iceRestartState for roomID, creating
// it on the first call (LoadOrStore semantics, safe for concurrent callers).
func (s *Server) roomICERestartState(roomID string) *iceRestartState {
	v, _ := s.iceRestartStates.LoadOrStore(roomID, newICERestartState())
	return v.(*iceRestartState)
}

// maybeEmitICERestart records fractionLost against the per-room loss tracker
// and, when the threshold is reached and the debounce interval has elapsed,
// sends an ICE_RESTART ServerMessage to the source session for roomID.
//
// Safe to call concurrently from multiple listener RTCP goroutines for the
// same room; state.mu serialises all mutations.
func (s *Server) maybeEmitICERestart(roomID string, fractionLost float64, state *iceRestartState) {
	state.mu.Lock()
	triggered := state.tracker.record(fractionLost)
	if !triggered {
		state.mu.Unlock()
		return
	}
	// Threshold reached — check debounce.
	if time.Since(state.lastSent) < iceRestartDebounce {
		// Still within cooldown; do not reset the tracker so we re-check on the
		// next report when the debounce may have expired.
		state.mu.Unlock()
		return
	}
	state.lastSent = time.Now()
	state.tracker.reset()
	state.mu.Unlock()

	// Locate the source session for this room. Takes s.mu briefly.
	s.mu.Lock()
	var sourceSess *session
	for _, sess := range s.sessions {
		if sess.room != nil && sess.room.ID == roomID && sess.role == auth.RoleSource {
			sourceSess = sess
			break
		}
	}
	s.mu.Unlock()

	if sourceSess == nil {
		return
	}
	_ = sourceSess.send(signaling.ServerMessage{
		Type:    signaling.TypeICERestart,
		UseTURN: s.useTURN,
	})
	s.Metrics.ICERestartTotal.Add(1)
}
