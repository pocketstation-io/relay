package server

// D13 — RTCP RR → CODEC_HINT adaptive bitrate feedback (ADR-021).
//
// Each listener PeerConnection sends RTCP Receiver Reports back to the relay
// describing packet-loss fraction on the relay→listener leg. This file reads
// those reports, maps loss fraction to an Opus bitrate tier, debounces the
// output to one hint per room per 2 seconds, and delivers a CODEC_HINT
// ServerMessage to the source session so the source can adjust its encoder.

import (
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// ADR-021 bitrate tiers and thresholds.
const (
	bitrateHighKbps   = 64
	bitrateMediumKbps = 32
	bitrateLowKbps    = 16

	// lossMediumThreshold is the lower loss boundary for the medium tier.
	// fraction_lost > 2% but ≤ 5% → 32 kbps with FEC.
	lossMediumThreshold = 0.02

	// lossHighThreshold is the lower loss boundary for the low tier.
	// fraction_lost > 5% → 16 kbps with FEC + DTX.
	lossHighThreshold = 0.05

	// codecHintDebounce is the minimum interval between CODEC_HINT messages
	// sent to a source for a single room. Prevents encoder thrash under
	// bursty RTCP from many listeners.
	codecHintDebounce = 2 * time.Second
)

// codecHintState holds per-room debounce state for CODEC_HINT emission.
// Stored in Server.codecHintStates keyed by room ID.
type codecHintState struct {
	mu       sync.Mutex
	lastSent time.Time
}

// bitrateForLoss maps a 0.0–1.0 fraction-lost value (from RTCP RR FractionLost/256)
// to a CodecHintPayload according to ADR-021 tier table.
func bitrateForLoss(fractionLost float64) signaling.CodecHintPayload {
	switch {
	case fractionLost > lossHighThreshold:
		// High loss / high RTT: drop bitrate aggressively, enable FEC + DTX.
		return signaling.CodecHintPayload{
			BitRateKbps: bitrateLowKbps,
			Complexity:  3,
			Fec:         true,
			Dtx:         true,
		}
	case fractionLost > lossMediumThreshold:
		// Moderate loss: reduce bitrate, enable FEC, keep DTX off.
		return signaling.CodecHintPayload{
			BitRateKbps: bitrateMediumKbps,
			Complexity:  5,
			Fec:         true,
			Dtx:         false,
		}
	default:
		// Clean link: full quality.
		return signaling.CodecHintPayload{
			BitRateKbps: bitrateHighKbps,
			Complexity:  5,
			Fec:         false,
			Dtx:         false,
		}
	}
}

// startRTCPReader spawns a goroutine that reads RTCP Receiver Reports from a
// listener's RTPSender and calls maybeEmitCodecHint on each report.
// The goroutine exits when ReadRTCP returns an error (connection closed).
//
// sender is the RTPSender returned by pc.AddTrack for the listener's audio track.
// state is the per-room debounce object; shared across all listener goroutines in the room.
func (s *Server) startRTCPReader(
	sender *webrtc.RTPSender,
	roomID string,
	state *codecHintState,
) {
	go func() {
		for {
			pkts, _, err := sender.ReadRTCP()
			if err != nil {
				return
			}
			for _, pkt := range pkts {
				rr, ok := pkt.(*rtcp.ReceiverReport)
				if !ok {
					continue
				}
				for _, report := range rr.Reports {
					fractionLost := float64(report.FractionLost) / 256.0
					hint := bitrateForLoss(fractionLost)
					s.maybeEmitCodecHint(roomID, hint, state)
				}
			}
		}
	}()
}

// maybeEmitCodecHint sends a CODEC_HINT to the source session of roomID if the
// debounce interval has elapsed. Safe to call concurrently from multiple listener
// RTCP goroutines for the same room.
func (s *Server) maybeEmitCodecHint(
	roomID string,
	hint signaling.CodecHintPayload,
	state *codecHintState,
) {
	state.mu.Lock()
	if time.Since(state.lastSent) < codecHintDebounce {
		state.mu.Unlock()
		return
	}
	state.lastSent = time.Now()
	state.mu.Unlock()

	// Find the source session for this room. Takes s.mu briefly.
	s.mu.Lock()
	var sourceSess *session
	for _, sess := range s.sessions {
		if sess.rm != nil && sess.rm.ID == roomID && sess.role == auth.RoleSource {
			sourceSess = sess
			break
		}
	}
	s.mu.Unlock()

	if sourceSess == nil {
		return
	}
	sourceSess.send(signaling.ServerMessage{
		Type:      signaling.TypeCodecHint,
		CodecHint: &hint,
	})
}

// roomCodecHintState returns the shared codecHintState for roomID, creating it
// if this is the first listener in the room.
func (s *Server) roomCodecHintState(roomID string) *codecHintState {
	v, _ := s.codecHintStates.LoadOrStore(roomID, &codecHintState{})
	return v.(*codecHintState)
}

