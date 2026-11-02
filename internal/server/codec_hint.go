package server

// RTCP receiver reports drive CODEC_HINT adaptive bitrate feedback.
//
// Each subscriber PeerConnection sends RTCP Receiver Reports back to Relay
// describing packet-loss fraction on the Relay-to-subscriber leg. This file reads
// those reports, maps loss fraction to an Opus bitrate tier, debounces the
// output to one hint per RelaySession per two seconds, and delivers a CODEC_HINT
// ServerMessage to the source session so the source can adjust its encoder.

import (
	"sync"
	"time"

	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// Bitrate tiers and packet-loss thresholds.
const (
	bitrateHighKbps   = 64
	bitrateMediumKbps = 32
	bitrateLowKbps    = 16

	// lossLowLatencyThreshold is the upper loss boundary for 10ms frame mode.
	// fraction_lost < 1% → prefer RESTRICTED_LOWDELAY 10ms frames for minimum
	// algorithmic delay (RFC 6716 §3.1). Above this threshold, 20ms frames
	// amortise FEC overhead more efficiently.
	lossLowLatencyThreshold = 0.01

	// lossMediumThreshold is the lower loss boundary for the medium tier.
	// fraction_lost > 2% but ≤ 5% → 32 kbps with FEC.
	lossMediumThreshold = 0.02

	// lossHighThreshold is the lower loss boundary for the low tier.
	// fraction_lost > 5% → 16 kbps with FEC + DTX.
	lossHighThreshold = 0.05

	// codecHintDebounce is the minimum interval between CODEC_HINT messages
	// sent to a source for one RelaySession. This prevents encoder thrash under
	// bursty RTCP from many subscribers.
	codecHintDebounce = 2 * time.Second
)

// codecHintState holds per-RelaySession debounce state for CODEC_HINT emission.
// Server.codecHintStates stores it by the compatibility room ID.
type codecHintState struct {
	mu       sync.Mutex
	lastSent time.Time
}

// bitrateForLoss maps a 0.0–1.0 fraction-lost value (from RTCP RR FractionLost/256)
// to a CodecHintPayload.
//
// FrameMs uses two frame durations:
//   - loss < 1%: 10ms (RESTRICTED_LOWDELAY) — minimum algorithmic delay, clean link
//   - loss ≥ 1%: 20ms — larger frame amortises FEC overhead more efficiently
func bitrateForLoss(fractionLost float64) signaling.CodecHintPayload {
	switch {
	case fractionLost > lossHighThreshold:
		// High loss / high RTT: drop bitrate aggressively, enable FEC + DTX.
		// Use 20ms frames: larger frame size spreads FEC bytes over fewer packets.
		return signaling.CodecHintPayload{
			BitRateKbps: bitrateLowKbps,
			Complexity:  3,
			Fec:         true,
			Dtx:         true,
			FrameMs:     20,
		}
	case fractionLost > lossMediumThreshold:
		// Moderate loss: reduce bitrate, enable FEC, keep DTX off.
		// Use 20ms frames for the same FEC efficiency reason.
		return signaling.CodecHintPayload{
			BitRateKbps: bitrateMediumKbps,
			Complexity:  5,
			Fec:         true,
			Dtx:         false,
			FrameMs:     20,
		}
	case fractionLost > lossLowLatencyThreshold:
		// Low-moderate loss (1–2%): maintain full quality but conservative frame
		// size until the link is confirmed clean.
		return signaling.CodecHintPayload{
			BitRateKbps: bitrateHighKbps,
			Complexity:  5,
			Fec:         false,
			Dtx:         false,
			FrameMs:     20,
		}
	default:
		// Clean link (loss < 1%): full quality + 10ms RESTRICTED_LOWDELAY frames.
		// Saves ~10ms of algorithmic delay vs 20ms frames (RFC 6716 §3.1).
		return signaling.CodecHintPayload{
			BitRateKbps: bitrateHighKbps,
			Complexity:  5,
			Fec:         false,
			Dtx:         false,
			FrameMs:     10,
		}
	}
}

// maybeEmitCodecHint sends a CODEC_HINT to the source peer for roomID if the
// debounce interval has elapsed. It is safe to call concurrently from multiple
// subscriber RTCP goroutines for the same RelaySession.
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

	// Find the source signaling peer for this RelaySession. Takes s.mu briefly.
	s.mu.Lock()
	var sourcePeer *signalPeer
	for _, peer := range s.signalPeers {
		if peer.room != nil && peer.room.ID == roomID && peer.role == auth.RoleSource {
			sourcePeer = peer
			break
		}
	}
	s.mu.Unlock()

	if sourcePeer == nil {
		return
	}
	_ = sourcePeer.send(signaling.ServerMessage{
		Type:      signaling.TypeCodecHint,
		CodecHint: &hint,
	})
}

// roomCodecHintState returns the shared codecHintState for the compatibility
// room ID, creating it when the first subscriber reports RTCP.
func (s *Server) roomCodecHintState(roomID string) *codecHintState {
	v, _ := s.codecHintStates.LoadOrStore(roomID, &codecHintState{})
	return v.(*codecHintState)
}
