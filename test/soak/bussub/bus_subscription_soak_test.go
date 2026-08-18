// Package bussub_test — Wave 13b media soak for per-bus subscriptions (spec §7).
//
// This soak forwards real RTP through a real RelaySession (real forwardLoop
// goroutines, real deliver hot path) for a long duration. It proves three
// things hold for the whole run:
//
//   - every packet a bus forwards reaches a BusMix subscriber AND that bus's
//     own per-bus subscriber (delivery is complete and correctly fanned out);
//   - a per-bus subscriber receives ONLY its bus (BusMix=all-buses is preserved
//     while bus filtering is honored);
//   - no goroutine leak and no panic across the run.
//
// It lives in its own package (not test/soak directly) because the sibling
// test/soak/reconnect_test.go still imports the removed v2.3 internal/room
// package and does not compile; isolating here keeps this soak runnable without
// touching that out-of-scope Phase 1 migration leftover.
//
// Run short (default, ~2s, two payload profiles):
//
//	go test -race -run TestGivenRelaySessionWhenLongMediaSoak ./test/soak/bussub/
//
// Run the full 24h endurance soak:
//
//	RELAY_SOAK_FULL=1 go test -timeout 25h -run TestGivenRelaySessionWhenLongMediaSoak ./test/soak/bussub/
package bussub_test

import (
	"io"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/session"
)

const (
	// busSoakShortDuration is the per-profile run length without RELAY_SOAK_FULL.
	busSoakShortDuration = 2 * time.Second
	// busSoakFullDuration is the endurance run length when RELAY_SOAK_FULL=1.
	busSoakFullDuration = 24 * time.Hour
	// busSoakGoroutineSlop tolerates timer/scheduler drift in the leak check.
	busSoakGoroutineSlop = 4
	// busSoakSettleTime lets forwardLoops drain and timers stop before sampling.
	busSoakSettleTime = 50 * time.Millisecond
)

// pacedSource emits one RTP packet per interval until stop is closed, then EOF.
// Exactly one forwardLoop reads it, so sent needs no extra synchronization
// beyond the atomic used for cross-goroutine assertion reads.
type pacedSource struct {
	interval time.Duration
	payload  []byte
	ssrc     uint32
	stop     chan struct{}
	seq      uint16
	sent     atomic.Uint64
}

func newPacedSource(interval time.Duration, payloadBytes int, ssrc uint32) *pacedSource {
	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte(i)
	}
	return &pacedSource{interval: interval, payload: payload, ssrc: ssrc, stop: make(chan struct{})}
}

func (s *pacedSource) ReadRTP() (*rtp.Packet, error) {
	select {
	case <-s.stop:
		return nil, io.EOF
	case <-time.After(s.interval):
	}
	s.seq++
	s.sent.Add(1)
	return &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    111,
			SequenceNumber: s.seq,
			SSRC:           s.ssrc,
		},
		Payload: s.payload,
	}, nil
}

// countingSubscription counts delivered packets without retaining them, so a
// 24h run does not accumulate memory.
type countingSubscription struct{ count atomic.Uint64 }

func (c *countingSubscription) WriteRTP(*rtp.Packet) error { c.count.Add(1); return nil }

type busSoakProfile struct {
	name         string
	interval     time.Duration
	payloadBytes int
}

func TestGivenRelaySessionWhenLongMediaSoakThenNoLeakAndCountsMatch(t *testing.T) {
	if testing.Short() {
		t.Skip("media soak is excluded from the short test tier")
	}
	if os.Getenv("RELAY_SOAK") != "1" && os.Getenv("RELAY_SOAK_BUS_SUBSCRIPTION") != "1" && os.Getenv("RELAY_SOAK_FULL") != "1" {
		t.Skip("set RELAY_SOAK=1, RELAY_SOAK_BUS_SUBSCRIPTION=1, or RELAY_SOAK_FULL=1")
	}
	duration := busSoakShortDuration
	if os.Getenv("RELAY_SOAK_FULL") == "1" {
		duration = busSoakFullDuration
		t.Logf("RELAY_SOAK_FULL=1: running full %v endurance soak per profile", duration)
	}

	profiles := []busSoakProfile{
		{name: "mono_20ms", interval: 20 * time.Millisecond, payloadBytes: 160},
		{name: "stereo_10ms", interval: 10 * time.Millisecond, payloadBytes: 320},
	}
	for _, p := range profiles {
		p := p
		t.Run(p.name, func(t *testing.T) {
			runBusSubscriptionSoak(t, p, duration)
		})
	}
}

func runBusSubscriptionSoak(t *testing.T, p busSoakProfile, duration time.Duration) {
	t.Helper()

	runtime.GC()
	baselineGoroutines := runtime.NumGoroutine()

	r := session.New("soak-" + p.name)

	mixSub := &countingSubscription{}   // BusMix: must receive voice + music
	voiceSub := &countingSubscription{} // "voice": must receive only voice
	musicSub := &countingSubscription{} // "music": must receive only music
	if err := r.AddSubscription("mix-peer", session.BusMix, mixSub); err != nil {
		t.Fatalf("AddSubscription mix: %v", err)
	}
	if err := r.AddSubscription("voice-peer", "voice", voiceSub); err != nil {
		t.Fatalf("AddSubscription voice: %v", err)
	}
	if err := r.AddSubscription("music-peer", "music", musicSub); err != nil {
		t.Fatalf("AddSubscription music: %v", err)
	}

	voiceSrc := newPacedSource(p.interval, p.payloadBytes, 0x11111111)
	musicSrc := newPacedSource(p.interval, p.payloadBytes, 0x22222222)
	r.SetSource("voice", session.BusRoleVoice, voiceSrc, nil)
	r.SetSource("music", session.BusRoleMusic, musicSrc, nil)

	time.Sleep(duration)

	close(voiceSrc.stop)
	close(musicSrc.stop)

	// Wait for both forwardLoops to observe EOF and clear their sources, so all
	// in-flight packets have been delivered before we compare counts.
	deadline := time.Now().Add(5 * time.Second)
	for r.SourceActive() {
		if time.Now().After(deadline) {
			t.Fatal("sources did not drain within 5s after stop")
		}
		time.Sleep(time.Millisecond)
	}

	voiceSent := voiceSrc.sent.Load()
	musicSent := musicSrc.sent.Load()
	if voiceSent == 0 || musicSent == 0 {
		t.Fatalf("soak produced no traffic: voiceSent=%d musicSent=%d", voiceSent, musicSent)
	}

	// Per-bus subscriber receives exactly its bus, nothing more.
	if got := voiceSub.count.Load(); got != voiceSent {
		t.Errorf("voice subscriber got %d packets, want %d (voice sent)", got, voiceSent)
	}
	if got := musicSub.count.Load(); got != musicSent {
		t.Errorf("music subscriber got %d packets, want %d (music sent)", got, musicSent)
	}
	// BusMix subscriber receives every bus — byte-for-byte the room-wide behavior.
	if got := mixSub.count.Load(); got != voiceSent+musicSent {
		t.Errorf("mix subscriber got %d packets, want %d (voice+music)", got, voiceSent+musicSent)
	}

	r.Close()

	runtime.GC()
	time.Sleep(busSoakSettleTime)
	runtime.GC()
	delta := runtime.NumGoroutine() - baselineGoroutines
	if delta > busSoakGoroutineSlop {
		t.Errorf("goroutine leak: baseline=%d after=%d delta=%d (limit=%d)",
			baselineGoroutines, runtime.NumGoroutine(), delta, busSoakGoroutineSlop)
	}

	t.Logf("soak[%s] dur=%v voiceSent=%d musicSent=%d mix=%d voice=%d music=%d goroutineDelta=%d",
		p.name, duration, voiceSent, musicSent,
		mixSub.count.Load(), voiceSub.count.Load(), musicSub.count.Load(), delta)
}
