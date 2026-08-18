package session

import (
	"testing"
	"time"

	"github.com/pion/rtp"
)

func TestGivenRelaySessionWhenInactivityTimerExpiresThenRoomClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("timer expiry test — skipped in -short mode")
	}
	const timeout = 50 * time.Millisecond
	r := newWithTimeout("expiry-room", timeout)
	waitFor(t, 500*time.Millisecond, func() bool {
		select {
		case <-r.done:
			return true
		default:
			return false
		}
	})
}

func TestGivenBusRolesWhenLatencyRankThenVoiceAndAgentAreLowest(t *testing.T) {
	if BusRoleVoice.LatencyRank() != 0 {
		t.Errorf("voice latency rank = %d, want 0", BusRoleVoice.LatencyRank())
	}
	if BusRoleAgentOutput.LatencyRank() != 0 {
		t.Errorf("agent_output latency rank = %d, want 0", BusRoleAgentOutput.LatencyRank())
	}
	if BusRoleMusic.LatencyRank() <= BusRoleVoice.LatencyRank() {
		t.Error("music must have higher latency rank than voice")
	}
	if BusRoleEvents.LatencyRank() <= BusRoleMusic.LatencyRank() {
		t.Error("events must have higher latency rank than music")
	}
}

func TestGivenBusWithNoSourceWhenStalledCheckedThenNotStalled(t *testing.T) {
	r := New("room-wd")
	defer r.Close()
	bus := r.GetOrCreateBus("voice", BusRoleVoice)

	if bus.SourceActive() {
		t.Error("a bus with no source must not report source active")
	}
	if bus.Stalled(time.Second) {
		t.Error("a bus with no source is idle, not stalled")
	}
}

func TestGivenActiveSourceWithRecentRTPWhenStalledCheckedThenNotStalled(t *testing.T) {
	r := New("room-wd")
	src := newMockSource()
	r.SetSource("voice", BusRoleVoice, src, nil)
	bus := r.GetOrCreateBus("voice", BusRoleVoice)

	src.send(&rtp.Packet{})
	waitFor(t, time.Second, func() bool { return bus.PacketCount.Load() == 1 })

	if bus.Stalled(time.Second) {
		t.Errorf("a bus forwarding RTP must not be stalled (age=%v)", bus.LastRTPAge())
	}

	r.Close()
	src.close()
}

func TestGivenActiveSourceButSilentWhenStalledCheckedThenStalled(t *testing.T) {
	r := New("room-wd")
	src := newMockSource()
	r.SetSource("voice", BusRoleVoice, src, nil) // seeds last-RTP=now, then blocks on ReadRTP
	bus := r.GetOrCreateBus("voice", BusRoleVoice)

	const threshold = 10 * time.Millisecond
	time.Sleep(5 * threshold) // let the seeded timestamp age past the threshold

	if !bus.Stalled(threshold) {
		t.Errorf("a live source silent past %v must be stalled (age=%v)", threshold, bus.LastRTPAge())
	}
	if !r.AnyBusStalled(threshold) {
		t.Error("room must report a stalled bus")
	}

	r.Close()
	src.close()
}

func TestGivenForwardingBusWhenHealthSnapshotThenReflectsCounters(t *testing.T) {
	r := New("room-wd")
	src := newMockSource()
	r.SetSource("music", BusRoleMusic, src, nil)
	bus := r.GetOrCreateBus("music", BusRoleMusic)

	src.send(&rtp.Packet{Payload: []byte{1, 2, 3, 4}})
	waitFor(t, time.Second, func() bool { return bus.PacketCount.Load() == 1 })

	h := bus.Health(time.Second)
	if h.BusID != "music" || h.Role != "music" {
		t.Errorf("unexpected health identity: %+v", h)
	}
	if !h.SourceActive || h.Stalled {
		t.Errorf("a forwarding bus must be source-active and not stalled: %+v", h)
	}
	if h.PacketCount != 1 || h.ByteCount != 4 {
		t.Errorf("health counters wrong: %+v", h)
	}

	list := r.BusHealthList(time.Second)
	if len(list) != 1 || list[0].BusID != "music" {
		t.Errorf("BusHealthList should hold the one music bus, got %+v", list)
	}

	r.Close()
	src.close()
}

func TestGivenRoomWithActiveSourceWhenPastInactivityTimeoutThenNotExpired(t *testing.T) {
	const timeout = 20 * time.Millisecond
	r := newWithTimeout("active-room", timeout)
	src := newMockSource()
	r.SetSource("voice", BusRoleVoice, src, nil)

	// Keep a live source attached well past several inactivity windows; a
	// WebSocket-open-but-media-aware room must stay open while media is live.
	time.Sleep(6 * timeout)

	select {
	case <-r.done:
		t.Error("a room with an active source must not expire on the inactivity timer")
	default:
	}

	r.Close()
	src.close()
}
