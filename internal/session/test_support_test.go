package session

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/media/clocklineage"
)

type mockSource struct {
	ch chan *rtp.Packet
}

type mockClockLineageSource struct {
	*mockSource
	timeline *clocklineage.Timeline
}

func (m *mockClockLineageSource) ClockLineage() *clocklineage.Timeline { return m.timeline }

func newMockSource() *mockSource           { return &mockSource{ch: make(chan *rtp.Packet, 16)} }
func (m *mockSource) send(pkt *rtp.Packet) { m.ch <- pkt }
func (m *mockSource) close()               { close(m.ch) }
func (m *mockSource) ReadRTP() (*rtp.Packet, error) {
	pkt, ok := <-m.ch
	if !ok {
		return nil, io.EOF
	}
	return pkt, nil
}

type mockSubscription struct {
	mu      sync.Mutex
	packets []*rtp.Packet
	err     error
}

type observedBusSubscription struct {
	writeError error
	stopCount  int
}

func (subscription *observedBusSubscription) WriteRTPWithSource(
	*rtp.Packet,
	time.Time,
	bool,
	SourceIdentity,
) error {
	return subscription.writeError
}

func (*observedBusSubscription) RequiresPacketCopy() bool { return false }

func (subscription *observedBusSubscription) StopForwarding() {
	subscription.stopCount++
}

func (*observedBusSubscription) Snapshot() SubscriptionSnapshot {
	return SubscriptionSnapshot{SubscriberID: "peer-observed", Mode: "observed"}
}

func (m *mockSubscription) WriteRTP(pkt *rtp.Packet) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.packets = append(m.packets, pkt)
	return nil
}

func (m *mockSubscription) received() []*rtp.Packet {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*rtp.Packet, len(m.packets))
	copy(out, m.packets)
	return out
}

func waitFor(t *testing.T, deadline time.Duration, fn func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %v", deadline)
}

func given_new_relay_session_when_inspected_then_empty(t *testing.T) {
	r := New("room-1")
	if r.ID != "room-1" {
		t.Errorf("got ID %q, want %q", r.ID, "room-1")
	}
	if r.SubscriptionCount() != 0 {
		t.Errorf("got %d subscriptions, want 0", r.SubscriptionCount())
	}
	if r.SourceActive() {
		t.Error("new RelaySession must not report source active")
	}
}
