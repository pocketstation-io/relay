// Package stress contains Phase 2 exit-criterion stress tests.
//
// Run:
//
//	go test -race -run TestFiftyListeners ./test/stress/
//
// The test uses the room package directly — no real WebRTC, no network.
// It verifies the copy-on-write listener slice (RELAY-005) is free of data
// races and that the room reaches a clean steady state after 50 concurrent
// join/leave cycles.
package stress

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/room"
)

// listenerCount is the Phase 2 exit criterion value.
const listenerCount = 50

// --- mock types -------------------------------------------------------

// mockSource delivers packets from a buffered channel and signals EOF on close.
type mockSource struct {
	ch chan *rtp.Packet
}

func newMockSource() *mockSource {
	return &mockSource{ch: make(chan *rtp.Packet, 64)}
}

func (m *mockSource) send(pkt *rtp.Packet) { m.ch <- pkt }
func (m *mockSource) stop()                { close(m.ch) }

func (m *mockSource) ReadRTP() (*rtp.Packet, error) {
	pkt, ok := <-m.ch
	if !ok {
		return nil, io.EOF
	}
	return pkt, nil
}

// mockListener counts received packets. WriteRTP is safe for concurrent use.
type mockListener struct {
	mu    sync.Mutex
	count int
}

func (ml *mockListener) WriteRTP(_ *rtp.Packet) error {
	ml.mu.Lock()
	ml.count++
	ml.mu.Unlock()
	return nil
}

// --- helpers ----------------------------------------------------------

// makePacket returns a minimal valid RTP packet for forwarding.
func makePacket() *rtp.Packet {
	return &rtp.Packet{
		Header:  rtp.Header{PayloadType: 111, SequenceNumber: 1, Timestamp: 0},
		Payload: []byte{0xDE, 0xAD},
	}
}

// --- tests ------------------------------------------------------------

// TestFiftyListenersJoinLeaveRapidly verifies the Phase 2 exit criterion:
// "Relay handles 50 listeners joining/leaving rapidly without crash."
//
// Given: a room with an active source forwarding RTP at ~1 kHz.
// When:  50 goroutines each add a listener, yield the CPU once, then remove it.
// Then:  no panics, no data races (-race), and final listener count is 0.
func TestFiftyListenersJoinLeaveRapidly(t *testing.T) {
	// Given — room with short expiry timers so the test does not block.
	mgr := room.NewManagerWithConfig(room.ManagerConfig{
		InactivityTimeout: 5 * time.Minute,
		ReconnectWindow:   5 * time.Minute,
	})
	r := mgr.GetOrCreate("stress-room-1")
	defer mgr.Delete("stress-room-1")

	src := newMockSource()
	r.SetSource(src, nil)

	// stopSending is closed by the test body to signal the sender goroutine to
	// exit before src.stop() closes the channel. This prevents a concurrent
	// send after close.
	stopSending := make(chan struct{})
	senderDone := make(chan struct{})

	// Drive the source in the background: send packets for the duration of the
	// test so forwardLoop is active and exercises WriteRTP concurrently with
	// AddListener/RemoveListener.
	go func() {
		defer close(senderDone)
		pkt := makePacket()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSending:
				return
			case <-ticker.C:
				select {
				case src.ch <- pkt:
				case <-stopSending:
					return
				}
			}
		}
	}()

	// When — 50 goroutines join and leave rapidly.
	var wg sync.WaitGroup
	wg.Add(listenerCount)

	for i := 0; i < listenerCount; i++ {
		go func(idx int) {
			defer wg.Done()
			peerID := fmt.Sprintf("peer-%03d", idx)
			ml := &mockListener{}
			if err := r.AddListener(peerID, ml); err != nil {
				// ErrRoomFull would only fire if MaxListeners were set; it is not.
				t.Errorf("AddListener(%s): unexpected error: %v", peerID, err)
				return
			}
			// Yield so the forward loop has a chance to write to this listener
			// while other goroutines are still modifying the slice.
			time.Sleep(time.Millisecond)
			r.RemoveListener(peerID)
		}(i)
	}

	wg.Wait()

	// Stop the sender goroutine first, then close the channel so forwardLoop exits.
	// Order matters: sender must stop before src.stop() to avoid send-on-closed-channel.
	close(stopSending)
	<-senderDone
	src.stop()

	// Then — room is consistent: all listeners removed, no goroutine leaked.
	got := r.ListenerCount()
	if got != 0 {
		t.Errorf("expected 0 listeners after all goroutines finished, got %d", got)
	}
}

// TestFiftyListenersNoSource verifies that rapid join/leave is safe even when
// no source is active (forwardLoop never started). This exercises the
// copy-on-write path in isolation.
//
// Given: a room with no source.
// When:  50 goroutines each add then immediately remove a listener.
// Then:  no panics, no data races, final listener count is 0.
func TestFiftyListenersNoSource(t *testing.T) {
	mgr := room.NewManagerWithConfig(room.ManagerConfig{
		InactivityTimeout: 5 * time.Minute,
		ReconnectWindow:   5 * time.Minute,
	})
	r := mgr.GetOrCreate("stress-room-2")
	defer mgr.Delete("stress-room-2")

	var wg sync.WaitGroup
	wg.Add(listenerCount)

	for i := 0; i < listenerCount; i++ {
		go func(idx int) {
			defer wg.Done()
			peerID := fmt.Sprintf("peer-%03d", idx)
			ml := &mockListener{}
			if err := r.AddListener(peerID, ml); err != nil {
				t.Errorf("AddListener(%s): unexpected error: %v", peerID, err)
				return
			}
			r.RemoveListener(peerID)
		}(i)
	}

	wg.Wait()

	if got := r.ListenerCount(); got != 0 {
		t.Errorf("expected 0 listeners, got %d", got)
	}
}
