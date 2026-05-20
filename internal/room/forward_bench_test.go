// Benchmarks for the RTP forward path.
//
// ADR-009: no zero-alloc claim for the relay is permitted until the
// *webrtc.TrackLocalStaticRTP write path has been measured. These benchmarks
// use a discard Listener to isolate room dispatch overhead; a separate
// benchmark wiring real Pion tracks is required before Phase 1 exit.
//
// Run with:
//
//	go test -bench=BenchmarkWriteRTP -benchmem ./internal/room/
package room

import (
	"fmt"
	"testing"

	"github.com/pion/rtp"
)

func BenchmarkWriteRTPToListeners_1(b *testing.B)   { benchmarkForward(b, 1) }
func BenchmarkWriteRTPToListeners_10(b *testing.B)  { benchmarkForward(b, 10) }
func BenchmarkWriteRTPToListeners_100(b *testing.B) { benchmarkForward(b, 100) }

// benchmarkForward measures the listener-snapshot + write loop lifted from
// forwardLoop with n mock listeners. This isolates room dispatch from any
// allocation inside *webrtc.TrackLocalStaticRTP.WriteRTP.
//
// TODO(Phase 1, ADR-009): add a companion benchmark that substitutes real
// *webrtc.TrackLocalStaticRTP instances so allocation inside Pion is visible.
func benchmarkForward(b *testing.B, n int) {
	b.Helper()
	r := New("bench-room")
	defer r.Close()
	for i := 0; i < n; i++ {
		r.AddListener(fmt.Sprintf("peer-%d", i), discardListener{})
	}
	pkt := &rtp.Packet{Payload: make([]byte, 200)} // typical 20 ms Opus frame

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Mirror exactly the hot path in forwardLoop.
		r.mu.RLock()
		ls := make([]Listener, 0, len(r.listeners))
		for _, l := range r.listeners {
			ls = append(ls, l)
		}
		r.mu.RUnlock()
		for _, l := range ls {
			_ = l.WriteRTP(pkt)
		}
	}
}

// discardListener drops every packet with zero allocation.
type discardListener struct{}

func (discardListener) WriteRTP(_ *rtp.Packet) error { return nil }
