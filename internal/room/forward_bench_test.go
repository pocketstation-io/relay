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
	"github.com/pion/webrtc/v4"
)

func BenchmarkWriteRTPToListeners_1(b *testing.B)   { benchmarkForward(b, 1) }
func BenchmarkWriteRTPToListeners_10(b *testing.B)  { benchmarkForward(b, 10) }
func BenchmarkWriteRTPToListeners_100(b *testing.B) { benchmarkForward(b, 100) }

// benchmarkForward measures the copy-on-write atomic load + write loop lifted
// from forwardLoop with n mock listeners. This isolates room dispatch from any
// allocation inside *webrtc.TrackLocalStaticRTP.WriteRTP.
//
// ADR-005: the hot path uses one atomic.Load; no lock is held during WriteRTP.
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
		// Mirror exactly the hot path in forwardLoop (ADR-005 copy-on-write).
		ls := *r.listeners.Load()
		for _, e := range ls {
			_ = e.l.WriteRTP(pkt)
		}
	}
}

// discardListener drops every packet with zero allocation.
type discardListener struct{}

func (discardListener) WriteRTP(_ *rtp.Packet) error { return nil }

// BenchmarkWriteRTPToPionTrack measures alloc/ns for WriteRTP on a real
// *webrtc.TrackLocalStaticRTP. This is the ADR-009 measurement.
// Run: go test -bench=BenchmarkWriteRTPToPionTrack -benchmem ./internal/room/
//
// To get alloc count per op, run:
//
//	go test -bench=BenchmarkWriteRTPToPionTrack -benchmem -count=1 ./internal/room/
func BenchmarkWriteRTPToPionTrack(b *testing.B) {
	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "pocketstation",
	)
	if err != nil {
		b.Fatal(err)
	}
	pkt := &rtp.Packet{Payload: make([]byte, 200)}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// WriteRTP returns error when no peer is connected; we measure
		// the allocation profile of the pre-send path.
		_ = track.WriteRTP(pkt)
	}
}
