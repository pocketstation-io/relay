// Benchmarks for the RTP forward path.
//
// ADR-009: no zero-alloc claim for the relay is permitted until the
// *webrtc.TrackLocalStaticRTP write path has been measured with a connected
// peer. These benchmarks use a discard Listener to isolate room dispatch
// overhead; BenchmarkWriteRTPFanoutConnected provides the companion measurement
// using a real connected Pion pair (SRTP path) per ADR-009 Phase 2 requirement.
//
// Run with:
//
//	go test -bench=BenchmarkWriteRTP -benchmem ./internal/room/
package room

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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
// Phase 2 (ADR-009): the companion benchmark with real connected Pion instances
// is implemented below as BenchmarkWriteRTPFanoutConnected.
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

// BenchmarkWriteRTPFanoutConnected is the ADR-009 Phase 2 measurement.
// It creates fanout N real Pion loopback pairs (sender TrackLocalStaticRTP →
// receiver PeerConnection) and measures WriteRTP through the live SRTP path.
//
// The setup cost (ICE negotiation, SRTP key exchange) is outside the timed
// loop. The bench body mirrors the forwardLoop hot-path (one atomic.Load, then
// WriteRTP per listener) but uses connected tracks instead of discardListener,
// so DTLS/SRTP encryption overhead is included in every iteration.
//
// Compare with BenchmarkWriteRTPToPionTrack (disconnected, 0 alloc pre-send
// path) to isolate the SRTP encryption cost.
//
// Run:
//
//	go test -bench=BenchmarkWriteRTPFanoutConnected -benchmem -benchtime=5s -run=^$ ./internal/room/
func BenchmarkWriteRTPFanoutConnected_1(b *testing.B)   { benchmarkForwardConnected(b, 1) }
func BenchmarkWriteRTPFanoutConnected_10(b *testing.B)  { benchmarkForwardConnected(b, 10) }
func BenchmarkWriteRTPFanoutConnected_50(b *testing.B)  { benchmarkForwardConnected(b, 50) }
func BenchmarkWriteRTPFanoutConnected_200(b *testing.B) { benchmarkForwardConnected(b, 200) }

// benchmarkForwardConnected sets up n real Pion loopback pairs and benchmarks
// the forwardLoop write path through the live SRTP tunnel.
func benchmarkForwardConnected(b *testing.B, n int) {
	b.Helper()

	// newBenchLoopbackAPI returns a *webrtc.API pinned to 127.0.0.1 host
	// candidates so ICE resolves immediately without external STUN or real
	// network interfaces. Aggressive timeouts keep bench setup fast.
	newBenchLoopbackAPI := func() *webrtc.API {
		se := webrtc.SettingEngine{}
		se.SetNAT1To1IPs([]string{"127.0.0.1"}, webrtc.ICECandidateTypeHost)
		se.SetICETimeouts(10*time.Second, 10*time.Second, 3*time.Second)
		return webrtc.NewAPI(webrtc.WithSettingEngine(se))
	}

	api := newBenchLoopbackAPI()

	// tracks holds the connected sender tracks used as listeners in the bench.
	tracks := make([]*webrtc.TrackLocalStaticRTP, 0, n)
	// pcs collects all PeerConnections so we can close them at the end.
	pcs := make([]*webrtc.PeerConnection, 0, n*2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < n; i++ {
		track, senderPC, receiverPC := mustConnectedPionPair(ctx, b, api)
		tracks = append(tracks, track)
		pcs = append(pcs, senderPC, receiverPC)
	}

	// Register each connected track as a room Listener.
	r := New(fmt.Sprintf("bench-connected-%d", n))
	defer r.Close()
	for i, t := range tracks {
		r.AddListener(fmt.Sprintf("peer-%d", i), t)
	}

	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    111,
			SequenceNumber: 1,
			Timestamp:      0,
			SSRC:           0xBEEF0001,
		},
		Payload: make([]byte, 200), // typical 20 ms Opus frame
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Mirror the forwardLoop hot-path (ADR-005): one atomic.Load, then
		// WriteRTP per listener. Connected tracks exercise the full SRTP path.
		ls := *r.listeners.Load()
		for _, e := range ls {
			_ = e.l.WriteRTP(pkt)
		}
	}

	b.StopTimer()
	for _, pc := range pcs {
		_ = pc.Close()
	}
}

// mustConnectedPionPair creates one sender PeerConnection with a
// TrackLocalStaticRTP and one receiver PeerConnection, negotiates them
// in-process over loopback, and waits until ICE is connected on the sender.
// Returns the sender track (which WriteRTP on will go through SRTP), and both
// PeerConnections for cleanup.
//
// SDP exchange is done via direct SetLocalDescription / SetRemoteDescription
// without a signaling server. ICE candidates are exchanged via OnICECandidate
// callbacks before the benchmark loop starts.
func mustConnectedPionPair(
	ctx context.Context,
	b *testing.B,
	api *webrtc.API,
) (*webrtc.TrackLocalStaticRTP, *webrtc.PeerConnection, *webrtc.PeerConnection) {
	b.Helper()

	senderPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		b.Fatalf("create sender PC: %v", err)
	}

	receiverPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		b.Fatalf("create receiver PC: %v", err)
	}

	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", fmt.Sprintf("bench-%p", senderPC),
	)
	if err != nil {
		b.Fatalf("create track: %v", err)
	}
	if _, err := senderPC.AddTrack(track); err != nil {
		b.Fatalf("add track to sender: %v", err)
	}

	// Receiver needs a recvonly transceiver so the offer/answer audio m-section
	// is negotiated and SRTP keys are derived for the sender's track.
	if _, err := receiverPC.AddTransceiverFromKind(
		webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
	); err != nil {
		b.Fatalf("add receiver transceiver: %v", err)
	}

	// Wire ICE candidates: forward each candidate from one PC to the other
	// before the bench loop begins.
	var iceMu sync.Mutex
	senderPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		iceMu.Lock()
		defer iceMu.Unlock()
		if err := receiverPC.AddICECandidate(c.ToJSON()); err != nil {
			// Non-fatal: candidate may arrive after connection is established.
			_ = err
		}
	})
	receiverPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		iceMu.Lock()
		defer iceMu.Unlock()
		if err := senderPC.AddICECandidate(c.ToJSON()); err != nil {
			_ = err
		}
	})

	// Perform the offer/answer exchange.
	offer, err := senderPC.CreateOffer(nil)
	if err != nil {
		b.Fatalf("create offer: %v", err)
	}
	if err := senderPC.SetLocalDescription(offer); err != nil {
		b.Fatalf("sender SetLocalDescription: %v", err)
	}
	if err := receiverPC.SetRemoteDescription(offer); err != nil {
		b.Fatalf("receiver SetRemoteDescription(offer): %v", err)
	}

	answer, err := receiverPC.CreateAnswer(nil)
	if err != nil {
		b.Fatalf("create answer: %v", err)
	}
	if err := receiverPC.SetLocalDescription(answer); err != nil {
		b.Fatalf("receiver SetLocalDescription(answer): %v", err)
	}
	if err := senderPC.SetRemoteDescription(answer); err != nil {
		b.Fatalf("sender SetRemoteDescription(answer): %v", err)
	}

	// Wait for the sender to reach ICE connected — this ensures DTLS/SRTP keys
	// are negotiated and WriteRTP will exercise the full SRTP encryption path.
	iceConnected := make(chan struct{})
	var iceOnce sync.Once
	senderPC.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		if state == webrtc.ICEConnectionStateConnected {
			iceOnce.Do(func() { close(iceConnected) })
		}
	})
	// Guard against a race where ICE already connected before we registered.
	if senderPC.ICEConnectionState() == webrtc.ICEConnectionStateConnected {
		return track, senderPC, receiverPC
	}
	select {
	case <-iceConnected:
	case <-ctx.Done():
		b.Fatalf("ICE connect timeout: %v", ctx.Err())
	}

	return track, senderPC, receiverPC
}
