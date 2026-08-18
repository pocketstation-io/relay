package integration_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// TestGivenSourceSendsKnownCountWhenForwardedThenListenerLossIsLow
// measures the actual end-to-end RTP loss rate through the relay. The source
// sends a known number of packets at the real 50 pkt/s production rate with
// monotonic sequence numbers, then stops; the listener identifies measured
// packets by payload marker and counts DISTINCT translated sequence numbers.
// This is the measurement the existing forwarding
// test cannot make: that test reads "until N arrive" from a 1000 pkt/s firehose,
// so it never observes loss. Here loss is the whole point.
//
// If a real Pion listener over real SRTP loses ~half the packets, the ~47%
// loss seen in Chrome is reproducible relay-side. If the listener receives
// essentially all of them, the loss is specific to the browser/cross-process
// path and not the relay's forwarding.
func TestGivenSourceSendsKnownCountWhenForwardedThenListenerLossIsLow(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test uses real Pion ICE — skipped in -short mode")
	}

	const (
		sendCount  = 500                   // 500 packets ...
		sendPeriod = 20 * time.Millisecond // ... at 50 pkt/s = 10 s of audio
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ts, clientAPI := newTestServer(t)

	roomPayload := createRoom(t, ts)
	sourceToken := roomPayload["source_token"]
	listenerToken := roomPayload["listener_token"]

	// Publisher.
	pubConn := dialSignal(t, ts)
	pubMsgs := readServerMessages(pubConn)
	pubPC, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create publisher PC: %v", err)
	}
	defer pubPC.Close()

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "pocketstation-pub")
	if err != nil {
		t.Fatalf("create publisher track: %v", err)
	}
	if _, err := pubPC.AddTrack(audioTrack); err != nil {
		t.Fatalf("add publisher track: %v", err)
	}
	doPublishHandshake(t, pubConn, pubPC, sourceToken, pubMsgs, 10*time.Second)
	waitICEConnected(ctx, t, pubPC)

	// Subscriber.
	subConn := dialSignal(t, ts)
	subMsgs := readServerMessages(subConn)
	subPC, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create subscriber PC: %v", err)
	}
	defer subPC.Close()

	trackCh := make(chan *webrtc.TrackRemote, 1)
	subPC.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		select {
		case trackCh <- track:
		default:
		}
	})

	// Start a warm-up sender so the relay's OnTrack fires and forwarding starts
	// before the subscriber connects (relay OnTrack only fires on first packet).
	warmStop := make(chan struct{})
	warmupPayload := bytes.Repeat([]byte{0xAB}, 80)
	measuredPayload := bytes.Repeat([]byte{0xCD}, 80)
	go func() {
		tk := time.NewTicker(sendPeriod)
		defer tk.Stop()
		var seq uint16 = 1 // warm-up uses high-bit space; counted separately below
		for {
			select {
			case <-warmStop:
				return
			case <-tk.C:
				_ = audioTrack.WriteRTP(&rtp.Packet{Header: rtp.Header{
					Version: 2, PayloadType: 111, SSRC: 0xDEADBEEF,
					SequenceNumber: 60000 + seq, Timestamp: uint32(seq) * 960,
				}, Payload: warmupPayload})
				seq++
			}
		}
	}()

	doSubscribeHandshake(t, subConn, subPC, listenerToken, subMsgs, 10*time.Second)
	waitICEConnected(ctx, t, subPC)

	var remoteTrack *webrtc.TrackRemote
	select {
	case remoteTrack = <-trackCh:
	case <-ctx.Done():
		t.Fatalf("context expired waiting for OnTrack: %v", ctx.Err())
	}
	close(warmStop) // stop warm-up; begin the measured run

	// Collect received sequence numbers until the read errors (we close subPC
	// after the measured send completes + grace).
	seen := make(map[uint16]struct{})
	var mu sync.Mutex
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			pkt, _, err := remoteTrack.ReadRTP()
			if err != nil {
				return
			}
			// The relay's per-subscriber forwarder translates sequence numbers,
			// so payload identity separates measured packets from warm-up traffic.
			if len(pkt.Payload) > 0 && pkt.Payload[0] == measuredPayload[0] {
				mu.Lock()
				seen[pkt.SequenceNumber] = struct{}{}
				mu.Unlock()
			}
		}
	}()

	// Measured send: exactly sendCount packets, monotonic seq 0..sendCount-1.
	tk := time.NewTicker(sendPeriod)
	defer tk.Stop()
	for i := 0; i < sendCount; i++ {
		<-tk.C
		_ = audioTrack.WriteRTP(&rtp.Packet{Header: rtp.Header{
			Version: 2, PayloadType: 111, SSRC: 0xDEADBEEF,
			SequenceNumber: uint16(i), Timestamp: uint32(i) * 960,
		}, Payload: measuredPayload})
	}

	time.Sleep(500 * time.Millisecond) // drain in-flight
	_ = subPC.Close()                  // unblock ReadRTP
	<-readDone

	mu.Lock()
	received := len(seen)
	mu.Unlock()

	lossPct := 100.0 * float64(sendCount-received) / float64(sendCount)
	t.Logf("E2E RTP loss: sent=%d received=%d loss=%.1f%% (real Pion source->relay->Pion listener, real SRTP)",
		sendCount, received, lossPct)

	// A healthy relay forwards essentially everything over loopback SRTP.
	// Allow a small margin for the ICE/DTLS warm-up window.
	if lossPct > 10.0 {
		t.Fatalf("E2E loss %.1f%% exceeds 10%% — relay is dropping packets in the forward path", lossPct)
	}

	_ = pubConn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeLeave})
	_ = subConn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeLeave})
}
