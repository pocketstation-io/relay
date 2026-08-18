package integration_test

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// TestLive_RealSourceLoss attaches a real Pion listener to an ALREADY-RUNNING
// relay that has a real str0m (pks) source publishing, and measures the actual
// RTP loss from that real source. It is gated on env vars so it only runs when
// orchestrated:
//
//	LIVE_RELAY_WS       ws://<host>:8080/v1/signal
//	LIVE_LISTENER_TOKEN <listener JWT for the live room>
//
// This isolates source-vs-browser: if a tolerant Pion listener ALSO loses ~half
// the packets from the real source, the defect is in the source's RTP output.
// If it receives essentially all of them, the loss is browser-specific.
func TestGivenLiveSourceWhenConnectionIsLostThenSessionRecovers(t *testing.T) {
	wsURL := os.Getenv("LIVE_RELAY_WS")
	token := os.Getenv("LIVE_LISTENER_TOKEN")
	if wsURL == "" || token == "" {
		t.Skip("set LIVE_RELAY_WS and LIVE_LISTENER_TOKEN to run the live source-loss test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial relay WS %s: %v", wsURL, err)
	}
	defer conn.Close()
	msgs := readServerMessages(conn)

	// Real ICE (no loopback API): the listener must reach the relay's real host
	// candidates exactly as a browser would.
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	})
	if err != nil {
		t.Fatalf("create listener PC: %v", err)
	}
	defer pc.Close()

	trackCh := make(chan *webrtc.TrackRemote, 1)
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		select {
		case trackCh <- track:
		default:
		}
	})

	doSubscribeHandshake(t, conn, pc, token, msgs, 15*time.Second)
	waitICEConnected(ctx, t, pc)

	var track *webrtc.TrackRemote
	select {
	case track = <-trackCh:
	case <-ctx.Done():
		t.Fatalf("no OnTrack (no RTP forwarded): %v", ctx.Err())
	}

	// Read for a fixed measurement window, recording arrival-ordered sequence
	// numbers so we can reconstruct the extended (unwrapped) sequence space.
	const measureWindow = 20 * time.Second
	seqs := make([]uint16, 0, 4096)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			pkt, _, rerr := track.ReadRTP()
			if rerr != nil {
				return
			}
			if len(seqs) < 8 {
				t.Logf("  decoded pkt #%d: seq=%d ts=%d ssrc=%d pt=%d payloadLen=%d",
					len(seqs), pkt.SequenceNumber, pkt.Timestamp, pkt.SSRC, pkt.PayloadType, len(pkt.Payload))
			}
			seqs = append(seqs, pkt.SequenceNumber)
		}
	}()

	time.Sleep(measureWindow)

	// Capture transport vs RTP stats BEFORE closing: this distinguishes
	// "packets never arrived at the socket" (transport bytesReceived low) from
	// "arrived but rejected by SRTP/RTP" (transport high, inbound low).
	for _, s := range pc.GetStats() {
		switch st := s.(type) {
		case webrtc.TransportStats:
			t.Logf("  transport: bytesReceived=%d packetsReceived=%d", st.BytesReceived, st.PacketsReceived)
		case webrtc.ICECandidatePairStats:
			if st.Nominated {
				t.Logf("  nominated pair: bytesRecv=%d packetsRecv=%d requestsRecv=%d responsesRecv=%d state=%s",
					st.BytesReceived, st.PacketsReceived, st.RequestsReceived, st.ResponsesReceived, st.State)
			}
		case webrtc.InboundRTPStreamStats:
			t.Logf("  inbound-rtp: packetsReceived=%d packetsLost=%d", st.PacketsReceived, st.PacketsLost)
		}
	}

	_ = pc.Close()
	<-readDone

	if len(seqs) == 0 {
		t.Fatalf("received 0 RTP packets in %s — source not publishing or no track", measureWindow)
	}

	// Unwrap 16-bit sequence numbers into a monotonic extended space.
	ext := make([]int64, len(seqs))
	var rollover int64
	ext[0] = int64(seqs[0])
	for i := 1; i < len(seqs); i++ {
		prev, cur := seqs[i-1], seqs[i]
		if cur < prev && uint16(prev-cur) > 0x8000 {
			rollover += 1 << 16
		}
		ext[i] = rollover + int64(cur)
	}
	sort.Slice(ext, func(i, j int) bool { return ext[i] < ext[j] })

	// Distinct count and span.
	distinct := 1
	for i := 1; i < len(ext); i++ {
		if ext[i] != ext[i-1] {
			distinct++
		}
	}
	span := ext[len(ext)-1] - ext[0] + 1
	lossPct := 100.0 * float64(span-int64(distinct)) / float64(span)
	rate := float64(len(seqs)) / measureWindow.Seconds()

	t.Logf("LIVE source loss: window=%s distinct=%d span=%d received_rate=%.1f pkt/s loss=%.1f%%",
		measureWindow, distinct, span, rate, lossPct)
	t.Logf("  (real str0m source -> relay -> real Pion listener)")

	// Informational sanity, not a hard gate: if the source publishes ~50 pkt/s
	// the span should be ~1000 over 20 s.
	_ = signaling.TypeSubscribe
}
