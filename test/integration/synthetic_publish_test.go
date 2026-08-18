package integration_test

import (
	"context"
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// TestSyntheticPublish is the ELIMINATION experiment for the radio-glitch bug.
//
// It replaces the real Rust CLI (audio-core / str0m) source with a synthetic,
// maximally-clean RTP source publishing to an ALREADY-RUNNING relay over real
// ICE: monotonic sequence numbers, fixed SSRC, 20 ms (960-sample) Opus cadence,
// and NO RTP header extensions. A browser then subscribes and measures loss.
//
//   - If Chrome STILL loses ~half the packets with this synthetic source, the
//     defect is purely in the relay's forward/encrypt path — the CLI/audio-core
//     is exonerated.
//   - If Chrome receives essentially all of them, the loss is triggered by some
//     characteristic of the REAL source's stream (audio-core/CLI), not the relay.
//
// Gated on env so it only runs when orchestrated:
//
//	SYNTH_PUBLISH=1
//	LIVE_RELAY_WS        ws://<host>:8080/v1/signal
//	SYNTH_SOURCE_TOKEN   <source JWT for the live room>
func TestGivenSyntheticPublisherWhenAudioIsSentThenSubscriberReceivesIt(t *testing.T) {
	if os.Getenv("SYNTH_PUBLISH") != "1" {
		t.Skip("set SYNTH_PUBLISH=1, LIVE_RELAY_WS, SYNTH_SOURCE_TOKEN to run the synthetic publisher")
	}
	wsURL := os.Getenv("LIVE_RELAY_WS")
	token := os.Getenv("SYNTH_SOURCE_TOKEN")
	if wsURL == "" || token == "" {
		t.Skip("set LIVE_RELAY_WS and SYNTH_SOURCE_TOKEN")
	}

	const publishFor = 45 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), publishFor+15*time.Second)
	defer cancel()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial relay WS %s: %v", wsURL, err)
	}
	defer conn.Close()
	msgs := readServerMessages(conn)

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	})
	if err != nil {
		t.Fatalf("create publisher PC: %v", err)
	}
	defer pc.Close()

	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "audio", "pocketstation-synth")
	if err != nil {
		t.Fatalf("create synthetic track: %v", err)
	}
	if _, err := pc.AddTrack(track); err != nil {
		t.Fatalf("add synthetic track: %v", err)
	}

	doPublishHandshake(t, conn, pc, token, msgs, 15*time.Second)
	waitICEConnected(ctx, t, pc)
	t.Logf("synthetic publisher ICE connected; emitting 50 pkt/s clean Opus-cadence RTP for %s", publishFor)

	// A 20 ms Opus silence frame (TOC byte + minimal payload). Content is
	// irrelevant to LOSS (which is computed from sequence-number gaps), but a
	// plausible Opus payload keeps the browser decoder from rejecting the track.
	payload := []byte{0xf8, 0xff, 0xfe}

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(publishFor)
	var seq uint16 = 1000
	var ts uint32 = 160000
	const synthSSRC = 0x5151AAAA
	sent := 0
	for {
		select {
		case <-deadline:
			t.Logf("synthetic publisher done: sent %d packets", sent)
			return
		case <-ctx.Done():
			t.Fatalf("context cancelled after %d packets: %v", sent, ctx.Err())
		case <-ticker.C:
			pkt := &rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					PayloadType:    111,
					SequenceNumber: seq,
					Timestamp:      ts,
					SSRC:           synthSSRC,
				},
				Payload: payload,
			}
			// SYNTH_EXT=1 reproduces the real CLI source's transport-wide-cc (TWCC)
			// header extension: one-byte profile (0xBEDE), id 3, a 2-byte sequence
			// that increments every packet. This is the one measured difference
			// between the clean synthetic stream and the glitchy real stream.
			if os.Getenv("SYNTH_EXT") == "1" {
				twcc := make([]byte, 2)
				binary.BigEndian.PutUint16(twcc, seq)
				if err := pkt.SetExtension(3, twcc); err != nil {
					t.Fatalf("set TWCC extension: %v", err)
				}
			}
			// SYNTH_PAD=1 reproduces the real CLI source's RTP padding bit WITHOUT
			// actual padding bytes — exactly the state pion leaves a packet in after
			// Unmarshal strips the padding but keeps Header.Padding=true. The
			// receiver then misreads the last payload byte as the padding count.
			if os.Getenv("SYNTH_PAD") == "1" {
				pkt.Header.Padding = true
			}
			if err := track.WriteRTP(pkt); err != nil {
				t.Logf("WriteRTP error at pkt %d: %v", sent, err)
			}
			seq++
			ts += 960
			sent++
		}
	}
}
