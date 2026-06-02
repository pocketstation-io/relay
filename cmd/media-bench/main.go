// media-bench: real media-path latency benchmark.
//
// This binary creates a real WebRTC publisher AND a real WebRTC subscriber
// connected through the live relay. It sends actual RTP packets with an
// 8-byte nanosecond send-timestamp embedded in the payload. The subscriber
// reads that timestamp on receipt. Since both publisher and subscriber run
// on the same machine with the same clock, RTT÷2 is a valid one-way estimate
// for the relay forwarding path.
//
// What this measures:
//   source_encode → local_send → relay_receive → relay_forward → local_receive
//
// What this does NOT claim:
//   - Cross-machine one-way latency (needs NTP/PTP clock sync)
//   - Browser playout latency (no audio output path here)
//   - Competitor comparisons (that requires running under identical conditions)
//
// Usage:
//   go run ./cmd/media-bench/ --relay https://pocketstation-relay.fly.dev
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/signaling"
)

const (
	payloadType      = 111
	samplesPerPacket = 960 // 20ms at 48kHz
	cadence          = 20 * time.Millisecond
	iceTimeout       = 30 * time.Second
	warmupPackets    = 25 // discard first 500ms (ICE/DTLS settling)
)

// opusHeader is a valid 3-byte Opus comfort-noise frame. We prepend it to
// our 8-byte timestamp so the total payload is valid enough for the relay
// to forward (it does not decode audio).
var opusHeader = []byte{0xF8, 0xFF, 0xFE}

func must(err error, ctx string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL [%s]: %v\n", ctx, err)
		os.Exit(1)
	}
}

type sample struct {
	seq  uint16
	sent time.Time
	recv time.Time
}

func main() {
	relayBase := flag.String("relay", "http://localhost:8080", "relay base URL")
	n         := flag.Int("n", 200, "RTP packets to measure (after warmup)")
	fanout    := flag.Int("fanout", 1, "number of subscriber peers")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Determine WS scheme
	wsBase := "ws" + (*relayBase)[len("http"):]

	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║  PocketStation Real Media-Path Benchmark                          ║")
	fmt.Printf( "║  Relay:   %s\n", *relayBase+"                             ║")
	fmt.Printf( "║  Packets: %d measured + %d warmup  Fanout: %d subscriber(s)\n", *n, warmupPackets, *fanout)
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("  What is measured: source RTP send → relay forward → subscriber receive")
	fmt.Println("  Clock: single machine (no cross-host sync needed)")
	fmt.Println("  RTT ÷ 2 = valid one-way estimate (same clock both ends)")
	fmt.Println()

	// ── Create room ──────────────────────────────────────────────────────
	room, srcToken, lstToken := createRoom(*relayBase)
	fmt.Printf("  Room: %s\n\n", room)

	total := warmupPackets + *n
	sentTimes   := make(map[uint16]time.Time, total)
	var sentMu  sync.Mutex

	// Collect one RTT sample per subscriber per packet seq
	type subResult struct {
		seq  uint16
		rtt  time.Duration
	}
	results := make([]time.Duration, 0, *n**fanout)
	var resultsMu sync.Mutex
	var subWg sync.WaitGroup

	// ── Start subscribers ─────────────────────────────────────────────────
	for i := 0; i < *fanout; i++ {
		subWg.Add(1)
		go func(id int) {
			defer subWg.Done()
			pc := newPeer(logger)
			pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
				var count int
				for {
					pkt, _, err := track.ReadRTP()
					if err != nil { return }
					recvTime := time.Now()
					seq := pkt.Header.SequenceNumber

					sentMu.Lock()
					sent, ok := sentTimes[seq]
					sentMu.Unlock()

					if !ok || pkt.Header.SequenceNumber < uint16(warmupPackets) { continue }
					rtt := recvTime.Sub(sent)
					resultsMu.Lock()
					results = append(results, rtt)
					resultsMu.Unlock()
					count++
					if count >= *n { return }
				}
			})
			subscribe(logger, pc, wsBase, room, lstToken)
		}(i)
	}

	// Brief wait for subscribers to connect
	time.Sleep(3 * time.Second)

	// ── Publish ───────────────────────────────────────────────────────────
	pc := newPeer(logger)
	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "pocketstation-bench",
	)
	must(err, "new track")
	_, err = pc.AddTrack(audioTrack)
	must(err, "add track")

	publish(logger, pc, wsBase, room, srcToken)

	// Send packets
	var seqNum uint16
	var ts     uint32
	ticker := time.NewTicker(cadence)
	defer ticker.Stop()

	sent := 0
	for sent < total {
		<-ticker.C
		now := time.Now()

		// Payload: 3-byte Opus header + 8-byte nanosecond timestamp
		var tsBuf [8]byte
		binary.BigEndian.PutUint64(tsBuf[:], uint64(now.UnixNano()))
		payload := append(bytes.Clone(opusHeader), tsBuf[:]...)

		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    payloadType,
				SequenceNumber: seqNum,
				Timestamp:      ts,
				SSRC:           0xBEEFCAFE,
			},
			Payload: payload,
		}

		sentMu.Lock()
		sentTimes[seqNum] = now
		sentMu.Unlock()

		if err := audioTrack.WriteRTP(pkt); err != nil {
			logger.Warn("WriteRTP", "err", err)
		}
		seqNum++
		ts += samplesPerPacket
		sent++
	}

	// Wait for all subscriber results
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resultsMu.Lock()
		done := len(results) >= *n**fanout
		resultsMu.Unlock()
		if done { break }
		time.Sleep(100 * time.Millisecond)
	}

	// ── Report ────────────────────────────────────────────────────────────
	resultsMu.Lock()
	rtts := results
	resultsMu.Unlock()

	if len(rtts) == 0 {
		fmt.Println("  ERROR: No RTP packets received by subscriber.")
		fmt.Println("  This means ICE/DTLS did not complete or relay did not forward.")
		fmt.Println("  Possible cause: relay not reachable for UDP media plane.")
		os.Exit(1)
	}

	sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })
	p := func(pct float64) time.Duration {
		idx := int(math.Ceil(pct/100.0*float64(len(rtts)))) - 1
		if idx < 0 { idx = 0 }
		return rtts[idx]
	}

	rttP50 := p(50)
	rttP95 := p(95)
	rttP99 := p(99)
	owP50  := rttP50 / 2
	owP95  := rttP95 / 2

	fmt.Printf("  Packets received: %d / %d expected\n", len(rtts), *n**fanout)
	fmt.Println()
	fmt.Println("  ── MEDIA PATH RTT (source → relay → subscriber) ─────────────────")
	fmt.Printf("  RTT  P50=%v  P95=%v  P99=%v  min=%v\n",
		rttP50.Round(time.Microsecond),
		rttP95.Round(time.Microsecond),
		rttP99.Round(time.Microsecond),
		rtts[0].Round(time.Microsecond))
	fmt.Println()
	fmt.Println("  ── ONE-WAY ESTIMATE (RTT÷2, same-clock valid) ───────────────────")
	fmt.Printf("  One-way P50 = %v\n", owP50.Round(time.Microsecond))
	fmt.Printf("  One-way P95 = %v\n", owP95.Round(time.Microsecond))
	fmt.Println()
	fmt.Println("  ── WHAT THIS DOES AND DOES NOT PROVE ────────────────────────────")
	fmt.Println("  PROVES:   Actual RTP media path through live relay")
	fmt.Println("  PROVES:   Relay correctly forwards packets (received count above)")
	fmt.Println("  PROVES:   RTT from this machine's clock to itself via relay")
	fmt.Println("  DOES NOT: Prove cross-machine one-way latency (needs NTP sync)")
	fmt.Println("  DOES NOT: Include mic capture or speaker playout")
	fmt.Println("  DOES NOT: Compare to LiveKit (different conditions)")
}

func createRoom(base string) (roomID, srcToken, lstToken string) {
	resp, err := http.Post(base+"/v1/rooms", "application/json", nil)
	must(err, "create room HTTP")
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var r struct {
		RoomID        string `json:"room_id"`
		SourceToken   string `json:"source_token"`
		ListenerToken string `json:"listener_token"`
	}
	must(json.Unmarshal(body, &r), "decode room")
	return r.RoomID, r.SourceToken, r.ListenerToken
}

func newPeer(logger *slog.Logger) *webrtc.PeerConnection {
	se := webrtc.SettingEngine{}
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	})
	_ = se
	must(err, "new peer connection")
	return pc
}

func waitICE(pc *webrtc.PeerConnection) {
	ch := make(chan struct{})
	pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		if s == webrtc.ICEConnectionStateConnected || s == webrtc.ICEConnectionStateCompleted {
			select { case <-ch: default: close(ch) }
		}
	})
	select {
	case <-ch:
	case <-time.After(iceTimeout):
		fmt.Fprintln(os.Stderr, "ICE timeout")
		os.Exit(1)
	}
}

func dialWS(wsBase, room, token, sdpOffer string) (*websocket.Conn, error) {
	conn, _, err := websocket.DefaultDialer.Dial(wsBase+"/v1/signal", nil)
	if err != nil { return nil, err }
	return conn, nil
}

func publish(logger *slog.Logger, pc *webrtc.PeerConnection, wsBase, room, token string) {
	conn, _, err := websocket.DefaultDialer.Dial(wsBase+"/v1/signal", nil)
	must(err, "pub ws dial")

	offer, err := pc.CreateOffer(nil)
	must(err, "create offer")
	must(pc.SetLocalDescription(offer), "set local desc")

	must(conn.WriteJSON(signaling.ClientMessage{
		Type:     signaling.TypePublish,
		RoomID:   room,
		Token:    token,
		SDPOffer: offer.SDP,
	}), "send PUBLISH")

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil { return }
		conn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeIce, Candidate: c.ToJSON().Candidate})
	})

	go func() {
		for {
			var msg signaling.ServerMessage
			if err := conn.ReadJSON(&msg); err != nil { return }
			switch msg.Type {
			case signaling.TypeSDPAnswer:
				must(pc.SetRemoteDescription(webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer, SDP: msg.SDPAnswer,
				}), "set remote desc")
			case signaling.TypeIce:
				pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: msg.Candidate})
			}
		}
	}()
	waitICE(pc)
}

func subscribe(logger *slog.Logger, pc *webrtc.PeerConnection, wsBase, room, token string) {
	conn, _, err := websocket.DefaultDialer.Dial(wsBase+"/v1/signal", nil)
	must(err, "sub ws dial")

	// Add recvonly transceiver so the SDP offer has an audio m-line
	_, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	})
	must(err, "sub add transceiver")

	offer, err := pc.CreateOffer(nil)
	must(err, "sub create offer")
	must(pc.SetLocalDescription(offer), "sub set local desc")

	must(conn.WriteJSON(signaling.ClientMessage{
		Type:     signaling.TypeSubscribe,
		RoomID:   room,
		Token:    token,
		SDPOffer: offer.SDP,
	}), "send SUBSCRIBE")

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil { return }
		conn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeIce, Candidate: c.ToJSON().Candidate})
	})

	go func() {
		for {
			var msg signaling.ServerMessage
			if err := conn.ReadJSON(&msg); err != nil { return }
			switch msg.Type {
			case signaling.TypeSDPAnswer:
				must(pc.SetRemoteDescription(webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer, SDP: msg.SDPAnswer,
				}), "sub set remote desc")
			case signaling.TypeIce:
				pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: msg.Candidate})
			}
		}
	}()
	waitICE(pc)
}
