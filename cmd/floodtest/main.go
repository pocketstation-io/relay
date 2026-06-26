// floodtest: high-throughput RTP packet flood for the PocketStation relay.
//
// Creates N parallel rooms, each with one publisher and one subscriber.
// Publishers send RTP packets as fast as WriteRTP allows (no artificial sleep),
// so the achieved rate reflects the real SRTP + relay forwarding ceiling.
// Subscribers drain arriving packets so the relay counts them as forwarded.
//
// Runs until --target packets have been sent across all workers, or --max-duration
// elapses. Reports every 5 s and writes a JSON artifact at the end.
//
// Usage:
//
//	go run ./cmd/floodtest/ --relay http://localhost:8080 --target 100000000 --workers 10
//	go run ./cmd/floodtest/ --relay https://pocketstation-relay.fly.dev --target 1000000 --workers 4
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/signaling"
)

const (
	opusPayloadType      = 111
	opusSamplesPerPacket = 960
	iceTimeout           = 30 * time.Second
	progressInterval     = 5 * time.Second
)

var validOpusSilence = []byte{0xF8, 0xFF, 0xFE}

func must(err error, ctx string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL [%s]: %v\n", ctx, err)
		os.Exit(1)
	}
}

// Artifact is the JSON proof file written at the end of the run.
type Artifact struct {
	Relay            string    `json:"relay"`
	Workers          int       `json:"workers"`
	Target           int64     `json:"target"`
	PacketsSent      int64     `json:"packets_sent"`
	PacketsForwarded int64     `json:"packets_forwarded_relay_counter"`
	ElapsedSeconds   float64   `json:"elapsed_seconds"`
	AvgRatePerSec    float64   `json:"avg_rate_pkt_per_sec"`
	PacketsDropped   int64     `json:"packets_dropped_relay_counter"`
	RoomsActive      int64     `json:"rooms_active_at_end"`
	SessionsTotal    int64     `json:"sessions_total"`
	StartedAt        time.Time `json:"started_at"`
	CompletedAt      time.Time `json:"completed_at"`
	ReachedTarget    bool      `json:"reached_target"`
}

func main() {
	relayURL := flag.String("relay", "http://localhost:8080", "relay base URL")
	target := flag.Int64("target", 100_000_000, "total packets to send across all workers")
	workers := flag.Int("workers", 10, "parallel publisher-subscriber pairs")
	maxDur := flag.Duration("max-duration", 4*time.Hour, "hard time limit")
	outFile := flag.String("out", "floodtest-result.json", "path for JSON artifact")
	flag.Parse()

	wsBase := "ws" + (*relayURL)[len("http"):]

	fmt.Println("╔═══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║  PocketStation Relay Flood Test                                    ║")
	fmt.Printf("║  Relay:   %-55s ║\n", *relayURL)
	fmt.Printf("║  Target:  %-8d packets    Workers: %-3d   Max: %s\n", *target, *workers, *maxDur)
	fmt.Println("╚═══════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Verify relay is reachable before starting.
	if _, err := http.Get(*relayURL + "/healthz"); err != nil {
		fmt.Fprintf(os.Stderr, "relay unreachable at %s: %v\n", *relayURL, err)
		os.Exit(1)
	}
	fmt.Println("  Relay health: OK")
	fmt.Println()

	var totalSent atomic.Int64
	var writeErrors atomic.Int64

	startTime := time.Now()
	deadline := startTime.Add(*maxDur)

	// Launch workers: each creates a room, a publisher, and a subscriber.
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(workerID, *relayURL, wsBase, &totalSent, &writeErrors, *target, *workers, deadline)
		}(i)
	}

	// Progress reporter.
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(progressInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sent := totalSent.Load()
				elapsed := time.Since(startTime).Seconds()
				rate := float64(sent) / elapsed
				pct := float64(sent) / float64(*target) * 100
				fwd, _, _ := fetchMetric(*relayURL, "relay_packets_forwarded_total")
				fmt.Printf("  [%6.0fs]  sent=%10d  fwd=%10d  rate=%8.0f pkt/s  %.1f%%\n",
					elapsed, sent, fwd, rate, pct)
				if sent >= *target || time.Now().After(deadline) {
					return
				}
			}
		}
	}()

	wg.Wait()

	elapsed := time.Since(startTime)
	sent := totalSent.Load()
	fwd, dropped, rooms := fetchMetrics(*relayURL)

	// Signal progress reporter to stop (it may already be done).
	select {
	case <-done:
	default:
	}

	art := Artifact{
		Relay:            *relayURL,
		Workers:          *workers,
		Target:           *target,
		PacketsSent:      sent,
		PacketsForwarded: fwd,
		PacketsDropped:   dropped,
		RoomsActive:      rooms,
		ElapsedSeconds:   elapsed.Seconds(),
		AvgRatePerSec:    float64(sent) / elapsed.Seconds(),
		StartedAt:        startTime,
		CompletedAt:      time.Now(),
		ReachedTarget:    sent >= *target,
	}

	printReport(art, writeErrors.Load())
	saveArtifact(*outFile, art)
}

func runWorker(
	id int,
	relayBase, wsBase string,
	totalSent *atomic.Int64,
	writeErrors *atomic.Int64,
	target int64,
	workers int,
	deadline time.Time,
) {
	// Create room.
	roomID, srcToken, lstToken := createRoom(relayBase)

	// Start subscriber first (so the relay has a listener before packets arrive).
	subPC := newPeer()
	_, err := subPC.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	})
	must(err, fmt.Sprintf("worker %d: sub transceiver", id))

	subPC.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		// Drain packets so the connection stays open and the relay can forward.
		buf := make([]byte, 1500)
		for {
			if _, _, err := track.ReadRTP(); err != nil {
				return
			}
			_ = buf
		}
	})
	doSignal(subPC, wsBase, roomID, lstToken, signaling.TypeSubscribe)
	waitICE(subPC)

	// Publisher.
	pubPC := newPeer()
	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", fmt.Sprintf("flood-%d", id),
	)
	must(err, fmt.Sprintf("worker %d: new track", id))
	_, err = pubPC.AddTrack(audioTrack)
	must(err, fmt.Sprintf("worker %d: add track", id))

	doSignal(pubPC, wsBase, roomID, srcToken, signaling.TypePublish)
	waitICE(pubPC)

	// Allow the relay to register both peers before the send flood starts.
	// Without this pause the relay may not have the subscriber in its listener
	// slice when the first RTP batch arrives, so packets are not forwarded.
	time.Sleep(2 * time.Second)

	// Send loop: no sleep — as fast as WriteRTP allows.
	var seqNum uint16
	var ts uint32
	workerTarget := target / int64(workers)
	if workerTarget < 1 {
		workerTarget = 1
	}
	var sent int64

	for sent < workerTarget && time.Now().Before(deadline) {
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    opusPayloadType,
				SequenceNumber: seqNum,
				Timestamp:      ts,
				SSRC:           uint32(0xF100_0000 | id),
			},
			Payload: validOpusSilence,
		}
		if err := audioTrack.WriteRTP(pkt); err != nil {
			writeErrors.Add(1)
			// Brief yield on error to avoid a busy-error loop.
			time.Sleep(time.Millisecond)
		} else {
			sent++
			totalSent.Add(1)
		}
		seqNum++
		ts += opusSamplesPerPacket
	}

	_ = pubPC.Close()
	_ = subPC.Close()
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

func newPeer() *webrtc.PeerConnection {
	cfg := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	}
	pc, err := webrtc.NewPeerConnection(cfg)
	must(err, "new peer connection")
	return pc
}

func doSignal(pc *webrtc.PeerConnection, wsBase, roomID, token string, msgType signaling.MessageType) {
	conn, _, err := websocket.DefaultDialer.Dial(wsBase+"/v1/signal", nil)
	must(err, "ws dial "+string(msgType))

	offer, err := pc.CreateOffer(nil)
	must(err, "create offer")
	must(pc.SetLocalDescription(offer), "set local desc")

	must(conn.WriteJSON(signaling.ClientMessage{
		Type:     msgType,
		RoomID:   roomID,
		Token:    token,
		SDPOffer: offer.SDP,
	}), "send "+string(msgType))

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		_ = conn.WriteJSON(signaling.ClientMessage{
			Type:      signaling.TypeIce,
			Candidate: c.ToJSON().Candidate,
		})
	})

	go func() {
		for {
			var msg signaling.ServerMessage
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
			switch msg.Type {
			case signaling.TypeSDPAnswer:
				_ = pc.SetRemoteDescription(webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer,
					SDP:  msg.SDPAnswer,
				})
			case signaling.TypeIce:
				_ = pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: msg.Candidate})
			}
		}
	}()
}

func waitICE(pc *webrtc.PeerConnection) {
	ch := make(chan struct{}, 1)
	pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		if s == webrtc.ICEConnectionStateConnected || s == webrtc.ICEConnectionStateCompleted {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	})
	// Guard against ICE already connected before handler registered.
	if st := pc.ICEConnectionState(); st == webrtc.ICEConnectionStateConnected || st == webrtc.ICEConnectionStateCompleted {
		return
	}
	select {
	case <-ch:
	case <-time.After(iceTimeout):
		fmt.Fprintln(os.Stderr, "ICE timeout")
		os.Exit(1)
	}
}

// fetchMetrics reads /metrics and returns forwarded, dropped, rooms_active counters.
func fetchMetrics(base string) (forwarded, dropped, rooms int64) {
	f, _, _ := fetchMetric(base, "relay_packets_forwarded_total")
	d, _, _ := fetchMetric(base, "relay_packets_dropped_total")
	r, _, _ := fetchMetric(base, "relay_rooms_active")
	return f, d, r
}

func fetchMetric(base, name string) (value int64, ok bool, err error) {
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false, err
	}
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") || !strings.HasPrefix(line, name+" ") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return 0, false, err
		}
		return int64(math.Round(v)), true, nil
	}
	return 0, false, nil
}

func printReport(a Artifact, writeErrors int64) {
	fwdPct := 0.0
	if a.PacketsSent > 0 {
		fwdPct = float64(a.PacketsForwarded) / float64(a.PacketsSent) * 100
	}
	fmt.Println()
	fmt.Println("  ╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("  ║  FLOOD TEST RESULT                                                ║")
	fmt.Println("  ╠══════════════════════════════════════════════════════════════════╣")
	fmt.Printf("  ║  Relay:              %-45s ║\n", a.Relay)
	fmt.Printf("  ║  Workers:            %-45d ║\n", a.Workers)
	fmt.Printf("  ║  Target:             %-45d ║\n", a.Target)
	fmt.Printf("  ║  Packets sent:       %-45d ║\n", a.PacketsSent)
	fmt.Printf("  ║  Packets forwarded:  %-45d ║\n", a.PacketsForwarded)
	fmt.Printf("  ║  Forwarded %%:        %-44.1f%% ║\n", fwdPct)
	fmt.Printf("  ║  Packets dropped:    %-45d ║\n", a.PacketsDropped)
	fmt.Printf("  ║  Write errors:       %-45d ║\n", writeErrors)
	fmt.Printf("  ║  Elapsed:            %-45s ║\n", time.Duration(a.ElapsedSeconds*float64(time.Second)).Round(time.Millisecond))
	fmt.Printf("  ║  Avg rate:           %-37.0f pkt/s ║\n", a.AvgRatePerSec)
	fmt.Printf("  ║  Reached target:     %-45v ║\n", a.ReachedTarget)
	fmt.Println("  ╚══════════════════════════════════════════════════════════════════╝")
}

func saveArtifact(path string, a Artifact) {
	data, err := json.MarshalIndent(a, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal artifact: %v\n", err)
		return
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "write artifact: %v\n", err)
		return
	}
	fmt.Printf("\n  Artifact saved: %s\n", path)
}
