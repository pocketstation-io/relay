// Package integration_test contains end-to-end tests for the relay server.
// Tests use httptest.NewServer and loopback Pion APIs so that no real network
// interfaces or STUN servers are required.
//
// Run with:
//
//	go test -race -timeout 60s ./test/integration/...
package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/server"
	"github.com/pocketstation-io/relay/internal/signaling"
)

const testJWTSecret = "test-secret"

// newLoopbackAPI returns a *webrtc.API configured for loopback testing.
// It constrains ICE to 127.0.0.1 host candidates so that peers resolve
// immediately without external STUN and the race detector has nothing to
// race against network I/O.
func newLoopbackAPI() *webrtc.API {
	se := webrtc.SettingEngine{}
	se.SetNAT1To1IPs([]string{"127.0.0.1"}, webrtc.ICECandidateTypeHost)
	// Aggressive timeouts keep tests fast; values are (disconnected, failed, keepalive).
	se.SetICETimeouts(10*time.Second, 10*time.Second, 3*time.Second)
	return webrtc.NewAPI(webrtc.WithSettingEngine(se))
}

// newTestServer creates an httptest.Server backed by a relay Server that uses
// the loopback Pion API.
func newTestServer(t *testing.T) (*httptest.Server, *webrtc.API) {
	t.Helper()
	api := newLoopbackAPI()
	srv := server.New(server.Config{
		JWTSecret: []byte(testJWTSecret),
		API:       api,
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, api
}

// createRoom POSTs to /v1/rooms and returns the decoded body.
func createRoom(t *testing.T, ts *httptest.Server) map[string]string {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/rooms", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("POST /v1/rooms: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, body)
	}
	var payload map[string]string
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return payload
}

// dialSignal dials the /v1/signal WebSocket endpoint on ts.
func dialSignal(t *testing.T, ts *httptest.Server) *websocket.Conn {
	t.Helper()
	u, _ := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/v1/signal"
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("dial WebSocket: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// readServerMessages drains msg from conn into the returned channel.
// The goroutine exits when the connection closes.
func readServerMessages(conn *websocket.Conn) <-chan signaling.ServerMessage {
	ch := make(chan signaling.ServerMessage, 64)
	go func() {
		defer close(ch)
		for {
			var msg signaling.ServerMessage
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
			ch <- msg
		}
	}()
	return ch
}

// doPublishHandshake performs the PUBLISH SDP/ICE exchange.
// It returns when SDP_ANSWER has been received and sets the remote description.
// ICE candidate exchange continues in background goroutines.
func doPublishHandshake(
	t *testing.T,
	conn *websocket.Conn,
	pc *webrtc.PeerConnection,
	token string,
	msgs <-chan signaling.ServerMessage,
	timeout time.Duration,
) {
	t.Helper()

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer: %v", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("set local description: %v", err)
	}

	if err := conn.WriteJSON(signaling.ClientMessage{
		Type:     signaling.TypePublish,
		Token:    token,
		SDPOffer: offer.SDP,
	}); err != nil {
		t.Fatalf("send PUBLISH: %v", err)
	}

	// Wire outgoing ICE candidates.
	var wmu sync.Mutex
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		wmu.Lock()
		defer wmu.Unlock()
		_ = conn.WriteJSON(signaling.ClientMessage{
			Type:      signaling.TypeIce,
			Candidate: c.ToJSON().Candidate,
		})
	})

	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				t.Fatal("WebSocket closed before SDP_ANSWER")
			}
			switch msg.Type {
			case signaling.TypeSDPAnswer:
				if err := pc.SetRemoteDescription(webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer,
					SDP:  msg.SDPAnswer,
				}); err != nil {
					t.Fatalf("set remote description: %v", err)
				}
				// Start relaying incoming ICE candidates in background.
				go func() {
					for msg := range msgs {
						if msg.Type == signaling.TypeIce {
							_ = pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: msg.Candidate})
						}
					}
				}()
				return
			case signaling.TypeError:
				t.Fatalf("relay returned ERROR: code=%s message=%s", msg.Code, msg.Message)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for SDP_ANSWER after %s", timeout)
		}
	}
}

// doSubscribeHandshake performs the SUBSCRIBE SDP/ICE exchange.
// Returns when SDP_ANSWER is received.
func doSubscribeHandshake(
	t *testing.T,
	conn *websocket.Conn,
	pc *webrtc.PeerConnection,
	token string,
	msgs <-chan signaling.ServerMessage,
	timeout time.Duration,
) {
	t.Helper()

	// For a subscriber, the relay sends its track via the answer, so we add a
	// recvonly transceiver to the offer to signal capability.
	if _, err := pc.AddTransceiverFromKind(
		webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
	); err != nil {
		t.Fatalf("add transceiver: %v", err)
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer: %v", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("set local description: %v", err)
	}

	if err := conn.WriteJSON(signaling.ClientMessage{
		Type:     signaling.TypeSubscribe,
		Token:    token,
		SDPOffer: offer.SDP,
	}); err != nil {
		t.Fatalf("send SUBSCRIBE: %v", err)
	}

	var wmu sync.Mutex
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		wmu.Lock()
		defer wmu.Unlock()
		_ = conn.WriteJSON(signaling.ClientMessage{
			Type:      signaling.TypeIce,
			Candidate: c.ToJSON().Candidate,
		})
	})

	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				t.Fatal("WebSocket closed before SDP_ANSWER")
			}
			switch msg.Type {
			case signaling.TypeSDPAnswer:
				if err := pc.SetRemoteDescription(webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer,
					SDP:  msg.SDPAnswer,
				}); err != nil {
					t.Fatalf("set remote description: %v", err)
				}
				go func() {
					for msg := range msgs {
						if msg.Type == signaling.TypeIce {
							_ = pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: msg.Candidate})
						}
					}
				}()
				return
			case signaling.TypeError:
				t.Fatalf("relay returned ERROR: code=%s message=%s", msg.Code, msg.Message)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for SDP_ANSWER after %s", timeout)
		}
	}
}

// waitICEConnected blocks until pc reaches the Connected ICE state or the
// context expires.
func waitICEConnected(ctx context.Context, t *testing.T, pc *webrtc.PeerConnection) {
	t.Helper()
	connected := make(chan struct{})
	var once sync.Once
	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		if state == webrtc.ICEConnectionStateConnected {
			once.Do(func() { close(connected) })
		}
	})
	// In case ICE already connected before we registered.
	if pc.ICEConnectionState() == webrtc.ICEConnectionStateConnected {
		return
	}
	select {
	case <-connected:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for ICE connected: %v", ctx.Err())
	}
}

// --- Tests ---

// TestGiven_RelayRoom_When_TokenUsedForSignal_Then_Accepted verifies that a
// valid source token results in an SDP_ANSWER being returned without ERROR.
func TestGiven_RelayRoom_When_TokenUsedForSignal_Then_Accepted(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test uses real Pion ICE — skipped in -short mode")
	}
	ts, clientAPI := newTestServer(t)

	room := createRoom(t, ts)
	sourceToken := room["source_token"]
	if sourceToken == "" {
		t.Fatal("no source_token in room response")
	}

	conn := dialSignal(t, ts)
	msgs := readServerMessages(conn)

	pc, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create peer connection: %v", err)
	}
	defer pc.Close()

	// Add a sendonly track so the offer contains an audio m-section.
	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "pocketstation-test",
	)
	if err != nil {
		t.Fatalf("create track: %v", err)
	}
	if _, err := pc.AddTrack(track); err != nil {
		t.Fatalf("add track: %v", err)
	}

	doPublishHandshake(t, conn, pc, sourceToken, msgs, 10*time.Second)
	// If we reach here, SDP_ANSWER was received with no ERROR.
}

// TestGiven_SourcePublishing_When_ListenerSubscribes_Then_RTPForwarded
// verifies that RTP packets sent by the publisher are forwarded to the
// subscriber with correct count and payload integrity.
//
// Ordering note: Pion fires OnTrack on the relay's publisher-side PC only
// when the first RTP packet arrives. The relay's forwardLoop starts from
// that OnTrack callback, so the subscriber cannot receive anything until
// the publisher has actually sent at least one packet. This test therefore
// starts the publisher send loop immediately after ICE connects, then
// connects the subscriber while packets are already flowing.
func TestGiven_SourcePublishing_When_ListenerSubscribes_Then_RTPForwarded(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test uses real Pion ICE — skipped in -short mode")
	}
	const packetCount = 30

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ts, clientAPI := newTestServer(t)

	roomPayload := createRoom(t, ts)
	sourceToken := roomPayload["source_token"]
	listenerToken := roomPayload["listener_token"]

	// --- Publisher setup ---
	pubConn := dialSignal(t, ts)
	pubMsgs := readServerMessages(pubConn)

	pubPC, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create publisher PC: %v", err)
	}
	defer pubPC.Close()

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "pocketstation-pub",
	)
	if err != nil {
		t.Fatalf("create publisher track: %v", err)
	}
	if _, err := pubPC.AddTrack(audioTrack); err != nil {
		t.Fatalf("add publisher track: %v", err)
	}

	doPublishHandshake(t, pubConn, pubPC, sourceToken, pubMsgs, 10*time.Second)
	waitICEConnected(ctx, t, pubPC)

	// Start sending immediately so the relay's OnTrack fires and the
	// forwardLoop begins before the subscriber connects.
	sendStop := make(chan struct{})
	payload := bytes.Repeat([]byte{0xAB}, 20)
	go func() {
		var seq uint16
		var ts uint32
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-sendStop:
				return
			case <-ticker.C:
				pkt := &rtp.Packet{
					Header: rtp.Header{
						Version: 2, PayloadType: 111,
						SequenceNumber: seq, Timestamp: ts,
						SSRC: 0xDEADBEEF,
					},
					Payload: payload,
				}
				_ = audioTrack.WriteRTP(pkt)
				seq++
				ts += 960
			}
		}
	}()
	defer close(sendStop)

	// --- Subscriber setup ---
	subConn := dialSignal(t, ts)
	subMsgs := readServerMessages(subConn)

	subPC, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create subscriber PC: %v", err)
	}
	defer subPC.Close()

	// onTrack fires when the relay starts forwarding to the subscriber.
	trackCh := make(chan *webrtc.TrackRemote, 1)
	subPC.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		select {
		case trackCh <- track:
		default:
		}
	})

	doSubscribeHandshake(t, subConn, subPC, listenerToken, subMsgs, 10*time.Second)
	waitICEConnected(ctx, t, subPC)

	// Wait for OnTrack — fires when the first forwarded RTP packet arrives.
	var remoteTrack *webrtc.TrackRemote
	select {
	case remoteTrack = <-trackCh:
	case <-ctx.Done():
		t.Fatalf("context expired waiting for OnTrack: %v", ctx.Err())
	}

	// Collect packetCount packets and verify all have the expected payload.
	h := sha256.New()
	for i := 0; i < packetCount; i++ {
		h.Write(payload)
	}
	expectedChecksum := h.Sum(nil)

	resultCh := make(chan struct {
		count    int
		checksum []byte
	}, 1)
	go func() {
		rh := sha256.New()
		count := 0
		for count < packetCount {
			pkt, _, err := remoteTrack.ReadRTP()
			if err != nil {
				break
			}
			rh.Write(pkt.Payload)
			count++
		}
		resultCh <- struct {
			count    int
			checksum []byte
		}{count, rh.Sum(nil)}
	}()

	select {
	case res := <-resultCh:
		if res.count != packetCount {
			t.Errorf("received %d packets, want %d", res.count, packetCount)
		}
		if !bytes.Equal(res.checksum, expectedChecksum) {
			t.Errorf("payload checksum mismatch: got %x, want %x", res.checksum, expectedChecksum)
		}
	case <-ctx.Done():
		t.Fatalf("context expired waiting for %d packets: %v", packetCount, ctx.Err())
	}

	_ = pubConn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeLeave})
	_ = subConn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeLeave})
}

// TestGiven_SourcePublishing_When_PacketForwarded_Then_OneWayLatencyUnder1ms
// verifies the relay's per-packet forwarding latency by embedding a send
// timestamp in the first 8 bytes of each RTP payload and measuring the delta
// when the packet arrives at the subscriber. P99 must be under 1 ms for
// loopback connections where SRTP crypto and UDP round-trips are all in-process.
//
// Warmup packets use a zero timestamp and are skipped by the receiver goroutine,
// so a single ReadRTP loop is used throughout — no drain race.
func TestGiven_SourcePublishing_When_PacketForwarded_Then_OneWayLatencyUnder1ms(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test uses real Pion ICE — skipped in -short mode")
	}
	const (
		warmupPackets   = 10
		measuredPackets = 50
		// p99Gate is 20 ms (one Opus frame): the relay's true P50 is ~0.5 ms.
		// macOS goroutine scheduling jitter regularly reaches 8–15 ms under
		// normal load, so a tighter gate fires on clean runs from scheduler
		// noise alone. 20 ms catches structural regressions (packet buffering,
		// blocking in the forwarding path) while ignoring OS-level noise.
		p99Gate = 20 * time.Millisecond
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ts, clientAPI := newTestServer(t)

	roomPayload := createRoom(t, ts)
	sourceToken := roomPayload["source_token"]
	listenerToken := roomPayload["listener_token"]

	// --- Publisher ---
	pubConn := dialSignal(t, ts)
	pubMsgs := readServerMessages(pubConn)
	pubPC, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create publisher PC: %v", err)
	}
	defer pubPC.Close()

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "latency-pub",
	)
	if err != nil {
		t.Fatalf("create track: %v", err)
	}
	if _, err := pubPC.AddTrack(audioTrack); err != nil {
		t.Fatalf("add track: %v", err)
	}

	doPublishHandshake(t, pubConn, pubPC, sourceToken, pubMsgs, 10*time.Second)
	waitICEConnected(ctx, t, pubPC)

	// --- Subscriber ---
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

	doSubscribeHandshake(t, subConn, subPC, listenerToken, subMsgs, 10*time.Second)
	waitICEConnected(ctx, t, subPC)

	// payload is re-used for every packet. The first 8 bytes hold the send
	// timestamp (big-endian uint64 Unix-nano). Zero means "warmup — skip".
	payload := make([]byte, 200)
	var seq uint16
	var rtpTS uint32

	sendPacket := func(stampNs uint64) {
		binary.BigEndian.PutUint64(payload[:8], stampNs)
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version: 2, PayloadType: 111,
				SequenceNumber: seq, Timestamp: rtpTS,
				SSRC: 0xCAFEBABE,
			},
			Payload: payload,
		}
		_ = audioTrack.WriteRTP(pkt)
		seq++
		rtpTS += 960
	}

	// Warmup: zero timestamp — triggers relay OnTrack/forwardLoop without
	// polluting the latency sample set.
	for i := 0; i < warmupPackets; i++ {
		sendPacket(0)
		time.Sleep(2 * time.Millisecond)
	}

	// Wait for relay OnTrack to fire (forwardLoop is now running).
	var remoteTrack *webrtc.TrackRemote
	select {
	case remoteTrack = <-trackCh:
	case <-ctx.Done():
		t.Fatalf("OnTrack timeout waiting for warmup packets: %v", ctx.Err())
	}

	// Single receiver goroutine: skips zero-timestamp warmup packets and
	// collects latencies for measured packets. No second goroutine reads
	// remoteTrack, so there is no drain race.
	latencies := make([]time.Duration, 0, measuredPackets)
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for len(latencies) < measuredPackets {
			pkt, _, err := remoteTrack.ReadRTP()
			if err != nil || len(pkt.Payload) < 8 {
				return
			}
			sentNs := int64(binary.BigEndian.Uint64(pkt.Payload[:8]))
			if sentNs == 0 {
				continue // warmup packet — discard
			}
			latencies = append(latencies, time.Duration(time.Now().UnixNano()-sentNs))
		}
	}()

	// Measurement phase: embed real send timestamps.
	for i := 0; i < measuredPackets; i++ {
		sendPacket(uint64(time.Now().UnixNano()))
		time.Sleep(1 * time.Millisecond)
	}

	select {
	case <-recvDone:
	case <-ctx.Done():
		t.Fatalf("context expired collecting latency measurements: %v", ctx.Err())
	}

	if len(latencies) == 0 {
		t.Fatal("no latency measurements collected")
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)/2]
	p95 := latencies[int(float64(len(latencies))*0.95)]
	p99 := latencies[int(float64(len(latencies))*0.99)]

	t.Logf("RTP forwarding latency (loopback SRTP): N=%d P50=%v P95=%v P99=%v",
		len(latencies), p50, p95, p99)

	if p99 > p99Gate {
		t.Errorf("P99 forwarding latency %v exceeds %v gate", p99, p99Gate)
	}
}

// Ensure unused imports compile.
var _ = strings.TrimSpace
var _ = fmt.Sprintf
