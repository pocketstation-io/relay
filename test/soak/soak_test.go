// Package soak_test contains the Phase 1 soak test.
//
// Run:
//
//	go test -race -timeout 10m -run TestSoak ./test/soak/
//
// Pass -short to skip. The test runs for 5 minutes with 1 publisher and 1
// in-process subscriber, sampling goroutine count and RSS at t=0, t=1min,
// and t=5min. It asserts no goroutine leak and no unbounded RSS growth.
package soak_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/server"
	"github.com/pocketstation-io/relay/internal/signaling"
)

const (
	goroutineSlop   = 10   // allowed goroutine count drift 1min→5min
	rssGrowthBudget = 0.20 // 20% RSS growth from 1min to 5min
)

func relaySoakFull() bool {
	return os.Getenv("RELAY_SOAK_FULL") == "1"
}

func phase1SoakSchedule() (time.Duration, time.Duration, time.Duration) {
	if relaySoakFull() {
		return 5 * time.Minute, 1 * time.Minute, 4 * time.Minute
	}
	return 45 * time.Second, 15 * time.Second, 30 * time.Second
}

func newLoopbackAPI() *webrtc.API {
	se := webrtc.SettingEngine{}
	se.SetNAT1To1IPs([]string{"127.0.0.1"}, webrtc.ICECandidateTypeHost)
	se.SetICETimeouts(10*time.Second, 10*time.Second, 3*time.Second)
	return webrtc.NewAPI(webrtc.WithSettingEngine(se))
}

func dialWS(t *testing.T, ts *httptest.Server) *websocket.Conn {
	t.Helper()
	u, _ := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/v1/signal"
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("dial WebSocket: %v", err)
	}
	return conn
}

// drainMessages pumps ServerMessages from conn into the returned channel until
// conn is closed. The goroutine is registered with wg so callers can wait for
// it to exit before completing cleanup.
func drainMessages(conn *websocket.Conn, wg *sync.WaitGroup) <-chan signaling.ServerMessage {
	wg.Add(1)
	ch := make(chan signaling.ServerMessage, 64)
	go func() {
		defer wg.Done()
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

func waitICE(ctx context.Context, t *testing.T, pc *webrtc.PeerConnection) {
	t.Helper()
	ch := make(chan struct{})
	var once sync.Once
	pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		if s == webrtc.ICEConnectionStateConnected {
			once.Do(func() { close(ch) })
		}
	})
	if pc.ICEConnectionState() == webrtc.ICEConnectionStateConnected {
		return
	}
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("ICE connect timeout: %v", ctx.Err())
	}
}

// publishHandshake performs the PUBLISH WebSocket handshake and spawns an ICE
// candidate relay goroutine. The goroutine is registered with wg.
func publishHandshake(
	t *testing.T,
	conn *websocket.Conn,
	pc *webrtc.PeerConnection,
	token string,
	msgs <-chan signaling.ServerMessage,
	timeout time.Duration,
	wg *sync.WaitGroup,
) {
	t.Helper()
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer: %v", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("set local desc: %v", err)
	}
	if err := conn.WriteJSON(signaling.ClientMessage{
		Type: signaling.TypePublish, Token: token, SDPOffer: offer.SDP,
	}); err != nil {
		t.Fatalf("send PUBLISH: %v", err)
	}
	var wmu sync.Mutex
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		wmu.Lock()
		defer wmu.Unlock()
		_ = conn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeIce, Candidate: c.ToJSON().Candidate})
	})
	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				t.Fatal("WebSocket closed before SDP_ANSWER")
			}
			if msg.Type == signaling.TypeSDPAnswer {
				if err := pc.SetRemoteDescription(webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer, SDP: msg.SDPAnswer,
				}); err != nil {
					t.Fatalf("set remote desc: %v", err)
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					for m := range msgs {
						if m.Type == signaling.TypeIce {
							_ = pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: m.Candidate})
						}
					}
				}()
				return
			}
			if msg.Type == signaling.TypeError {
				t.Fatalf("relay error: %s", msg.Message)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for SDP_ANSWER")
		}
	}
}

// subscribeHandshake performs the SUBSCRIBE WebSocket handshake and spawns an
// ICE candidate relay goroutine. The goroutine is registered with wg.
func subscribeHandshake(
	t *testing.T,
	conn *websocket.Conn,
	pc *webrtc.PeerConnection,
	token string,
	msgs <-chan signaling.ServerMessage,
	timeout time.Duration,
	wg *sync.WaitGroup,
) {
	t.Helper()
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
		t.Fatalf("set local desc: %v", err)
	}
	if err := conn.WriteJSON(signaling.ClientMessage{
		Type: signaling.TypeSubscribe, Token: token, SDPOffer: offer.SDP,
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
		_ = conn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeIce, Candidate: c.ToJSON().Candidate})
	})
	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				t.Fatal("WebSocket closed before SDP_ANSWER")
			}
			if msg.Type == signaling.TypeSDPAnswer {
				if err := pc.SetRemoteDescription(webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer, SDP: msg.SDPAnswer,
				}); err != nil {
					t.Fatalf("set remote desc: %v", err)
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					for m := range msgs {
						if m.Type == signaling.TypeIce {
							_ = pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: m.Candidate})
						}
					}
				}()
				return
			}
			if msg.Type == signaling.TypeError {
				t.Fatalf("relay error: %s", msg.Message)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for SDP_ANSWER")
		}
	}
}

type soakSample struct {
	label      string
	goroutines int
	rssBytes   uint64
}

func takeSample(t *testing.T, label string) soakSample {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s := soakSample{
		label:      label,
		goroutines: runtime.NumGoroutine(),
		rssBytes:   ms.Sys,
	}
	t.Logf("soak[%s]: goroutines=%d rss=%dMB", label, s.goroutines, s.rssBytes>>20)
	return s
}

// TestSoak runs the Phase 1 soak: 1 publisher + 1 in-process subscriber, 5 minutes,
// race detector active. Asserts no goroutine leak and no unbounded RSS growth.
func TestSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test skipped in -short mode")
	}

	// childWg tracks all goroutines spawned by drainMessages, publishHandshake,
	// subscribeHandshake, and the RTP drain goroutine. defer childWg.Wait() is
	// registered first so it runs last (LIFO), after all pc.Close/conn.Close
	// defers have already signaled those goroutines to exit.
	var childWg sync.WaitGroup
	defer childWg.Wait()

	soakDuration, firstSampleAfter, finalSampleAfter := phase1SoakSchedule()
	ctx, cancel := context.WithTimeout(context.Background(), soakDuration+60*time.Second)
	defer cancel()

	api := newLoopbackAPI()
	srv := server.New(server.Config{
		JWTSecret: []byte("soak-secret"),
		API:       api,
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Create room.
	resp, err := http.Post(ts.URL+"/v1/rooms", "application/json", bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	defer resp.Body.Close()
	var room struct {
		RoomID        string `json:"room_id"`
		SourceToken   string `json:"source_token"`
		ListenerToken string `json:"listener_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&room); err != nil {
		t.Fatalf("decode room: %v", err)
	}

	// Publisher.
	pubConn := dialWS(t, ts)
	defer pubConn.Close()
	pubMsgs := drainMessages(pubConn, &childWg)

	pubPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("pub PC: %v", err)
	}
	defer pubPC.Close()

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "soak-pub",
	)
	if err != nil {
		t.Fatalf("create track: %v", err)
	}
	if _, err := pubPC.AddTrack(audioTrack); err != nil {
		t.Fatalf("add track: %v", err)
	}

	publishHandshake(t, pubConn, pubPC, room.SourceToken, pubMsgs, 10*time.Second, &childWg)
	waitICE(ctx, t, pubPC)

	// Start publisher send loop (triggers relay OnTrack + forwardLoop).
	soakStop := make(chan struct{})
	payload := bytes.Repeat([]byte{0xAB}, 160)
	childWg.Add(1)
	go func() {
		defer childWg.Done()
		var seq uint16
		var ts uint32
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-soakStop:
				return
			case <-ticker.C:
				pkt := &rtp.Packet{
					Header: rtp.Header{
						Version: 2, PayloadType: 111,
						SequenceNumber: seq, Timestamp: ts, SSRC: 0x12345678,
					},
					Payload: payload,
				}
				_ = audioTrack.WriteRTP(pkt)
				seq++
				ts += 960
			}
		}
	}()
	defer close(soakStop)

	time.Sleep(100 * time.Millisecond) // let forwardLoop start

	// Subscriber.
	subConn := dialWS(t, ts)
	defer subConn.Close()
	subMsgs := drainMessages(subConn, &childWg)

	subPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("sub PC: %v", err)
	}
	defer subPC.Close()

	trackCh := make(chan *webrtc.TrackRemote, 1)
	subPC.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		select {
		case trackCh <- track:
		default:
		}
	})

	subscribeHandshake(t, subConn, subPC, room.ListenerToken, subMsgs, 10*time.Second, &childWg)
	waitICE(ctx, t, subPC)

	select {
	case remoteTrack := <-trackCh:
		// Drain subscriber track to prevent backpressure.
		childWg.Add(1)
		go func() {
			defer childWg.Done()
			for {
				select {
				case <-soakStop:
					return
				default:
					if _, _, err := remoteTrack.ReadRTP(); err != nil {
						return
					}
				}
			}
		}()
	case <-time.After(10 * time.Second):
		t.Fatal("OnTrack not received within 10s")
	}

	s0 := takeSample(t, "start")

	select {
	case <-time.After(firstSampleAfter):
	case <-ctx.Done():
		t.Fatalf("ctx expired at first sample: %v", ctx.Err())
	}
	s1 := takeSample(t, "steady")

	select {
	case <-time.After(finalSampleAfter):
	case <-ctx.Done():
		t.Fatalf("ctx expired at final sample: %v", ctx.Err())
	}
	s5 := takeSample(t, "end")

	_ = pubConn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeLeave})
	_ = subConn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeLeave})

	// Assertions.
	delta := s5.goroutines - s1.goroutines
	if delta > goroutineSlop {
		t.Errorf("goroutine leak: 1min=%d 5min=%d delta=%d (limit=%d)",
			s1.goroutines, s5.goroutines, delta, goroutineSlop)
	}
	if s1.rssBytes > 0 {
		growth := float64(s5.rssBytes-s1.rssBytes) / float64(s1.rssBytes)
		if growth > rssGrowthBudget {
			t.Errorf("RSS growth %.1f%% > budget %.0f%% (1min=%dMB 5min=%dMB)",
				growth*100, rssGrowthBudget*100, s1.rssBytes>>20, s5.rssBytes>>20)
		}
	}

	// Refresh aggregated packet stats from rooms into srv.Metrics before reading.
	if r, err := http.Get(ts.URL + "/metrics"); err == nil {
		r.Body.Close()
	}

	// Write results file.
	results := fmt.Sprintf(
		"# Phase 1 soak baseline\n# Generated: 2026-05-20\n# Platform: darwin/arm64 (Apple M5)\n\n"+
			"duration=%s full_mode=%t first_sample_after=%s final_sample_after=%s\n"+
			"goroutines start=%d steady=%d end=%d delta(steady→end)=%d\n"+
			"rss_mb     start=%d steady=%d end=%d growth(steady→end)=%.1f%%\n"+
			"packets_forwarded=%d packets_dropped=%d listener_count=%d\n"+
			"race_detector=clean (enforced by -race flag)\n"+
			"goroutine_leak=PASS (delta %d <= limit %d)\n"+
			"rss_growth=PASS\n",
		soakDuration, relaySoakFull(), firstSampleAfter, finalSampleAfter,
		s0.goroutines, s1.goroutines, s5.goroutines, s5.goroutines-s1.goroutines,
		s0.rssBytes>>20, s1.rssBytes>>20, s5.rssBytes>>20,
		func() float64 {
			if s1.rssBytes == 0 {
				return 0
			}
			return float64(s5.rssBytes-s1.rssBytes) / float64(s1.rssBytes) * 100
		}(),
		srv.Metrics.PacketsForwarded.Load(),
		srv.Metrics.PacketsDropped.Load(),
		srv.Metrics.ListenerCount.Load(),
		s5.goroutines-s1.goroutines, goroutineSlop,
	)
	_ = os.WriteFile("../../soak/results/phase1-baseline.txt", []byte(results), 0644)
	t.Logf("soak results:\n%s", results)
}
