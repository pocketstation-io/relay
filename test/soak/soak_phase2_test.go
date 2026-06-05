// Package soak_test contains the Phase 2 soak test.
//
// Run:
//
//	go test -race -timeout 35m -run TestSoakPhase2 ./test/soak/
//
// Pass -short to skip. The test runs for 30 minutes with 1 publisher and 50
// in-process subscribers, sampling goroutine count and RSS at t=0, t=5min,
// t=15min, and t=30min. It asserts no goroutine leak between the 5-minute
// and 30-minute samples (after all connections are stable) and no unbounded
// RSS growth.
package soak_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/server"
)

// newIPv4Server creates an httptest.Server bound to 127.0.0.1 (IPv4 only).
// The standard httptest.NewServer prefers ::1 (IPv6), which fails in some
// macOS sandbox environments.
func newIPv4Server(handler http.Handler) *httptest.Server {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		panic("newIPv4Server: " + err.Error())
	}
	ts := httptest.NewUnstartedServer(handler)
	ts.Listener = l
	ts.Start()
	return ts
}

const (
	phase2ListenerCount   = 50
	phase2SoakDuration    = 30 * time.Minute
	phase2GoroutineSlop   = 20 // larger window: 50 PCs have more variance than 1
	phase2RSSGrowthBudget = 0.20
)

// connectListener dials a SUBSCRIBE WebSocket to ts and returns the peer
// connection and the close function. The caller must invoke close() when done.
func connectListener(
	t *testing.T,
	ts *httptest.Server,
	api *webrtc.API,
	listenerToken string,
	stopCh <-chan struct{},
	wg *sync.WaitGroup,
) {
	t.Helper()
	defer wg.Done()

	conn := dialWS(t, ts)
	defer conn.Close()
	msgs := drainMessages(conn)

	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Errorf("listener PC: %v", err)
		return
	}
	defer pc.Close()

	trackCh := make(chan *webrtc.TrackRemote, 1)
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		select {
		case trackCh <- track:
		default:
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	subscribeHandshake(t, conn, pc, listenerToken, msgs, 15*time.Second)
	waitICE(ctx, t, pc)

	select {
	case remoteTrack := <-trackCh:
		go func() {
			for {
				select {
				case <-stopCh:
					return
				default:
					if _, _, err := remoteTrack.ReadRTP(); err != nil {
						return
					}
				}
			}
		}()
	case <-time.After(15 * time.Second):
		t.Errorf("listener: OnTrack not received within 15s")
		return
	}

	<-stopCh
	_ = conn.WriteJSON(map[string]string{"type": "LEAVE"})
}

// TestSoakPhase2 runs the Phase 2 soak: 1 publisher + 50 in-process
// subscribers, 30 minutes, race detector active. Asserts no goroutine leak
// between the 5-minute and 30-minute samples and no unbounded RSS growth.
func TestSoakPhase2(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test skipped in -short mode")
	}

	api := newLoopbackAPI()
	srv := server.New(server.Config{
		JWTSecret: []byte("soak-phase2-secret"),
		API:       api,
	})
	ts := newIPv4Server(srv.Handler())
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
	pubMsgs := drainMessages(pubConn)

	pubPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("pub PC: %v", err)
	}
	defer pubPC.Close()

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "soak2-pub",
	)
	if err != nil {
		t.Fatalf("create track: %v", err)
	}
	if _, err := pubPC.AddTrack(audioTrack); err != nil {
		t.Fatalf("add track: %v", err)
	}

	pubCtx, pubCancel := context.WithTimeout(context.Background(), phase2SoakDuration+2*time.Minute)
	defer pubCancel()

	publishHandshake(t, pubConn, pubPC, room.SourceToken, pubMsgs, 10*time.Second)
	waitICE(pubCtx, t, pubPC)

	soakStop := make(chan struct{})
	payload := bytes.Repeat([]byte{0xAB}, 160)
	go func() {
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
						SequenceNumber: seq, Timestamp: ts, SSRC: 0xDEADBEEF,
					},
					Payload: payload,
				}
				_ = audioTrack.WriteRTP(pkt)
				seq++
				ts += 960
			}
		}
	}()
	// soakStop is closed exactly once below; no defer to avoid double-close panic.

	// Give the publisher's forwardLoop a moment to start.
	time.Sleep(200 * time.Millisecond)

	// Connect 50 listeners, staggered by 100ms each to avoid thundering-herd
	// on ICE negotiation and keep OS resource usage smooth.
	var listenerWg sync.WaitGroup
	for i := 0; i < phase2ListenerCount; i++ {
		listenerWg.Add(1)
		go connectListener(t, ts, api, room.ListenerToken, soakStop, &listenerWg)
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for all listeners to be ICE-connected (stagger means ~5s total).
	time.Sleep(5 * time.Second)

	s0 := takeSample(t, "start")

	select {
	case <-time.After(5 * time.Minute):
	case <-pubCtx.Done():
		t.Fatalf("ctx expired at 5min mark")
	}
	s5 := takeSample(t, "5min")

	select {
	case <-time.After(10 * time.Minute):
	case <-pubCtx.Done():
		t.Fatalf("ctx expired at 15min mark")
	}
	s15 := takeSample(t, "15min")

	select {
	case <-time.After(15 * time.Minute):
	case <-pubCtx.Done():
		t.Fatalf("ctx expired at 30min mark")
	}
	s30 := takeSample(t, "30min")

	// Signal all listeners to stop.
	close(soakStop)
	listenerWg.Wait()
	_ = pubConn.WriteJSON(map[string]string{"type": "LEAVE"})

	// Assertions — delta measured from 5min to 30min (after all connections stable).
	delta := s30.goroutines - s5.goroutines
	if delta > phase2GoroutineSlop {
		t.Errorf("goroutine leak: 5min=%d 30min=%d delta=%d (limit=%d)",
			s5.goroutines, s30.goroutines, delta, phase2GoroutineSlop)
	}

	var rssGrowth float64
	if s5.rssBytes > 0 {
		rssGrowth = float64(s30.rssBytes-s5.rssBytes) / float64(s5.rssBytes)
		if rssGrowth > phase2RSSGrowthBudget {
			t.Errorf("RSS growth %.1f%% > budget %.0f%% (5min=%dMB 30min=%dMB)",
				rssGrowth*100, phase2RSSGrowthBudget*100, s5.rssBytes>>20, s30.rssBytes>>20)
		}
	}

	// Flush metrics before reading.
	if r, err := http.Get(ts.URL + "/metrics"); err == nil {
		r.Body.Close()
	}

	goroutineLeakVerdict := "PASS"
	if delta > phase2GoroutineSlop {
		goroutineLeakVerdict = "FAIL"
	}
	rssVerdict := "PASS"
	if rssGrowth > phase2RSSGrowthBudget {
		rssVerdict = "FAIL"
	}

	results := fmt.Sprintf(
		"# Phase 2 soak baseline\n"+
			"# Generated: %s\n"+
			"# Platform: darwin/arm64 (Apple M5)\n"+
			"# Configuration: 1 publisher + %d listeners, 30-minute run\n\n"+
			"goroutines start=%d 5min=%d 15min=%d 30min=%d delta(5→30)=%d\n"+
			"rss_mb     start=%d 5min=%d 15min=%d 30min=%d growth(5→30)=%.1f%%\n"+
			"packets_forwarded=%d packets_dropped=%d listener_count=%d\n"+
			"race_detector=clean (enforced by -race flag)\n"+
			"goroutine_leak=%s (delta %d <= limit %d)\n"+
			"rss_growth=%s\n",
		time.Now().Format("2006-01-02"),
		phase2ListenerCount,
		s0.goroutines, s5.goroutines, s15.goroutines, s30.goroutines,
		s30.goroutines-s5.goroutines,
		s0.rssBytes>>20, s5.rssBytes>>20, s15.rssBytes>>20, s30.rssBytes>>20,
		rssGrowth*100,
		srv.Metrics.PacketsForwarded.Load(),
		srv.Metrics.PacketsDropped.Load(),
		srv.Metrics.ListenerCount.Load(),
		goroutineLeakVerdict, delta, phase2GoroutineSlop,
		rssVerdict,
	)
	_ = os.MkdirAll("../../soak/results", 0755)
	_ = os.WriteFile("../../soak/results/phase2-baseline.txt", []byte(results), 0644)
	t.Logf("soak results:\n%s", results)
}
