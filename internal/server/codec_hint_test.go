package server

import (
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/graph"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// Pure-function unit tests for the D13 codec-hint logic (RELAY-021).
// End-to-end WebSocket delivery is covered by
// TestGiven_HighLossRTCPRR_When_MaybeEmitCodecHint_Then_CodecHintSentToSource.

func TestGiven_ZeroLoss_When_BitrateForLoss_Then_HighTier(t *testing.T) {
	hint := bitrateForLoss(0.0)
	if hint.BitRateKbps != bitrateHighKbps {
		t.Fatalf("want %d kbps, got %d", bitrateHighKbps, hint.BitRateKbps)
	}
	if hint.Fec || hint.Dtx {
		t.Fatal("want fec=false, dtx=false on clean link")
	}
}

func TestGiven_MediumLoss_When_BitrateForLoss_Then_MediumTierWithFEC(t *testing.T) {
	hint := bitrateForLoss(0.03) // 3% — between 2% and 5%
	if hint.BitRateKbps != bitrateMediumKbps {
		t.Fatalf("want %d kbps, got %d", bitrateMediumKbps, hint.BitRateKbps)
	}
	if !hint.Fec {
		t.Fatal("want fec=true at medium loss")
	}
	if hint.Dtx {
		t.Fatal("want dtx=false at medium loss")
	}
}

func TestGiven_HighLoss_When_BitrateForLoss_Then_LowTierWithFECAndDTX(t *testing.T) {
	hint := bitrateForLoss(0.10) // 10% — above 5%
	if hint.BitRateKbps != bitrateLowKbps {
		t.Fatalf("want %d kbps, got %d", bitrateLowKbps, hint.BitRateKbps)
	}
	if !hint.Fec {
		t.Fatal("want fec=true at high loss")
	}
	if !hint.Dtx {
		t.Fatal("want dtx=true at high loss")
	}
}

func TestGiven_LossAtMediumBoundary_When_BitrateForLoss_Then_HighTier(t *testing.T) {
	// Exactly 2% is NOT > threshold → still high tier.
	hint := bitrateForLoss(lossMediumThreshold)
	if hint.BitRateKbps != bitrateHighKbps {
		t.Fatalf("want %d kbps at exact lower boundary, got %d", bitrateHighKbps, hint.BitRateKbps)
	}
}

func TestGiven_LossJustAboveHighBoundary_When_BitrateForLoss_Then_LowTier(t *testing.T) {
	hint := bitrateForLoss(lossHighThreshold + 0.001)
	if hint.BitRateKbps != bitrateLowKbps {
		t.Fatalf("want %d kbps just above high threshold, got %d", bitrateLowKbps, hint.BitRateKbps)
	}
}

func TestGiven_FractionLostFromRTCP_When_Converted_Then_MapsCorrectly(t *testing.T) {
	// RTCP FractionLost field is uint8 on scale 0–255 (255 = 100% loss).
	cases := []struct {
		rtcpValue uint8
		wantKbps  int
	}{
		{0, bitrateHighKbps},   // 0/256 = 0%
		{5, bitrateHighKbps},   // 5/256 ≈ 2.0% — at boundary → high
		{8, bitrateMediumKbps}, // 8/256 ≈ 3.1% → medium
		{14, bitrateLowKbps},   // 14/256 ≈ 5.5% → low
		{255, bitrateLowKbps},  // 255/256 ≈ 100% → low
	}
	for _, c := range cases {
		frac := float64(c.rtcpValue) / 256.0
		hint := bitrateForLoss(frac)
		if hint.BitRateKbps != c.wantKbps {
			t.Errorf("rtcpValue=%d (frac=%.4f): want %d kbps, got %d",
				c.rtcpValue, frac, c.wantKbps, hint.BitRateKbps)
		}
	}
}

func TestGiven_NoSourceInRoom_When_MaybeEmitCodecHint_Then_NoPanic(t *testing.T) {
	srv := &Server{signalPeers: make(map[string]*signalPeer)}
	state := &codecHintState{}
	hint := bitrateForLoss(0.10)
	// Must not panic when no source session exists for the room.
	srv.maybeEmitCodecHint("nonexistent-room", hint, state)
}

func TestGiven_DebouncePeriodNotElapsed_When_MaybeEmitCodecHint_Then_StateUnchanged(t *testing.T) {
	srv := &Server{signalPeers: make(map[string]*signalPeer)}
	before := time.Now().Add(-1 * time.Millisecond) // lastSent is effectively "just now"
	state := &codecHintState{lastSent: before}
	hint := bitrateForLoss(0.10)

	srv.maybeEmitCodecHint("room-debounce", hint, state)

	// lastSent must not have advanced — the debounce check returned early.
	if !state.lastSent.Equal(before) {
		t.Fatal("debounce should have blocked emission; lastSent was updated unexpectedly")
	}
}

func TestGiven_RoomCodecHintState_When_CalledTwice_Then_SamePointer(t *testing.T) {
	srv := &Server{}
	s1 := srv.roomCodecHintState("room-x")
	s2 := srv.roomCodecHintState("room-x")
	if s1 != s2 {
		t.Fatal("LoadOrStore must return the same *codecHintState on repeated calls")
	}
}

func TestGiven_TwoRooms_When_RoomCodecHintState_Then_DifferentPointers(t *testing.T) {
	srv := &Server{}
	s1 := srv.roomCodecHintState("room-a")
	s2 := srv.roomCodecHintState("room-b")
	if s1 == s2 {
		t.Fatal("different rooms must have different *codecHintState instances")
	}
}

// TestGiven_HighLossRTCPRR_When_MaybeEmitCodecHint_Then_CodecHintSentToSource
// verifies the end-to-end path from RTCP RR high-loss detection through
// maybeEmitCodecHint to actual CODEC_HINT delivery on the source WebSocket.
//
// Setup: a real gorilla WebSocket pair (source side) is injected directly into
// Server.signalPeers so the white-box call to maybeEmitCodecHint can find it.
// The debounce is cleared (lastSent = zero) so the first call always emits.
//
// The RTCP→sender.ReadRTCP path that calls maybeEmitCodecHint is exercised via
// startRTCPReader. Here we call maybeEmitCodecHint directly with the computed
// CodecHintPayload for >5% loss, which is the value startRTCPReader would pass
// after parsing a ReceiverReport with FractionLost > 13 (13/256 ≈ 5.1%).
func TestGiven_HighLossRTCPRR_When_MaybeEmitCodecHint_Then_CodecHintSentToSource(t *testing.T) {
	const roomID = "room-codec-hint-e2e"

	// --- Given: a WebSocket pair representing the source connection. ---
	//
	// We spin up a minimal HTTP server that upgrades the connection, capture
	// the server-side *websocket.Conn, and inject it as the source session.
	serverConnCh := make(chan *websocket.Conn, 1)
	wsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverConnCh <- conn
	})
	// Use IPv4 listener explicitly: httptest.NewServer may prefer ::1 on macOS,
	// which can be blocked in sandbox environments.
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	wsServer := httptest.NewUnstartedServer(wsHandler)
	wsServer.Listener = l
	wsServer.Start()
	defer wsServer.Close()

	u, _ := url.Parse(wsServer.URL)
	u.Scheme = "ws"
	clientConn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("dial WebSocket: %v", err)
	}
	defer clientConn.Close()

	serverConn := <-serverConnCh
	defer serverConn.Close()

	// Build a relay Server whose signaling-peer map contains one source peer
	// backed by the server-side WebSocket connection.
	srv := &Server{signalPeers: make(map[string]*signalPeer)}
	rm := graph.New(roomID)
	defer rm.Close()

	sourcePeer := &signalPeer{
		id:   "src-session-e2e",
		srv:  srv,
		conn: serverConn,
		room: rm,
		role: auth.RoleSource,
	}
	srv.mu.Lock()
	srv.signalPeers[sourcePeer.id] = sourcePeer
	srv.mu.Unlock()

	// Drain source WebSocket messages into a buffered channel.
	received := make(chan signaling.ServerMessage, 8)
	var drainOnce sync.Once
	go func() {
		defer drainOnce.Do(func() { close(received) })
		for {
			var msg signaling.ServerMessage
			if err := clientConn.ReadJSON(&msg); err != nil {
				return
			}
			received <- msg
		}
	}()

	// --- When: maybeEmitCodecHint is called with a high-loss CodecHintPayload
	// and a cleared debounce state (simulating what startRTCPReader does after
	// parsing a ReceiverReport where FractionLost/256 > lossHighThreshold). ---
	//
	// fractionLost = 14/256 ≈ 5.47% → above lossHighThreshold (5%) → low tier.
	const rtcpFractionLost = uint8(14) // 14/256 ≈ 5.47%
	fractionLost := float64(rtcpFractionLost) / 256.0
	hint := bitrateForLoss(fractionLost)

	state := &codecHintState{} // lastSent is zero → debounce not active
	srv.maybeEmitCodecHint(roomID, hint, state)

	// --- Then: the source WebSocket receives a CODEC_HINT with the low tier. ---
	select {
	case msg, ok := <-received:
		if !ok {
			t.Fatal("source WebSocket closed before receiving CODEC_HINT")
		}
		if msg.Type != signaling.TypeCodecHint {
			t.Fatalf("want message type CODEC_HINT, got %q", msg.Type)
		}
		if msg.CodecHint == nil {
			t.Fatal("CODEC_HINT message has nil codec_hint payload")
		}
		if msg.CodecHint.BitRateKbps != bitrateLowKbps {
			t.Errorf("want bitrate %d kbps (low tier for >5%% loss), got %d",
				bitrateLowKbps, msg.CodecHint.BitRateKbps)
		}
		if !msg.CodecHint.Fec {
			t.Error("want fec=true at high loss tier")
		}
		if !msg.CodecHint.Dtx {
			t.Error("want dtx=true at high loss tier")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for CODEC_HINT on source WebSocket")
	}

	// Verify the debounce state was updated: a second call within 2s must not
	// emit another message.
	srv.maybeEmitCodecHint(roomID, hint, state)
	select {
	case msg := <-received:
		t.Fatalf("unexpected second CODEC_HINT within debounce window: %+v", msg)
	case <-time.After(100 * time.Millisecond):
		// Pass: no second message arrived.
	}

	// Close the server-side conn so the drain goroutine exits cleanly.
	serverConn.Close()
	clientConn.Close()

	// Wait for the drain goroutine so the test does not exit with a live goroutine.
	for range received {
	}
}
