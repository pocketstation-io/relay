// Package integration_test — KEY_EXCHANGE fan-out and late-join tests.
//
// TestGivenSourceInRoomWhenKeyExchangeSentThenAllListenersReceiveKey
// verifies that a KEY_EXCHANGE message sent by the source is:
//   - forwarded to all current listeners in the room
//   - NOT echoed back to the source
//   - delivered to a listener that joins AFTER the key was sent (late-join path)
//
// Run with:
//
//	go test -race -run TestGivenSourceInRoom ./test/integration/
package integration_test

import (
	"context"
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/signaling"
)

// fanOutMessages reads from rawMsgs and sends each message to both iceCh and
// keyExchangeCh, routing by message type. All other message types are dropped.
// This allows the same WebSocket connection to be used for both the ICE
// handshake goroutine (inside doSubscribeHandshake) and the test assertion.
//
// The caller must start the background goroutine before calling
// doSubscribeHandshake so the ICE candidates are not missed.
func fanOutMessages(
	rawMsgs <-chan signaling.ServerMessage,
	iceCh chan<- signaling.ServerMessage,
	keyExchangeCh chan<- signaling.ServerMessage,
) {
	go func() {
		for msg := range rawMsgs {
			switch msg.Type {
			case signaling.TypeIce, signaling.TypeSDPAnswer, signaling.TypeSessionState, signaling.TypeRoomState, signaling.TypeError:
				// Forward to the handshake channel (must not block).
				select {
				case iceCh <- msg:
				default:
				}
			case signaling.TypeKeyExchange:
				// Forward to the KEY_EXCHANGE channel (must not block).
				select {
				case keyExchangeCh <- msg:
				default:
				}
			}
		}
		close(iceCh)
		close(keyExchangeCh)
	}()
}

// TestGivenSourceInRoomWhenKeyExchangeSentThenAllListenersReceiveKey
// connects 1 source and 3 listeners to a room, has the source send a
// KEY_EXCHANGE message, then asserts:
//
//  1. All 3 listeners receive a KEY_EXCHANGE ServerMessage with the correct key.
//  2. The source does NOT receive the KEY_EXCHANGE back.
//  3. A 4th listener joining after the KEY_EXCHANGE immediately receives the
//     stored key via the late-join path (RELAY-014).
func TestGivenSourceInRoomWhenKeyExchangeSentThenAllListenersReceiveKey(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test uses real Pion ICE — skipped in -short mode")
	}

	const (
		listenerCount    = 3
		testKey          = "dGVzdC1zZnJhbWUta2V5LTMyYnl0ZXMhISEhISEhISE=" // base64("test-sframe-key-32bytes!!!!!!!!")
		handshakeTimeout = 10 * time.Second
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ts, clientAPI := newTestServer(t)
	roomPayload := createRoom(t, ts)
	sourceToken := roomPayload["source_token"]
	listenerToken := roomPayload["listener_token"]

	// Source setup.
	sourceConn := dialSignal(t, ts)
	sourceMsgs := readServerMessages(sourceConn)

	sourcePC, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create source PC: %v", err)
	}
	defer sourcePC.Close()

	audioTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "key-exchange-src",
	)
	if err != nil {
		t.Fatalf("create audio track: %v", err)
	}
	if _, err := sourcePC.AddTrack(audioTrack); err != nil {
		t.Fatalf("add track: %v", err)
	}

	doPublishHandshake(t, sourceConn, sourcePC, sourceToken, sourceMsgs, handshakeTimeout)
	waitICEConnected(ctx, t, sourcePC)

	// 3 listeners setup.
	// For each listener: create two channels — one for handshake messages
	// (SDP_ANSWER, ICE, SESSION_STATE) and one for KEY_EXCHANGE messages.
	// fanOutMessages routes the raw WebSocket stream to both channels.
	listenerConns := make([]*websocket.Conn, listenerCount)
	listenerPCs := make([]*webrtc.PeerConnection, listenerCount)
	listenerKeyChans := make([]chan signaling.ServerMessage, listenerCount)

	for i := 0; i < listenerCount; i++ {
		listenerConns[i] = dialSignal(t, ts)
		rawMsgs := readServerMessages(listenerConns[i])

		handshakeCh := make(chan signaling.ServerMessage, 64)
		listenerKeyChans[i] = make(chan signaling.ServerMessage, 8)
		fanOutMessages(rawMsgs, handshakeCh, listenerKeyChans[i])

		listenerPCs[i], err = clientAPI.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			t.Fatalf("create listener %d PC: %v", i, err)
		}
		defer listenerPCs[i].Close()

		doSubscribeHandshake(t, listenerConns[i], listenerPCs[i], listenerToken, handshakeCh, handshakeTimeout)
		waitICEConnected(ctx, t, listenerPCs[i])
	}

	// Source sends KEY_EXCHANGE.
	if err := sourceConn.WriteJSON(signaling.ClientMessage{
		Type:      signaling.TypeKeyExchange,
		SFrameKey: testKey,
	}); err != nil {
		t.Fatalf("send KEY_EXCHANGE: %v", err)
	}

	// Assertion 1: all 3 listeners receive the key.
	var wg sync.WaitGroup
	for i := 0; i < listenerCount; i++ {
		wg.Add(1)
		go func(idx int, keyCh <-chan signaling.ServerMessage) {
			defer wg.Done()
			select {
			case msg, ok := <-keyCh:
				if !ok {
					t.Errorf("listener %d: KEY_EXCHANGE channel closed unexpectedly", idx)
					return
				}
				if msg.SFrameKey != testKey {
					t.Errorf("listener %d: got SFrameKey=%q, want %q", idx, msg.SFrameKey, testKey)
				}
			case <-time.After(10 * time.Second):
				t.Errorf("listener %d: timed out waiting for KEY_EXCHANGE", idx)
			}
		}(i, listenerKeyChans[i])
	}
	wg.Wait()

	// Assertion 2: source does NOT receive KEY_EXCHANGE back.
	// sourceMsgs is the channel from doPublishHandshake's background goroutine.
	// Drain briefly and fail if any KEY_EXCHANGE arrives.
	timeout := time.After(200 * time.Millisecond)
drain:
	for {
		select {
		case msg, ok := <-sourceMsgs:
			if !ok {
				break drain
			}
			if msg.Type == signaling.TypeKeyExchange {
				t.Errorf("source received KEY_EXCHANGE back (must not be echoed to sender)")
			}
		case <-timeout:
			break drain
		}
	}

	// Assertion 3: late-joining listener receives the stored key immediately.
	lateConn := dialSignal(t, ts)
	defer lateConn.Close()
	lateRaw := readServerMessages(lateConn)

	lateHandshakeCh := make(chan signaling.ServerMessage, 64)
	lateKeyCh := make(chan signaling.ServerMessage, 8)
	fanOutMessages(lateRaw, lateHandshakeCh, lateKeyCh)

	latePC, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create late-join PC: %v", err)
	}
	defer latePC.Close()

	doSubscribeHandshake(t, lateConn, latePC, listenerToken, lateHandshakeCh, handshakeTimeout)

	// The stored key is delivered during the SUBSCRIBE handshake itself
	// (inside session.handleJoin after AddListener). We wait up to 5 seconds.
	select {
	case msg, ok := <-lateKeyCh:
		if !ok {
			t.Error("late-join listener: KEY_EXCHANGE channel closed before key arrived")
		} else if msg.SFrameKey != testKey {
			t.Errorf("late-join listener: got SFrameKey=%q, want %q", msg.SFrameKey, testKey)
		}
	case <-time.After(5 * time.Second):
		t.Error("late-join listener: did not receive stored KEY_EXCHANGE within 5 seconds")
	}

	// Verify the test key is valid base64 (sanity check on the constant itself).
	if _, err := base64.StdEncoding.DecodeString(testKey); err != nil {
		t.Fatalf("testKey constant is not valid base64: %v", err)
	}

	_ = sourceConn.WriteJSON(signaling.ClientMessage{Type: signaling.TypeLeave})
	for i := 0; i < listenerCount; i++ {
		_ = listenerConns[i].WriteJSON(signaling.ClientMessage{Type: signaling.TypeLeave})
	}
}
