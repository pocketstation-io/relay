package integration_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/signaling"
)

type receivedBusPacket struct {
	busID   string
	payload []byte
}

func TestGivenOnePublisherWithTwoDeclaredTracksWhenSubscribersSelectBusesThenAudioRemainsIsolated(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test uses real Pion ICE — skipped in -short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testServer, clientAPI := newTestServer(t)
	session := createRoom(t, testServer)
	sessionID := session["session_id"]

	publisherConnection := dialSignal(t, testServer)
	publisherMessages := readServerMessages(publisherConnection)
	publisher, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create publisher PeerConnection: %v", err)
	}
	defer publisher.Close()

	applicationTrack := addPublisherTrack(t, publisher, "application-track", "application")
	microphoneTrack := addPublisherTrack(t, publisher, "microphone-track", "microphone")
	doPublishHandshakeWithMessage(
		t,
		publisherConnection,
		publisher,
		session["source_token"],
		signaling.ClientMessage{PublishBuses: []signaling.PublishBusBinding{
			{StreamID: "application", BusID: "application"},
			{StreamID: "microphone", BusID: "microphone"},
		}},
		publisherMessages,
		10*time.Second,
	)
	waitICEConnected(ctx, t, publisher)

	received := make(chan receivedBusPacket, 2)
	applicationSubscriber := subscribeToBus(t, ctx, testServer, clientAPI, sessionID, "application", received)
	defer applicationSubscriber.Close()
	microphoneSubscriber := subscribeToBus(t, ctx, testServer, clientAPI, sessionID, "microphone", received)
	defer microphoneSubscriber.Close()

	applicationPayload := []byte{0xA1, 0xA2, 0xA3}
	microphonePayload := []byte{0xB1, 0xB2, 0xB3}
	stop := make(chan struct{})
	var senders sync.WaitGroup
	senders.Add(2)
	go sendTrackPackets(applicationTrack, 0xA11CA710, applicationPayload, stop, &senders)
	go sendTrackPackets(microphoneTrack, 0xB11C0F0E, microphonePayload, stop, &senders)
	defer func() {
		close(stop)
		senders.Wait()
	}()

	observed := make(map[string][]byte, 2)
	for len(observed) < 2 {
		select {
		case packet := <-received:
			observed[packet.busID] = packet.payload
		case <-ctx.Done():
			t.Fatalf("timed out waiting for isolated bus packets: %v", ctx.Err())
		}
	}

	if !bytes.Equal(observed["application"], applicationPayload) {
		t.Fatalf("application subscriber payload = %x, want %x", observed["application"], applicationPayload)
	}
	if !bytes.Equal(observed["microphone"], microphonePayload) {
		t.Fatalf("microphone subscriber payload = %x, want %x", observed["microphone"], microphonePayload)
	}
}

func addPublisherTrack(
	t *testing.T,
	publisher *webrtc.PeerConnection,
	trackID string,
	streamID string,
) *webrtc.TrackLocalStaticRTP {
	t.Helper()
	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		trackID,
		streamID,
	)
	if err != nil {
		t.Fatalf("create %s publisher track: %v", streamID, err)
	}
	if _, err := publisher.AddTrack(track); err != nil {
		t.Fatalf("add %s publisher track: %v", streamID, err)
	}
	return track
}

func subscribeToBus(
	t *testing.T,
	ctx context.Context,
	testServer *httptest.Server,
	clientAPI *webrtc.API,
	sessionID string,
	busID string,
	received chan<- receivedBusPacket,
) *webrtc.PeerConnection {
	t.Helper()
	token, err := auth.SignBus(
		[]byte(testJWTSecret),
		sessionID,
		busID,
		auth.RoleSubscriber,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("sign %s subscriber token: %v", busID, err)
	}

	connection := dialSignal(t, testServer)
	messages := readServerMessages(connection)
	subscriber, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create %s subscriber PeerConnection: %v", busID, err)
	}
	t.Cleanup(func() { _ = subscriber.Close() })
	subscriber.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		packet, _, readErr := track.ReadRTP()
		if readErr != nil {
			return
		}
		payload := append([]byte(nil), packet.Payload...)
		select {
		case received <- receivedBusPacket{busID: busID, payload: payload}:
		case <-ctx.Done():
		}
	})

	doSubscribeHandshake(t, connection, subscriber, token, messages, 10*time.Second)
	waitICEConnected(ctx, t, subscriber)
	return subscriber
}

func sendTrackPackets(
	track *webrtc.TrackLocalStaticRTP,
	ssrc uint32,
	payload []byte,
	stop <-chan struct{},
	done *sync.WaitGroup,
) {
	defer done.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var sequence uint16
	var timestamp uint32
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			_ = track.WriteRTP(&rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					PayloadType:    111,
					SequenceNumber: sequence,
					Timestamp:      timestamp,
					SSRC:           ssrc,
				},
				Payload: payload,
			})
			sequence++
			timestamp += 960
		}
	}
}
