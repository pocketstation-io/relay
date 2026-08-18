package server

import (
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
)

// TestGivenRelayMediaEngineWhenOfferCreatedThenAdvertisesStereoOpus is the
// relay-side Stage-B gate for the stereo music pipeline. Per RFC 7587, the relay
// (an Opus-only SFU that forwards stereo without transcoding) must advertise
// stereo in its SDP on both legs or libwebrtc downmixes music to mono. This
// verifies the registered Opus codec surfaces stereo=1 and sprop-stereo=1 in a
// generated offer, at 48 kHz / 2 channels.
func TestGivenRelayMediaEngineWhenOfferCreatedThenAdvertisesStereoOpus(t *testing.T) {
	m, err := NewMediaEngineWithAudioNACK()
	if err != nil {
		t.Fatalf("NewMediaEngineWithAudioNACK: %v", err)
	}

	ir, err := NewInterceptorRegistry(m)
	if err != nil {
		t.Fatalf("NewInterceptorRegistry: %v", err)
	}
	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(m),
		webrtc.WithInterceptorRegistry(ir),
	)
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	defer func() { _ = pc.Close() }()

	if _, err := pc.AddTransceiverFromKind(
		webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv},
	); err != nil {
		t.Fatalf("AddTransceiverFromKind: %v", err)
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	sdp := offer.SDP

	for _, want := range []string{
		"opus/48000/2",   // 48 kHz, 2 channels
		"stereo=1",       // relay accepts stereo (receive)
		"sprop-stereo=1", // relay will send stereo
		"a=rtcp-fb:111 nack",
		"http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
		"http://www.webrtc.org/experiments/rtp-hdrext/abs-capture-time",
		"http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01",
	} {
		if !strings.Contains(sdp, want) {
			t.Errorf("offer SDP must contain %q for stereo Opus negotiation; SDP:\n%s", want, sdp)
		}
	}
}
