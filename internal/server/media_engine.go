package server

import (
	"strconv"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/nack"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/media/clocklineage"
)

const opusPayloadType = 111
const opusStereoFmtp = "minptime=10;useinbandfec=1;stereo=1;sprop-stereo=1;maxaveragebitrate=131072"

const (
	redMimeType    = "audio/red"
	redPayloadType = 63
)

func NewMediaEngineWithAudioNACK() (*webrtc.MediaEngine, error) {
	mediaEngine := &webrtc.MediaEngine{}
	for _, uri := range []string{
		"http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
		"http://www.webrtc.org/experiments/rtp-hdrext/abs-capture-time",
	} {
		if err := mediaEngine.RegisterHeaderExtension(
			webrtc.RTPHeaderExtensionCapability{URI: uri},
			webrtc.RTPCodecTypeAudio,
		); err != nil {
			return nil, err
		}
	}

	if err := mediaEngine.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeOpus,
			ClockRate:    48000,
			Channels:     2,
			SDPFmtpLine:  opusStereoFmtp,
			RTCPFeedback: []webrtc.RTCPFeedback{{Type: "nack"}},
		},
		PayloadType: opusPayloadType,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, err
	}
	if redEnabled() {
		if err := mediaEngine.RegisterCodec(webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:     redMimeType,
				ClockRate:    48000,
				Channels:     2,
				SDPFmtpLine:  strconv.Itoa(opusPayloadType) + "/" + strconv.Itoa(opusPayloadType),
				RTCPFeedback: []webrtc.RTCPFeedback{{Type: "nack"}},
			},
			PayloadType: redPayloadType,
		}, webrtc.RTPCodecTypeAudio); err != nil {
			return nil, err
		}
	}
	return mediaEngine, nil
}

func NewInterceptorRegistry(mediaEngine *webrtc.MediaEngine) (*interceptor.Registry, error) {
	registry, _, err := newInterceptorRegistryWithClockLineage(mediaEngine)
	return registry, err
}

func newInterceptorRegistryWithClockLineage(
	mediaEngine *webrtc.MediaEngine,
) (*interceptor.Registry, *clocklineage.Registry, error) {
	registry := &interceptor.Registry{}
	lineage := clocklineage.NewRegistry()
	registry.Add(&clocklineage.InterceptorFactory{Registry: lineage})

	nackGenerator, err := nack.NewGeneratorInterceptor()
	if err != nil {
		return nil, nil, err
	}
	registry.Add(nackGenerator)

	if err := webrtc.ConfigureRTCPReports(registry); err != nil {
		return nil, nil, err
	}
	if err := webrtc.ConfigureSimulcastExtensionHeaders(mediaEngine); err != nil {
		return nil, nil, err
	}
	if err := webrtc.ConfigureTWCCSender(mediaEngine, registry); err != nil {
		return nil, nil, err
	}
	if err := webrtc.ConfigureTWCCHeaderExtensionSender(mediaEngine, registry); err != nil {
		return nil, nil, err
	}
	return registry, lineage, nil
}
