package server

import (
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/media/clocklineage"
)

type trackSource struct {
	track    *webrtc.TrackRemote
	timeline *clocklineage.Timeline
}

func (source *trackSource) ReadRTP() (*rtp.Packet, error) {
	packet, _, err := source.track.ReadRTP()
	return packet, err
}

func (source *trackSource) ClockLineage() *clocklineage.Timeline { return source.timeline }

func drainPublisherRTCP(receiver *webrtc.RTPReceiver) {
	for {
		if _, _, err := receiver.ReadRTCP(); err != nil {
			return
		}
	}
}
