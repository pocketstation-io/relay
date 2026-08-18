package server

import (
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/media/clocklineage"
	"github.com/pocketstation-io/relay/internal/signaling"
)

func (peer *signalPeer) newPeerConnection() (*webrtc.PeerConnection, *clocklineage.Registry, error) {
	iceServers := peer.srv.iceServers
	if len(iceServers) == 0 {
		iceServers = []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}}
	}
	configuration := webrtc.Configuration{ICEServers: iceServers}

	mediaEngine, err := NewMediaEngineWithAudioNACK()
	if err != nil {
		return nil, nil, err
	}
	interceptors, lineage, err := newInterceptorRegistryWithClockLineage(mediaEngine)
	if err != nil {
		return nil, nil, err
	}

	var api *webrtc.API
	switch {
	case peer.srv.settingEngine != nil:
		api = webrtc.NewAPI(
			webrtc.WithSettingEngine(*peer.srv.settingEngine),
			webrtc.WithMediaEngine(mediaEngine),
			webrtc.WithInterceptorRegistry(interceptors),
		)
	case peer.srv.api != nil:
		connection, connectionErr := peer.srv.api.NewPeerConnection(configuration)
		if connectionErr != nil {
			return nil, nil, connectionErr
		}
		peer.configureICEForwarding(connection)
		return connection, nil, nil
	default:
		settingEngine := webrtc.SettingEngine{}
		if peer.srv.iceUDPMux != nil {
			settingEngine.SetICEUDPMux(peer.srv.iceUDPMux)
		}
		if peer.srv.iceTCPMux != nil {
			settingEngine.SetICETCPMux(peer.srv.iceTCPMux)
		}
		if len(peer.srv.nat1to1IPs) > 0 {
			settingEngine.SetNAT1To1IPs(peer.srv.nat1to1IPs, webrtc.ICECandidateTypeHost)
		}
		api = webrtc.NewAPI(
			webrtc.WithSettingEngine(settingEngine),
			webrtc.WithMediaEngine(mediaEngine),
			webrtc.WithInterceptorRegistry(interceptors),
		)
	}

	connection, err := api.NewPeerConnection(configuration)
	if err != nil {
		return nil, nil, err
	}
	peer.configureICEForwarding(connection)
	return connection, lineage, nil
}

func (peer *signalPeer) configureICEForwarding(connection *webrtc.PeerConnection) {
	connection.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		_ = peer.send(signaling.ServerMessage{
			Type:      signaling.TypeIce,
			Candidate: candidate.ToJSON().Candidate,
		})
	})
}
