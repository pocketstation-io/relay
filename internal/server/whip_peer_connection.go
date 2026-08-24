package server

import (
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/media/clocklineage"
)

func (s *Server) newWHIPPeerConnection() (*webrtc.PeerConnection, *clocklineage.Registry, error) {
	iceServers := s.iceServers
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

	settingEngine := webrtc.SettingEngine{}
	if s.iceUDPMux != nil {
		settingEngine.SetICEUDPMux(s.iceUDPMux)
	}
	if s.iceTCPMux != nil {
		settingEngine.SetICETCPMux(s.iceTCPMux)
	}
	if len(s.nat1to1IPs) > 0 {
		settingEngine.SetNAT1To1IPs(s.nat1to1IPs, webrtc.ICECandidateTypeHost)
	}
	api := webrtc.NewAPI(
		webrtc.WithSettingEngine(settingEngine),
		webrtc.WithMediaEngine(mediaEngine),
		webrtc.WithInterceptorRegistry(interceptors),
	)
	connection, err := api.NewPeerConnection(configuration)
	return connection, lineage, err
}
