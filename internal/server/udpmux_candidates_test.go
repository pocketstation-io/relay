package server

import (
	"net"
	"regexp"
	"testing"

	"github.com/pion/webrtc/v4"
)

// gatherUDPHostCandidates builds a PeerConnection with the given SettingEngine,
// runs ICE gathering to completion, and returns the number of UDP host
// candidates in the resulting local SDP. This is the exact quantity that
// determines the multi-socket egress split: more than one UDP host candidate
// means media can leave from a socket the remote peer never consented to.
func gatherUDPHostCandidates(t *testing.T, se webrtc.SettingEngine) []string {
	t.Helper()
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })

	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly}); err != nil {
		t.Fatalf("AddTransceiver: %v", err)
	}

	done := webrtc.GatheringCompletePromise(pc)
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("SetLocalDescription: %v", err)
	}
	<-done

	re := regexp.MustCompile(`(?m)^a=candidate:\S+ \d+ udp \d+ \S+ \d+ typ host`)
	return re.FindAllString(pc.LocalDescription().SDP, -1)
}

// TestGivenNoUDPMuxWhenGatheringOnMultiHomedHostThenMayGatherMultipleUDPCandidates
// documents the failure mode: with the default UDP gathering pion binds one
// socket per local interface, so a multi-homed host produces several UDP host
// candidates. This is the root cause of the ~50% RTP loss — the remote peer
// consents to one and drops media arriving from the others.
func TestGivenNoUDPMuxWhenGatheringOnMultiHomedHostThenMayGatherMultipleUDPCandidates(t *testing.T) {
	se := webrtc.SettingEngine{}
	cands := gatherUDPHostCandidates(t, se)
	t.Logf("default gathering produced %d UDP host candidate(s):", len(cands))
	for _, c := range cands {
		t.Logf("  %s", c)
	}
	if len(cands) == 0 {
		t.Skip("no UDP host candidates gathered (no usable interface in this env)")
	}
}

// TestGivenProductionICEConfigWhenGatheringThenOneUDPAddressPort is the
// real proof of the fix under the EXACT production SettingEngine: a single
// ICE-UDP mux plus NAT1To1IPs (RELAY_PUBLIC_IPS). The mux forces one socket
// (one port); NAT1To1 rewrites every interface IP to the single public IP. The
// combination must collapse all interfaces to ONE distinct ip:port host
// candidate, so the remote peer can only ever send to — and receive from — that
// one socket. Multiple distinct ip:port host candidates is the split that
// dropped ~half the RTP.
func TestGivenProductionICEConfigWhenGatheringThenOneUDPAddressPort(t *testing.T) {
	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	t.Cleanup(func() { _ = udpConn.Close() })

	const publicIP = "192.168.111.117"
	se := webrtc.SettingEngine{}
	se.SetICEUDPMux(webrtc.NewICEUDPMux(nil, udpConn))
	se.SetNAT1To1IPs([]string{publicIP}, webrtc.ICECandidateTypeHost)

	cands := gatherUDPHostCandidates(t, se)
	t.Logf("production config (mux + NAT1To1) gathered %d UDP host candidate line(s):", len(cands))

	// Count DISTINCT ip:port pairs (ignoring the RTP/RTCP component duplicates).
	addrRe := regexp.MustCompile(`udp \d+ (\S+ \d+) typ host`)
	distinct := map[string]struct{}{}
	for _, c := range cands {
		t.Logf("  %s", c)
		if m := addrRe.FindStringSubmatch(c); m != nil {
			distinct[m[1]] = struct{}{}
		}
	}
	t.Logf("distinct ip:port host candidates: %d", len(distinct))
	if len(distinct) != 1 {
		t.Fatalf("expected exactly 1 distinct ip:port UDP host candidate under production config, got %d: %v", len(distinct), distinct)
	}
}
