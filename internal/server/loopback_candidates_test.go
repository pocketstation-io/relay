package server

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

func TestGivenExplicitLoopbackUDPMuxWhenGatheringThenBothPeerPathsAdvertiseBoundCandidate(t *testing.T) {
	for _, protocol := range []string{"signaling", "whip"} {
		t.Run(protocol, func(t *testing.T) {
			udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			mux := webrtc.NewICEUDPMux(nil, udp)
			t.Cleanup(func() { _ = mux.Close() })
			s := &Server{iceUDPMux: mux, nat1to1IPs: []string{"127.0.0.1"}, iceServers: []webrtc.ICEServer{{}}}
			var pc *webrtc.PeerConnection
			if protocol == "signaling" {
				pc, _, err = (&signalPeer{srv: s}).newPeerConnection()
			} else {
				pc, _, err = s.newWHIPPeerConnection()
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = pc.Close() })
			// Observe candidates in the final SDP without a signaling transport.
			pc.OnICECandidate(nil)
			if _, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
				t.Fatal(err)
			}
			done := webrtc.GatheringCompletePromise(pc)
			offer, err := pc.CreateOffer(nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = pc.SetLocalDescription(offer); err != nil {
				t.Fatal(err)
			}
			const gatherTimeout = 2 * time.Second
			select {
			case <-done:
			case <-time.After(gatherTimeout):
				t.Fatal("candidate gathering exceeded bounded deadline")
			}
			addresses := make(map[string]struct{})
			for _, line := range strings.Split(pc.LocalDescription().SDP, "\r\n") {
				if !strings.HasPrefix(line, "a=candidate:") {
					continue
				}
				fields := strings.Fields(line)
				if len(fields) >= 8 && fields[2] == "udp" && fields[7] == "host" {
					address := net.JoinHostPort(fields[4], fields[5])
					addresses[address] = struct{}{}
					if address != udp.LocalAddr().String() {
						t.Fatalf("candidate %s differs from explicit socket %s", address, udp.LocalAddr())
					}
				}
			}
			if len(addresses) != 1 {
				t.Fatalf("expected one explicit loopback UDP address/port, got %d", len(addresses))
			}
		})
	}
}

func TestGivenICEAddressConfigurationWhenSelectingLoopbackThenOnlyExplicitMatchingSocketEnablesIt(t *testing.T) {
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	mux := webrtc.NewICEUDPMux(nil, udp)
	t.Cleanup(func() { _ = mux.Close() })
	for _, test := range []struct {
		name       string
		advertised []string
		want       bool
	}{
		{"default", nil, false},
		{"public_only", []string{"203.0.113.8"}, false},
		{"unspecified", []string{"0.0.0.0"}, false},
		{"hostname", []string{"localhost"}, false},
		{"other_loopback", []string{"127.0.0.2"}, false},
		{"other_family", []string{"::1"}, false},
		{"matching", []string{"127.0.0.1"}, true},
		{"mapped_ipv4", []string{"::ffff:127.0.0.1"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := &Server{iceUDPMux: mux, nat1to1IPs: test.advertised}
			if got := s.explicitLoopbackICE(); got != test.want {
				t.Fatalf("loopback inclusion=%v, want %v", got, test.want)
			}
		})
	}
	if (&Server{nat1to1IPs: []string{"127.0.0.1"}}).explicitLoopbackICE() {
		t.Fatal("advertisement alone enabled loopback without a bound mux")
	}
}
