package server

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
)

type tcpMuxCandidateReceipt struct {
	Case              string                   `json:"case"`
	Constructor       string                   `json:"constructor"`
	Policy            string                   `json:"policy"`
	TCPMuxConfigured  bool                     `json:"tcp_mux_configured"`
	UDPMuxPort        int                      `json:"udp_mux_port"`
	TCPMuxPort        int                      `json:"tcp_mux_port"`
	UDPHostPresent    bool                     `json:"udp_host_present"`
	TCP4PassiveHost   bool                     `json:"tcp4_passive_host"`
	GatherElapsedMS   int64                    `json:"gather_elapsed_ms"`
	CleanupElapsedMS  int64                    `json:"cleanup_elapsed_ms"`
	CleanupComplete   bool                     `json:"cleanup_complete"`
	UDPPortRebound    bool                     `json:"udp_port_rebound"`
	TCPPortRebound    bool                     `json:"tcp_port_rebound"`
	Candidates        []tcpMuxParsedCandidate  `json:"candidates"`
	RejectedCandidate *tcpMuxRejectedCandidate `json:"rejected_candidate"`
}

type tcpMuxRejectedCandidate struct {
	NetworkType   string `json:"network_type"`
	Type          string `json:"type"`
	AddressFamily string `json:"address_family"`
	Port          int    `json:"port"`
	TCPType       string `json:"tcp_type"`
}

type tcpMuxParsedCandidate struct {
	NetworkType string `json:"network_type"`
	Type        string `json:"type"`
	Address     string `json:"address"`
	Port        int    `json:"port"`
	TCPType     string `json:"tcp_type"`
}

func TestGivenConfiguredTCPMuxWhenProductionPeersGatherThenTCP4AndCallerPoliciesArePreserved(t *testing.T) {
	cases := []struct {
		name, constructor, policy string
		tcpMux, wantTCP           bool
	}{
		{"signaling_default_tcp", "signaling", "default", true, true},
		{"whip_default_tcp", "whip", "default", true, true},
		{"signaling_no_tcp", "signaling", "default", false, false},
		{"whip_no_tcp", "whip", "default", false, false},
		{"signaling_caller_udp", "signaling", "setting_engine_udp4", true, false},
		{"whip_caller_udp", "whip", "setting_engine_udp4", true, false},
		{"signaling_api_udp", "signaling", "api_udp4", true, false},
	}
	receipts := make([]tcpMuxCandidateReceipt, 0, len(cases))
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			udpAddress := udp.LocalAddr().String()
			udpMux := webrtc.NewICEUDPMux(nil, udp)
			var tcp net.Listener
			var tcpMux ice.TCPMux
			var tcpAddress string
			var pc *webrtc.PeerConnection
			receipt := tcpMuxCandidateReceipt{Case: test.name, Constructor: test.constructor, Policy: test.policy, TCPMuxConfigured: test.tcpMux}
			receipt.UDPMuxPort = udp.LocalAddr().(*net.UDPAddr).Port
			t.Cleanup(func() {
				start := time.Now()
				closed := true
				// Close every owned object before checking reuse of its exact port.
				// A failed or blocked close fails this case; no skip or retry qualifies it.
				if pc != nil {
					closed = closeCandidateResource(t, "peer", pc.Close) && closed
				}
				if tcpMux != nil {
					closed = closeCandidateResource(t, "tcp_mux", tcpMux.Close) && closed
				}
				closed = closeCandidateResource(t, "udp_mux", udpMux.Close) && closed
				if tcpAddress != "" {
					rebound, rebindErr := net.Listen("tcp4", tcpAddress)
					if rebindErr != nil {
						t.Errorf("TCP mux port did not rebind: %v", rebindErr)
					} else {
						receipt.TCPPortRebound = true
						closed = closeCandidateResource(t, "rebound_tcp", rebound.Close) && closed
					}
				}
				address, resolveErr := net.ResolveUDPAddr("udp4", udpAddress)
				if resolveErr != nil {
					t.Errorf("owned UDP address invalid: %v", resolveErr)
				} else if rebound, rebindErr := net.ListenUDP("udp4", address); rebindErr != nil {
					t.Errorf("UDP mux port did not rebind: %v", rebindErr)
				} else {
					receipt.UDPPortRebound = true
					closed = closeCandidateResource(t, "rebound_udp", rebound.Close) && closed
				}
				receipt.CleanupElapsedMS = time.Since(start).Milliseconds()
				receipt.CleanupComplete = closed && receipt.UDPPortRebound && (!test.tcpMux || receipt.TCPPortRebound)
				receipts = append(receipts, receipt)
			})
			if test.tcpMux {
				tcp, err = net.Listen("tcp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				tcpAddress = tcp.Addr().String()
				receipt.TCPMuxPort = tcp.Addr().(*net.TCPAddr).Port
				tcpMux = webrtc.NewICETCPMux(nil, tcp, 8)
			}
			s := &Server{iceUDPMux: udpMux, iceTCPMux: tcpMux, nat1to1IPs: []string{"127.0.0.1"}, iceServers: []webrtc.ICEServer{{}}}
			if test.policy != "default" {
				settings := webrtc.SettingEngine{}
				settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
				settings.SetIncludeLoopbackCandidate(true)
				settings.SetICEUDPMux(udpMux)
				settings.SetICETCPMux(tcpMux)
				if test.policy == "setting_engine_udp4" {
					s.settingEngine = &settings
				} else {
					engine := &webrtc.MediaEngine{}
					if err = engine.RegisterDefaultCodecs(); err != nil {
						t.Fatal(err)
					}
					s.api = webrtc.NewAPI(webrtc.WithSettingEngine(settings), webrtc.WithMediaEngine(engine))
				}
			}
			if test.constructor == "signaling" {
				pc, _, err = (&signalPeer{srv: s}).newPeerConnection()
			} else {
				pc, _, err = s.newWHIPPeerConnection()
			}
			if err != nil {
				t.Fatal(err)
			}
			pc.OnICECandidate(nil) // SDP observation only; no signaling socket or media.
			if _, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
				t.Fatal(err)
			}
			done := webrtc.GatheringCompletePromise(pc)
			offer, err := pc.CreateOffer(nil)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			if err = pc.SetLocalDescription(offer); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("production candidate gathering exceeded two seconds")
			}
			receipt.GatherElapsedMS = time.Since(start).Milliseconds()
			for _, line := range strings.Split(pc.LocalDescription().SDP, "\r\n") {
				if !strings.HasPrefix(line, "a=candidate:") {
					continue
				}
				candidate, parseErr := ice.UnmarshalCandidate(strings.TrimPrefix(line, "a=candidate:"))
				if parseErr != nil {
					t.Fatal(parseErr)
				}
				if candidate.Type() != ice.CandidateTypeHost {
					t.Fatal("explicit no-STUN fixture produced a non-host candidate")
				}
				address := candidate.Address()
				if address != "127.0.0.1" {
					ip := net.ParseIP(address)
					family := "ipv6"
					if ip == nil {
						t.Fatal("unexpected candidate address is not an IP literal")
					}
					if ip.To4() != nil {
						family = "ipv4"
					}
					// Diagnostic metadata only: retain the rejected family, never its address.
					// The original assertion still fails; this is not full gathering acceptance.
					receipt.RejectedCandidate = &tcpMuxRejectedCandidate{
						NetworkType: candidate.NetworkType().String(), Type: candidate.Type().String(),
						AddressFamily: family, Port: candidate.Port(), TCPType: candidate.TCPType().String(),
					}
					t.Fatal("unexpected candidate address outside the owned loopback fixture")
				}
				if len(receipt.Candidates) >= 16 {
					t.Fatal("parsed candidate receipt exceeded finite bound")
				}
				receipt.Candidates = append(receipt.Candidates, tcpMuxParsedCandidate{
					NetworkType: candidate.NetworkType().String(), Type: candidate.Type().String(),
					Address: address, Port: candidate.Port(), TCPType: candidate.TCPType().String(),
				})
				switch candidate.NetworkType() {
				case ice.NetworkTypeUDP4:
					if candidate.Port() != udp.LocalAddr().(*net.UDPAddr).Port {
						t.Fatal("UDP candidate does not use the owned mux port")
					}
					receipt.UDPHostPresent = true
				case ice.NetworkTypeTCP4:
					if tcp == nil || candidate.Port() != tcp.Addr().(*net.TCPAddr).Port || candidate.TCPType() != ice.TCPTypePassive {
						t.Fatal("TCP candidate is not passive TCP4 at the owned mux port")
					}
					receipt.TCP4PassiveHost = true
				default:
					t.Fatal("unexpected candidate network type")
				}
			}
			if !receipt.UDPHostPresent || receipt.TCP4PassiveHost != test.wantTCP {
				t.Fatalf("UDP host=%v TCP4 passive host=%v, expected UDP and TCP=%v", receipt.UDPHostPresent, receipt.TCP4PassiveHost, test.wantTCP)
			}
		})
	}
	if path := os.Getenv("PKS_ICE_TCP_RECEIPTS"); path != "" {
		data, err := json.MarshalIndent(struct {
			Classification string                   `json:"classification"`
			Notes          string                   `json:"notes"`
			Cases          []tcpMuxCandidateReceipt `json:"cases"`
		}{"LOOPBACK-ONLY", "A rejected candidate remains a failed observation; no full gathering PASS is claimed.", receipts}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGivenIPv4TCPMuxWhenFamiliesDifferThenDelegationAndOwnershipArePreserved(t *testing.T) {
	t.Run("known_ipv4_listener", func(t *testing.T) {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		underlying := webrtc.NewICETCPMux(nil, listener, 8)
		t.Cleanup(func() {
			if !closeCandidateResource(t, "family_guard_mux", underlying.Close) {
				return
			}
			rebound, err := net.Listen("tcp4", address)
			if err != nil {
				t.Fatalf("family guard TCP port did not rebind: %v", err)
			}
			closeCandidateResource(t, "family_guard_rebound", rebound.Close)
		})
		guarded := relayDefaultTCPMux(underlying)
		if _, ok := guarded.(ipv4TCPMux); !ok {
			t.Fatal("known IPv4 listener did not receive the production family guard")
		}
		if conn, err := guarded.GetConnByUfrag("qualification", true, net.ParseIP("::1")); conn != nil || err != errIPv6OnIPv4TCPMux {
			t.Fatal("IPv6 request was not rejected before the IPv4 mux")
		}
		conn, err := guarded.GetConnByUfrag("qualification", false, net.IPv4(127, 0, 0, 1))
		if err != nil || conn == nil {
			t.Fatal("IPv4 request was not delegated to the real mux")
		}
		actual, ok := conn.LocalAddr().(*net.TCPAddr)
		if !ok || !actual.IP.Equal(net.IPv4(127, 0, 0, 1)) || actual.Port != listener.Addr().(*net.TCPAddr).Port {
			t.Fatal("delegated IPv4 connection changed the owned listener identity")
		}
		guarded.RemoveConnByUfrag("qualification")
	})
	t.Run("unknown_and_multiport_identity", func(t *testing.T) {
		var typedNil *ice.TCPMuxDefault
		for _, input := range []ice.TCPMux{nil, typedNil, &tcpMuxPolicyProbe{}, ice.NewMultiTCPMuxDefault(&tcpMuxPolicyProbe{})} {
			projected := relayDefaultTCPMux(input)
			if projected != input {
				t.Fatal("unknown, multiport or nil mux identity changed")
			}
			if _, original := input.(ice.AllConnsGetter); original {
				if _, retained := projected.(ice.AllConnsGetter); !retained {
					t.Fatal("caller optional multiport interface was lost")
				}
			}
		}
	})
	t.Run("mocked_delegation_and_ownership", func(t *testing.T) {
		// MOCKED interface operations isolate the adapter's delegation contract;
		// the separate constructor cases use real Pion muxes and SDP candidates.
		failure := errors.New("mocked mux outcome")
		probe := &tcpMuxPolicyProbe{failure: failure}
		guarded := ipv4TCPMux{TCPMux: probe}
		for _, request := range []struct {
			isIPv6 bool
			local  net.IP
		}{{true, net.IPv4(127, 0, 0, 1)}, {false, net.ParseIP("::1")}, {false, nil}, {false, net.IP{1, 2}}} {
			if conn, err := guarded.GetConnByUfrag("qualification", request.isIPv6, request.local); conn != nil || err != errIPv6OnIPv4TCPMux {
				t.Fatal("invalid family was not rejected")
			}
		}
		if probe.getCalls != 0 {
			t.Fatal("invalid family reached the underlying mux")
		}
		// Non-loopback IPv4 remains eligible for production NAT advertisement.
		local := net.IPv4(192, 0, 2, 1)
		if conn, err := guarded.GetConnByUfrag("qualification", false, local); conn != nil || err != failure {
			t.Fatal("underlying outcome was not preserved")
		}
		if probe.getCalls != 1 || probe.ufrag != "qualification" || probe.isIPv6 || !probe.local.Equal(local) {
			t.Fatal("valid IPv4 delegation changed request arguments")
		}
		guarded.RemoveConnByUfrag("qualification")
		if probe.removeCalls != 1 || probe.removed != "qualification" {
			t.Fatal("mux removal ownership was not delegated")
		}
		if err := guarded.Close(); err != failure || probe.closeCalls != 1 {
			t.Fatal("mux close ownership or outcome changed")
		}
	})
}

type tcpMuxPolicyProbe struct {
	failure                           error
	getCalls, removeCalls, closeCalls int
	ufrag, removed                    string
	isIPv6                            bool
	local                             net.IP
}

func (probe *tcpMuxPolicyProbe) GetConnByUfrag(ufrag string, isIPv6 bool, local net.IP) (net.PacketConn, error) {
	probe.getCalls++
	probe.ufrag, probe.isIPv6, probe.local = ufrag, isIPv6, local
	return nil, probe.failure
}

func (probe *tcpMuxPolicyProbe) RemoveConnByUfrag(ufrag string) {
	probe.removeCalls++
	probe.removed = ufrag
}

func (probe *tcpMuxPolicyProbe) Close() error {
	probe.closeCalls++
	return probe.failure
}

func (probe *tcpMuxPolicyProbe) GetAllConns(_ string, _ bool, _ net.IP) ([]net.PacketConn, error) {
	return nil, probe.failure
}

func closeCandidateResource(t *testing.T, name string, closeResource func() error) bool {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- closeResource() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("%s close failed: %v", name, err)
			return false
		}
		return true
	case <-time.After(time.Second):
		t.Errorf("%s close exceeded one second", name)
		return false
	}
}
