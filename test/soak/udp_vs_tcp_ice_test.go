// Package soak_test — Script C: UDP vs TCP ICE candidate selection test.
//
// This test is a placeholder that defines the measurement structure and asserts
// nothing until real TURN/STUN infrastructure with UDP support is available.
//
// Skip reason: The relay currently operates with ICE-TCP only (Fly.io TCP
// proxy, port 443). Verifying that UDP is selected and faster requires:
//   - A TURN server configured with UDP support (RELAY_TURN_URL set).
//   - fly.toml [services] udp = true, port 3478 or 5349 UDP exposed.
//   - NAT1To1IPs set to the relay's public IP.
//   - An ICEServer list that includes both the TURN UDP and TCP URLs.
//
// When those conditions are met, remove the t.Skip call and run:
//
//	RELAY_SOAK_FULL=1 go test -v -race -timeout 10m \
//	  -run TestGiven_ICECandidates_When_BothUDPAndTCP_Then_UDPSelectedAndFaster \
//	  ./test/soak/
//
// The test structure below matches the measurements that will be asserted once
// the infrastructure is in place:
//   - Negotiate two PeerConnections: one with UDP+TCP candidates, one TCP-only.
//   - Record ICE candidate type selected (host/srflx/relay) and transport.
//   - Record time-to-ICE-connected for each.
//   - Assert UDP path selected when available.
//   - Assert UDP time-to-connected < TCP time-to-connected.
package soak_test

import (
	"testing"
	"time"
)

// iceCandidateTransport describes the transport protocol of the selected ICE
// candidate pair.
type iceCandidateTransport string

const (
	iceTransportUDP     iceCandidateTransport = "udp"
	iceTransportTCP     iceCandidateTransport = "tcp"
	iceTransportUnknown iceCandidateTransport = "unknown"
)

// iceSelectionResult records the outcome of one ICE negotiation for comparison.
type iceSelectionResult struct {
	label           string
	transport       iceCandidateTransport
	candidateType   string        // "host", "srflx", "relay"
	timeToConnected time.Duration // time from dial to ICEConnectionStateConnected
	err             error
}

// TestGiven_ICECandidates_When_BothUDPAndTCP_Then_UDPSelectedAndFaster is a
// placeholder test. It is unconditionally skipped until TURN/STUN infrastructure
// with UDP support is deployed and configured.
//
// When enabled, the test will:
//  1. Start an in-process relay configured with both UDP and TCP ICE servers.
//  2. Negotiate a UDP+TCP PeerConnection and record the selected transport.
//  3. Negotiate a TCP-only PeerConnection and record time-to-connected.
//  4. Assert that the UDP+TCP negotiation selected UDP as the transport.
//  5. Assert that UDP time-to-connected < TCP time-to-connected.
func TestGiven_ICECandidates_When_BothUDPAndTCP_Then_UDPSelectedAndFaster(t *testing.T) {
	t.Skip("Requires TURN/STUN infra with UDP support; run manually after fly.toml UDP config")

	// Measurement variables — populated once the skip is removed.
	var (
		udpResult iceSelectionResult
		tcpResult iceSelectionResult
	)

	// TODO: Start in-process relay with ICEServers containing both UDP and TCP
	// TURN URLs. Example Config fragment (values from environment):
	//
	//   server.Config{
	//     ICEServers: []webrtc.ICEServer{
	//       {URLs: []string{"turn:relay.example.com:3478?transport=udp"},
	//        Username: "...", Credential: "..."},
	//       {URLs: []string{"turn:relay.example.com:443?transport=tcp"},
	//        Username: "...", Credential: "..."},
	//     },
	//     ICETCPMux: <configured tcp mux>,
	//     NAT1To1IPs: []string{os.Getenv("RELAY_PUBLIC_IP")},
	//   }

	// TODO: Negotiate UDP+TCP PeerConnection:
	//   udpResult = negotiateAndMeasure(t, ts, api, "udp+tcp", udpTcpICEServers)

	// TODO: Negotiate TCP-only PeerConnection:
	//   tcpResult = negotiateAndMeasure(t, ts, api, "tcp-only", tcpOnlyICEServers)

	// Assertion: UDP transport selected when both are available.
	if udpResult.transport != iceTransportUDP {
		t.Errorf("expected UDP transport selected, got %s (candidate type: %s)",
			udpResult.transport, udpResult.candidateType)
	}

	// Assertion: UDP time-to-connected is strictly less than TCP.
	if udpResult.timeToConnected >= tcpResult.timeToConnected {
		t.Errorf("UDP time-to-connected (%s) not faster than TCP (%s)",
			udpResult.timeToConnected, tcpResult.timeToConnected)
	}

	t.Logf("ICE transport comparison: UDP=%s TCP=%s (speedup=%.2fx)",
		udpResult.timeToConnected,
		tcpResult.timeToConnected,
		float64(tcpResult.timeToConnected)/float64(udpResult.timeToConnected),
	)
}
