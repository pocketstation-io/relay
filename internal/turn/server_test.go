package turn

import (
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/pion/stun/v3"
	pionTurn "github.com/pion/turn/v4"
)

func TestGivenNoRelayRangeWhenConfiguredThenOperatingSystemPortsRemainAvailable(t *testing.T) {
	generator, err := relayAddressGenerator(ServerConfig{PublicIP: net.ParseIP("192.0.2.1")})
	if err != nil {
		t.Fatalf("create relay address generator: %v", err)
	}
	if _, ok := generator.(*pionTurn.RelayAddressGeneratorStatic); !ok {
		t.Fatalf("generator type = %T, want static", generator)
	}
}

func TestGivenFiniteRelayRangeWhenConfiguredThenAllocationPortsAreBounded(t *testing.T) {
	generator, err := relayAddressGenerator(ServerConfig{
		PublicIP:     net.ParseIP("192.0.2.1"),
		RelayMinPort: 49160,
		RelayMaxPort: 49167,
	})
	if err != nil {
		t.Fatalf("create relay address generator: %v", err)
	}
	ranged, ok := generator.(*pionTurn.RelayAddressGeneratorPortRange)
	if !ok {
		t.Fatalf("generator type = %T, want port range", generator)
	}
	if ranged.MinPort != 49160 || ranged.MaxPort != 49167 {
		t.Fatalf("relay range = %d..%d, want 49160..49167", ranged.MinPort, ranged.MaxPort)
	}
}

func TestGivenInvalidRelayRangeWhenConfiguredThenStartupFailsClosed(t *testing.T) {
	for _, cfg := range []ServerConfig{
		{PublicIP: net.ParseIP("192.0.2.1"), RelayMinPort: 49160},
		{PublicIP: net.ParseIP("192.0.2.1"), RelayMaxPort: 49167},
		{PublicIP: net.ParseIP("192.0.2.1"), RelayMinPort: 49167, RelayMaxPort: 49160},
		{PublicIP: net.ParseIP("192.0.2.1"), RelayMinPort: 1, RelayMaxPort: 49160},
		{PublicIP: net.ParseIP("192.0.2.1"), RelayMinPort: 49160, RelayMaxPort: 65536},
	} {
		if _, err := relayAddressGenerator(cfg); err == nil {
			t.Fatalf("relay range %d..%d unexpectedly accepted", cfg.RelayMinPort, cfg.RelayMaxPort)
		}
	}
}

func TestGivenConfiguredUDPBindWhenRelayAllocatesThenBothGeneratorsUseIt(t *testing.T) {
	for _, finiteRange := range []bool{false, true} {
		t.Run(strconv.FormatBool(finiteRange), func(t *testing.T) {
			cfg := ServerConfig{PublicIP: net.ParseIP("192.0.2.1"), UDPBindAddress: "127.0.0.1"}
			if finiteRange {
				cfg.RelayMinPort = availableUDPPort(t)
				cfg.RelayMaxPort = cfg.RelayMinPort
			}
			generator, err := relayAddressGenerator(cfg)
			if err != nil {
				t.Fatal(err)
			}
			switch bound := generator.(type) {
			case *pionTurn.RelayAddressGeneratorStatic:
				if bound.Address != cfg.UDPBindAddress {
					t.Fatalf("allocation bind = %q, want %q", bound.Address, cfg.UDPBindAddress)
				}
			case *pionTurn.RelayAddressGeneratorPortRange:
				if bound.Address != cfg.UDPBindAddress {
					t.Fatalf("allocation bind = %q, want %q", bound.Address, cfg.UDPBindAddress)
				}
			default:
				t.Fatalf("unexpected generator %T", generator)
			}
			if err := generator.Validate(); err != nil {
				t.Fatal(err)
			}
			conn, advertised, err := generator.AllocatePacketConn("udp4", 0)
			if err != nil {
				t.Fatalf("allocate real UDP socket: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			address := advertised.(*net.UDPAddr)
			if !address.IP.Equal(cfg.PublicIP) || address.Port == 0 {
				t.Fatalf("advertised allocation = %v", address)
			}
			if finiteRange && address.Port != cfg.RelayMinPort {
				t.Fatalf("allocation port = %d, want %d", address.Port, cfg.RelayMinPort)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			rebound, err := net.ListenPacket("udp4", net.JoinHostPort(cfg.UDPBindAddress, strconv.Itoa(address.Port)))
			if err != nil {
				t.Fatalf("allocation socket not released: %v", err)
			}
			_ = rebound.Close()
		})
	}
}

func TestGivenUnavailableUDPBindWhenTURNStartsThenStartupFailsClosed(t *testing.T) {
	srv, err := Start(ServerConfig{
		PublicIP: net.ParseIP("192.0.2.1"), UDPBindAddress: "192.0.2.1", UDPPort: availableUDPPort(t),
	})
	if err == nil {
		srv.Stop()
		t.Fatal("TURN ignored the unavailable UDP bind address")
	}
}

func TestGivenInvalidUDPBindWhenTURNStartsThenStartupFailsClosed(t *testing.T) {
	if _, err := Start(ServerConfig{
		PublicIP: net.ParseIP("192.0.2.1"), UDPBindAddress: "127.0.0.1:invalid", TCPPort: 3478,
	}); err == nil {
		t.Fatal("TURN accepted an invalid UDP bind address")
	}
}

func TestGivenConfiguredUDPListenerWhenBindingRequestArrivesThenSTUNResponds(t *testing.T) {
	port := availableUDPPort(t)
	srv, err := Start(ServerConfig{
		PublicIP: net.ParseIP("192.0.2.1"), UDPBindAddress: "localhost", UDPPort: port,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	conn, err := net.DialTimeout("udp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	assertTURNBindingResponse(t, conn)
}

func TestGivenSeparateUDPBindWhenTCPOnlyStartsThenTCPRemainsReachable(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	srv, err := Start(ServerConfig{
		PublicIP: net.ParseIP("192.0.2.1"), UDPBindAddress: "192.0.2.1", TCPPort: port,
	})
	if err != nil {
		t.Fatalf("TCP used the UDP-only bind address: %v", err)
	}
	t.Cleanup(srv.Stop)
	conn, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	assertTURNBindingResponse(t, conn)
}

func TestGivenTCPBindFailureWhenTURNStartupRollsBackThenUDPSocketIsReleased(t *testing.T) {
	occupied, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })
	port := availableUDPPort(t)
	srv, err := Start(ServerConfig{
		PublicIP: net.ParseIP("192.0.2.1"), UDPBindAddress: "127.0.0.1", UDPPort: port,
		TCPPort: occupied.Addr().(*net.TCPAddr).Port,
	})
	if err == nil {
		srv.Stop()
		t.Fatal("TURN unexpectedly acquired the occupied TCP port")
	}
	rebound, err := net.ListenPacket("udp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("failed startup leaked UDP socket: %v", err)
	}
	_ = rebound.Close()
}

func availableUDPPort(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port
	_ = conn.Close()
	return port
}

func assertTURNBindingResponse(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	if _, err := conn.Write(request.Raw); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 2048)
	var count int
	if _, udp := conn.(*net.UDPConn); udp {
		var err error
		count, err = conn.Read(buffer)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err := io.ReadFull(conn, buffer[:20]); err != nil {
			t.Fatal(err)
		}
		count = 20 + int(buffer[2])<<8 + int(buffer[3])
		if count > len(buffer) {
			t.Fatal("oversized STUN response")
		}
		if _, err := io.ReadFull(conn, buffer[20:count]); err != nil {
			t.Fatal(err)
		}
	}
	response := &stun.Message{Raw: buffer[:count]}
	if err := response.Decode(); err != nil {
		t.Fatal(err)
	}
	if response.Type != stun.BindingSuccess || response.TransactionID != request.TransactionID {
		t.Fatalf("unexpected STUN response: %v", response.Type)
	}
}
