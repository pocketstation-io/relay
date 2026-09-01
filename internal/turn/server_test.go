package turn

import (
	"net"
	"testing"

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
