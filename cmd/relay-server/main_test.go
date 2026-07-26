package main

import (
	"net"
	"testing"
)

func TestGivenFlyRuntimeWhenSelectingICEUDPAddressThenUsesGlobalServices(t *testing.T) {
	network, address := iceUDPListenAddress(true, net.IPv4zero, 10_000)
	if network != "udp" {
		t.Fatalf("network = %q, want udp", network)
	}
	if address != "fly-global-services:10000" {
		t.Fatalf("address = %q, want fly-global-services:10000", address)
	}
}

func TestGivenLocalRuntimeWhenSelectingICEUDPAddressThenUsesConfiguredIPv4(t *testing.T) {
	network, address := iceUDPListenAddress(false, net.IPv4(127, 0, 0, 1), 10_000)
	if network != "udp4" {
		t.Fatalf("network = %q, want udp4", network)
	}
	if address != "127.0.0.1:10000" {
		t.Fatalf("address = %q, want 127.0.0.1:10000", address)
	}
}
