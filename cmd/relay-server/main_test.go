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

func TestGivenInvalidProxyIngressConfigurationWhenStartingThenRejectsInsteadOfSilentlyDisabling(t *testing.T) {
	for _, configured := range []string{"invalid", "-1", "65536", " 8082"} {
		if ingress, err := openProxyIngress(configured, true); err == nil || ingress != nil {
			t.Fatalf("invalid ingress port %q was accepted", configured)
		}
	}
	if ingress, err := openProxyIngress("8082", false); err == nil || ingress != nil {
		t.Fatal("proxy ingress accepted without embedded TURN")
	}
	for _, configured := range []string{"", "0"} {
		if ingress, err := openProxyIngress(configured, false); err != nil || ingress != nil {
			t.Fatalf("disabled ingress %q = %v, %v", configured, ingress, err)
		}
	}
}
