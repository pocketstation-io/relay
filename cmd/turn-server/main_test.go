package main

import "testing"

func TestGivenCompleteEnvironmentWhenLoadingThenBoundedTURNConfigIsReturned(t *testing.T) {
	values := map[string]string{
		"TURN_PUBLIC_IP":      "192.0.2.10",
		"TURN_SHARED_SECRET":  "abcdef0123456789abcdef0123456789",
		"TURN_RELAY_MIN_PORT": "50000",
		"TURN_RELAY_MAX_PORT": "50007",
	}
	config, err := loadConfig(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if config.UDPPort != defaultTURNPort || config.TCPPort != defaultTURNPort {
		t.Fatalf("TURN ports = udp %d tcp %d, want %d", config.UDPPort, config.TCPPort, defaultTURNPort)
	}
	if config.RelayMinPort != 50000 || config.RelayMaxPort != 50007 {
		t.Fatalf("relay range = %d..%d", config.RelayMinPort, config.RelayMaxPort)
	}
}

func TestGivenUnsafeEnvironmentWhenLoadingThenStartupFailsClosed(t *testing.T) {
	tests := []map[string]string{
		{"TURN_SHARED_SECRET": "abcdef0123456789abcdef0123456789"},
		{"TURN_PUBLIC_IP": "192.0.2.10", "TURN_SHARED_SECRET": "short"},
		{
			"TURN_PUBLIC_IP":      "192.0.2.10",
			"TURN_SHARED_SECRET":  "abcdef0123456789abcdef0123456789",
			"TURN_RELAY_MIN_PORT": "50008",
			"TURN_RELAY_MAX_PORT": "50000",
		},
	}
	for _, values := range tests {
		if _, err := loadConfig(func(name string) string { return values[name] }); err == nil {
			t.Fatalf("unsafe TURN environment unexpectedly accepted: %#v", values)
		}
	}
}
