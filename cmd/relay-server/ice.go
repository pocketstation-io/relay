package main

import (
	"net"
	"strconv"
)

func iceUDPListenAddress(onFlyIO bool, bindIP net.IP, port int) (network, address string) {
	if onFlyIO {
		return "udp", net.JoinHostPort("fly-global-services", strconv.Itoa(port))
	}
	return "udp4", net.JoinHostPort(bindIP.String(), strconv.Itoa(port))
}
