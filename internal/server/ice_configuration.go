package server

import (
	"errors"
	"net"

	"github.com/pion/ice/v4"
)

var errIPv6OnIPv4TCPMux = errors.New("IPv4 ICE TCP mux cannot serve an IPv6 interface")

// relayDefaultTCPMux constrains only Pion's known IPv4 listener. Unknown and
// multiport muxes retain their own policy and optional interfaces unchanged.
func relayDefaultTCPMux(mux ice.TCPMux) ice.TCPMux {
	known, ok := mux.(*ice.TCPMuxDefault)
	if !ok || known == nil {
		return mux
	}
	address, ok := known.LocalAddr().(*net.TCPAddr)
	if !ok || address == nil || address.IP.To4() == nil {
		return mux
	}
	return ipv4TCPMux{TCPMux: mux}
}

type ipv4TCPMux struct {
	ice.TCPMux
}

func (mux ipv4TCPMux) GetConnByUfrag(ufrag string, isIPv6 bool, local net.IP) (net.PacketConn, error) {
	if isIPv6 || local.To4() == nil {
		return nil, errIPv6OnIPv4TCPMux
	}
	return mux.TCPMux.GetConnByUfrag(ufrag, isIPv6, local)
}
