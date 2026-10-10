package server

import "net"

// explicitLoopbackICE preserves Pion's default exclusion unless the deployment
// both binds its UDP mux and advertises the same loopback address. NAT mapping
// alone does not opt ordinary interface gathering into loopback candidates.
func (s *Server) explicitLoopbackICE() bool {
	if s.iceUDPMux == nil {
		return false
	}
	for _, advertised := range s.nat1to1IPs {
		ip := net.ParseIP(advertised)
		if ip == nil || !ip.IsLoopback() {
			continue
		}
		for _, address := range s.iceUDPMux.GetListenAddresses() {
			bound, ok := address.(*net.UDPAddr)
			if ok && bound.IP.IsLoopback() && bound.IP.Equal(ip) {
				return true
			}
		}
	}
	return false
}
