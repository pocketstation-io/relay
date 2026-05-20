package metrics

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Registry holds all Prometheus-text-format counters/gauges for the relay.
// All fields are safe for concurrent access.
type Registry struct {
	PacketsForwarded atomic.Uint64
	PacketsDropped   atomic.Uint64
	ListenerCount    atomic.Int64
	RoomsActive      atomic.Int64
	SessionsTotal    atomic.Uint64
}

// New returns an initialized Registry.
func New() *Registry { return &Registry{} }

// WriteTo writes Prometheus text format to w.
func (r *Registry) WriteTo(w io.Writer) {
	write := func(name, help, typ string, val uint64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", name, help, name, typ, name, val)
	}
	writeI := func(name, help, typ string, val int64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", name, help, name, typ, name, val)
	}
	write("relay_packets_forwarded_total", "Total RTP packets forwarded to listeners.", "counter", r.PacketsForwarded.Load())
	write("relay_packets_dropped_total", "Total RTP packets dropped (listener write error).", "counter", r.PacketsDropped.Load())
	writeI("relay_listener_count", "Current number of active listeners.", "gauge", r.ListenerCount.Load())
	writeI("relay_rooms_active", "Current number of active rooms.", "gauge", r.RoomsActive.Load())
	write("relay_sessions_total", "Total WebSocket sessions created.", "counter", r.SessionsTotal.Load())
}
