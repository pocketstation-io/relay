package metrics

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Registry holds all Prometheus-text-format counters/gauges for the relay.
// All fields are safe for concurrent access.
type Registry struct {
	PacketsForwarded       atomic.Uint64
	PacketsDropped         atomic.Uint64
	ListenerCount          atomic.Int64
	RoomsActive            atomic.Int64
	SessionsTotal          atomic.Uint64
	ICERestartTotal        atomic.Int64
	WebhookErrorsTotal     atomic.Int64
	WebhookDroppedTotal    atomic.Int64
	CallbackDroppedTotal   atomic.Uint64
	KeyExchangeTotal       atomic.Int64
	HandshakeRejectedTotal atomic.Uint64
	HandshakeActive        atomic.Int64
}

// New returns an initialized Registry.
func New() *Registry { return &Registry{} }

// WritePrometheus writes Prometheus text format to w.
func (r *Registry) WritePrometheus(w io.Writer) {
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
	writeI("relay_ice_restart_total", "Total ICE restart messages sent to sources.", "counter", r.ICERestartTotal.Load())
	writeI("relay_webhook_errors_total", "Total webhook delivery failures (non-2xx or transport error).", "counter", r.WebhookErrorsTotal.Load())
	writeI("relay_webhook_dropped_total", "Total webhook events rejected by the bounded delivery gate.", "counter", r.WebhookDroppedTotal.Load())
	write("relay_callback_dropped_total", "Total control-plane callbacks rejected by the bounded delivery gate.", "counter", r.CallbackDroppedTotal.Load())
	writeI("relay_key_exchange_total", "Total KEY_EXCHANGE messages successfully forwarded to listeners.", "counter", r.KeyExchangeTotal.Load())
	write("relay_handshake_rejected_total", "Total signaling and WHIP handshakes rejected before media allocation.", "counter", r.HandshakeRejectedTotal.Load())
	writeI("relay_handshake_active", "Current signaling and WHIP handshakes awaiting media allocation.", "gauge", r.HandshakeActive.Load())
}
