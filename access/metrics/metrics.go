package metrics

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Registry holds all Prometheus-text-format counters and gauges for the control-plane.
// All fields are safe for concurrent access.
type Registry struct {
	SessionsCreated      atomic.Uint64
	SessionsDeleted      atomic.Uint64
	SubscriptionRequests atomic.Uint64
}

// New returns an initialized Registry.
func New() *Registry { return &Registry{} }

// WriteMetrics writes Prometheus text format to w.
func (r *Registry) WriteMetrics(w io.Writer, activeSessions int64) {
	write := func(name, help, typ string, val uint64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", name, help, name, typ, name, val)
	}
	writeI := func(name, help, typ string, val int64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", name, help, name, typ, name, val)
	}
	write("pocketstation_control_plane_sessions_created_total", "Sessions created by this process.", "counter", r.SessionsCreated.Load())
	write("pocketstation_control_plane_sessions_deleted_total", "Sessions deleted by this process.", "counter", r.SessionsDeleted.Load())
	write("pocketstation_control_plane_subscription_requests_total", "Subscription requests handled by this process.", "counter", r.SubscriptionRequests.Load())
	writeI("pocketstation_control_plane_sessions_active", "Currently active sessions.", "gauge", activeSessions)
}
