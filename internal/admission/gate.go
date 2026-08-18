package admission

import "sync/atomic"

// Gate bounds concurrent control-plane work before expensive media resources
// are allocated. A non-positive limit rejects every acquisition.
type Gate struct {
	limit  int64
	active atomic.Int64
}

func NewGate(limit int) *Gate {
	return &Gate{limit: int64(limit)}
}

func (gate *Gate) TryAcquire() bool {
	for {
		active := gate.active.Load()
		if active >= gate.limit {
			return false
		}
		if gate.active.CompareAndSwap(active, active+1) {
			return true
		}
	}
}

func (gate *Gate) Release() {
	for {
		active := gate.active.Load()
		if active == 0 {
			return
		}
		if gate.active.CompareAndSwap(active, active-1) {
			return
		}
	}
}

func (gate *Gate) Active() int64 {
	return gate.active.Load()
}
