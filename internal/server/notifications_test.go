package server

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestGivenCallbackDeliveryAtCapacityWhenAnotherEventArrivesThenItIsDropped(t *testing.T) {
	server := New(Config{MaxConcurrentCallbacks: 1})
	started := make(chan struct{})
	release := make(chan struct{})
	server.dispatchCallback(func() {
		close(started)
		<-release
	})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first callback did not start")
	}

	var secondRan atomic.Bool
	server.dispatchCallback(func() { secondRan.Store(true) })
	if secondRan.Load() {
		t.Fatal("callback above capacity executed")
	}
	if got := server.Metrics.CallbackDroppedTotal.Load(); got != 1 {
		t.Fatalf("callback drops = %d, want 1", got)
	}
	close(release)
}
