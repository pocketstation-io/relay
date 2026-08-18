package session

import (
	"errors"
	"testing"

	"github.com/pion/rtp"
)

func TestGivenNewRelaySessionWhenInspectedThenEmpty(t *testing.T) {
	given_new_relay_session_when_inspected_then_empty(t)
}

func TestGivenRelaySessionWhenAddSubscriptionThenCountIncreases(t *testing.T) {
	r := New("room-1")
	if err := r.AddSubscription("peer-1", BusMix, &mockSubscription{}); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	if r.SubscriptionCount() != 1 {
		t.Errorf("got %d subscriptions, want 1", r.SubscriptionCount())
	}
}

func TestGivenRelaySessionWhenMultipleSubscriptionsThenAllCounted(t *testing.T) {
	r := New("room-1")
	for _, id := range []string{"peer-1", "peer-2", "peer-3"} {
		if err := r.AddSubscription(id, BusMix, &mockSubscription{}); err != nil {
			t.Fatalf("AddSubscription %s: %v", id, err)
		}
	}
	if r.SubscriptionCount() != 3 {
		t.Errorf("got %d subscriptions, want 3", r.SubscriptionCount())
	}
}

func TestGivenRelaySessionWhenRemoveSubscriptionThenCountDecreases(t *testing.T) {
	r := New("room-1")
	if err := r.AddSubscription("peer-1", BusMix, &mockSubscription{}); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	r.RemoveSubscription("peer-1")
	if r.SubscriptionCount() != 0 {
		t.Errorf("got %d subscriptions, want 0", r.SubscriptionCount())
	}
}

func TestGivenRelaySessionWhenRemoveUnknownSubscriptionThenNoop(t *testing.T) {
	r := New("room-1")
	r.RemoveSubscription("does-not-exist") // must not panic
}

func TestGivenRelaySessionWhenSubscriberWriteFailsRepeatedlyThenEvicted(t *testing.T) {
	r := New("room-1")
	errCounts := make(map[string]int, 1)
	deadSubs := make([]string, 0, 1)
	sub := &mockSubscription{err: errors.New("write failed")}

	if err := r.AddSubscription("peer-1", BusMix, sub); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}

	for seq := uint16(0); seq < 5; seq++ {
		r.deliver(
			"voice",
			&rtp.Packet{Header: rtp.Header{SequenceNumber: seq}},
			errCounts,
			&deadSubs,
		)
	}

	if got := r.SubscriptionCount(); got != 0 {
		t.Fatalf("subscription count after repeated write failures = %d, want 0", got)
	}
	if _, ok := errCounts["peer-1"]; ok {
		t.Fatal("errCounts still contains evicted subscriber")
	}
	if got := r.SubscriptionEvictionsTotal(); got != 1 {
		t.Fatalf("subscription evictions = %d, want 1", got)
	}
}

func TestGivenBusSubscriptionWhenRegisteredThenSessionOwnsLifecycleAndObservation(t *testing.T) {
	r := New("session-observed")
	subscription := &observedBusSubscription{}
	if err := r.AddBusSubscription("peer-observed", "application", subscription); err != nil {
		t.Fatalf("AddBusSubscription: %v", err)
	}

	snapshots := r.SubscriptionSnapshots()
	if len(snapshots) != 1 {
		t.Fatalf("subscription snapshots = %d, want 1", len(snapshots))
	}
	if snapshots[0].BusID != "application" || snapshots[0].Mode != "observed" {
		t.Fatalf("subscription snapshot = %+v", snapshots[0])
	}

	r.RemoveSubscription("peer-observed")
	r.RemoveSubscription("peer-observed")
	if subscription.stopCount != 1 {
		t.Fatalf("stop count = %d, want 1", subscription.stopCount)
	}
}

func TestGivenBusSubscriptionWhenWritesRepeatedlyFailThenSessionEvictsAndStopsIt(t *testing.T) {
	r := New("session-failed-subscription")
	subscription := &observedBusSubscription{writeError: errors.New("transport unavailable")}
	if err := r.AddBusSubscription("peer-observed", "application", subscription); err != nil {
		t.Fatalf("AddBusSubscription: %v", err)
	}

	errCounts := make(map[string]int, 1)
	deadSubscriptions := make([]string, 0, 1)
	for sequence := uint16(0); sequence < 5; sequence++ {
		r.deliver("application", &rtp.Packet{Header: rtp.Header{SequenceNumber: sequence}}, errCounts, &deadSubscriptions)
	}

	if subscription.stopCount != 1 {
		t.Fatalf("stop count = %d, want 1", subscription.stopCount)
	}
	if got := r.SubscriptionEvictionsTotal(); got != 1 {
		t.Fatalf("subscription evictions = %d, want 1", got)
	}
}

func TestGivenRelaySessionWhenClosedMultipleTimesThenIdempotent(t *testing.T) {
	r := New("room-1")
	r.Close()
	r.Close()
	r.Close() // must not panic
}
