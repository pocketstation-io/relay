package session

import "testing"

func TestGivenUnchangedRelayStateWhenReconciledThenRevisionRemainsStable(t *testing.T) {
	relaySession := New("session-1")
	defer relaySession.Close()

	initial := relaySession.ControlState("relay-epoch-1")
	reconciled := relaySession.ControlState("relay-epoch-1")
	if initial.Revision == 0 || reconciled.Revision != initial.Revision {
		t.Fatalf("reconciliation changed revision: initial=%d reconciled=%d", initial.Revision, reconciled.Revision)
	}

	if err := relaySession.AddSubscription("subscriber-1", BusMix, &mockSubscription{}); err != nil {
		t.Fatal(err)
	}
	changed := relaySession.ControlState("relay-epoch-1")
	if changed.Revision <= initial.Revision || len(changed.Subscriptions) != 1 {
		t.Fatalf("state change not represented: %#v", changed)
	}
	again := relaySession.ControlState("relay-epoch-1")
	if again.Revision != changed.Revision {
		t.Fatalf("periodic snapshot revision=%d, want %d", again.Revision, changed.Revision)
	}
}
