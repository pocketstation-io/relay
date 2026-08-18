package session

import (
	"testing"
)

func TestGivenSessionRegistryWhenGetOrCreateThenSameRoomReturned(t *testing.T) {
	reg := NewRegistry()
	r1 := reg.GetOrCreate("r1")
	r2 := reg.GetOrCreate("r1")
	if r1 != r2 {
		t.Error("GetOrCreate returned different pointers for same ID")
	}
}

func TestGivenSessionRegistryWhenGetOrCreateDifferentIDsThenDistinctRooms(t *testing.T) {
	reg := NewRegistry()
	r1 := reg.GetOrCreate("r1")
	r2 := reg.GetOrCreate("r2")
	if r1 == r2 {
		t.Error("GetOrCreate returned same pointer for different IDs")
	}
}

func TestGivenSessionRegistryWhenGetUnknownIDThenNotFound(t *testing.T) {
	reg := NewRegistry()
	_, ok := reg.Get("unknown")
	if ok {
		t.Error("Get returned true for a room that was never created")
	}
}

func TestGivenSessionRegistryWhenDeleteThenRoomRemoved(t *testing.T) {
	reg := NewRegistry()
	reg.GetOrCreate("r1")
	reg.Delete("r1")
	_, ok := reg.Get("r1")
	if ok {
		t.Error("room still present after Delete")
	}
}

func TestGivenSessionRegistryWhenDeleteUnknownThenNoop(t *testing.T) {
	reg := NewRegistry()
	reg.Delete("does-not-exist") // must not panic
}

func TestGivenSessionRegistryWhenListPublicThenOnlyPublicReturned(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	pub := reg.GetOrCreate("pub-room")
	pub.Public.Store(true)
	_ = reg.GetOrCreate("priv-room")

	summaries := reg.ListPublic()
	if len(summaries) != 1 {
		t.Fatalf("ListPublic: got %d results, want 1", len(summaries))
	}
	if summaries[0].SessionID != "pub-room" {
		t.Errorf("ListPublic: got session_id %q, want %q", summaries[0].SessionID, "pub-room")
	}
}

func TestGivenEmptySessionRegistryWhenListPublicThenNonNilEmpty(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	summaries := reg.ListPublic()
	if summaries == nil {
		t.Error("ListPublic: returned nil, want non-nil empty slice")
	}
	if len(summaries) != 0 {
		t.Errorf("ListPublic: got %d results, want 0", len(summaries))
	}
}

func TestGivenRegistryAtSessionLimitWhenNewIdentityArrivesThenCreationIsRejectedAtomically(t *testing.T) {
	registry := NewRegistry()
	first, created, accepted := registry.GetOrCreateWithinLimit("first", 1)
	if first == nil || !created || !accepted {
		t.Fatalf("first acquisition = (%v, %t, %t), want created and accepted", first, created, accepted)
	}
	if existing, created, accepted := registry.GetOrCreateWithinLimit("first", 1); existing != first || created || !accepted {
		t.Fatalf("existing acquisition = (%v, %t, %t), want same accepted session", existing, created, accepted)
	}
	if second, created, accepted := registry.GetOrCreateWithinLimit("second", 1); second != nil || created || accepted {
		t.Fatalf("second acquisition = (%v, %t, %t), want rejected", second, created, accepted)
	}
	if got := registry.RoomCount(); got != 1 {
		t.Fatalf("session count = %d, want 1", got)
	}
}
