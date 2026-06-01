// White-box tests for public broadcast channel listing (spec §3.1, Phase 6).
// Package room (not room_test) to access unexported fields.
package room

import (
	"testing"
)

// TestListPublicChannels_ReturnsOnlyPublicRooms verifies that ListPublic
// returns only rooms that have Public==true.
func TestListPublicChannels_ReturnsOnlyPublicRooms(t *testing.T) {
	t.Parallel()

	// Given: a manager with one public room and one private room
	m := NewManager()
	pub := m.GetOrCreate("pub-room")
	pub.Public = true
	_ = m.GetOrCreate("priv-room") // Public defaults to false

	// When: ListPublic is called
	summaries := m.ListPublic()

	// Then: only the public room is returned
	if len(summaries) != 1 {
		t.Fatalf("ListPublic: got %d results, want 1", len(summaries))
	}
	if summaries[0].RoomID != "pub-room" {
		t.Errorf("ListPublic: got room_id %q, want %q", summaries[0].RoomID, "pub-room")
	}
	if !summaries[0].Public {
		t.Errorf("ListPublic: Public field should be true")
	}
}

// TestPublicRoomAppearsInChannelsList verifies that marking a room public via
// Room.Public makes it appear in the listing with correct metadata.
func TestPublicRoomAppearsInChannelsList(t *testing.T) {
	t.Parallel()

	// Given: a manager with a public room that has a listener
	m := NewManager()
	rm := m.GetOrCreate("broadcast-room")
	rm.Public = true
	_ = rm.AddListener("peer-1", &mockListener{})

	// When: ListPublic is called
	summaries := m.ListPublic()

	// Then: the room appears with listener_count == 1 and source_active == false
	if len(summaries) != 1 {
		t.Fatalf("ListPublic: got %d results, want 1", len(summaries))
	}
	s := summaries[0]
	if s.RoomID != "broadcast-room" {
		t.Errorf("room_id: got %q, want %q", s.RoomID, "broadcast-room")
	}
	if s.ListenerCount != 1 {
		t.Errorf("listener_count: got %d, want 1", s.ListenerCount)
	}
	if s.SourceActive {
		t.Errorf("source_active: got true, want false (no source attached)")
	}
	if s.CreatedAt.IsZero() {
		t.Errorf("created_at should not be zero")
	}
}

// TestPrivateRoomExcludedFromChannelsList verifies that rooms with Public==false
// do not appear in ListPublic, even when they have listeners.
func TestPrivateRoomExcludedFromChannelsList(t *testing.T) {
	t.Parallel()

	// Given: a manager with only private rooms
	m := NewManager()
	priv1 := m.GetOrCreate("priv-1")
	priv1.Public = false
	priv2 := m.GetOrCreate("priv-2")
	priv2.Public = false
	_ = priv2.AddListener("peer-x", &mockListener{})

	// When: ListPublic is called
	summaries := m.ListPublic()

	// Then: no rooms are returned
	if len(summaries) != 0 {
		t.Errorf("ListPublic: got %d results, want 0 (all rooms are private)", len(summaries))
	}
}

// TestListPublic_EmptyManager verifies that ListPublic returns a non-nil empty
// slice when the manager has no rooms at all.
func TestListPublic_EmptyManager(t *testing.T) {
	t.Parallel()

	// Given: an empty manager
	m := NewManager()

	// When: ListPublic is called
	summaries := m.ListPublic()

	// Then: a non-nil empty slice is returned (safe for JSON encoding as [])
	if summaries == nil {
		t.Error("ListPublic: returned nil, want non-nil empty slice")
	}
	if len(summaries) != 0 {
		t.Errorf("ListPublic: got %d results, want 0", len(summaries))
	}
}
