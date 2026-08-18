package server

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/signaling"
)

func TestGivenMultiBusPublishWhenEncodedThenWireNamesMatchCanonicalV2Schema(t *testing.T) {
	message := signaling.ClientMessage{
		Type: signaling.TypePublish,
		PublishBuses: []signaling.PublishBusBinding{
			{StreamID: "application", BusID: "application"},
			{StreamID: "microphone", BusID: "microphone"},
		},
	}

	encoded, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("marshal publish message: %v", err)
	}

	var envelope map[string]any
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatalf("unmarshal publish envelope: %v", err)
	}
	bindings, ok := envelope["publish_buses"].([]any)
	if !ok || len(bindings) != 2 {
		t.Fatalf("publish_buses = %#v, want two bindings", envelope["publish_buses"])
	}
	first, ok := bindings[0].(map[string]any)
	if !ok || first["stream_id"] != "application" || first["bus_id"] != "application" {
		t.Fatalf("first binding = %#v", bindings[0])
	}
}

func TestGivenExplicitEmptyMultiBusDeclarationOnWireWhenPlannedThenItIsRejected(t *testing.T) {
	var message signaling.ClientMessage
	if err := json.Unmarshal([]byte(`{"type":"PUBLISH","publish_buses":[]}`), &message); err != nil {
		t.Fatalf("decode explicit empty declaration: %v", err)
	}
	if message.PublishBuses == nil {
		t.Fatal("explicit publish_buses must retain presence after JSON decoding")
	}

	_, err := newPublishBusPlan(message, &auth.Claims{})
	if !errors.Is(err, errPublishBusCount) {
		t.Fatalf("error = %v, want %v", err, errPublishBusCount)
	}
}

func TestGivenLegacyPublishWhenTrackArrivesThenSingleBusIsClaimedOnce(t *testing.T) {
	plan, err := newPublishBusPlan(
		signaling.ClientMessage{Type: signaling.TypePublish, BusID: "application"},
		&auth.Claims{},
	)
	if err != nil {
		t.Fatalf("new publish bus plan: %v", err)
	}

	busID, err := plan.claim("legacy-stream")
	if err != nil {
		t.Fatalf("claim legacy track: %v", err)
	}
	if busID != "application" {
		t.Fatalf("bus ID = %q, want application", busID)
	}
	if _, err := plan.claim("second-stream"); !errors.Is(err, errPublishTrackClaimed) {
		t.Fatalf("second legacy track error = %v, want %v", err, errPublishTrackClaimed)
	}
}

func TestGivenMultiBusPublishWhenTracksArriveThenEachIndependentBusIsClaimed(t *testing.T) {
	plan, err := newPublishBusPlan(
		signaling.ClientMessage{
			Type: signaling.TypePublish,
			PublishBuses: []signaling.PublishBusBinding{
				{StreamID: "application", BusID: "application"},
				{StreamID: "microphone", BusID: "microphone"},
			},
		},
		&auth.Claims{},
	)
	if err != nil {
		t.Fatalf("new publish bus plan: %v", err)
	}

	var wait sync.WaitGroup
	wait.Add(2)
	results := make(chan string, 2)
	for _, streamID := range []string{"application", "microphone"} {
		streamID := streamID
		go func() {
			defer wait.Done()
			busID, claimErr := plan.claim(streamID)
			if claimErr != nil {
				results <- "error:" + claimErr.Error()
				return
			}
			results <- busID
		}()
	}
	wait.Wait()
	close(results)

	seen := make(map[string]bool, 2)
	for result := range results {
		seen[result] = true
	}
	if !seen["application"] || !seen["microphone"] || len(seen) != 2 {
		t.Fatalf("claimed buses = %#v", seen)
	}
}

func TestGivenInvalidMultiBusDeclarationsWhenPlannedThenTheyAreRejected(t *testing.T) {
	tests := []struct {
		name    string
		message signaling.ClientMessage
		claims  auth.Claims
		want    error
	}{
		{
			name: "ambiguous legacy and multi bus fields",
			message: signaling.ClientMessage{
				BusID:        "application",
				PublishBuses: []signaling.PublishBusBinding{{StreamID: "application", BusID: "application"}},
			},
			want: errAmbiguousPublishBus,
		},
		{
			name: "duplicate stream",
			message: signaling.ClientMessage{PublishBuses: []signaling.PublishBusBinding{
				{StreamID: "capture", BusID: "application"},
				{StreamID: "capture", BusID: "microphone"},
			}},
			want: errPublishBusDuplicate,
		},
		{
			name: "duplicate bus",
			message: signaling.ClientMessage{PublishBuses: []signaling.PublishBusBinding{
				{StreamID: "application", BusID: "capture"},
				{StreamID: "microphone", BusID: "capture"},
			}},
			want: errPublishBusDuplicate,
		},
		{
			name: "invalid identity",
			message: signaling.ClientMessage{PublishBuses: []signaling.PublishBusBinding{
				{StreamID: "application audio", BusID: "application"},
			}},
			want: errPublishBusIdentity,
		},
		{
			name: "token bus scope",
			message: signaling.ClientMessage{PublishBuses: []signaling.PublishBusBinding{
				{StreamID: "microphone", BusID: "microphone"},
			}},
			claims: auth.Claims{BusID: "application"},
			want:   errPublishBusScope,
		},
		{
			name:    "legacy token bus scope",
			message: signaling.ClientMessage{BusID: "microphone"},
			claims:  auth.Claims{BusID: "application"},
			want:    errPublishBusScope,
		},
	}

	tooMany := make([]signaling.PublishBusBinding, maxPublishBusBindings+1)
	for index := range tooMany {
		tooMany[index] = signaling.PublishBusBinding{
			StreamID: "stream-" + string(rune('a'+index)),
			BusID:    "bus-" + string(rune('a'+index)),
		}
	}
	tests = append(tests, struct {
		name    string
		message signaling.ClientMessage
		claims  auth.Claims
		want    error
	}{
		name:    "finite binding count",
		message: signaling.ClientMessage{PublishBuses: tooMany},
		want:    errPublishBusCount,
	})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newPublishBusPlan(test.message, &test.claims)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestGivenMultiBusPlanWhenUnknownOrRepeatedTrackArrivesThenClaimFails(t *testing.T) {
	plan, err := newPublishBusPlan(
		signaling.ClientMessage{PublishBuses: []signaling.PublishBusBinding{
			{StreamID: "application", BusID: "application"},
		}},
		&auth.Claims{},
	)
	if err != nil {
		t.Fatalf("new publish bus plan: %v", err)
	}

	if _, err := plan.claim("unknown"); !errors.Is(err, errPublishTrackUnknown) {
		t.Fatalf("unknown track error = %v, want %v", err, errPublishTrackUnknown)
	}
	if _, err := plan.claim("application"); err != nil {
		t.Fatalf("claim application: %v", err)
	}
	if _, err := plan.claim("application"); !errors.Is(err, errPublishTrackClaimed) {
		t.Fatalf("repeated track error = %v, want %v", err, errPublishTrackClaimed)
	}
}
