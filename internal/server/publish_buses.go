package server

import (
	"errors"
	"fmt"
	"sync"

	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/session"
	"github.com/pocketstation-io/relay/internal/signaling"
)

const (
	maxPublishBusBindings = 16
	maxPublishIdentifier  = 64
)

var (
	errAmbiguousPublishBus = errors.New("PUBLISH cannot combine bus_id with publish_buses")
	errPublishBusCount     = errors.New("publish_buses must contain between 1 and 16 bindings")
	errPublishBusIdentity  = errors.New("publish_buses contains an invalid stream_id or bus_id")
	errPublishBusDuplicate = errors.New("publish_buses stream_id and bus_id values must be unique")
	errPublishBusScope     = errors.New("publish_buses exceeds the source token bus scope")
	errPublishTrackUnknown = errors.New("publisher track is not declared in publish_buses")
	errPublishTrackClaimed = errors.New("publisher track has already attached to an AudioBus")
)

type publishBusPlan struct {
	mu          sync.Mutex
	legacyBusID session.BusID
	byStreamID  map[string]session.BusID
	claimed     map[string]struct{}
}

func newPublishBusPlan(
	message signaling.ClientMessage,
	claims *auth.Claims,
) (*publishBusPlan, error) {
	if message.PublishBuses == nil {
		busID := message.BusID
		if busID == "" && len(claims.BusIDs) == 1 {
			busID = claims.BusIDs[0]
		}
		if busID == "" || !claims.AllowsBus(string(busID)) {
			return nil, errPublishBusScope
		}
		return &publishBusPlan{legacyBusID: busID}, nil
	}
	if message.BusID != "" {
		return nil, errAmbiguousPublishBus
	}
	if len(message.PublishBuses) == 0 || len(message.PublishBuses) > maxPublishBusBindings {
		return nil, errPublishBusCount
	}

	byStreamID := make(map[string]session.BusID, len(message.PublishBuses))
	busIDs := make(map[session.BusID]struct{}, len(message.PublishBuses))
	for _, binding := range message.PublishBuses {
		if !validPublishIdentifier(binding.StreamID) || !validPublishIdentifier(binding.BusID) {
			return nil, errPublishBusIdentity
		}
		if !claims.AllowsBus(string(binding.BusID)) {
			return nil, errPublishBusScope
		}
		if _, exists := byStreamID[binding.StreamID]; exists {
			return nil, errPublishBusDuplicate
		}
		if _, exists := busIDs[binding.BusID]; exists {
			return nil, errPublishBusDuplicate
		}
		byStreamID[binding.StreamID] = binding.BusID
		busIDs[binding.BusID] = struct{}{}
	}

	return &publishBusPlan{
		byStreamID: byStreamID,
		claimed:    make(map[string]struct{}, len(byStreamID)),
	}, nil
}

func (plan *publishBusPlan) primaryBusID() session.BusID {
	if plan.legacyBusID != "" {
		return plan.legacyBusID
	}
	return ""
}

func (plan *publishBusPlan) claim(streamID string) (session.BusID, error) {
	plan.mu.Lock()
	defer plan.mu.Unlock()

	if plan.legacyBusID != "" {
		if len(plan.claimed) != 0 {
			return "", errPublishTrackClaimed
		}
		if plan.claimed == nil {
			plan.claimed = make(map[string]struct{}, 1)
		}
		plan.claimed[streamID] = struct{}{}
		return plan.legacyBusID, nil
	}

	busID, exists := plan.byStreamID[streamID]
	if !exists {
		return "", fmt.Errorf("%w: %q", errPublishTrackUnknown, streamID)
	}
	if _, exists := plan.claimed[streamID]; exists {
		return "", fmt.Errorf("%w: %q", errPublishTrackClaimed, streamID)
	}
	plan.claimed[streamID] = struct{}{}
	return busID, nil
}

func validPublishIdentifier(value string) bool {
	if len(value) == 0 || len(value) > maxPublishIdentifier {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}
