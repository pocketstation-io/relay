package access

import (
	"errors"
	"github.com/pocketstation-io/relay/access/storage"
	"sort"
)

const RelayStateContractVersion = 1

const (
	maxBusCount          = 16
	maxBusIdentifierSize = 64
	maxRelayEpochSize    = 128
	maxSubscriberIDSize  = 128
)

var ErrInvalidRelayState = errors.New("invalid RelaySession state snapshot")

type BusState = storage.BusState
type SubscriptionState = storage.SubscriptionState
type RelayStateSnapshot = storage.RelayState

// State is the complete public control-plane snapshot. StateRevision is the
// SSE event ID and increases for every accepted control-plane transition.
type State struct {
	SessionID         string              `json:"session_id"`
	StateRevision     uint64              `json:"state_revision"`
	RelayEpoch        string              `json:"relay_epoch,omitempty"`
	RelayRevision     uint64              `json:"relay_revision"`
	RequiredBuses     []string            `json:"required_buses"`
	Buses             []BusState          `json:"buses"`
	Subscriptions     []SubscriptionState `json:"subscriptions"`
	Ready             bool                `json:"ready"`
	SubscriptionCount int                 `json:"subscription_count"`
	Codec             string              `json:"codec"`
}

func validateRequiredBuses(values []string) ([]string, error) {
	if len(values) == 0 || len(values) > maxBusCount {
		return nil, ErrInvalidRelayState
	}
	result := append([]string(nil), values...)
	sort.Strings(result)
	for index, value := range result {
		if !validIdentifier(value, maxBusIdentifierSize) || (index > 0 && value == result[index-1]) {
			return nil, ErrInvalidRelayState
		}
	}
	return result, nil
}

func validateRelaySnapshot(snapshot RelayStateSnapshot) error {
	if snapshot.ContractVersion != RelayStateContractVersion ||
		snapshot.SessionID == "" ||
		!validIdentifier(snapshot.RelayEpoch, maxRelayEpochSize) ||
		snapshot.Revision == 0 ||
		snapshot.ObservedAt.IsZero() ||
		len(snapshot.Buses) > maxBusCount ||
		len(snapshot.Subscriptions) > 1024 {
		return ErrInvalidRelayState
	}
	busIDs := make(map[string]struct{}, len(snapshot.Buses))
	for _, bus := range snapshot.Buses {
		if !validIdentifier(bus.BusID, maxBusIdentifierSize) ||
			!validIdentifier(bus.Role, maxBusIdentifierSize) {
			return ErrInvalidRelayState
		}
		if _, found := busIDs[bus.BusID]; found {
			return ErrInvalidRelayState
		}
		busIDs[bus.BusID] = struct{}{}
	}
	subscriberIDs := make(map[string]struct{}, len(snapshot.Subscriptions))
	for _, subscription := range snapshot.Subscriptions {
		if !validIdentifier(subscription.SubscriberID, maxSubscriberIDSize) ||
			!validIdentifier(subscription.BusID, maxBusIdentifierSize) {
			return ErrInvalidRelayState
		}
		if subscription.BusID != "mix" {
			if _, found := busIDs[subscription.BusID]; !found {
				return ErrInvalidRelayState
			}
		}
		if _, found := subscriberIDs[subscription.SubscriberID]; found {
			return ErrInvalidRelayState
		}
		subscriberIDs[subscription.SubscriberID] = struct{}{}
	}
	return nil
}

func normalizeRelaySnapshot(snapshot RelayStateSnapshot) RelayStateSnapshot {
	snapshot.Buses = append([]BusState(nil), snapshot.Buses...)
	snapshot.Subscriptions = append([]SubscriptionState(nil), snapshot.Subscriptions...)
	sort.Slice(snapshot.Buses, func(i, j int) bool { return snapshot.Buses[i].BusID < snapshot.Buses[j].BusID })
	sort.Slice(snapshot.Subscriptions, func(i, j int) bool {
		return snapshot.Subscriptions[i].SubscriberID < snapshot.Subscriptions[j].SubscriberID
	})
	snapshot.ObservedAt = snapshot.ObservedAt.UTC()
	return snapshot
}

func validIdentifier(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
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
