package session

import (
	"sort"
	"sync/atomic"
	"time"
)

const RelayStateContractVersion = 1

// ControlBusState is the control-plane view of one named AudioBus.
type ControlBusState struct {
	BusID            string `json:"bus_id"`
	Role             string `json:"role"`
	SourceActive     bool   `json:"source_active"`
	SourceGeneration uint64 `json:"source_generation"`
}

// ControlSubscriptionState identifies one attached receiver and selected bus.
type ControlSubscriptionState struct {
	SubscriberID string `json:"subscriber_id"`
	BusID        string `json:"bus_id"`
}

// ControlState is the complete idempotent RelaySession state sent to the
// control plane. Revision is monotonic within RelayEpoch.
type ControlState struct {
	ContractVersion int                        `json:"contract_version"`
	SessionID       string                     `json:"session_id"`
	RelayEpoch      string                     `json:"relay_epoch"`
	Revision        uint64                     `json:"revision"`
	ObservedAt      time.Time                  `json:"observed_at"`
	Buses           []ControlBusState          `json:"buses"`
	Subscriptions   []ControlSubscriptionState `json:"subscriptions"`
}

type controlStateOwner struct {
	revision atomic.Uint64
}

// ControlState returns a deterministic full snapshot. Revision changes only
// when Relay-owned attachment state changes, so periodic reconciliation can
// resend the same idempotent snapshot after a lost callback.
func (relaySession *RelaySession) ControlState(relayEpoch string) ControlState {
	for {
		revision := relaySession.controlState.revision.Load()
		buses, subscriptions := relaySession.controlStatePayload()
		if revision == relaySession.controlState.revision.Load() {
			return ControlState{
				ContractVersion: RelayStateContractVersion,
				SessionID:       relaySession.ID,
				RelayEpoch:      relayEpoch,
				Revision:        revision,
				ObservedAt:      time.Now().UTC(),
				Buses:           buses,
				Subscriptions:   subscriptions,
			}
		}
	}
}

func (relaySession *RelaySession) controlStatePayload() ([]ControlBusState, []ControlSubscriptionState) {
	relaySession.busesMu.RLock()
	buses := make([]ControlBusState, 0, len(relaySession.buses))
	for _, bus := range relaySession.buses {
		buses = append(buses, ControlBusState{
			BusID:            string(bus.ID),
			Role:             bus.Role.String(),
			SourceActive:     bus.SourceActive(),
			SourceGeneration: bus.SourceGeneration(),
		})
	}
	relaySession.busesMu.RUnlock()
	sort.Slice(buses, func(i, j int) bool { return buses[i].BusID < buses[j].BusID })

	entries := *relaySession.subscriptions.Load()
	subscriptions := make([]ControlSubscriptionState, 0, len(entries))
	for _, entry := range entries {
		subscriptions = append(subscriptions, ControlSubscriptionState{
			SubscriberID: entry.subscriberID,
			BusID:        string(entry.busID),
		})
	}
	sort.Slice(subscriptions, func(i, j int) bool {
		return subscriptions[i].SubscriberID < subscriptions[j].SubscriberID
	})
	return buses, subscriptions
}
