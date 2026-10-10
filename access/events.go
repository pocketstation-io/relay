package access

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/pocketstation-io/relay/access/storage"
)

const eventPollInterval = 250 * time.Millisecond

// SubscribeEventsContext returns a complete snapshot and bounded updates. The
// caller must unsubscribe when it stops reading; HTTP handlers defer this.
func (s *Service) SubscribeEventsContext(ctx context.Context, id string) (State, *EventBus, string, <-chan Event, error) {
	initial, err := s.GetContext(ctx, id)
	if err != nil {
		return State{}, nil, "", nil, err
	}
	s.mu.Lock()
	bus := s.sessionBuses[id]
	created := bus == nil
	if created {
		bus = newEventBus()
		s.sessionBuses[id] = bus
	}
	subscriptionID, events, err := bus.subscribeAfter(s.config.MaxSSEPerSession, initial.State().StateRevision)
	if err != nil && created {
		delete(s.sessionBuses, id)
		bus.Close()
	}
	s.mu.Unlock()
	if err != nil {
		return State{}, nil, "", nil, err
	}
	if created {
		go s.pollEvents(id, bus)
	}
	return initial.State(), bus, subscriptionID, events, nil
}

// Exactly one poller observes each locally subscribed Session, independent of
// which consumer happened to connect first. No lock spans a storage operation.
func (s *Service) pollEvents(id string, bus *EventBus) {
	ticker := time.NewTicker(eventPollInterval)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		if s.sessionBuses[id] != bus {
			s.mu.Unlock()
			return
		}
		if bus.subscriberCount() == 0 {
			delete(s.sessionBuses, id)
			bus.Close()
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		query, cancel := context.WithTimeout(context.Background(), time.Second)
		current, err := s.GetContext(query, id)
		cancel()
		if err != nil {
			s.closeEventBus(id, bus)
			return
		}
		state := current.State()
		payload, err := json.Marshal(state)
		if err != nil {
			s.closeEventBus(id, bus)
			return
		}
		bus.Publish(Event{Revision: state.StateRevision, Payload: string(payload)})
		select {
		case <-bus.done:
			return
		case <-ticker.C:
		}
	}
}
func (s *Service) closeEventBus(id string, expected *EventBus) {
	s.mu.Lock()
	if s.sessionBuses[id] == expected {
		delete(s.sessionBuses, id)
		expected.Close()
	}
	s.mu.Unlock()
}
func (s *Service) closeLocalEventBus(id string) {
	s.mu.Lock()
	bus := s.sessionBuses[id]
	delete(s.sessionBuses, id)
	if bus != nil {
		bus.Close()
	}
	s.mu.Unlock()
}

// StartMaintenance renews the configured writer lease and prunes expired
// records once per second. Cancellation stops it and closes local SSE mailboxes.
func (s *Service) StartMaintenance(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		defer func() {
			s.mu.Lock()
			for id, bus := range s.sessionBuses {
				bus.Close()
				delete(s.sessionBuses, id)
			}
			s.mu.Unlock()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				query, cancel := context.WithTimeout(ctx, time.Second)
				err := s.update(query, func(_ storage.Tx) error { return nil })
				cancel()
				if errors.Is(err, ErrWriterFenced) {
					return
				}
			}
		}
	}()
}
