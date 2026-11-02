package access

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pocketstation-io/relay/access/storage"
	"github.com/pocketstation-io/relay/access/storage/memory"
)

func TestGivenEventFanoutWhenConsumerIsSlowThenLatestRevisionWinsWithoutRegression(t *testing.T) {
	bus := newEventBus()
	defer bus.Close()
	id, events, err := bus.subscribeAfter(2, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Unsubscribe(id)
	bus.Publish(Event{Revision: 4})
	bus.Publish(Event{Revision: 6})
	bus.Publish(Event{Revision: 7})
	bus.Publish(Event{Revision: 6})
	if event := <-events; event.Revision != 7 {
		t.Fatalf("latest=%d", event.Revision)
	}
	bus.Publish(Event{Revision: 7})
	select {
	case e := <-events:
		t.Fatalf("duplicate revision %d", e.Revision)
	default:
	}
	_, late, err := bus.subscribeAfter(2, 6)
	if err != nil {
		t.Fatal(err)
	}
	if event := <-late; event.Revision != 7 {
		t.Fatalf("late subscriber missed latest state: %d", event.Revision)
	}
}
func TestGivenConcurrentEventConsumersWhenPublishingAndLeavingThenCloseIsSafe(t *testing.T) {
	bus := newEventBus()
	var workers sync.WaitGroup
	var ready sync.WaitGroup
	ready.Add(16)
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			id, events, err := bus.Subscribe(16)
			ready.Done()
			if err != nil {
				return
			}
			defer bus.Unsubscribe(id)
			for range events {
			}
		}()
	}
	ready.Wait()
	for revision := uint64(1); revision < 100; revision++ {
		bus.Publish(Event{Revision: revision})
	}
	bus.Close()
	bus.Close()
	workers.Wait()
	if _, _, err := bus.Subscribe(16); err == nil {
		t.Fatal("closed bus accepted consumer")
	}
}
func TestGivenSharedSessionSubscriptionsWhenFirstLeavesThenRemainingConsumerReceivesAndIdleBusIsRemoved(t *testing.T) {
	backend := memory.New()
	defer backend.Close()
	service, err := NewService([]byte("12345678901234567890123456789012"), backend, Config{})
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.Create("application")
	if err != nil {
		t.Fatal(err)
	}
	_, bus, first, _, err := service.SubscribeEventsContext(context.Background(), session.ID())
	if err != nil {
		t.Fatal(err)
	}
	_, same, second, events, err := service.SubscribeEventsContext(context.Background(), session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if same != bus {
		t.Fatal("multiple pollers for one Session")
	}
	bus.Unsubscribe(first)
	err = backend.Update(context.Background(), func(tx storage.Tx) error {
		r, _, err := tx.Session(session.ID())
		if err != nil {
			return err
		}
		r.StateRevision++
		r.Codec = "changed"
		return tx.PutSession(r)
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case event, ok := <-events:
		var state State
		if !ok || json.Unmarshal([]byte(event.Payload), &state) != nil || state.Codec != "changed" {
			t.Fatalf("missing committed update: %+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("remaining consumer lost poller")
	}
	bus.Unsubscribe(second)
	select {
	case <-bus.done:
	case <-time.After(2 * time.Second):
		t.Fatal("idle bus retained")
	}
	service.mu.Lock()
	count := len(service.sessionBuses)
	service.mu.Unlock()
	if count != 0 {
		t.Fatalf("retained %d historical buses", count)
	}
}

func TestGivenPartiallyReadySessionWhenObservationExpiresThenEventRevisionAdvancesAndPresenceClears(t *testing.T) {
	var elapsed atomic.Int64
	base := time.Now().UTC()
	backend := memory.NewWithClock(func() time.Time { return base.Add(time.Duration(elapsed.Load())) })
	defer backend.Close()
	service, err := NewService([]byte("12345678901234567890123456789012"), backend, Config{ObservationTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.Create("application", "microphone")
	if err != nil {
		t.Fatal(err)
	}
	err = backend.Update(context.Background(), func(tx storage.Tx) error {
		r, _, err := tx.Session(session.ID())
		if err != nil {
			return err
		}
		r.StateRevision++
		r.Relay = storage.RelayState{ObservedAt: base, Buses: []storage.BusState{{BusID: "application", SourceActive: true}}, Subscriptions: []storage.SubscriptionState{{SubscriberID: "one", BusID: "application"}}}
		return tx.PutSession(r)
	})
	if err != nil {
		t.Fatal(err)
	}
	initial, bus, id, events, err := service.SubscribeEventsContext(context.Background(), session.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Unsubscribe(id)
	if initial.Ready || initial.SubscriptionCount != 1 {
		t.Fatalf("invalid partial fixture: %+v", initial)
	}
	elapsed.Store(int64(2 * time.Second))
	select {
	case event, ok := <-events:
		var state State
		if !ok || json.Unmarshal([]byte(event.Payload), &state) != nil || state.StateRevision <= initial.StateRevision || state.SubscriptionCount != 0 || len(state.Buses) != 0 {
			t.Fatalf("stale presence or revision: %+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expiry did not emit a revisioned observation")
	}
}
