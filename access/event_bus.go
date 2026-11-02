package access

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
)

// Event carries one complete Session state. Revision becomes the SSE id.
type Event struct {
	Revision uint64
	Payload  string
}
type eventSubscription struct {
	channel  chan Event
	revision uint64
}

// EventBus retains at most one complete state per consumer. Revisions prevent
// an in-flight older observation from replacing a subscriber's newer snapshot.
type EventBus struct {
	mu          sync.Mutex
	subscribers map[string]*eventSubscription
	latest      Event
	closed      bool
	done        chan struct{}
}

func newEventBus() *EventBus {
	return &EventBus{subscribers: make(map[string]*eventSubscription), done: make(chan struct{})}
}
func (bus *EventBus) Subscribe(limit int) (string, <-chan Event, error) {
	return bus.subscribeAfter(limit, 0)
}
func (bus *EventBus) subscribeAfter(limit int, revision uint64) (string, <-chan Event, error) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if bus.closed || len(bus.subscribers) >= limit {
		return "", nil, ErrSSECapacity
	}
	id, err := randomSubscriptionID()
	if err != nil {
		return "", nil, err
	}
	sub := &eventSubscription{channel: make(chan Event, 1), revision: revision}
	if bus.latest.Revision > revision {
		sub.channel <- bus.latest
		sub.revision = bus.latest.Revision
	}
	bus.subscribers[id] = sub
	return id, sub.channel, nil
}
func (bus *EventBus) Unsubscribe(id string) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if sub, found := bus.subscribers[id]; found {
		delete(bus.subscribers, id)
		close(sub.channel)
	}
}
func (bus *EventBus) HasSubscriber(id string) bool {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	_, found := bus.subscribers[id]
	return found
}
func (bus *EventBus) subscriberCount() int {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return len(bus.subscribers)
}
func (bus *EventBus) Publish(event Event) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if bus.closed || event.Revision <= bus.latest.Revision {
		return
	}
	bus.latest = event
	for _, sub := range bus.subscribers {
		if event.Revision <= sub.revision {
			continue
		}
		sub.revision = event.Revision
		select {
		case sub.channel <- event:
		default:
			select {
			case <-sub.channel:
			default:
			}
			sub.channel <- event
		}
	}
}
func (bus *EventBus) Close() {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if bus.closed {
		return
	}
	bus.closed = true
	close(bus.done)
	for id, sub := range bus.subscribers {
		delete(bus.subscribers, id)
		close(sub.channel)
	}
}
func randomSubscriptionID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}
