package session

import (
	"errors"
	"time"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/media/clocklineage"
)

var ErrBusLimitExceeded = errors.New("relay_session: AudioBus limit exceeded")

// SourceSession is the readable side of a live RTP source.
type SourceSession interface {
	ReadRTP() (*rtp.Packet, error)
}

type clockLineageSource interface {
	ClockLineage() *clocklineage.Timeline
}

func (relaySession *RelaySession) GetOrCreateBus(id BusID, role BusRole) *AudioBus {
	relaySession.busesMu.RLock()
	if bus, found := relaySession.buses[id]; found {
		relaySession.busesMu.RUnlock()
		return bus
	}
	relaySession.busesMu.RUnlock()

	relaySession.busesMu.Lock()
	defer relaySession.busesMu.Unlock()
	if bus, found := relaySession.buses[id]; found {
		return bus
	}
	if len(relaySession.buses) >= relaySession.maxBuses {
		return nil
	}
	bus := newAudioBus(
		id,
		role,
		relaySession.ID,
		relaySession.inactivityTimeout,
		relaySession.reconnectWindow,
	)
	relaySession.buses[id] = bus
	return bus
}

func (relaySession *RelaySession) SetSource(
	busID BusID,
	role BusRole,
	sourceSession SourceSession,
	closeSource func(),
) error {
	bus := relaySession.GetOrCreateBus(busID, role)
	if bus == nil {
		return ErrBusLimitExceeded
	}
	errorCounts := make(map[string]int, 8)
	deadSubscriptions := make([]string, 0, 8)

	bus.SetSource(sourceSession, closeSource, func(packet *rtp.Packet, generation uint64) {
		captureTime, captureTimeKnown := bus.CaptureTime(packet.Timestamp, time.Now())
		relaySession.deliverWithSource(
			busID,
			packet,
			captureTime,
			captureTimeKnown,
			SourceIdentity{BusID: string(busID), Generation: generation, SSRC: packet.SSRC},
			errorCounts,
			&deadSubscriptions,
		)
	}, relaySession.notifyStateChange)
	relaySession.notifyStateChange()
	return nil
}

func (relaySession *RelaySession) SourceActive() bool {
	relaySession.busesMu.RLock()
	defer relaySession.busesMu.RUnlock()
	for _, bus := range relaySession.buses {
		if bus.SourceActive() {
			return true
		}
	}
	return false
}

func (relaySession *RelaySession) BusSourceActive(id BusID) bool {
	relaySession.busesMu.RLock()
	bus, found := relaySession.buses[id]
	relaySession.busesMu.RUnlock()
	if !found {
		return false
	}
	return bus.SourceActive()
}

func (relaySession *RelaySession) SourceClockSnapshots() []SourceClockSnapshot {
	relaySession.busesMu.RLock()
	defer relaySession.busesMu.RUnlock()
	snapshots := make([]SourceClockSnapshot, 0, len(relaySession.buses))
	for _, bus := range relaySession.buses {
		snapshots = append(snapshots, bus.SourceClockSnapshot())
	}
	return snapshots
}
