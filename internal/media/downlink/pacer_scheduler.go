package downlink

import (
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
)

func (p *AudioCadencePacer) run() {
	defer close(p.workerDone)
	if !p.paceMedia {
		p.runForward()
		return
	}

	timeline := newCadenceTimeline()
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	var lastSendAtNs int64
	var lastPrimaryWriteStartedAt time.Time
	var pending [defaultPacerCapacity + 1]pacedPacket
	pendingLen := 0
	var forwarder sequenceForwarder

	for {
		if pendingLen == 0 {
			select {
			case <-p.done:
				return
			case seq := <-p.nacks:
				p.retransmit(seq)
				continue
			case pending[0] = <-p.queue:
				pending[0].extSeq = forwarder.unwrap(pending[0].packet.SequenceNumber)
				pendingLen = 1
			}
		}
		for pendingLen < len(pending) {
			select {
			case pending[pendingLen] = <-p.queue:
				pending[pendingLen].extSeq = forwarder.unwrap(pending[pendingLen].packet.SequenceNumber)
				pendingLen++
			default:
				goto drained
			}
		}
	drained:
		sortPacedPacketsByExtendedSequence(pending[:pendingLen])
	initialWait:
		for p.paceMedia && !forwarder.haveReleased && pendingLen < len(pending) {
			wait := time.Until(pending[0].enqueuedAt.Add(defaultPacerInitialWait))
			if wait <= 0 {
				break
			}
			timer.Reset(wait)
			select {
			case <-p.done:
				return
			case seq := <-p.nacks:
				p.retransmit(seq)
				stopTimer(timer)
				continue
			case pending[pendingLen] = <-p.queue:
				pending[pendingLen].extSeq = forwarder.unwrap(pending[pendingLen].packet.SequenceNumber)
				pendingLen++
				stopTimer(timer)
				sortPacedPacketsByExtendedSequence(pending[:pendingLen])
			case <-timer.C:
				break initialWait
			}
		}
	gapWait:
		for forwarder.hasGap(pending[0].extSeq) && pendingLen < len(pending) {
			wait := time.Until(pending[0].enqueuedAt.Add(p.reorderWait))
			if wait <= 0 {
				break
			}
			timer.Reset(wait)
			select {
			case <-p.done:
				return
			case seq := <-p.nacks:
				p.retransmit(seq)
				stopTimer(timer)
				continue
			case pending[pendingLen] = <-p.queue:
				pending[pendingLen].extSeq = forwarder.unwrap(pending[pendingLen].packet.SequenceNumber)
				pendingLen++
				stopTimer(timer)
				sortPacedPacketsByExtendedSequence(pending[:pendingLen])
				continue
			case <-timer.C:
				p.gapTimeoutCount.Add(1)
				break gapWait
			}
		}

		item := pending[0]
		copy(pending[:], pending[1:pendingLen])
		pendingLen--
		if item.packet.Padding {
			p.paddingPacketCount.Add(1)
		}
		if stripSourceMediaPadding(item.packet) {
			p.sourcePaddingStrippedCount.Add(1)
		}
		switch forwarder.release(item.packet, item.extSeq) {
		case dropPadding:
			p.completedCount.Add(1)
			continue
		case dropLate:
			p.latePacketDropCount.Add(1)
			if item.packet.Padding {
				p.latePaddingDropCount.Add(1)
			} else {
				p.lateMediaDropCount.Add(1)
			}
			p.completedCount.Add(1)
			continue
		}
		if time.Since(item.enqueuedAt) > p.maxQueueAge {
			p.staleDrops.Add(1)
			p.completedCount.Add(1)
			timeline.reset()
			continue
		}

		isPrimaryMedia := true
		if p.paceMedia {
			timelineWasInitialized := timeline.initialized
			previousDueAt := timeline.lastDueAt
			dueAt, reset, advances := timeline.dueAt(item.packet.Timestamp, item.enqueuedAt)
			isPrimaryMedia = advances
			if reset {
				p.timelineResets.Add(1)
			}
			if !isPrimaryMedia {
				p.recoveryPacketCount.Add(1)
			}
			if timelineWasInitialized && advances && !reset && !lastPrimaryWriteStartedAt.IsZero() {
				mediaSpacingNs := dueAt.Sub(previousDueAt)
				minimumDueAt := lastPrimaryWriteStartedAt.Add(minimumCadenceSpacingNs(mediaSpacingNs))
				if minimumDueAt.After(dueAt) {
					dueAt = minimumDueAt
				}
			}
			waitStarted := time.Now()
			requestedWait := max(dueAt.Sub(waitStarted), 0)
			observeAtomicMax(&p.maxTimerWaitNs, uint64(requestedWait))
			if !p.waitUntil(timer, dueAt) {
				return
			}
			if oversleep := time.Since(dueAt); oversleep > 0 {
				observeAtomicMax(&p.maxTimerOversleepNs, uint64(oversleep))
			}
		}

		now := time.Now()
		queueAgeNs := now.Sub(item.enqueuedAt).Nanoseconds()
		queueAge := uint64(max(queueAgeNs, 0))
		p.queueAgeHist.observe(queueAgeNs)
		observeAtomicMax(&p.maxQueueAgeNs, queueAge)
		if queueAgeNs > p.maxQueueAge.Nanoseconds() {
			p.staleDrops.Add(1)
			p.completedCount.Add(1)
			timeline.reset()
			continue
		}

		writeStarted := time.Now()
		if p.observeTranslated != nil {
			p.observeTranslated(item.packet, item.captureTime, item.captureTimeKnown)
		}
		writeErr := p.write(item.packet)
		writeDuration := time.Since(writeStarted)
		observeAtomicMax(&p.maxWriterDurationNs, uint64(writeDuration))
		if writeDuration > 100*time.Millisecond {
			p.writerBlockedCount.Add(1)
		}
		if writeErr == nil {
			p.sentCount.Add(1)
			p.retransmissionCache.store(item.packet)
		}
		p.completedCount.Add(1)
		nowNs := time.Now().UnixNano()
		if isPrimaryMedia && lastSendAtNs != 0 && nowNs > lastSendAtNs {
			spacingNs := uint64(nowNs - lastSendAtNs)
			p.lastSpacingNs.Store(spacingNs)
			p.spacingHist.observe(int64(spacingNs))
			observeAtomicMax(&p.maxSpacingNs, spacingNs)
		}
		if isPrimaryMedia {
			lastSendAtNs = nowNs
			lastPrimaryWriteStartedAt = writeStarted
		}
	}
}

func stripSourceMediaPadding(packet *rtp.Packet) bool {
	if !packet.Padding || len(packet.Payload) == 0 {
		return false
	}
	packet.Padding = false
	packet.PaddingSize = 0
	return true
}

func sortPacedPacketsByExtendedSequence(items []pacedPacket) {
	for index := 1; index < len(items); index++ {
		item := items[index]
		position := index
		for position > 0 && item.extSeq < items[position-1].extSeq {
			items[position] = items[position-1]
			position--
		}
		items[position] = item
	}
}

func (p *AudioCadencePacer) waitUntil(timer *time.Timer, dueAt time.Time) bool {
	for {
		wait := time.Until(dueAt)
		if wait <= 0 {
			return true
		}
		timer.Reset(wait)
		select {
		case <-timer.C:
			return true
		case sequence := <-p.nacks:
			p.retransmit(sequence)
			stopTimer(timer)
		case <-p.done:
			stopTimer(timer)
			return false
		}
	}
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func observeAtomicMax(target *atomic.Uint64, value uint64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}
