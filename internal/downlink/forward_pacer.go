package downlink

import "time"

const defaultForwardReorderWaitMs = 40 * time.Millisecond

type forwardOutputState struct {
	haveOutput                 bool
	lastOutputSequence         uint16
	lastOutputTimestampSamples uint32
	lastSendAtNs               int64
}

// runForward serializes subscriber writes without imposing a relay cadence.
// A bounded gap-only hold restores short ingress reordering while ordered
// packets still pass immediately. Real gaps and late repairs retain their RTP
// sequence numbers so downstream NACK can distinguish loss from continuity.
func (p *AudioCadencePacer) runForward() {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	var translator rtpForwardTranslator
	var unwrapper sequenceUnwrapper
	var output forwardOutputState
	var pending [defaultPacerCapacity + 1]pacedPacket
	pendingLen := 0
	var haveReleased bool
	var lastReleasedExtSeq int64
	defer func() { p.completedCount.Add(uint64(pendingLen)) }()

	for {
		if pendingLen == 0 {
			select {
			case <-p.done:
				return
			case seq := <-p.nacks:
				p.retransmit(seq)
				continue
			case pending[0] = <-p.queue:
				p.prepareForwardItem(&pending[0], &translator, &unwrapper)
				pendingLen = 1
			}
		}

		for pendingLen < len(pending) {
			select {
			case pending[pendingLen] = <-p.queue:
				p.prepareForwardItem(&pending[pendingLen], &translator, &unwrapper)
				pendingLen++
			default:
				goto drained
			}
		}
	drained:
		sortPacedPacketsByExtendedSequence(pending[:pendingLen])

	gapWait:
		for haveReleased && pending[0].extSeq > lastReleasedExtSeq+1 && pendingLen < len(pending) {
			gapStartedAt := pending[0].enqueuedAt
			for i := 1; i < pendingLen; i++ {
				if pending[i].enqueuedAt.Before(gapStartedAt) {
					gapStartedAt = pending[i].enqueuedAt
				}
			}
			deadline := gapStartedAt.Add(defaultForwardReorderWaitMs)
			wait := time.Until(deadline)
			if wait <= 0 {
				p.gapTimeoutCount.Add(1)
				break
			}
			observeAtomicMax(&p.maxTimerWaitNs, uint64(wait))
			timer.Reset(wait)
			select {
			case <-p.done:
				return
			case seq := <-p.nacks:
				p.retransmit(seq)
				stopAndDrainTimer(timer)
			case pending[pendingLen] = <-p.queue:
				p.prepareForwardItem(&pending[pendingLen], &translator, &unwrapper)
				pendingLen++
				stopAndDrainTimer(timer)
				sortPacedPacketsByExtendedSequence(pending[:pendingLen])
			case <-timer.C:
				p.gapTimeoutCount.Add(1)
				if oversleep := time.Since(deadline); oversleep > 0 {
					observeAtomicMax(&p.maxTimerOversleepNs, uint64(oversleep))
				}
				break gapWait
			}
		}

		item := pending[0]
		copy(pending[:], pending[1:pendingLen])
		pendingLen--
		if !haveReleased || item.extSeq > lastReleasedExtSeq {
			haveReleased = true
			lastReleasedExtSeq = item.extSeq
		}
		p.writeForwardItem(item, &output)
	}
}

func (p *AudioCadencePacer) prepareForwardItem(
	item *pacedPacket,
	translator *rtpForwardTranslator,
	unwrapper *sequenceUnwrapper,
) {
	if translator.translateAt(item.packet, item.source, item.enqueuedAt) {
		p.timelineResets.Add(1)
	}
	item.extSeq = unwrapper.unwrap(item.packet.SequenceNumber)
}

func (p *AudioCadencePacer) writeForwardItem(item pacedPacket, output *forwardOutputState) {
	queueAgeNs := time.Since(item.enqueuedAt).Nanoseconds()
	queueAge := uint64(max(queueAgeNs, 0))
	p.queueAgeHist.observe(queueAgeNs)
	observeAtomicMax(&p.maxQueueAgeNs, queueAge)
	if queueAgeNs > p.maxQueueAge.Nanoseconds() {
		p.staleDrops.Add(1)
		p.completedCount.Add(1)
		return
	}

	if item.packet.Padding {
		p.paddingPacketCount.Add(1)
	}
	if stripSourceMediaPadding(item.packet) {
		p.sourcePaddingStrippedCount.Add(1)
	}
	if p.observeTranslated != nil {
		p.observeTranslated(item.packet, item.captureTime, item.captureTimeKnown)
	}
	writeStarted := time.Now()
	writeErr := p.write(item.packet)
	writeDuration := time.Since(writeStarted)
	observeAtomicMax(&p.maxWriterDurationNs, uint64(writeDuration))
	if writeDuration > 100*time.Millisecond {
		p.writerBlockedCount.Add(1)
	}
	if writeErr == nil {
		p.sentCount.Add(1)
		p.retransmissionCache.store(item.packet)
		if output.haveOutput {
			sequenceDeltaCount := uint16(item.packet.SequenceNumber - output.lastOutputSequence)
			timestampDeltaSamples := uint32(item.packet.Timestamp - output.lastOutputTimestampSamples)
			if sequenceDeltaCount != 1 {
				p.outputSequenceDiscontinuityCount.Add(1)
			}
			if timestampDeltaSamples != defaultAudioFrameSamples {
				p.outputTimestampDiscontinuityCount.Add(1)
			}
			observeAtomicMax(&p.outputMaxSequenceDelta, uint64(sequenceDeltaCount))
			observeAtomicMax(&p.outputMaxTimestampDeltaSamples, uint64(timestampDeltaSamples))
		}
		output.haveOutput = true
		output.lastOutputSequence = item.packet.SequenceNumber
		output.lastOutputTimestampSamples = item.packet.Timestamp
	}
	p.completedCount.Add(1)

	if item.packet.Padding {
		return
	}
	nowNs := time.Now().UnixNano()
	if output.lastSendAtNs != 0 && nowNs > output.lastSendAtNs {
		spacingNs := uint64(nowNs - output.lastSendAtNs)
		p.lastSpacingNs.Store(spacingNs)
		p.spacingHist.observe(int64(spacingNs))
		observeAtomicMax(&p.maxSpacingNs, spacingNs)
	}
	output.lastSendAtNs = nowNs
}

func stopAndDrainTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
