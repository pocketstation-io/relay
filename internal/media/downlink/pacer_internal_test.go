package downlink

import (
	"encoding/binary"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/session"
)

func assertOwnedPacketsReleased(t *testing.T, pacer *AudioCadencePacer) {
	t.Helper()
	for slotIndex := range pacer.ownedPackets {
		if pacer.ownedPackets[slotIndex].claimed.Load() {
			t.Errorf("owned slot %d remains claimed after joined stop", slotIndex)
		}
	}
	if pacer.enqueuedCount.Load() != pacer.completedCount.Load() {
		t.Errorf("enqueued/completed = %d/%d after stop", pacer.enqueuedCount.Load(), pacer.completedCount.Load())
	}
}

func TestGivenBoundedOwnershipStorageWhenMeasuredThenFiftySubscribersStayWithinBudget(t *testing.T) {
	slotBytes := reflect.TypeFor[ownedPacerPacket]().Size()
	scratchBytes := reflect.TypeFor[pacerWritePacket]().Size()
	cacheOwnerBytes := uintptr(retransmissionCachePackets) * reflect.TypeFor[*ownedPacerPacket]().Size()
	queuePendingOwnerBytes := uintptr(2*defaultPacerCapacity+1) * reflect.TypeFor[*ownedPacerPacket]().Size()
	// Include the added admission/cursor/slice state; round up its padding.
	ownershipBytes := uintptr(ownedPacerPacketCount)*slotBytes + scratchBytes + cacheOwnerBytes + queuePendingOwnerBytes + 64
	if ownershipBytes > 2*1024*1024 {
		t.Fatalf("ownership storage %d bytes exceeds2MiB per subscriber", ownershipBytes)
	}
	t.Logf("slot_count=%d slot_bytes=%d scratch_bytes=%d ownership_bytes=%d fifty_subscribers_bytes=%d", ownedPacerPacketCount, slotBytes, scratchBytes, ownershipBytes, 50*ownershipBytes)
}

func TestGivenOwnedHeadersAndBorrowedPayloadWhenEnqueuedThenNoHeapAllocationIsAdded(t *testing.T) {
	pacer := &AudioCadencePacer{
		queue:        make(chan pacedPacket, defaultPacerCapacity),
		ownedPackets: make([]ownedPacerPacket, ownedPacerPacketCount),
	}
	packet := &rtp.Packet{Header: rtp.Header{Version: 2, CSRC: []uint32{7}}, Payload: []byte{0xf8, 1, 2}}
	if err := packet.SetExtension(8, []byte{3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	allocationsCount := testing.AllocsPerRun(1000, func() {
		if err := pacer.Enqueue(packet); err != nil {
			panic(err)
		}
		item := <-pacer.queue
		item.release()
		pacer.completedCount.Add(1)
	})
	if allocationsCount != 0 {
		t.Fatalf("owned enqueue allocations = %g, want0", allocationsCount)
	}
	assertOwnedPacketsReleased(t, pacer)
	t.Logf("owned_enqueue_allocations_per_packet=%g", allocationsCount)
}

func TestGivenCanonicalPacketWhenMutablePrimaryAndNackScratchWritesThenNoHeapOrAliasingOccurs(t *testing.T) {
	packet := &rtp.Packet{Header: rtp.Header{Version: 2, CSRC: []uint32{7}}, Payload: []byte{0xf8, 1, 2}}
	if err := packet.SetExtension(8, []byte{3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	pacer := &AudioCadencePacer{write: func(scratch *rtp.Packet) error {
		scratch.CSRC[0] = 0
		scratch.GetExtension(8)[0] = 0
		scratch.Payload[0] = 0
		return nil
	}}
	allocationsCount := testing.AllocsPerRun(1000, func() {
		if err := pacer.writeOwnedPacket(packet); err != nil {
			panic(err)
		}
	})
	if allocationsCount != 0 || packet.CSRC[0] != 7 || packet.GetExtension(8)[0] != 3 || packet.Payload[0] != 0xf8 {
		t.Fatalf("scratch allocations=%g or canonical packet mutated", allocationsCount)
	}
	t.Logf("deep_writer_scratch_allocations_per_packet=%g", allocationsCount)
}

func TestGivenInvalidOrOversizedInputWhenEnqueuedThenBoundsErrorClaimsNoSlot(t *testing.T) {
	pacer := newAudioForwardPacer(func(*rtp.Packet) error { return nil })
	defer pacer.Stop()
	packets := []*rtp.Packet{nil, {Header: rtp.Header{CSRC: make([]uint32, 16)}},
		{Header: rtp.Header{Extension: true, ExtensionProfile: 0x1234}},
		{Payload: make([]byte, maxPacerPacketBytes)},
		{Header: rtp.Header{Extensions: make([]rtp.Extension, maxPacerHeaderExtensionsCount+1)}},
	}
	for _, packet := range packets {
		if err := pacer.Enqueue(packet); !errors.Is(err, errPacerPacketBounds) {
			t.Fatalf("invalid input error=%v, want bounded rejection", err)
		}
		if pacer.writeScratch.copy(packet) {
			t.Fatal("writer scratch accepted malformed or oversized input")
		}
	}
	if pacer.Snapshot().QueueFullDrops != 0 || pacer.enqueuedCount.Load() != 0 {
		t.Fatal("bounds failure was mislabeled as queue pressure or admitted")
	}
	pacer.Stop()
	assertOwnedPacketsReleased(t, pacer)
}

func TestGivenEmptyMediaWithHeaderExtensionsWhenCopiedThenNoSentinelIsEmitted(t *testing.T) {
	for _, extensionProfile := range []uint16{0xbede, 0x1000} {
		packet := &rtp.Packet{Header: rtp.Header{Version: 2, ExtensionProfile: extensionProfile}}
		if err := packet.SetExtension(8, []byte{3, 4, 5}); err != nil {
			t.Fatal(err)
		}
		packet.ExtensionProfile = extensionProfile
		pacer := &AudioCadencePacer{write: func(copied *rtp.Packet) error {
			if len(copied.Payload) != 0 || copied.MarshalSize() != packet.MarshalSize() || string(copied.GetExtension(8)) != string(packet.GetExtension(8)) {
				t.Error("empty packet gained media or lost its header extension")
			}
			return nil
		}}
		if err := pacer.writeOwnedPacket(packet); err != nil {
			t.Fatalf("empty packet with profile%04x: %v", extensionProfile, err)
		}
	}
}

func TestGivenRetainedCacheWhenEvictedAndStoppedThenEachPayloadSlotIsReleased(t *testing.T) {
	writes := make(chan uint16, 1)
	pacer := newAudioForwardPacer(func(packet *rtp.Packet) error { writes <- packet.SequenceNumber; return nil })
	defer pacer.Stop()
	for sequenceCount := 0; sequenceCount <= retransmissionCachePackets; sequenceCount++ {
		packet := &rtp.Packet{Header: rtp.Header{Version: 2, SequenceNumber: uint16(sequenceCount)}, Payload: []byte{0xf8}}
		if err := pacer.Enqueue(packet); err != nil {
			t.Fatal(err)
		}
		receiveWrite(t, writes)
	}
	pacer.HandleNACK(retransmissionCachePackets)
	receiveWrite(t, writes) // Worker passed the final canonical cache insertion.
	claimedCount := 0
	for slotIndex := range pacer.ownedPackets {
		if pacer.ownedPackets[slotIndex].claimed.Load() {
			claimedCount++
		}
	}
	if claimedCount != retransmissionCachePackets {
		t.Fatalf("cache retains %d owned slots, want512 after eviction", claimedCount)
	}
	pacer.HandleNACK(0)
	waitForPacerSnapshot(t, pacer, func(snapshot PacerSnapshot) bool { return snapshot.NackCacheMisses == 1 })
	pacer.Stop()
	assertOwnedPacketsReleased(t, pacer)
}

func TestGivenManyTwoByteExtensionsWhenCopiedThenParserCapacityRemainsBoundedAndAllocationFree(t *testing.T) {
	const extensionCount = 300
	wire := make([]byte, 16+2*extensionCount+1)
	wire[0] = 0x90 // Version2, extension flag, no CSRCs.
	binary.BigEndian.PutUint16(wire[12:14], 0x1000)
	binary.BigEndian.PutUint16(wire[14:16], extensionCount/2)
	for extensionIndex := 0; extensionIndex < extensionCount; extensionIndex++ {
		wire[16+2*extensionIndex] = 8 // Repeated ID with a zero-length value.
	}
	wire[len(wire)-1] = 0xf8
	packet := &rtp.Packet{}
	if err := packet.Unmarshal(wire); err != nil || len(packet.Extensions) != extensionCount {
		t.Fatalf("Pion fixture parse=%v extension_count=%d", err, len(packet.Extensions))
	}
	pacer := &AudioCadencePacer{write: func(copied *rtp.Packet) error {
		if len(copied.Extensions) != extensionCount || len(copied.Payload) != 1 || copied.Payload[0] != 0xf8 {
			t.Error("bounded header copy truncated parsed extension entries")
		}
		return nil
	}}
	allocationsCount := testing.AllocsPerRun(1000, func() {
		if err := pacer.writeOwnedPacket(packet); err != nil {
			panic(err)
		}
	})
	if allocationsCount != 0 {
		t.Fatalf("many-extension scratch allocates%g times", allocationsCount)
	}
}

func TestGivenStaleOrFailedWritesWhenWorkerCompletesThenPayloadSlotsAreReleased(t *testing.T) {
	for _, test := range []struct {
		name  string
		stale bool
	}{
		{name: "stale", stale: true}, {name: "writer-error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			pacer := newAudioForwardPacer(func(*rtp.Packet) error { return errPacerPacketBounds })
			defer pacer.Stop()
			if test.stale {
				pacer.maxQueueAge = -time.Nanosecond
			}
			if err := pacer.Enqueue(&rtp.Packet{Payload: []byte{0xf8}}); err != nil {
				t.Fatal(err)
			}
			waitForPacerSnapshot(t, pacer, func(snapshot PacerSnapshot) bool {
				return snapshot.EnqueuedCount == 1 && snapshot.QueueDepth == 0
			})
			if test.stale && pacer.Snapshot().StaleDrops != 1 {
				t.Fatal("stale worker path was not exercised")
			}
			pacer.Stop()
			assertOwnedPacketsReleased(t, pacer)
		})
	}
}

func TestGivenLateCadencePacketWhenDroppedThenOwnedPayloadIsReleased(t *testing.T) {
	writes := make(chan uint16, 1)
	pacer := newAudioCadencePacer(func(packet *rtp.Packet) error { writes <- packet.SequenceNumber; return nil })
	defer pacer.Stop()
	for packetIndex := 0; packetIndex < 2; packetIndex++ {
		if err := pacer.Enqueue(&rtp.Packet{Header: rtp.Header{SequenceNumber: 100, Timestamp: 1000}, Payload: []byte{0xf8}}); err != nil {
			t.Fatal(err)
		}
		if packetIndex == 0 {
			receiveWrite(t, writes)
		}
	}
	waitForPacerSnapshot(t, pacer, func(snapshot PacerSnapshot) bool { return snapshot.LatePacketDropCount == 1 })
	pacer.Stop()
	assertOwnedPacketsReleased(t, pacer)
}

func TestGivenExhaustedPayloadPoolWhenEnqueuedThenAdmissionDropsWithoutAllocationOrLeak(t *testing.T) {
	pacer := &AudioCadencePacer{queue: make(chan pacedPacket, defaultPacerCapacity), ownedPackets: make([]ownedPacerPacket, ownedPacerPacketCount)}
	for slotIndex := range pacer.ownedPackets {
		pacer.ownedPackets[slotIndex].claimed.Store(true)
	}
	packet := &rtp.Packet{}
	allocationsCount := testing.AllocsPerRun(1000, func() { _ = pacer.Enqueue(packet) })
	if allocationsCount != 0 || pacer.Snapshot().QueueFullDrops != 1001 || len(pacer.queue) != 0 {
		t.Fatalf("pool exhaustion alloc=%g drops=%d queue=%d", allocationsCount, pacer.Snapshot().QueueFullDrops, len(pacer.queue))
	}
	for slotIndex := range pacer.ownedPackets {
		pacer.ownedPackets[slotIndex].release()
	}
	assertOwnedPacketsReleased(t, pacer)
}

func TestGivenAdmittedProducerWhenStopClosesAdmissionThenLateQueuePublicationIsJoined(t *testing.T) {
	pacer := newAudioForwardPacer(func(*rtp.Packet) error { return nil })
	if !pacer.beginEnqueue() {
		t.Fatal("fresh producer was not admitted")
	}
	stopped := make(chan struct{})
	go func() { pacer.Stop(); close(stopped) }()
	deadline := time.Now().Add(2 * time.Second)
	for pacer.enqueueState.Load()&pacerAdmissionClosed == 0 {
		if time.Now().After(deadline) {
			pacer.enqueueState.Add(^uint64(0))
			t.Fatal("Stop did not close the atomic admission gate")
		}
		runtime.Gosched()
	}
	select {
	case <-stopped:
		t.Fatal("Stop returned while an admitted producer could still publish")
	default:
	}
	_ = pacer.enqueueOwned(&rtp.Packet{}, session.SourceIdentity{}, time.Time{}, false)
	pacer.enqueueState.Add(^uint64(0))
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not join the admitted producer and worker")
	}
	assertOwnedPacketsReleased(t, pacer)
	if err := pacer.Enqueue(&rtp.Packet{}); err != nil || len(pacer.queue) != 0 {
		t.Fatal("stopped pacer admitted another packet")
	}
}

func TestGivenConcurrentBusMixProducersWhenStopRacesEnqueueThenAllOwnedSlotsAreJoined(t *testing.T) {
	pacer := newAudioForwardPacer(func(*rtp.Packet) error { return nil })
	start := make(chan struct{})
	progress := make(chan struct{}, 8)
	continueProducing := make(chan struct{})
	var producers sync.WaitGroup
	for producerIndex := 0; producerIndex < 8; producerIndex++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			<-start
			payload := [3]byte{0xf8, 1, 2}
			first := &rtp.Packet{Header: rtp.Header{Version: 2}, Payload: payload[:]}
			_ = pacer.EnqueueSource(first, session.SourceIdentity{BusID: "mix", Generation: uint64(producerIndex + 1)}, time.Time{}, false)
			progress <- struct{}{}
			<-continueProducing
			for sequenceCount := 0; sequenceCount < 100; sequenceCount++ {
				packet := &rtp.Packet{Header: rtp.Header{Version: 2, SequenceNumber: uint16(sequenceCount)}, Payload: payload[:]}
				_ = pacer.EnqueueSource(packet, session.SourceIdentity{BusID: "mix", Generation: uint64(producerIndex + 1)}, time.Time{}, false)
				payload[1]++ // Reuse after the synchronous copy returned.
			}
		}()
	}
	close(start)
	for producerIndex := 0; producerIndex < 8; producerIndex++ {
		select {
		case <-progress:
		case <-time.After(2 * time.Second):
			close(continueProducing)
			pacer.Stop()
			producers.Wait()
			t.Fatal("concurrent producers did not reach the production enqueue boundary")
		}
	}
	if pacer.enqueuedCount.Load() == 0 {
		close(continueProducing)
		pacer.Stop()
		producers.Wait()
		t.Fatal("concurrent-producer test made no real pre-stop progress")
	}
	close(continueProducing)
	pacer.Stop()
	producers.Wait()
	assertOwnedPacketsReleased(t, pacer)
}

func TestGivenClustered20msRTPWhenTimelineSchedulesThenDueTimesAre20msApart(t *testing.T) {
	timeline := newCadenceTimeline()
	arrival := time.Unix(0, 1_000_000_000)

	due0, _, _ := timeline.dueAt(10_000, arrival)
	due1, _, _ := timeline.dueAt(10_960, arrival)
	due2, _, _ := timeline.dueAt(11_920, arrival.Add(40*time.Millisecond))

	if got := due1.Sub(due0); got != 20*time.Millisecond {
		t.Fatalf("first spacing = %s, want 20ms", got)
	}
	if got := due2.Sub(due1); got != 20*time.Millisecond {
		t.Fatalf("second spacing = %s, want 20ms", got)
	}
}

func TestGivenTimestampWrapWhenTimelineSchedulesThenNoReset(t *testing.T) {
	timeline := newCadenceTimeline()
	arrival := time.Unix(0, 1_000_000_000)
	due0, _, _ := timeline.dueAt(^uint32(0)-479, arrival)
	due1, reset, _ := timeline.dueAt(480, arrival)

	if reset {
		t.Fatal("timestamp wrap unexpectedly reset timeline")
	}
	if got := due1.Sub(due0); got != 20*time.Millisecond {
		t.Fatalf("wrap spacing = %s, want 20ms", got)
	}
}

func TestGivenLargeTimestampJumpWhenTimelineSchedulesThenReanchors(t *testing.T) {
	timeline := newCadenceTimeline()
	arrival := time.Unix(0, 1_000_000_000)
	_, _, _ = timeline.dueAt(1_000, arrival)
	later := arrival.Add(time.Second)
	due, reset, _ := timeline.dueAt(1_000+defaultPacerResetSamples+1, later)

	if !reset {
		t.Fatal("large timestamp jump did not reset timeline")
	}
	wantDue := later.Add(defaultPacerTargetDelay)
	if !due.Equal(wantDue) {
		t.Fatalf("reset due = %s, want %s", due, wantDue)
	}
}

func TestGivenLongContinuousStreamWhenTimelineSchedulesThenDurationDoesNotCauseReset(t *testing.T) {
	timeline := newCadenceTimeline()
	arrival := time.Unix(0, 1_000_000_000)
	var resets int
	for i := 0; i < 1_000; i++ {
		_, reset, _ := timeline.dueAt(uint32(i*960), arrival.Add(time.Duration(i)*20*time.Millisecond))
		if reset {
			resets++
		}
	}
	if resets != 0 {
		t.Fatalf("continuous 20-second stream reset timeline %d times, want 0", resets)
	}
}

func TestGivenReorderedTimestampWhenTimelineSchedulesThenRecoveryIsImmediateWithoutReset(t *testing.T) {
	timeline := newCadenceTimeline()
	arrival := time.Unix(0, 1_000_000_000)
	_, _, _ = timeline.dueAt(10_000, arrival)
	_, _, _ = timeline.dueAt(11_920, arrival.Add(40*time.Millisecond))
	recoveryArrival := arrival.Add(45 * time.Millisecond)
	due, reset, advances := timeline.dueAt(10_960, recoveryArrival)

	if reset || advances {
		t.Fatalf("reordered packet reset=%v advances=%v, want false/false", reset, advances)
	}
	if !due.Equal(recoveryArrival) {
		t.Fatalf("recovery due = %s, want immediate arrival %s", due, recoveryArrival)
	}
	nextDue, reset, advances := timeline.dueAt(12_880, arrival.Add(60*time.Millisecond))
	wantNextDue := arrival.Add(defaultPacerTargetDelay + 60*time.Millisecond)
	if reset || !advances || !nextDue.Equal(wantNextDue) {
		t.Fatalf("primary timeline changed by recovery: due=%s reset=%v advances=%v", nextDue, reset, advances)
	}
}

func TestGivenMissingIngressPacketWhenTimelineSchedulesThenWallClockGapIsOneFrame(t *testing.T) {
	timeline := newCadenceTimeline()
	arrival := time.Unix(0, 1_000_000_000)
	due0, _, _ := timeline.dueAt(10_000, arrival)
	due1, _, _ := timeline.dueAt(10_960, arrival)
	dueAfterLoss, reset, _ := timeline.dueAt(12_880, arrival)

	if reset {
		t.Fatal("single missing packet unexpectedly reset timeline")
	}
	if got := due1.Sub(due0); got != 20*time.Millisecond {
		t.Fatalf("nominal spacing = %s, want 20ms", got)
	}
	if got := dueAfterLoss.Sub(due1); got != 20*time.Millisecond {
		t.Fatalf("spacing after ingress loss = %s, want 20ms", got)
	}
}

func TestGivenLateArrivalWhenTimelineReanchorsThenReserveIsNotReapplied(t *testing.T) {
	timeline := newCadenceTimeline()
	arrival := time.Unix(0, 1_000_000_000)
	_, _, _ = timeline.dueAt(10_000, arrival)
	lateArrival := arrival.Add(100 * time.Millisecond)
	due, reset, _ := timeline.dueAt(10_960, lateArrival)

	if !reset {
		t.Fatal("late packet did not reanchor timeline")
	}
	if !due.Equal(lateArrival) {
		t.Fatalf("late due = %s, want immediate %s", due, lateArrival)
	}
}

func TestGivenMediaCadenceWhenMinimumSpacingCalculatedThenCatchUpIsBounded(t *testing.T) {
	tests := []struct {
		name           string
		mediaSpacingNs time.Duration
		wantNs         time.Duration
	}{
		{name: "20ms frame", mediaSpacingNs: 20 * time.Millisecond, wantNs: 16 * time.Millisecond},
		{name: "10ms frame", mediaSpacingNs: 10 * time.Millisecond, wantNs: 8 * time.Millisecond},
		{name: "non-advancing timestamp", mediaSpacingNs: 0, wantNs: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if gotNs := minimumCadenceSpacingNs(test.mediaSpacingNs); gotNs != test.wantNs {
				t.Fatalf("minimum spacing = %s, want %s", gotNs, test.wantNs)
			}
		})
	}
}

func TestGivenKnownSpacingsWhenHistogramQueriedThenPercentilesUseExpectedBuckets(t *testing.T) {
	var histogram cadenceSpacingHistogram
	histogram.observe(int64(16 * time.Millisecond))
	histogram.observe(int64(20 * time.Millisecond))

	if got := histogram.percentileMs(0.50); got != 18 {
		t.Fatalf("spacing p50 = %.1fms, want 18ms bucket", got)
	}
	if got := histogram.percentileMs(0.95); got != 22 {
		t.Fatalf("spacing p95 = %.1fms, want 22ms bucket", got)
	}
}

func TestGivenReorderedQueueWhenSortedThenSequenceOrderIsRestored(t *testing.T) {
	items := []pacedPacket{
		{packet: &rtp.Packet{Header: rtp.Header{SequenceNumber: 102}}, extSeq: 102},
		{packet: &rtp.Packet{Header: rtp.Header{SequenceNumber: 100}}, extSeq: 100},
		{packet: &rtp.Packet{Header: rtp.Header{SequenceNumber: 101}}, extSeq: 101},
	}
	sortPacedPacketsByExtendedSequence(items)
	for i, want := range []uint16{100, 101, 102} {
		if got := items[i].packet.SequenceNumber; got != want {
			t.Fatalf("sequence[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestGivenForwardSequenceGapWhenCheckedThenWaitOnlyForMissingForwardPacket(t *testing.T) {
	forwarder := sequenceForwarder{
		haveReleased: true, lastReleasedExtSeq: 100,
	}
	if !forwarder.hasGap(102) {
		t.Fatal("sequence gap 100 -> 102 was not detected")
	}
	if forwarder.hasGap(101) {
		t.Fatal("contiguous sequence 100 -> 101 reported a gap")
	}
	if forwarder.hasGap(99) {
		t.Fatal("late recovery sequence 102 -> 101 reported a forward gap")
	}
}

func TestGivenClusteredPacketsWhenCadencePacerRunsThenWritesAreSpaced(t *testing.T) {
	writes := make(chan time.Time, 3)
	pacer := newAudioCadencePacer(func(*rtp.Packet) error {
		writes <- time.Now()
		return nil
	})
	defer pacer.Stop()

	for i := 0; i < 3; i++ {
		if err := pacer.Enqueue(&rtp.Packet{Header: rtp.Header{SequenceNumber: uint16(i), Timestamp: uint32(i * 960)}}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	t0 := receiveWriteTime(t, writes)
	t1 := receiveWriteTime(t, writes)
	t2 := receiveWriteTime(t, writes)
	for i, spacing := range []time.Duration{t1.Sub(t0), t2.Sub(t1)} {
		if spacing < 15*time.Millisecond || spacing > 35*time.Millisecond {
			t.Fatalf("spacing[%d] = %s, want 15ms..35ms", i, spacing)
		}
	}
	snapshot := pacer.Snapshot()
	if snapshot.MaxTimerWaitNs > uint64(35*time.Millisecond) {
		t.Fatalf("max timer wait = %s, want <= 35ms", time.Duration(snapshot.MaxTimerWaitNs))
	}
	if snapshot.WriterBlockedCount != 0 {
		t.Fatalf("writer blocked count = %d, want 0", snapshot.WriterBlockedCount)
	}
}

func TestGivenClusteredPacketsWhenForwardPacerRunsThenWritesWithoutCadenceDelay(t *testing.T) {
	writes := make(chan time.Time, 3)
	pacer := newAudioForwardPacer(func(*rtp.Packet) error {
		writes <- time.Now()
		return nil
	})
	defer pacer.Stop()

	for i := 0; i < 3; i++ {
		if err := pacer.Enqueue(&rtp.Packet{Header: rtp.Header{
			SequenceNumber: uint16(i),
			Timestamp:      uint32(i * 960),
		}}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	first := receiveWriteTime(t, writes)
	_ = receiveWriteTime(t, writes)
	last := receiveWriteTime(t, writes)
	if elapsed := last.Sub(first); elapsed > 10*time.Millisecond {
		t.Fatalf("forwarding three queued packets took %s, want <= 10ms", elapsed)
	}
	if got := pacer.Snapshot().MaxTimerWaitNs; got != 0 {
		t.Fatalf("forward pacer timer wait = %s, want 0", time.Duration(got))
	}
}

func TestGivenMissingPacketWhenForwardPacerRunsThenGapIsReleasedAfterBoundedWait(t *testing.T) {
	writes := make(chan time.Time, 2)
	pacer := newAudioForwardPacer(func(*rtp.Packet) error {
		writes <- time.Now()
		return nil
	})
	defer pacer.Stop()

	if err := pacer.Enqueue(&rtp.Packet{Header: rtp.Header{
		SequenceNumber: 100,
		Timestamp:      10_000,
	}}); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	_ = receiveWriteTime(t, writes)
	started := time.Now()
	if err := pacer.Enqueue(&rtp.Packet{Header: rtp.Header{
		SequenceNumber: 102,
		Timestamp:      11_920,
	}}); err != nil {
		t.Fatalf("enqueue after gap: %v", err)
	}
	_ = receiveWriteTime(t, writes)

	elapsed := time.Since(started)
	minimumWaitMs := defaultForwardReorderWaitMs - 5*time.Millisecond
	if elapsed < minimumWaitMs || elapsed > 150*time.Millisecond {
		t.Fatalf("forward gap wait = %s, want %s..150ms", elapsed, minimumWaitMs)
	}
	snapshot := waitForPacerSnapshot(t, pacer, func(snapshot PacerSnapshot) bool {
		return snapshot.SentCount == 2
	})
	if got := snapshot.GapTimeoutCount; got != 1 {
		t.Fatalf("gap timeout count = %d, want 1", got)
	}
	if snapshot.OutputSequenceDiscontinuityCount != 1 || snapshot.OutputTimestampDiscontinuityCount != 1 {
		t.Fatalf(
			"output sequence/timestamp discontinuities = %d/%d, want 1/1",
			snapshot.OutputSequenceDiscontinuityCount,
			snapshot.OutputTimestampDiscontinuityCount,
		)
	}
	if snapshot.OutputMaxSequenceDelta != 2 || snapshot.OutputMaxTimestampDeltaSamples != 1_920 {
		t.Fatalf(
			"output max sequence/timestamp delta = %d/%d, want 2/1920",
			snapshot.OutputMaxSequenceDelta,
			snapshot.OutputMaxTimestampDeltaSamples,
		)
	}
}

func TestGivenPublisherReconnectWhenForwardPacerRunsThenOutgoingRTPIsContinuousAndCached(t *testing.T) {
	writes := make(chan *rtp.Packet, 4)
	pacer := newAudioForwardPacer(func(packet *rtp.Packet) error {
		writes <- packet.Clone()
		return nil
	})
	defer pacer.Stop()
	firstSource := session.SourceIdentity{BusID: "voice", Generation: 1, SSRC: 7}
	secondSource := session.SourceIdentity{BusID: "voice", Generation: 2, SSRC: 7}

	packets := []*rtp.Packet{
		{Header: rtp.Header{SequenceNumber: 100, Timestamp: 1000}},
		{Header: rtp.Header{SequenceNumber: 101, Timestamp: 1960}},
		{Header: rtp.Header{SequenceNumber: 5, Timestamp: 100}},
	}
	for i, packet := range packets {
		source := firstSource
		if i == 2 {
			source = secondSource
		}
		if err := pacer.EnqueueSource(packet, source, time.Time{}, false); err != nil {
			t.Fatalf("enqueue packet %d: %v", i, err)
		}
	}

	for i := 0; i < 3; i++ {
		packet := receiveWrite(t, writes)
		wantSequence := uint16(100 + i)
		wantTimestamp := uint32(1000 + i*960)
		if packet.SequenceNumber != wantSequence || packet.Timestamp != wantTimestamp {
			t.Fatalf("packet %d = seq %d ts %d, want seq %d ts %d", i, packet.SequenceNumber, packet.Timestamp, wantSequence, wantTimestamp)
		}
	}
	if got := pacer.Snapshot().TimelineResets; got != 1 {
		t.Fatalf("timeline resets = %d, want 1", got)
	}

	pacer.HandleNACK(102)
	retransmit := receiveWrite(t, writes)
	if retransmit.SequenceNumber != 102 || retransmit.Timestamp != 2920 {
		t.Fatalf("retransmit = seq %d ts %d, want translated seq 102 ts 2920", retransmit.SequenceNumber, retransmit.Timestamp)
	}
	// A source/timeline rebase does not invalidate already translated outgoing
	// packets. Their immutable cache entries live until eviction or Stop.
	pacer.HandleNACK(101)
	previous := receiveWrite(t, writes)
	if previous.SequenceNumber != 101 || previous.Timestamp != 1960 {
		t.Fatal("source rebase lost the prior outgoing repair packet")
	}
	pacer.Stop()
	assertOwnedPacketsReleased(t, pacer)
}

func TestGivenShortReorderWhenForwardPacerRunsThenSequenceAndTimestampAreMonotonic(t *testing.T) {
	writes := make(chan *rtp.Packet, 3)
	pacer := newAudioForwardPacer(func(pkt *rtp.Packet) error {
		writes <- pkt.Clone()
		return nil
	})
	defer pacer.Stop()

	packets := []*rtp.Packet{
		{Header: rtp.Header{SequenceNumber: 100, Timestamp: 1_000}},
		{Header: rtp.Header{SequenceNumber: 102, Timestamp: 2_920}},
		{Header: rtp.Header{SequenceNumber: 101, Timestamp: 1_960}},
	}
	for _, packet := range packets {
		if err := pacer.Enqueue(packet); err != nil {
			t.Fatalf("enqueue %d: %v", packet.SequenceNumber, err)
		}
	}
	for i, wantSequence := range []uint16{100, 101, 102} {
		select {
		case got := <-writes:
			wantTimestamp := uint32(1_000 + i*960)
			if got.SequenceNumber != wantSequence || got.Timestamp != wantTimestamp {
				t.Fatalf(
					"forwarded packet = seq %d ts %d, want seq %d ts %d",
					got.SequenceNumber,
					got.Timestamp,
					wantSequence,
					wantTimestamp,
				)
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("timed out waiting for sequence %d", wantSequence)
		}
	}
	snapshot := pacer.Snapshot()
	if snapshot.GapTimeoutCount != 0 {
		t.Fatalf("gap timeout count = %d, want 0", snapshot.GapTimeoutCount)
	}
	if snapshot.OutputSequenceDiscontinuityCount != 0 || snapshot.OutputTimestampDiscontinuityCount != 0 {
		t.Fatalf(
			"output sequence/timestamp discontinuities = %d/%d, want 0/0",
			snapshot.OutputSequenceDiscontinuityCount,
			snapshot.OutputTimestampDiscontinuityCount,
		)
	}
}

func TestGivenGapWaitWhenForwardPacerStopsThenPendingQueueIsDrained(t *testing.T) {
	writes := make(chan uint16, 2)
	pacer := newAudioForwardPacer(func(pkt *rtp.Packet) error {
		writes <- pkt.SequenceNumber
		return nil
	})

	if err := pacer.Enqueue(&rtp.Packet{Header: rtp.Header{SequenceNumber: 100, Timestamp: 1_000}}); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	select {
	case <-writes:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for first packet")
	}
	if err := pacer.Enqueue(&rtp.Packet{Header: rtp.Header{SequenceNumber: 102, Timestamp: 2_920}}); err != nil {
		t.Fatalf("enqueue gap: %v", err)
	}

	deadline := time.Now().Add(30 * time.Millisecond)
	for pacer.Snapshot().MaxTimerWaitNs == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if pacer.Snapshot().MaxTimerWaitNs == 0 {
		t.Fatal("forward pacer did not enter the gap wait")
	}

	started := time.Now()
	pacer.Stop()
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("stop during gap wait took %s, want <= 100ms", elapsed)
	}
	assertOwnedPacketsReleased(t, pacer)
	if got := pacer.Snapshot().QueueDepth; got != 0 {
		t.Fatalf("queue depth after stop = %d, want 0", got)
	}
}

func TestGivenMediaWithSourcePaddingWhenForwardPacerRunsThenPaddingIsStripped(t *testing.T) {
	writes := make(chan *rtp.Packet, 1)
	pacer := newAudioForwardPacer(func(pkt *rtp.Packet) error {
		writes <- pkt.Clone()
		return nil
	})
	defer pacer.Stop()

	payload := []byte{0xAA, 0xBB, 0xCC}
	packet := &rtp.Packet{
		Header: rtp.Header{
			Padding:        true,
			SequenceNumber: 100,
			Timestamp:      10_000,
		},
		Payload:     payload,
		PaddingSize: 7,
	}
	if err := pacer.Enqueue(packet); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	written := receiveWrite(t, writes)
	if written.Padding || written.PaddingSize != 0 {
		t.Fatalf("forwarded padding = %t/%d, want false/0", written.Padding, written.PaddingSize)
	}
	if string(written.Payload) != string(payload) {
		t.Fatalf("forwarded payload = %v, want %v", written.Payload, payload)
	}
	snapshot := pacer.Snapshot()
	if snapshot.PaddingPacketCount != 1 || snapshot.SourcePaddingStrippedCount != 1 {
		t.Fatalf(
			"padding observed/stripped = %d/%d, want 1/1",
			snapshot.PaddingPacketCount,
			snapshot.SourcePaddingStrippedCount,
		)
	}
}

func TestGivenPaddingBetweenAudioWhenCadencePacerRunsThenMediaTimelineIgnoresPadding(t *testing.T) {
	type write struct {
		at  time.Time
		seq uint16
	}
	writes := make(chan write, 3)
	pacer := newAudioCadencePacer(func(pkt *rtp.Packet) error {
		writes <- write{at: time.Now(), seq: pkt.SequenceNumber}
		return nil
	})
	defer pacer.Stop()

	packets := []*rtp.Packet{
		{Header: rtp.Header{SequenceNumber: 100, Timestamp: 10_000}},
		{Header: rtp.Header{Padding: true, SequenceNumber: 101, Timestamp: 10_000}, PaddingSize: 20},
		{Header: rtp.Header{SequenceNumber: 102, Timestamp: 10_960}},
	}
	for i, packet := range packets {
		if err := pacer.Enqueue(packet); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	first := receiveWrite(t, writes)
	last := receiveWrite(t, writes)
	if first.seq != 100 || last.seq != 101 {
		t.Fatalf("translated media sequences = %d/%d, want 100/101", first.seq, last.seq)
	}
	if spacing := last.at.Sub(first.at); spacing < 15*time.Millisecond || spacing > 35*time.Millisecond {
		t.Fatalf("media spacing across padding = %s, want 15ms..35ms", spacing)
	}
	snapshot := pacer.Snapshot()
	if snapshot.PaddingPacketCount != 1 {
		t.Fatalf("padding drops = %d, want 1", snapshot.PaddingPacketCount)
	}
	pacer.Stop()
	assertOwnedPacketsReleased(t, pacer)
	if resets := snapshot.TimelineResets; resets != 0 {
		t.Fatalf("timeline resets = %d, want 0 when only padding repeats timestamp", resets)
	}
}

func TestGivenSentPacketWhenNACKHandledThenPacketIsRetransmittedFromCache(t *testing.T) {
	writes := make(chan *rtp.Packet, 2)
	pacer := newAudioCadencePacer(func(pkt *rtp.Packet) error {
		// The writer borrows this scratch only during its callback. Retain an
		// owned observation, then try to corrupt the canonical NACK data.
		writes <- pkt.Clone()
		pkt.Payload[0] = 0
		pkt.SequenceNumber = 999
		return nil
	})
	defer pacer.Stop()

	original := &rtp.Packet{
		Header:  rtp.Header{Version: 2, SequenceNumber: 77, Timestamp: 9_600},
		Payload: []byte{0x01, 0x02},
	}
	if err := pacer.Enqueue(original); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	first := receiveWrite(t, writes)
	pacer.HandleNACK(first.SequenceNumber)
	second := receiveWrite(t, writes)

	if second.SequenceNumber != first.SequenceNumber || string(second.Payload) != string(first.Payload) {
		t.Fatalf("retransmission seq/payload = %d/%v, want %d/%v", second.SequenceNumber, second.Payload, first.SequenceNumber, first.Payload)
	}
	snapshot := waitForPacerSnapshot(t, pacer, func(snapshot PacerSnapshot) bool { return snapshot.RetransmitSentCount == 1 })
	if snapshot.NackCacheHits != 1 || snapshot.RetransmitSentCount != 1 {
		t.Fatalf("nack hits/retransmits = %d/%d, want 1/1", snapshot.NackCacheHits, snapshot.RetransmitSentCount)
	}
}

func TestGivenUnknownSequenceWhenNACKHandledThenCacheMissIsCounted(t *testing.T) {
	pacer := newAudioCadencePacer(func(*rtp.Packet) error { return nil })
	defer pacer.Stop()
	pacer.HandleNACK(999)

	deadline := time.Now().Add(250 * time.Millisecond)
	for pacer.Snapshot().NackCacheMisses == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := pacer.Snapshot().NackCacheMisses; got != 1 {
		t.Fatalf("nack_cache_misses = %d, want 1", got)
	}
}

func TestGivenBlockedWriterWhenQueueFillsThenEnqueueDoesNotBlockAndDropsAreCounted(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var writes atomic.Uint64
	pacer := newAudioCadencePacer(func(*rtp.Packet) error {
		if writes.Add(1) == 1 {
			entered <- struct{}{}
			<-release
		}
		return nil
	})

	if err := pacer.Enqueue(&rtp.Packet{}); err != nil {
		t.Fatalf("enqueue first packet: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("writer did not receive first packet")
	}

	started := time.Now()
	for i := 0; i < defaultPacerCapacity+4; i++ {
		if err := pacer.Enqueue(&rtp.Packet{}); err != nil {
			t.Fatalf("enqueue packet %d: %v", i, err)
		}
	}
	if elapsed := time.Since(started); elapsed > 20*time.Millisecond {
		t.Fatalf("bounded enqueue blocked for %s", elapsed)
	}
	if got := pacer.Snapshot().QueueFullDrops; got == 0 {
		t.Fatal("queue_full_drops = 0, want at least one")
	}

	close(release)
	pacer.Stop()
	assertOwnedPacketsReleased(t, pacer)
	if got := pacer.Snapshot().QueueDepth; got != 0 {
		t.Fatalf("queue_depth after Stop = %d, want 0", got)
	}
}

func receiveWriteTime(t *testing.T, writes <-chan time.Time) time.Time {
	t.Helper()
	select {
	case at := <-writes:
		return at
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for paced write")
		return time.Time{}
	}
}

func receiveWrite[T any](t *testing.T, writes <-chan T) T {
	t.Helper()
	select {
	case value := <-writes:
		return value
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for paced write")
		var zero T
		return zero
	}
}

func waitForPacerSnapshot(
	t *testing.T,
	pacer *AudioCadencePacer,
	ready func(PacerSnapshot) bool,
) PacerSnapshot {
	t.Helper()
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		snapshot := pacer.Snapshot()
		if ready(snapshot) {
			return snapshot
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for pacer observation")
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}
}
