package downlink

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
)

func TestGivenClustered20msRTP_WhenTimelineSchedules_ThenDueTimesAre20msApart(t *testing.T) {
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

func TestGivenTimestampWrap_WhenTimelineSchedules_ThenNoReset(t *testing.T) {
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

func TestGivenLargeTimestampJump_WhenTimelineSchedules_ThenReanchors(t *testing.T) {
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

func TestGivenLongContinuousStream_WhenTimelineSchedules_ThenDurationDoesNotCauseReset(t *testing.T) {
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

func TestGivenReorderedTimestamp_WhenTimelineSchedules_ThenRecoveryIsImmediateWithoutReset(t *testing.T) {
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

func TestGivenMissingIngressPacket_WhenTimelineSchedules_ThenWallClockGapIsOneFrame(t *testing.T) {
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

func TestGivenLateArrival_WhenTimelineReanchors_ThenReserveIsNotReapplied(t *testing.T) {
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

func TestGivenReorderedQueue_WhenSorted_ThenSequenceOrderIsRestored(t *testing.T) {
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

func TestGivenForwardSequenceGap_WhenChecked_ThenWaitOnlyForMissingForwardPacket(t *testing.T) {
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

func TestGivenClusteredPackets_WhenCadencePacerRuns_ThenWritesAreSpaced(t *testing.T) {
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
	if snapshot.SpacingP50Ms != 22 || snapshot.SpacingP95Ms != 22 {
		t.Fatalf("spacing percentiles = %.1f/%.1fms, want 22/22ms buckets", snapshot.SpacingP50Ms, snapshot.SpacingP95Ms)
	}
	if snapshot.MaxTimerWaitNs > uint64(35*time.Millisecond) {
		t.Fatalf("max timer wait = %s, want <= 35ms", time.Duration(snapshot.MaxTimerWaitNs))
	}
	if snapshot.WriterBlockedCount != 0 {
		t.Fatalf("writer blocked count = %d, want 0", snapshot.WriterBlockedCount)
	}
}

func TestGivenClusteredPackets_WhenForwardPacerRuns_ThenWritesWithoutCadenceDelay(t *testing.T) {
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

func TestGivenMissingPacket_WhenForwardPacerRuns_ThenGapIsForwardedImmediately(t *testing.T) {
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
	if elapsed > 10*time.Millisecond {
		t.Fatalf("forward gap wait = %s, want <= 10ms", elapsed)
	}
	if got := pacer.Snapshot().GapTimeoutCount; got != 0 {
		t.Fatalf("gap timeout count = %d, want 0", got)
	}
}

func TestGivenPublisherReconnect_WhenForwardPacerRuns_ThenOutgoingRTPIsContinuousAndCached(t *testing.T) {
	writes := make(chan *rtp.Packet, 4)
	pacer := newAudioForwardPacer(func(packet *rtp.Packet) error {
		writes <- packet.Clone()
		return nil
	})
	defer pacer.Stop()
	firstSource := SourceIdentity{BusID: "voice", Generation: 1, SSRC: 7}
	secondSource := SourceIdentity{BusID: "voice", Generation: 2, SSRC: 7}

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
}

func TestGivenLateRepair_WhenForwardPacerRuns_ThenOriginalSequenceIsPreserved(t *testing.T) {
	writes := make(chan uint16, 3)
	pacer := newAudioForwardPacer(func(pkt *rtp.Packet) error {
		writes <- pkt.SequenceNumber
		return nil
	})
	defer pacer.Stop()

	for _, seq := range []uint16{100, 102, 101} {
		if err := pacer.Enqueue(&rtp.Packet{Header: rtp.Header{SequenceNumber: seq}}); err != nil {
			t.Fatalf("enqueue %d: %v", seq, err)
		}
	}
	for _, want := range []uint16{100, 102, 101} {
		select {
		case got := <-writes:
			if got != want {
				t.Fatalf("forwarded sequence = %d, want %d", got, want)
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("timed out waiting for sequence %d", want)
		}
	}
}

func TestGivenPaddingBetweenAudio_WhenCadencePacerRuns_ThenMediaTimelineIgnoresPadding(t *testing.T) {
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
	if resets := snapshot.TimelineResets; resets != 0 {
		t.Fatalf("timeline resets = %d, want 0 when only padding repeats timestamp", resets)
	}
}

func TestGivenSentPacket_WhenNACKHandled_ThenPacketIsRetransmittedFromCache(t *testing.T) {
	writes := make(chan *rtp.Packet, 2)
	pacer := newAudioCadencePacer(func(pkt *rtp.Packet) error {
		writes <- pkt
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

	if second == first {
		t.Fatal("retransmission reused mutable packet pointer, want deep clone")
	}
	if second.SequenceNumber != first.SequenceNumber || string(second.Payload) != string(first.Payload) {
		t.Fatalf("retransmission seq/payload = %d/%v, want %d/%v", second.SequenceNumber, second.Payload, first.SequenceNumber, first.Payload)
	}
	snapshot := pacer.Snapshot()
	if snapshot.NackCacheHits != 1 || snapshot.RetransmitSentCount != 1 {
		t.Fatalf("nack hits/retransmits = %d/%d, want 1/1", snapshot.NackCacheHits, snapshot.RetransmitSentCount)
	}
}

func TestGivenUnknownSequence_WhenNACKHandled_ThenCacheMissIsCounted(t *testing.T) {
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

func TestGivenBlockedWriter_WhenQueueFills_ThenEnqueueDoesNotBlockAndDropsAreCounted(t *testing.T) {
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
