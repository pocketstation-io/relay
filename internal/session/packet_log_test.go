package session

import (
	"testing"
	"time"

	"github.com/pion/rtp"
)

func TestGivenEmptyPacketLogWhenLastCalledThenEmptySlice(t *testing.T) {
	pl := newPacketLogStore()
	entries := pl.last(10)
	if entries == nil {
		t.Error("last() must return empty slice, not nil, when no entries exist")
	}
	if len(entries) != 0 {
		t.Errorf("got %d entries, want 0", len(entries))
	}
}

func TestGivenPacketLogWhenFewerThanLimitRecordedThenAllReturned(t *testing.T) {
	pl := newPacketLogStore()
	pl.record(PacketLogEntry{Seq: 1, RxTsNs: 100, TxTsNs: 110})
	pl.record(PacketLogEntry{Seq: 2, RxTsNs: 120, TxTsNs: 130})

	entries := pl.last(10)
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if entries[0].Seq != 1 || entries[1].Seq != 2 {
		t.Errorf("got seqs %d,%d, want 1,2", entries[0].Seq, entries[1].Seq)
	}
}

func TestGivenPacketLogWhenLimitRequestedThenMostRecentReturned(t *testing.T) {
	pl := newPacketLogStore()
	for i := 0; i < 5; i++ {
		pl.record(PacketLogEntry{Seq: uint16(i), RxTsNs: int64(i * 10), TxTsNs: int64(i*10 + 1)})
	}

	entries := pl.last(3)
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	// Most recent 3: seq 2, 3, 4
	if entries[0].Seq != 2 || entries[1].Seq != 3 || entries[2].Seq != 4 {
		t.Errorf("got seqs %d,%d,%d, want 2,3,4", entries[0].Seq, entries[1].Seq, entries[2].Seq)
	}
}

func TestGivenPacketLogWhenWindowFullThenOldestOverwritten(t *testing.T) {
	pl := newPacketLogStore()
	// Fill the ring completely.
	for i := 0; i < packetLogWindowSize+10; i++ {
		pl.record(PacketLogEntry{Seq: uint16(i % 65536), RxTsNs: int64(i), TxTsNs: int64(i + 1)})
	}
	entries := pl.last(5)
	if len(entries) != 5 {
		t.Fatalf("got %d entries, want 5 after ring wrap", len(entries))
	}
	// Last 5 entries should be the most recently recorded.
	last := packetLogWindowSize + 10 - 1
	for i, e := range entries {
		wantRx := int64(last - (4 - i))
		if e.RxTsNs != wantRx {
			t.Errorf("entry[%d]: got RxTsNs=%d, want %d", i, e.RxTsNs, wantRx)
		}
	}
}

func TestGivenPacketLogWhenReadWhileRecordingThenPublishedEntriesStayCoherent(t *testing.T) {
	pl := newPacketLogStore()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10_000; i++ {
			seq := uint16(i)
			pl.record(PacketLogEntry{
				Seq:          seq,
				RtpTsSamples: uint32(seq) * 960,
				PayloadBytes: uint64(seq) + 1,
			})
		}
	}()

	for {
		for _, entry := range pl.last(100) {
			if entry.RtpTsSamples != uint32(entry.Seq)*960 || entry.PayloadBytes != uint64(entry.Seq)+1 {
				t.Fatalf("observed partially published packet-log entry: %+v", entry)
			}
		}
		select {
		case <-done:
			return
		default:
		}
	}
}

func TestGivenRelaySessionWhenBusNotFoundThenPacketLogNil(t *testing.T) {
	r := New("room-pl-1")
	entries := r.BusPacketLog("voice", 10)
	if entries != nil {
		t.Errorf("BusPacketLog on non-existent bus must return nil, got %v", entries)
	}
}

func TestGivenActiveBusWhenPacketForwardedThenPacketLogPopulated(t *testing.T) {
	r := New("room-pl-2")
	src := newMockSource()
	sub := &mockSubscription{}
	if err := r.AddSubscription("peer-1", BusMix, sub); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	r.SetSource("voice", BusRoleVoice, src, nil)

	pkt := &rtp.Packet{}
	pkt.Header.SequenceNumber = 42
	pkt.Header.Timestamp = 96_000
	pkt.Header.PayloadType = 111
	pkt.Header.SSRC = 7
	pkt.Header.Padding = true
	pkt.PaddingSize = 4
	pkt.Payload = []byte{0x01, 0x02}
	src.send(pkt)

	// Wait for the subscriber to receive the packet (proves forwardLoop ran).
	waitFor(t, 100*time.Millisecond, func() bool { return len(sub.received()) == 1 })

	entries := r.BusPacketLog("voice", 10)
	if len(entries) != 1 {
		t.Fatalf("got %d packet log entries, want 1", len(entries))
	}
	if entries[0].Seq != 42 {
		t.Errorf("got seq %d, want 42", entries[0].Seq)
	}
	if entries[0].RtpTsSamples != 96_000 {
		t.Errorf("got RTP timestamp %d, want 96000", entries[0].RtpTsSamples)
	}
	if entries[0].PayloadType != 111 || entries[0].Ssrc != 7 || entries[0].PayloadBytes != 2 {
		t.Errorf("unexpected RTP identity fields: %+v", entries[0])
	}
	if !entries[0].Padding || entries[0].PaddingBytes != 4 {
		t.Errorf("padding identity not preserved: %+v", entries[0])
	}
	if entries[0].RxTsNs == 0 {
		t.Error("RxTsNs must not be zero after packet forwarded")
	}
	if entries[0].TxTsNs == 0 {
		t.Error("TxTsNs must not be zero after packet forwarded")
	}
	if entries[0].TxTsNs < entries[0].RxTsNs {
		t.Errorf("TxTsNs (%d) must be >= RxTsNs (%d)", entries[0].TxTsNs, entries[0].RxTsNs)
	}

	r.Close()
	src.close()
}
