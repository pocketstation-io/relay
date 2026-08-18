package downlink

import (
	"testing"
	"time"

	"github.com/pion/rtp"
)

func TestGivenCachedPacketWhenLoadedBySequenceThenPacketIsReturned(t *testing.T) {
	var cache retransmissionCache
	pkt := &rtp.Packet{Header: rtp.Header{SequenceNumber: 42}}
	cache.store(pkt)

	got, hit, throttled := cache.load(42, time.Now())
	if !hit || throttled || got != pkt {
		t.Fatalf("load = (%p, %v, %v), want (%p, true, false)", got, hit, throttled, pkt)
	}
}

func TestGivenOverwrittenCacheSlotWhenOldSequenceLoadedThenMissIsReturned(t *testing.T) {
	var cache retransmissionCache
	cache.store(&rtp.Packet{Header: rtp.Header{SequenceNumber: 7}})
	newSeq := uint16(7 + retransmissionCachePackets)
	cache.store(&rtp.Packet{Header: rtp.Header{SequenceNumber: newSeq}})

	if _, hit, _ := cache.load(7, time.Now()); hit {
		t.Fatal("old sequence unexpectedly remained after cache slot overwrite")
	}
}

func TestGivenRepeatedNACKWithinThrottleWhenLoadedThenSecondRequestIsThrottled(t *testing.T) {
	var cache retransmissionCache
	cache.store(&rtp.Packet{Header: rtp.Header{SequenceNumber: 9}})
	now := time.Now()
	if _, hit, throttled := cache.load(9, now); !hit || throttled {
		t.Fatalf("first load hit=%v throttled=%v, want true/false", hit, throttled)
	}
	if _, hit, throttled := cache.load(9, now.Add(time.Millisecond)); !hit || !throttled {
		t.Fatalf("second load hit=%v throttled=%v, want true/true", hit, throttled)
	}
}

func TestGivenMeasuredRTTWhenRepeatedNACKArrivesThenTwoRTTsAreRespected(t *testing.T) {
	var cache retransmissionCache
	cache.setRTT(40 * time.Millisecond)
	cache.store(&rtp.Packet{Header: rtp.Header{SequenceNumber: 10}})
	now := time.Now()
	_, _, _ = cache.load(10, now)
	if _, hit, throttled := cache.load(10, now.Add(79*time.Millisecond)); !hit || !throttled {
		t.Fatalf("79ms repeat hit=%v throttled=%v, want true/true", hit, throttled)
	}
	if _, hit, throttled := cache.load(10, now.Add(81*time.Millisecond)); !hit || throttled {
		t.Fatalf("81ms repeat hit=%v throttled=%v, want true/false", hit, throttled)
	}
}

func TestGivenThreeRetransmissionsWhenNACKRepeatsThenFurtherRequestsAreThrottled(t *testing.T) {
	var cache retransmissionCache
	cache.store(&rtp.Packet{Header: rtp.Header{SequenceNumber: 11}})
	now := time.Now()
	for i := 0; i < maxRetransmissionsPerPacket; i++ {
		at := now.Add(time.Duration(i) * (maxRetransmissionThrottle + time.Millisecond))
		if _, hit, throttled := cache.load(11, at); !hit || throttled {
			t.Fatalf("load[%d] hit=%v throttled=%v, want true/false", i, hit, throttled)
		}
	}
	if _, hit, throttled := cache.load(11, now.Add(time.Second)); !hit || !throttled {
		t.Fatalf("fourth load hit=%v throttled=%v, want true/true", hit, throttled)
	}
}
