package downlink

import (
	"errors"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
)

type rtcpReadResult struct {
	packets []rtcp.Packet
	err     error
}

type channelRTCPSource struct {
	results chan rtcpReadResult
}

func (s *channelRTCPSource) ReadRTCP() ([]rtcp.Packet, interceptor.Attributes, error) {
	result := <-s.results
	return result.packets, nil, result.err
}

func TestGivenNACKPair_WhenFeedbackRead_ThenEverySequenceIsCountedAndDelivered(t *testing.T) {
	source := &channelRTCPSource{results: make(chan rtcpReadResult, 2)}
	stats := &SenderStats{}
	sequences := make(chan uint16, 3)
	reader := StartFeedbackReader(source, stats, nil, func(seq uint16) { sequences <- seq })
	source.results <- rtcpReadResult{packets: []rtcp.Packet{&rtcp.TransportLayerNack{
		Nacks: []rtcp.NackPair{{PacketID: 100, LostPackets: 0b101}},
	}}}

	for i, want := range []uint16{100, 101, 103} {
		select {
		case got := <-sequences:
			if got != want {
				t.Fatalf("sequence[%d] = %d, want %d", i, got, want)
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("timed out waiting for sequence[%d]", i)
		}
	}
	if got := stats.NackCount.Load(); got != 3 {
		t.Fatalf("nack_count = %d, want 3 requested packets", got)
	}

	source.results <- rtcpReadResult{err: errors.New("closed")}
	reader.Stop()
}

func TestGivenReceiverReportTiming_WhenRTTComputed_ThenUsesRFC3550Fields(t *testing.T) {
	now := time.Unix(1_700_000_000, 500_000_000)
	senderReportAt := now.Add(-100 * time.Millisecond)
	receiverDelay := 50 * time.Millisecond
	report := rtcp.ReceptionReport{
		LastSenderReport: compactNTP(senderReportAt),
		Delay:            uint32(uint64(receiverDelay) * (uint64(1) << 16) / uint64(time.Second)),
	}

	rtt, ok := receptionReportRTT(report, now)
	if !ok {
		t.Fatal("receiver report RTT was not available")
	}
	if delta := rtt - 50*time.Millisecond; delta < -time.Millisecond || delta > time.Millisecond {
		t.Fatalf("RTT = %s, want 50ms +/- 1ms", rtt)
	}
}
