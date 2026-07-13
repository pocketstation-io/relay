package downlink_test

import (
	"errors"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/clocklineage"
	"github.com/pocketstation-io/relay/internal/downlink"
)

// --- stubs ---

type writeRecorder struct {
	packets []*rtp.Packet
	err     error
}

type channelWriter chan *rtp.Packet

func (w channelWriter) WriteRTP(pkt *rtp.Packet) error {
	w <- pkt.Clone()
	return nil
}

func (w *writeRecorder) WriteRTP(pkt *rtp.Packet) error {
	if w.err != nil {
		return w.err
	}
	w.packets = append(w.packets, pkt)
	return nil
}

// --- Downlink tests ---

func TestGivenDownlink_WhenWriteRTP_ThenPacketsSentIncrements(t *testing.T) {
	rec := &writeRecorder{}
	dl := downlink.NewPassthrough("sub-1", rec)
	pkt := &rtp.Packet{Payload: []byte{0xAA, 0xBB}}

	if err := dl.WriteRTP(pkt); err != nil {
		t.Fatalf("WriteRTP: %v", err)
	}
	snap := dl.Snapshot()
	if snap.PacketsSent != 1 {
		t.Errorf("packets_sent = %d, want 1", snap.PacketsSent)
	}
	// BytesSent is the full on-wire size (header + payload).
	wantBytes := uint64(pkt.MarshalSize())
	if snap.BytesSent != wantBytes {
		t.Errorf("bytes_sent = %d, want %d (MarshalSize)", snap.BytesSent, wantBytes)
	}
}

func TestGivenProductionDownlink_WhenSnapshotted_ThenModeIsForward(t *testing.T) {
	dl := downlink.NewForwardingDownlink("sub-mode", &writeRecorder{}, nil)
	defer dl.StopPacer()
	if got := dl.Snapshot().Mode; got != "forward" {
		t.Fatalf("downlink mode = %q, want forward", got)
	}
}

func TestGivenDownlink_WhenWriteRTPFails_ThenWriteErrCountIncrements(t *testing.T) {
	rec := &writeRecorder{err: errors.New("send buffer full")}
	dl := downlink.NewPassthrough("sub-2", rec)
	pkt := &rtp.Packet{}

	err := dl.WriteRTP(pkt)
	if err == nil {
		t.Fatal("expected error from WriteRTP, got nil")
	}
	snap := dl.Snapshot()
	if snap.WriteErrCount != 1 {
		t.Errorf("write_err_count = %d, want 1", snap.WriteErrCount)
	}
	if snap.PacketsSent != 0 {
		t.Errorf("packets_sent = %d, want 0 on error", snap.PacketsSent)
	}
}

func TestGivenDownlink_WhenSnapshotCalled_ThenSubscriberIDPreserved(t *testing.T) {
	dl := downlink.NewPassthrough("subscriber-abc", &writeRecorder{})
	snap := dl.Snapshot()
	if snap.SubscriberID != "subscriber-abc" {
		t.Errorf("subscriber_id = %q, want %q", snap.SubscriberID, "subscriber-abc")
	}
}

func TestGivenPionRTTSample_WhenObserved_ThenSnapshotUsesMilliseconds(t *testing.T) {
	dl := downlink.NewPassthrough("sub-rtt", &writeRecorder{})
	dl.ObserveRTT(18_500 * time.Microsecond)

	if got := dl.Snapshot().ReceiverReportRttMs; got != 18.5 {
		t.Fatalf("receiver_report_rtt_ms = %v, want 18.5", got)
	}
}

func TestGivenPassthrough_WhenStopCalled_ThenNoPanic(t *testing.T) {
	dl := downlink.NewPassthrough("sub-3", &writeRecorder{})
	dl.Stop() // must not panic when no FeedbackReader is attached
}

// --- ExtensionMapper tests ---

func TestGivenSDPWithAbsSendTime_WhenDiscover_ThenReturnsCorrectID(t *testing.T) {
	sdp := "v=0\r\na=extmap:3 http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time\r\n"
	id := downlink.DiscoverAbsSendTimeID(sdp)
	if id != 3 {
		t.Errorf("abs-send-time ID = %d, want 3", id)
	}
}

func TestGivenSDPWithoutAbsSendTime_WhenDiscover_ThenReturnsZero(t *testing.T) {
	sdp := "v=0\r\na=extmap:1 urn:ietf:params:rtp-hdrext:sdes:mid\r\n"
	id := downlink.DiscoverAbsSendTimeID(sdp)
	if id != 0 {
		t.Errorf("abs-send-time ID = %d, want 0", id)
	}
}

func TestGivenSDPWithAbsCaptureTime_WhenDiscover_ThenReturnsCorrectID(t *testing.T) {
	sdp := "v=0\r\na=extmap:5 http://www.webrtc.org/experiments/rtp-hdrext/abs-capture-time\r\n"
	id := downlink.DiscoverAbsCaptureTimeID(sdp)
	if id != 5 {
		t.Errorf("abs-capture-time ID = %d, want 5", id)
	}
}

func TestGivenSDPWithoutAbsCaptureTime_WhenDiscover_ThenReturnsZero(t *testing.T) {
	sdp := "v=0\r\na=extmap:1 urn:ietf:params:rtp-hdrext:sdes:mid\r\n"
	id := downlink.DiscoverAbsCaptureTimeID(sdp)
	if id != 0 {
		t.Errorf("abs-capture-time ID = %d, want 0", id)
	}
}

func TestGivenAbsCaptureTimeSet_WhenHasAbsCaptureTime_ThenTrue(t *testing.T) {
	var em downlink.ExtensionMapper
	em.SetAbsCaptureTimeID(4)
	if !em.HasAbsCaptureTime() {
		t.Error("HasAbsCaptureTime() = false, want true after SetAbsCaptureTimeID(4)")
	}
}

func TestGivenAbsCaptureTimeNotSet_WhenHasAbsCaptureTime_ThenFalse(t *testing.T) {
	var em downlink.ExtensionMapper
	if em.HasAbsCaptureTime() {
		t.Error("HasAbsCaptureTime() = true, want false for zero-value ExtensionMapper")
	}
}

func TestGivenPublisherCaptureTime_WhenNegotiated_ThenPacketCarriesAbsoluteCaptureTime(t *testing.T) {
	rec := &writeRecorder{}
	dl := downlink.NewPassthrough("sub-capture", rec)
	dl.ConfigureExtensions("v=0\r\na=extmap:5/sendonly http://www.webrtc.org/experiments/rtp-hdrext/abs-capture-time\r\n")
	captureTime := time.Date(2026, 7, 12, 10, 0, 0, 123_000_000, time.UTC)
	pkt := &rtp.Packet{Header: rtp.Header{Version: 2}, Payload: []byte{0x01}}

	if err := dl.WriteRTPWithCaptureTime(pkt, captureTime, true); err != nil {
		t.Fatalf("WriteRTPWithCaptureTime: %v", err)
	}
	var extension rtp.AbsCaptureTimeExtension
	if err := extension.Unmarshal(rec.packets[0].GetExtension(5)); err != nil {
		t.Fatalf("Unmarshal abs-capture-time: %v", err)
	}
	if delta := extension.CaptureTime().Sub(captureTime); delta < -time.Nanosecond || delta > time.Nanosecond {
		t.Fatalf("capture time delta = %v, want <= 1ns", delta)
	}
	if got := dl.Snapshot().AbsCaptureTimePatchedCount; got != 1 {
		t.Fatalf("patched count = %d, want 1", got)
	}
}

func TestGivenReconnectTranslation_WhenCaptureObserved_ThenSenderReportUsesOutgoingTimestamp(t *testing.T) {
	writes := make(channelWriter, 2)
	dl := downlink.NewForwardingDownlink("sub-clock", writes, nil)
	defer dl.StopPacer()
	timeline := clocklineage.NewTimeline(77)
	dl.SetSenderTimeline(timeline)
	base := time.Now().UTC().Truncate(time.Millisecond)
	firstSource := downlink.SourceIdentity{BusID: "voice", Generation: 1, SSRC: 7}
	secondSource := downlink.SourceIdentity{BusID: "voice", Generation: 2, SSRC: 7}

	if err := dl.WriteRTPWithSource(
		&rtp.Packet{Header: rtp.Header{SequenceNumber: 100, Timestamp: 1000}},
		base,
		true,
		firstSource,
	); err != nil {
		t.Fatalf("first WriteRTPWithSource: %v", err)
	}
	<-writes
	if err := dl.WriteRTPWithSource(
		&rtp.Packet{Header: rtp.Header{SequenceNumber: 5, Timestamp: 100}},
		base.Add(20*time.Millisecond),
		true,
		secondSource,
	); err != nil {
		t.Fatalf("reconnect WriteRTPWithSource: %v", err)
	}
	reconnected := <-writes
	if reconnected.Timestamp != 1960 {
		t.Fatalf("translated timestamp = %d, want 1960", reconnected.Timestamp)
	}

	report := &rtcp.SenderReport{SSRC: 77, RTPTime: reconnected.Timestamp}
	if !timeline.NormalizeSenderReport(report, time.Now(), 48_000) {
		t.Fatal("NormalizeSenderReport rejected translated capture mapping")
	}
	extension := rtp.NewAbsCaptureTimeExtension(base.Add(20 * time.Millisecond))
	delta := report.NTPTime - extension.Timestamp
	if report.NTPTime < extension.Timestamp {
		delta = extension.Timestamp - report.NTPTime
	}
	if delta > 5 {
		t.Fatalf("normalized NTP delta = %d UQ32.32 ticks, want <= 5", delta)
	}
}

func TestGivenNegotiatedAbsSendTime_WhenWriteRTP_ThenPacketCarriesExtension(t *testing.T) {
	rec := &writeRecorder{}
	dl := downlink.NewPassthrough("sub-ext", rec)
	dl.ConfigureExtensions("v=0\r\na=extmap:3/sendonly http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time\r\n")
	pkt := &rtp.Packet{Header: rtp.Header{Version: 2}, Payload: []byte{0x01}}

	if err := dl.WriteRTP(pkt); err != nil {
		t.Fatalf("WriteRTP: %v", err)
	}
	if !dl.RequiresPacketCopy() {
		t.Fatal("RequiresPacketCopy = false, want true for abs-send-time")
	}
	got := rec.packets[0].GetExtension(3)
	if len(got) != 3 {
		t.Fatalf("abs-send-time extension length = %d, want 3", len(got))
	}
}

// --- PassThroughPacer tests ---

func TestGivenDownlink_WhenWriteRTPCalledManyTimes_ThenP95InExpectedBucket(t *testing.T) {
	// All writes complete in < 1 ms on a stub recorder, so both P50 and P95
	// should fall in one of the first five buckets (upper bounds: 10μs … 1 ms).
	rec := &writeRecorder{}
	dl := downlink.NewPassthrough("sub-hist", rec)
	for i := 0; i < 200; i++ {
		pkt := &rtp.Packet{Payload: []byte{byte(i)}}
		if err := dl.WriteRTP(pkt); err != nil {
			t.Fatalf("WriteRTP iteration %d: %v", i, err)
		}
	}
	snap := dl.Snapshot()
	if snap.WriteP95Ms <= 0 {
		t.Errorf("WriteP95Ms = %v, want > 0 after 200 writes", snap.WriteP95Ms)
	}
	if snap.WriteP95Ms > 5.0 {
		t.Errorf("WriteP95Ms = %v ms, want ≤ 5 ms for in-process stub writes", snap.WriteP95Ms)
	}
	if snap.WriteP50Ms <= 0 {
		t.Errorf("WriteP50Ms = %v, want > 0 after 200 writes", snap.WriteP50Ms)
	}
}
