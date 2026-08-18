package clocklineage

import (
	"testing"
	"time"

	"github.com/pion/rtcp"
)

func TestGivenUnknownLineageWhenCaptureTimeRequestedThenMappingIsRejected(t *testing.T) {
	timeline := NewTimeline(42)
	if _, ok := timeline.CaptureTime(960, time.Now(), 48_000, time.Second); ok {
		t.Fatal("CaptureTime returned a mapping without a Sender Report")
	}
}

func TestGivenPublisherSenderReportWhenRTPAdvancesThenCaptureTimeFollowsNTP(t *testing.T) {
	base := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	timeline := NewTimeline(42)
	timeline.Observe(&rtcp.SenderReport{SSRC: 42, NTPTime: toNTP(base), RTPTime: 1000}, base)

	captureTime, ok := timeline.CaptureTime(1960, base.Add(time.Second), 48_000, 2*time.Second)
	if !ok {
		t.Fatal("CaptureTime rejected fresh lineage")
	}
	want := base.Add(20 * time.Millisecond)
	if delta := captureTime.Sub(want); delta < -time.Microsecond || delta > time.Microsecond {
		t.Fatalf("capture time delta = %v, want within 1us of %v", delta, want)
	}
}

func TestGivenRTPWraparoundWhenCaptureTimeRequestedThenSerialDeltaIsPreserved(t *testing.T) {
	base := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	timeline := NewTimeline(7)
	timeline.Observe(&rtcp.SenderReport{SSRC: 7, NTPTime: toNTP(base), RTPTime: 0xFFFF_FF00}, base)

	captureTime, ok := timeline.CaptureTime(0x0000_02C0, base, 48_000, time.Second)
	if !ok {
		t.Fatal("CaptureTime rejected wraparound lineage")
	}
	want := base.Add(20 * time.Millisecond)
	if delta := captureTime.Sub(want); delta < -time.Microsecond || delta > time.Microsecond {
		t.Fatalf("capture time delta = %v, want within 1us of %v", delta, want)
	}
}

func TestGivenStaleSenderReportWhenCaptureTimeRequestedThenMappingIsRejected(t *testing.T) {
	base := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	timeline := NewTimeline(9)
	timeline.Observe(&rtcp.SenderReport{SSRC: 9, NTPTime: toNTP(base), RTPTime: 1000}, base)

	if _, ok := timeline.CaptureTime(1960, base.Add(11*time.Second), 48_000, 10*time.Second); ok {
		t.Fatal("CaptureTime accepted stale lineage")
	}
}

func TestGivenSourceReconnectWhenNewTimelineHasNoReportThenOldMappingIsNotReused(t *testing.T) {
	base := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	oldTimeline := NewTimeline(1)
	oldTimeline.Observe(&rtcp.SenderReport{SSRC: 1, NTPTime: toNTP(base), RTPTime: 1000}, base)
	newTimeline := NewTimeline(2)

	if _, ok := newTimeline.CaptureTime(1000, base, 48_000, time.Second); ok {
		t.Fatal("new source timeline reused the previous source mapping")
	}
}

func TestGivenCaptureMappingAcrossWrapWhenSubscriberSenderReportGeneratedThenNTPIsNormalized(t *testing.T) {
	base := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	timeline := NewTimeline(77)
	timeline.ObserveCapture(0xFFFF_FF00, base, base)
	report := &rtcp.SenderReport{SSRC: 77, NTPTime: toNTP(base.Add(time.Hour)), RTPTime: 0x0000_02C0}

	if !timeline.NormalizeSenderReport(report, base, 48_000) {
		t.Fatal("NormalizeSenderReport rejected a fresh capture mapping")
	}
	want := base.Add(20 * time.Millisecond)
	if delta := ntpTime(report.NTPTime).Sub(want); delta < -time.Microsecond || delta > time.Microsecond {
		t.Fatalf("normalized NTP delta = %v, want within 1us of %v", delta, want)
	}
}

func TestGivenNoCaptureMappingWhenSubscriberSenderReportGeneratedThenPionNTPIsPreserved(t *testing.T) {
	base := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	timeline := NewTimeline(77)
	wantNTP := toNTP(base)
	report := &rtcp.SenderReport{SSRC: 77, NTPTime: wantNTP, RTPTime: 960}

	if timeline.NormalizeSenderReport(report, base, 48_000) {
		t.Fatal("NormalizeSenderReport accepted unknown capture lineage")
	}
	if report.NTPTime != wantNTP {
		t.Fatalf("NTP time = %d, want original %d", report.NTPTime, wantNTP)
	}
}

func toNTP(value time.Time) uint64 {
	seconds := uint64(value.Unix()) + ntpUnixEpochOffsetSeconds
	fraction := uint64(value.Nanosecond()) * (uint64(1) << 32) / uint64(time.Second)
	return seconds<<32 | fraction
}
