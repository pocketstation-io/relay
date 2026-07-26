// Package clocklineage observes RTCP Sender Reports and maps RTP timestamps to
// the publisher's NTP clock without adding a second packet-timing authority.
package clocklineage

import (
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
)

const (
	ntpUnixEpochOffsetSeconds = uint64(2_208_988_800)
	defaultMaxReportAge       = 10 * time.Second
)

// SenderReportSnapshot is the latest immutable RTCP Sender Report observation
// for one SSRC. A Timeline retains exactly one snapshot.
type SenderReportSnapshot struct {
	SSRC         uint32 `json:"ssrc"`
	NTPTime      uint64 `json:"ntp_time"`
	RTPTime      uint32 `json:"rtp_time"`
	PacketCount  uint32 `json:"packet_count"`
	OctetCount   uint32 `json:"octet_count"`
	ObservedAtNs int64  `json:"observed_at_ns"`
	ReportCount  uint64 `json:"report_count"`
	Normalized   bool   `json:"normalized"`
}

// Timeline retains the latest Sender Report for one SSRC. RTCP updates publish
// an immutable snapshot; RTP readers perform one lock-free pointer load.
type Timeline struct {
	ssrc              uint32
	latest            atomic.Pointer[SenderReportSnapshot]
	captureVersion    atomic.Uint64
	captureRTPTime    atomic.Uint32
	captureNTPTime    atomic.Uint64
	captureObservedNs atomic.Int64
}

// NewTimeline creates an empty timeline for ssrc.
func NewTimeline(ssrc uint32) *Timeline { return &Timeline{ssrc: ssrc} }

// Observe records sr as the latest clock mapping. This is called on the RTCP
// path, not the RTP forwarding path.
func (t *Timeline) Observe(sr *rtcp.SenderReport, observedAt time.Time) {
	t.observe(sr, observedAt, false)
}

func (t *Timeline) observe(sr *rtcp.SenderReport, observedAt time.Time, normalized bool) {
	if sr == nil || sr.SSRC != t.ssrc {
		return
	}
	count := uint64(1)
	if previous := t.latest.Load(); previous != nil {
		count = previous.ReportCount + 1
	}
	t.latest.Store(&SenderReportSnapshot{
		SSRC:         sr.SSRC,
		NTPTime:      sr.NTPTime,
		RTPTime:      sr.RTPTime,
		PacketCount:  sr.PacketCount,
		OctetCount:   sr.OctetCount,
		ObservedAtNs: observedAt.UnixNano(),
		ReportCount:  count,
		Normalized:   normalized,
	})
}

// ObserveCapture records one RTP-to-capture-clock mapping without allocation.
// A seqlock keeps readers from combining fields from adjacent RTP packets.
func (t *Timeline) ObserveCapture(rtpTimestamp uint32, captureTime, observedAt time.Time) {
	for {
		version := t.captureVersion.Load()
		if version&1 != 0 || !t.captureVersion.CompareAndSwap(version, version+1) {
			continue
		}
		t.captureRTPTime.Store(rtpTimestamp)
		t.captureNTPTime.Store(timeToNTP(captureTime))
		t.captureObservedNs.Store(observedAt.UnixNano())
		t.captureVersion.Store(version + 2)
		return
	}
}

// NormalizeSenderReport rewrites sr.NTPTime from the latest genuine publisher
// capture mapping while preserving Pion's outgoing RTP timestamp.
func (t *Timeline) NormalizeSenderReport(sr *rtcp.SenderReport, now time.Time, clockRateHz uint32) bool {
	if sr == nil || sr.SSRC != t.ssrc || clockRateHz == 0 {
		return false
	}
	rtpTime, captureNTP, observedAtNs, ok := t.captureMapping()
	if !ok {
		return false
	}
	observedAt := time.Unix(0, observedAtNs)
	if age := now.Sub(observedAt); age < 0 || age > defaultMaxReportAge {
		return false
	}
	deltaSamples := int64(int32(sr.RTPTime - rtpTime))
	delta := time.Duration(deltaSamples) * time.Second / time.Duration(clockRateHz)
	sr.NTPTime = timeToNTP(ntpTime(captureNTP).Add(delta))
	return true
}

func (t *Timeline) captureMapping() (uint32, uint64, int64, bool) {
	for attempts := 0; attempts < 4; attempts++ {
		before := t.captureVersion.Load()
		if before == 0 || before&1 != 0 {
			continue
		}
		rtpTime := t.captureRTPTime.Load()
		ntp := t.captureNTPTime.Load()
		observedAtNs := t.captureObservedNs.Load()
		after := t.captureVersion.Load()
		if before == after {
			return rtpTime, ntp, observedAtNs, true
		}
	}
	return 0, 0, 0, false
}

// Snapshot returns the latest report and whether one has been observed.
func (t *Timeline) Snapshot() (SenderReportSnapshot, bool) {
	snapshot := t.latest.Load()
	if snapshot == nil {
		return SenderReportSnapshot{SSRC: t.ssrc}, false
	}
	return *snapshot, true
}

// CaptureTime maps rtpTimestamp onto the publisher NTP clock. The int32
// subtraction implements RFC 3550 serial arithmetic across uint32 wraparound.
// Reports older than maxAge are rejected so stale lineage is never stamped.
func (t *Timeline) CaptureTime(rtpTimestamp uint32, now time.Time, clockRateHz uint32, maxAge time.Duration) (time.Time, bool) {
	snapshot := t.latest.Load()
	if snapshot == nil || clockRateHz == 0 {
		return time.Time{}, false
	}
	if maxAge <= 0 {
		maxAge = defaultMaxReportAge
	}
	observedAt := time.Unix(0, snapshot.ObservedAtNs)
	if age := now.Sub(observedAt); age < 0 || age > maxAge {
		return time.Time{}, false
	}
	deltaSamples := int64(int32(rtpTimestamp - snapshot.RTPTime))
	delta := time.Duration(deltaSamples) * time.Second / time.Duration(clockRateHz)
	return ntpTime(snapshot.NTPTime).Add(delta), true
}

func ntpTime(value uint64) time.Time {
	seconds := int64(value>>32) - int64(ntpUnixEpochOffsetSeconds)
	fraction := value & 0xFFFF_FFFF
	nanoseconds := int64(fraction * uint64(time.Second) >> 32)
	return time.Unix(seconds, nanoseconds)
}

func timeToNTP(value time.Time) uint64 {
	seconds := uint64(value.Unix()) + ntpUnixEpochOffsetSeconds
	fraction := uint64(value.Nanosecond()) * (uint64(1) << 32) / uint64(time.Second)
	return seconds<<32 | fraction
}
