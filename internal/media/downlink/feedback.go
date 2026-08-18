package downlink

import (
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
)

// RTCPSource is the read side of RTCP for a WebRTC sender.
// *webrtc.RTPSender satisfies this interface directly.
type RTCPSource interface {
	ReadRTCP() ([]rtcp.Packet, interceptor.Attributes, error)
}

// RecvReportCallback is invoked for each RTCP ReceptionReport block received.
// fractionLost is in [0.0, 1.0] (RTCP raw byte / 256).
type RecvReportCallback func(fractionLost float64)

// NackCallback receives each subscriber-hop RTP sequence number requested by RTCP NACK.
type NackCallback func(seq uint16)

// FeedbackReader reads RTCP from an RTCPSource in a dedicated goroutine and
// updates a SenderStats. It also invokes an optional RecvReportCallback so
// callers can react to loss (e.g., emit a CODEC_HINT or ICE_RESTART).
//
// The goroutine exits automatically when src.ReadRTCP returns an error.
type FeedbackReader struct {
	stats    *SenderStats
	onReport RecvReportCallback
	onNACK   NackCallback
	stopOnce sync.Once
	stop     chan struct{}
	stopped  chan struct{}
}

// StartFeedbackReader launches the RTCP reader goroutine and returns immediately.
// stats must not be nil. onReport may be nil.
func StartFeedbackReader(
	src RTCPSource,
	stats *SenderStats,
	onReport RecvReportCallback,
	onNACK NackCallback,
) *FeedbackReader {
	fr := &FeedbackReader{
		stats:    stats,
		onReport: onReport,
		onNACK:   onNACK,
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	go fr.loop(src)
	return fr
}

// Stop signals the reader goroutine to exit and waits for it to finish.
// Safe to call multiple times; only the first call has effect.
func (fr *FeedbackReader) Stop() {
	fr.stopOnce.Do(func() { close(fr.stop) })
	<-fr.stopped
}

func (fr *FeedbackReader) loop(src RTCPSource) {
	defer close(fr.stopped)
	for {
		select {
		case <-fr.stop:
			return
		default:
		}

		pkts, _, err := src.ReadRTCP()
		if err != nil {
			return // connection closed; exit cleanly
		}

		for _, pkt := range pkts {
			switch p := pkt.(type) {
			case *rtcp.ReceiverReport:
				for _, report := range p.Reports {
					fr.handleReport(report)
				}
			case *rtcp.TransportLayerNack:
				for i := range p.Nacks {
					p.Nacks[i].Range(func(seq uint16) bool {
						fr.stats.NackCount.Add(1)
						if fr.onNACK != nil {
							fr.onNACK(seq)
						}
						return true
					})
				}
			case *rtcp.TransportLayerCC:
				fr.stats.TwccFeedbackCount.Add(1)
				_ = p
			default:
				_ = p
			}
		}
	}
}

// handleReport extracts per-report metrics and invokes the callback.
// Called from the loop goroutine; all writes go through atomic fields on stats.
func (fr *FeedbackReader) handleReport(report rtcp.ReceptionReport) {
	fr.stats.ReceiverReportCount.Add(1)
	fr.stats.FractionLostLast.Store(uint32(report.FractionLost))

	// Jitter is reported in RTP clock units (Opus uses 48 kHz).
	// 1 unit = 1/48000 s = 20.83 μs.
	jitterUs := int64(report.Jitter) * 1_000_000 / 48_000
	fr.stats.JitterLastUs.Store(jitterUs)
	if rtt, ok := receptionReportRTT(report, time.Now()); ok {
		fr.stats.RttLastUs.Store(rtt.Microseconds())
	}

	if fr.onReport != nil {
		fractionLost := float64(report.FractionLost) / 256.0
		fr.onReport(fractionLost)
	}
}

const ntpUnixEpochOffsetSeconds = uint64(2_208_988_800)

func compactNTP(t time.Time) uint32 {
	seconds := uint64(t.Unix()) + ntpUnixEpochOffsetSeconds
	fraction := uint64(t.Nanosecond()) * (uint64(1) << 32) / uint64(time.Second)
	return uint32(((seconds << 32) | fraction) >> 16)
}

func receptionReportRTT(report rtcp.ReceptionReport, now time.Time) (time.Duration, bool) {
	if report.LastSenderReport == 0 {
		return 0, false
	}
	rttCompact := compactNTP(now) - report.LastSenderReport - report.Delay
	rtt := time.Duration(uint64(rttCompact) * uint64(time.Second) / (uint64(1) << 16))
	if rtt < 0 || rtt > time.Minute {
		return 0, false
	}
	return rtt, true
}
