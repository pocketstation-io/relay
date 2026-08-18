package downlink

import (
	"encoding/binary"
	"regexp"
	"strconv"
	"time"

	"github.com/pion/rtp"
)

// absSendTimeURI is the canonical URI for the abs-send-time RTP extension.
const absSendTimeURI = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time"

// absCaptureTimeURI is the canonical URI for the abs-capture-time RTP extension.
const absCaptureTimeURI = "http://www.webrtc.org/experiments/rtp-hdrext/abs-capture-time"

var (
	absSendTimePattern    = regexp.MustCompile(`a=extmap:(\d+)(?:/\w+)? ` + regexp.QuoteMeta(absSendTimeURI))
	absCaptureTimePattern = regexp.MustCompile(`a=extmap:(\d+)(?:/\w+)? ` + regexp.QuoteMeta(absCaptureTimeURI))
)

// ntpEpochOffset is the difference in seconds between the NTP epoch (1900-01-01)
// and the Unix epoch (1970-01-01).
const ntpEpochOffset uint64 = 2208988800

// ExtensionMapper holds the negotiated RTP header extension IDs.
// Zero value is safe: all IDs default to 0 (disabled).
type ExtensionMapper struct {
	absSendTimeID    uint8 // 0 = not negotiated; no-op
	absCaptureTimeID uint8 // 0 = not negotiated; no-op
}

// DiscoverAbsSendTimeID parses an SDP string and returns the extension ID
// assigned to the abs-send-time URI. Returns 0 if the extension is absent.
func DiscoverAbsSendTimeID(sdp string) uint8 {
	m := absSendTimePattern.FindStringSubmatch(sdp)
	if m == nil {
		return 0
	}
	id, err := strconv.Atoi(m[1])
	if err != nil || id < 1 || id > 14 {
		return 0
	}
	return uint8(id)
}

// SetAbsSendTimeID configures the extension slot for abs-send-time patching.
// id == 0 disables patching (default).
func (e *ExtensionMapper) SetAbsSendTimeID(id uint8) {
	e.absSendTimeID = id
}

// DiscoverAbsCaptureTimeID parses an SDP string and returns the extension ID
// assigned to the abs-capture-time URI. Returns 0 if the extension is absent.
func DiscoverAbsCaptureTimeID(sdp string) uint8 {
	m := absCaptureTimePattern.FindStringSubmatch(sdp)
	if m == nil {
		return 0
	}
	id, err := strconv.Atoi(m[1])
	if err != nil || id < 1 || id > 14 {
		return 0
	}
	return uint8(id)
}

// SetAbsCaptureTimeID configures the extension slot for abs-capture-time.
// id == 0 means the extension was not negotiated.
func (e *ExtensionMapper) SetAbsCaptureTimeID(id uint8) {
	e.absCaptureTimeID = id
}

// HasAbsCaptureTime reports whether abs-capture-time was negotiated in the SDP
// answer and is therefore expected to be present in forwarded packets.
func (e *ExtensionMapper) HasAbsCaptureTime() bool {
	return e.absCaptureTimeID != 0
}

// MutatesPackets reports whether this mapper patches outbound RTP packets.
func (e *ExtensionMapper) MutatesPackets() bool {
	return e.absSendTimeID != 0 || e.absCaptureTimeID != 0
}

// PatchAbsSendTime writes the current wall-clock time as the middle 24 bits of
// an NTP timestamp (6-bit seconds, 18-bit fraction) into the packet's
// abs-send-time header extension. No-op if the extension ID is 0.
//
// Hot-path cost: ~50 ns (time.Now + 2 arithmetic ops + pkt.SetExtension).
func (e *ExtensionMapper) PatchAbsSendTime(pkt *rtp.Packet) {
	if e.absSendTimeID == 0 {
		return
	}
	buf := encodeAbsSendTime(time.Now())
	_ = pkt.SetExtension(e.absSendTimeID, buf[:])
}

// PatchAbsCaptureTime writes the publisher-derived capture clock into the
// negotiated abs-capture-time extension. It returns true only when a packet was
// patched. The fixed-size payload avoids allocation on the forwarding path.
func (e *ExtensionMapper) PatchAbsCaptureTime(pkt *rtp.Packet, captureTime time.Time) bool {
	if e.absCaptureTimeID == 0 || captureTime.IsZero() {
		return false
	}
	var payload [8]byte
	seconds := uint64(captureTime.Unix()) + ntpEpochOffset
	fraction := uint64(captureTime.Nanosecond()) * (uint64(1) << 32) / uint64(time.Second)
	binary.BigEndian.PutUint64(payload[:], seconds<<32|fraction)
	if err := pkt.SetExtension(e.absCaptureTimeID, payload[:]); err != nil {
		return false
	}
	return true
}

func encodeAbsSendTime(now time.Time) [3]byte {
	secs := uint64(now.Unix()) + ntpEpochOffset
	fracBits := uint64(now.Nanosecond()) * (1 << 18) / 1_000_000_000
	value := uint32((secs&0x3F)<<18 | (fracBits & 0x3FFFF))
	return [3]byte{byte(value >> 16), byte(value >> 8), byte(value)}
}
