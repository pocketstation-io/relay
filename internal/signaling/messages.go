package signaling

type MessageType string

const (
	TypePublish     MessageType = "PUBLISH"
	TypeSubscribe   MessageType = "SUBSCRIBE"
	TypeIce         MessageType = "ICE"
	TypeLeave       MessageType = "LEAVE"
	TypeSDPAnswer   MessageType = "SDP_ANSWER"
	TypeRoomState   MessageType = "ROOM_STATE"
	TypeError       MessageType = "ERROR"
	// TypeKeyExchange is sent by the source to distribute an SFrame encryption
	// key to all listeners (ADR-014). The relay forwards the message to every
	// listener in the room without reading the key material.
	TypeKeyExchange MessageType = "KEY_EXCHANGE"
	// TypeCodecHint is sent by the relay to the source when RTCP Receiver
	// Reports indicate a change in packet-loss tier (ADR-021). The source
	// adjusts its Opus encoder parameters on receipt.
	TypeCodecHint MessageType = "CODEC_HINT"
	// TypeLatencyReport is sent by source or listener clients to report
	// per-segment latency measurements. The relay accumulates reports and
	// exposes aggregated percentiles via GET /v1/rooms/{id}/latency (spec §13.4).
	TypeLatencyReport MessageType = "LATENCY_REPORT"
)

// LatencyReport is sent by source/listener clients to report per-segment latency.
// All duration fields are in milliseconds. The relay aggregates these into a
// rolling window and exposes P50 percentiles via GET /v1/rooms/{id}/latency.
type LatencyReport struct {
	SessionID      string  `json:"session_id"`
	CaptureMs      float64 `json:"capture_ms"`
	EncodeMs       float64 `json:"encode_ms"`
	RelayRttMs     float64 `json:"relay_rtt_ms"`
	JitterBufferMs float64 `json:"jitter_buffer_ms"`
	DecodeMs       float64 `json:"decode_ms"`
	PacketLossPct  float64 `json:"packet_loss_pct"`
	ClockDriftPpm  float64 `json:"clock_drift_ppm"`
}

// CodecHintPayload carries the encoder parameters for the CODEC_HINT message.
// All fields are advisory: the source applies them best-effort on the next
// encode call; no acknowledgement is sent.
type CodecHintPayload struct {
	// BitRateKbps is the target Opus bitrate in kbps (e.g. 32, 64, 96).
	BitRateKbps int `json:"bitrate_kbps"`
	// Complexity is the Opus complexity parameter (0–10).
	Complexity int `json:"complexity"`
	// Fec enables or disables in-band FEC.
	Fec bool `json:"fec"`
	// Dtx enables or disables discontinuous transmission.
	Dtx bool `json:"dtx"`
}

type ClientMessage struct {
	Type      MessageType `json:"type"`
	RoomID    string      `json:"room_id,omitempty"`
	Token     string      `json:"token,omitempty"`
	SDPOffer  string      `json:"sdp_offer,omitempty"`
	Candidate string      `json:"candidate,omitempty"`
	// SFrameKey is the base64-encoded SFrame key material for KEY_EXCHANGE messages.
	// The relay forwards this to all room listeners without reading it.
	SFrameKey string `json:"sframe_key,omitempty"`
	// LatencyReport is populated on LATENCY_REPORT messages sent by clients to
	// report per-segment latency measurements (spec §13.4).
	LatencyReport *LatencyReport `json:"latency_report,omitempty"`
}

type ServerMessage struct {
	Type          MessageType       `json:"type"`
	SDPAnswer     string            `json:"sdp_answer,omitempty"`
	Candidate     string            `json:"candidate,omitempty"`
	SourceActive  bool              `json:"source_active,omitempty"`
	ListenerCount int               `json:"listener_count,omitempty"`
	Codec         string            `json:"codec,omitempty"`
	Code          string            `json:"code,omitempty"`
	Message       string            `json:"message,omitempty"`
	// SFrameKey is populated on KEY_EXCHANGE forwards from source to listeners.
	SFrameKey     string            `json:"sframe_key,omitempty"`
	// CodecHint is populated on CODEC_HINT messages from relay to source.
	CodecHint     *CodecHintPayload `json:"codec_hint,omitempty"`
}
