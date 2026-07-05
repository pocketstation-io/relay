package signaling

type MessageType string

const (
	TypePublish   MessageType = "PUBLISH"
	TypeSubscribe MessageType = "SUBSCRIBE"
	TypeIce       MessageType = "ICE"
	TypeLeave     MessageType = "LEAVE"
	TypeSDPAnswer MessageType = "SDP_ANSWER"
	TypeSessionState MessageType = "SESSION_STATE"
	// TypeRoomState is the deprecated v2.3 wire alias. The relay only sends
	// TypeSessionState, but accepts both on incoming messages for backward
	// compatibility with older clients.
	TypeRoomState MessageType = "ROOM_STATE"
	TypeError     MessageType = "ERROR"
	// TypeKeyExchange is sent by the source to distribute an SFrame encryption
	// key to all subscribers (RELAY-014). The relay forwards verbatim without
	// reading the key material.
	TypeKeyExchange MessageType = "KEY_EXCHANGE"
	// TypeCodecHint is sent by the relay to the source when RTCP Receiver Reports
	// indicate a change in packet-loss tier (RELAY-021).
	TypeCodecHint MessageType = "CODEC_HINT"
	// TypeLatencyReport is sent by source or subscriber clients to report
	// per-segment latency measurements (spec §13.4).
	TypeLatencyReport MessageType = "LATENCY_REPORT"
	// TypeICERestart is sent by the relay to the source when sustained packet
	// loss indicates the ICE path has degraded beyond recovery (spec §10.4).
	TypeICERestart MessageType = "ICE_RESTART"
)

// LatencyReport is sent by source/subscriber clients to report per-segment
// latency. All duration fields carry ms suffixes per CODE_PROTOCOL LAW 1.
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

// CodecHintPayload carries encoder parameters for the CODEC_HINT message.
// All fields are advisory; the source applies them best-effort.
type CodecHintPayload struct {
	BitRateKbps int  `json:"bitrate_kbps"` // target Opus bitrate (e.g. 32, 64, 96)
	Complexity  int  `json:"complexity"`   // Opus complexity 0–10
	Fec         bool `json:"fec"`          // enable in-band FEC
	Dtx         bool `json:"dtx"`          // enable discontinuous transmission
	FrameMs     int  `json:"frame_ms"`     // Opus frame duration: 10 or 20
}

// ClientMessage is sent from a client to the relay over the /v1/signal WebSocket.
//
// v3.0 fields (SessionID, BusID, GraphID) identify the graph session and named
// bus. v2.3 RoomID is kept as a wire alias — the relay treats RoomID as SessionID
// when SessionID is absent, so existing clients continue to work.
type ClientMessage struct {
	Type MessageType `json:"type"`

	// SessionID identifies the RelaySession. Preferred v3.0 field.
	// Falls back to RoomID when absent for backward compatibility.
	SessionID string `json:"session_id,omitempty"`
	// RoomID is the v2.3 wire alias for SessionID. Clients may send either.
	RoomID string `json:"room_id,omitempty"`
	// GraphID is the graph template name (e.g. "room-demo"). Optional.
	GraphID string `json:"graph_id,omitempty"`
	// BusID names the audio bus to publish to or subscribe from.
	// "voice", "music", "agent_voice", "events". Absent or "mix" = all buses.
	BusID string `json:"bus_id,omitempty"`

	Token     string `json:"token,omitempty"`
	SDPOffer  string `json:"sdp_offer,omitempty"`
	Candidate string `json:"candidate,omitempty"`

	// SFrameKey is the base64-encoded SFrame key for KEY_EXCHANGE messages.
	SFrameKey string `json:"sframe_key,omitempty"`
	// LatencyReport is populated on LATENCY_REPORT messages.
	LatencyReport *LatencyReport `json:"latency_report,omitempty"`
	// Public marks the session as a public broadcast channel (spec §3.1).
	Public bool `json:"public,omitempty"`
}

// EffectiveSessionID returns the session/room ID from the message, preferring
// SessionID (v3.0) over RoomID (v2.3) for backward compatibility.
func (m *ClientMessage) EffectiveSessionID() string {
	if m.SessionID != "" {
		return m.SessionID
	}
	return m.RoomID
}

// ServerMessage is sent from the relay to a client over the /v1/signal WebSocket.
type ServerMessage struct {
	Type MessageType `json:"type"`

	SDPAnswer         string `json:"sdp_answer,omitempty"`
	Candidate         string `json:"candidate,omitempty"`
	SourceActive      bool   `json:"source_active,omitempty"`
	SubscriptionCount int    `json:"subscription_count,omitempty"`
	Codec             string `json:"codec,omitempty"`
	Code              string `json:"code,omitempty"`
	Message           string `json:"message,omitempty"`

	// SFrameKey is populated on KEY_EXCHANGE forwards from source to subscribers.
	SFrameKey string `json:"sframe_key,omitempty"`
	// CodecHint is populated on CODEC_HINT messages from relay to source.
	CodecHint *CodecHintPayload `json:"codec_hint,omitempty"`
	// UseTURN is set on ICE_RESTART messages when the relay has an embedded TURN
	// server configured (spec §10.4, RELAY-023).
	UseTURN bool `json:"use_turn,omitempty"`

	// v3.0 session state fields
	SessionID string `json:"session_id,omitempty"`
	BusID     string `json:"bus_id,omitempty"`
}
