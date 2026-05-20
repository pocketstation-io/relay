package signaling

type MessageType string

const (
	TypePublish   MessageType = "PUBLISH"
	TypeSubscribe MessageType = "SUBSCRIBE"
	TypeIce       MessageType = "ICE"
	TypeLeave     MessageType = "LEAVE"
	TypeSDPAnswer MessageType = "SDP_ANSWER"
	TypeRoomState MessageType = "ROOM_STATE"
	TypeError     MessageType = "ERROR"
)

type ClientMessage struct {
	Type      MessageType `json:"type"`
	RoomID    string      `json:"room_id,omitempty"`
	Token     string      `json:"token,omitempty"`
	SDPOffer  string      `json:"sdp_offer,omitempty"`
	Candidate string      `json:"candidate,omitempty"`
}

type ServerMessage struct {
	Type          MessageType `json:"type"`
	SDPAnswer     string      `json:"sdp_answer,omitempty"`
	Candidate     string      `json:"candidate,omitempty"`
	SourceActive  bool        `json:"source_active,omitempty"`
	ListenerCount int         `json:"listener_count,omitempty"`
	Codec         string      `json:"codec,omitempty"`
	Code          string      `json:"code,omitempty"`
	Message       string      `json:"message,omitempty"`
}
