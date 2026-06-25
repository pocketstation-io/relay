package signaling

// ErrorCode identifies the category of a signaling ERROR message.
// ServerMessage.Code carries this value as a string in JSON; the string type
// is kept for wire compatibility with existing clients that match on the raw
// string value.
type ErrorCode string

const (
	ErrCodeBadToken              ErrorCode = "bad_token"
	ErrCodeAlreadyJoined         ErrorCode = "already_joined"
	ErrCodeRoleMismatch          ErrorCode = "role_mismatch"
	ErrCodePCError               ErrorCode = "pc_error"
	ErrCodeSDPError              ErrorCode = "sdp_error"
	ErrCodeICEError              ErrorCode = "ice_error"
	ErrCodeTrackError            ErrorCode = "track_error"
	ErrCodeListenerLimitExceeded ErrorCode = "listener_limit_exceeded"
	ErrCodeRoomLimitExceeded     ErrorCode = "room_limit_exceeded"
	ErrCodeNotJoined             ErrorCode = "not_joined"
	ErrCodeBadRequest            ErrorCode = "bad_request"
	ErrCodeUnknownType           ErrorCode = "unknown_type"
)
