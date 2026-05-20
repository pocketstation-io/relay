package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/room"
	"github.com/pocketstation-io/relay/internal/signaling"
)

func main() {
	s := &Server{
		rooms:     room.NewManager(),
		jwtSecret: []byte(getenv("POCKETSTATION_JWT_SECRET", "dev-secret-change-me")),
	}
	http.HandleFunc("/healthz", s.healthz)
	http.HandleFunc("/v1/rooms", s.createRoom)
	http.HandleFunc("/v1/signal", s.signal)
	log.Println("relay listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// Server is the top-level relay server.
type Server struct {
	rooms     *room.Manager
	jwtSecret []byte
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) createRoom(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := newID()
	s.rooms.GetOrCreate(id)
	sourceToken, err := auth.Sign(s.jwtSecret, id, auth.RoleSource, 15*time.Minute)
	if err != nil {
		http.Error(w, "token error", http.StatusInternalServerError)
		return
	}
	listenerToken, err := auth.Sign(s.jwtSecret, id, auth.RoleListener, 2*time.Hour)
	if err != nil {
		http.Error(w, "token error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"room_id":        id,
		"source_token":   sourceToken,
		"listener_token": listenerToken,
		"qr_url":         "/listen?room=" + id,
	})
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool { return true },
}

func (s *Server) signal(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	sess := &session{
		id:   newID(),
		srv:  s,
		conn: conn,
	}
	defer sess.cleanup()
	sess.run()
}

// session represents one WebSocket peer connection.
// The wmu mutex serialises WebSocket writes, which may come from the read
// loop goroutine (SDP answer, error messages) and from Pion's ICE goroutine
// (candidate notifications) concurrently.
type session struct {
	id  string
	srv *Server

	wmu  sync.Mutex
	conn *websocket.Conn

	pc   *webrtc.PeerConnection
	rm   *room.Room
	role auth.Role
}

func (s *session) run() {
	for {
		var msg signaling.ClientMessage
		if err := s.conn.ReadJSON(&msg); err != nil {
			return
		}
		switch msg.Type {
		case signaling.TypePublish, signaling.TypeSubscribe:
			s.handleJoin(msg)
		case signaling.TypeIce:
			s.handleICE(msg)
		case signaling.TypeLeave:
			return
		default:
			s.sendError("unknown_type", "unknown message type: "+string(msg.Type))
		}
	}
}

func (s *session) cleanup() {
	if s.pc != nil {
		_ = s.pc.Close()
	}
	if s.rm != nil && s.role == auth.RoleListener {
		s.rm.RemoveListener(s.id)
	}
}

// handleJoin processes a PUBLISH or SUBSCRIBE message.
// It verifies the JWT, creates a Pion PeerConnection, performs the SDP
// offer/answer exchange, and wires up ICE candidate forwarding.
func (s *session) handleJoin(msg signaling.ClientMessage) {
	if s.pc != nil {
		s.sendError("already_joined", "session has already joined a room")
		return
	}

	claims, err := auth.Verify(s.srv.jwtSecret, msg.Token)
	if err != nil {
		s.sendError("bad_token", err.Error())
		return
	}

	if msg.Type == signaling.TypePublish && claims.Role != auth.RoleSource {
		s.sendError("role_mismatch", "PUBLISH requires a source token")
		return
	}
	if msg.Type == signaling.TypeSubscribe && claims.Role != auth.RoleListener {
		s.sendError("role_mismatch", "SUBSCRIBE requires a listener token")
		return
	}

	rm := s.srv.rooms.GetOrCreate(claims.RoomID)
	s.rm = rm
	s.role = claims.Role

	pc, err := s.newPeerConnection()
	if err != nil {
		s.sendError("pc_error", "failed to create peer connection")
		return
	}
	s.pc = pc

	switch msg.Type {
	case signaling.TypePublish:
		// When the source's track arrives (after ICE connects), set it on the room.
		pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			rm.SetSource(&trackSource{track: track})
		})

	case signaling.TypeSubscribe:
		audioTrack, err := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
			"audio", "pocketstation",
		)
		if err != nil {
			s.sendError("track_error", "failed to create audio track")
			return
		}
		if _, err := pc.AddTrack(audioTrack); err != nil {
			s.sendError("track_error", "failed to add audio track to peer connection")
			return
		}
		rm.AddListener(s.id, audioTrack)
	}

	offer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  msg.SDPOffer,
	}
	if err := pc.SetRemoteDescription(offer); err != nil {
		s.sendError("sdp_error", "failed to set remote description")
		return
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		s.sendError("sdp_error", "failed to create answer")
		return
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		s.sendError("sdp_error", "failed to set local description")
		return
	}

	s.send(signaling.ServerMessage{
		Type:      signaling.TypeSDPAnswer,
		SDPAnswer: answer.SDP,
	})
	s.send(signaling.ServerMessage{
		Type:          signaling.TypeRoomState,
		SourceActive:  rm.SourceActive(),
		ListenerCount: rm.ListenerCount(),
		Codec:         "opus",
	})
}

func (s *session) handleICE(msg signaling.ClientMessage) {
	if s.pc == nil {
		s.sendError("not_joined", "join a room before sending ICE candidates")
		return
	}
	if err := s.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: msg.Candidate}); err != nil {
		s.sendError("ice_error", err.Error())
	}
}

// newPeerConnection creates a Pion PeerConnection and wires up ICE candidate
// forwarding to the WebSocket peer.
func (s *session) newPeerConnection() (*webrtc.PeerConnection, error) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{getenv("POCKETSTATION_STUN", "stun:stun.l.google.com:19302")}},
		},
	})
	if err != nil {
		return nil, err
	}
	// Forward locally gathered ICE candidates to the client.
	// OnICECandidate fires from Pion's ICE goroutine, so send holds wmu.
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		s.send(signaling.ServerMessage{
			Type:      signaling.TypeIce,
			Candidate: c.ToJSON().Candidate,
		})
	})
	return pc, nil
}

func (s *session) send(msg signaling.ServerMessage) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_ = s.conn.WriteJSON(msg)
}

func (s *session) sendError(code, message string) {
	s.send(signaling.ServerMessage{Type: signaling.TypeError, Code: code, Message: message})
}

// trackSource adapts *webrtc.TrackRemote to room.Source.
// TrackRemote.ReadRTP returns (pkt, interceptor.Attributes, error); we drop
// the attributes because room.Source does not need them.
type trackSource struct {
	track *webrtc.TrackRemote
}

func (t *trackSource) ReadRTP() (*rtp.Packet, error) {
	pkt, _, err := t.track.ReadRTP()
	return pkt, err
}

// newID returns a random UUID v4-formatted identifier.
// Uses crypto/rand; panics if the OS PRNG is unavailable (should never happen).
func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits RFC 4122
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
