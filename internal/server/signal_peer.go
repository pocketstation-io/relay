package server

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/media/clocklineage"
	"github.com/pocketstation-io/relay/internal/notifications/webhook"
	"github.com/pocketstation-io/relay/internal/session"
	"github.com/pocketstation-io/relay/internal/signaling"
)

const (
	wsKeepAlivePingInterval  = 30 * time.Second
	wsKeepAliveTimeout       = 90 * time.Second
	maxSignalingMessageBytes = 64 * 1024
	maxPendingICECandidates  = 64
)

// signalPeer owns one WebSocket signaling and WebRTC peer connection.
// wmu serialises WebSocket writes from the read loop and Pion's ICE goroutine.
type signalPeer struct {
	id  string
	srv *Server

	wmu  sync.Mutex
	conn *websocket.Conn

	pc         *webrtc.PeerConnection
	lineage    *clocklineage.Registry
	room       *session.RelaySession
	role       auth.Role
	busID      session.BusID // bus this session publishes to or subscribes from
	pendingICE []string      // candidates received before PUBLISH/SUBSCRIBE

	// done is closed by cleanup() to signal teardown to scoped goroutines.
	done             chan struct{}
	doneOnce         sync.Once
	handshakeOnce    sync.Once
	releaseHandshake func()

	// slotReserved and subscriptionRegistered guard the subscription lifecycle.
	// slotReserved: true between TryReserveSlot and ReleaseSlot.
	// subscriptionRegistered: true after AddSubscription succeeds (deferred to DTLS-complete).
	slotReserved           atomic.Bool
	subscriptionRegistered atomic.Bool
}

func (s *signalPeer) run() {
	s.conn.SetReadLimit(maxSignalingMessageBytes)
	_ = s.conn.SetReadDeadline(time.Now().Add(wsKeepAliveTimeout))
	s.conn.SetPongHandler(func(string) error {
		return s.conn.SetReadDeadline(time.Now().Add(wsKeepAliveTimeout))
	})

	done := make(chan struct{})
	defer close(done)
	go func() {
		ticker := time.NewTicker(wsKeepAlivePingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.wmu.Lock()
				err := s.conn.WriteMessage(websocket.PingMessage, nil)
				s.wmu.Unlock()
				if err != nil {
					return
				}
			case <-done:
				return
			}
		}
	}()

	for {
		var msg signaling.ClientMessage
		if err := s.conn.ReadJSON(&msg); err != nil {
			return
		}
		_ = s.conn.SetReadDeadline(time.Now().Add(wsKeepAliveTimeout))
		switch msg.Type {
		case signaling.TypePublish, signaling.TypeSubscribe:
			s.handleJoin(msg)
			if s.pc != nil {
				s.finishHandshake()
			}
		case signaling.TypeIce:
			s.handleICE(msg)
		case signaling.TypeSDPAnswer:
			s.handleSDPAnswer(msg)
		case signaling.TypeKeyExchange:
			s.handleKeyExchange(msg)
		case signaling.TypeLatencyReport:
			s.handleLatencyReport(msg)
		case signaling.TypeLeave:
			return
		default:
			s.sendError(signaling.ErrCodeUnknownType, "unknown message type: "+string(msg.Type))
		}
	}
}

func (s *signalPeer) cleanup() {
	s.finishHandshake()
	s.doneOnce.Do(func() { close(s.done) })

	if s.pc != nil {
		_ = s.pc.Close()
	}
	if s.room != nil {
		switch s.role {
		case auth.RoleSubscriber:
			if s.slotReserved.Swap(false) {
				s.room.ReleaseSlot()
			}
			if s.subscriptionRegistered.Load() {
				s.room.RemoveSubscription(s.id)
				s.srv.Metrics.ListenerCount.Add(-1)
				s.srv.broadcastSessionState(s.room)
			} else {
				s.room.RemoveSubscription(s.id)
			}
		case auth.RoleSource:
			s.srv.broadcastSessionState(s.room)
		}
		s.srv.webhookDispatcher.Send(webhook.Event{
			Type:      webhook.EventSessionEnded,
			RoomID:    s.room.ID,
			SessionID: s.id,
		})
	}
	slog.Info("session cleaned up", "session_id", s.id)
}

func (s *signalPeer) finishHandshake() {
	s.handshakeOnce.Do(func() {
		if s.releaseHandshake != nil {
			s.releaseHandshake()
		}
	})
}

func (s *signalPeer) closeConn() {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_ = s.conn.WriteMessage(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutdown"),
	)
	_ = s.conn.Close()
}

func (s *signalPeer) handleICE(msg signaling.ClientMessage) {
	if s.pc == nil || s.pc.RemoteDescription() == nil {
		if !s.queuePendingICE(msg.Candidate) {
			s.sendError(signaling.ErrCodeICEError, "pending ICE candidate limit exceeded")
		}
		return
	}
	if err := s.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: msg.Candidate}); err != nil {
		s.sendError(signaling.ErrCodeICEError, err.Error())
	}
}

func (s *signalPeer) queuePendingICE(candidate string) bool {
	if len(s.pendingICE) >= maxPendingICECandidates {
		return false
	}
	s.pendingICE = append(s.pendingICE, candidate)
	return true
}

func (s *signalPeer) handleSDPAnswer(msg signaling.ClientMessage) {
	if s.pc == nil || s.pc.LocalDescription() == nil || s.pc.RemoteDescription() != nil {
		s.sendError(signaling.ErrCodeSDPError, "SDP_ANSWER is not expected")
		return
	}
	answer := webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: msg.SDPAnswer}
	if err := s.pc.SetRemoteDescription(answer); err != nil {
		s.sendError(signaling.ErrCodeSDPError, "failed to set remote answer")
		return
	}
	for _, candidate := range s.pendingICE {
		_ = s.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: candidate})
	}
	s.pendingICE = nil
}

func (s *signalPeer) handleLatencyReport(msg signaling.ClientMessage) {
	if s.room == nil {
		s.sendError(signaling.ErrCodeNotJoined, "join a session before sending LATENCY_REPORT")
		return
	}
	rpt := msg.LatencyReport
	if rpt == nil {
		s.sendError(signaling.ErrCodeBadRequest, "LATENCY_REPORT requires a latency_report payload")
		return
	}
	s.room.RecordLatency(rpt.CaptureMs, rpt.EncodeMs, rpt.RelayRttMs, rpt.JitterBufferMs, rpt.DecodeMs, rpt.PacketLossPct)
	if rpt.CaptureMs > 0 {
		s.srv.webhookDispatcher.Send(webhook.Event{
			Type:      webhook.EventUtteranceDetected,
			RoomID:    s.room.ID,
			SessionID: s.id,
		})
	}
}

// busRoleFor maps a well-known BusID to its BusRole. Unknown IDs default to BusRoleVoice.
func busRoleFor(id session.BusID) session.BusRole {
	switch id {
	case "music":
		return session.BusRoleMusic
	case "agent_voice", "agent_output":
		return session.BusRoleAgentOutput
	case "events":
		return session.BusRoleEvents
	case "monitor":
		return session.BusRoleMonitor
	default:
		return session.BusRoleVoice
	}
}

func (s *signalPeer) send(msg signaling.ServerMessage) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	err := s.conn.WriteJSON(msg)
	if err != nil {
		slog.Warn("websocket write failed", "session_id", s.id, "error", err)
	}
	return err
}

func (s *signalPeer) sendError(code signaling.ErrorCode, message string) {
	s.send(signaling.ServerMessage{Type: signaling.TypeError, Code: string(code), Message: message})
}
