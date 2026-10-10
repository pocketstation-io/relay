package server

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pocketstation-io/relay/internal/auth"
	"github.com/pocketstation-io/relay/internal/media/ingress"
	"github.com/pocketstation-io/relay/internal/notifications/callback"
	"github.com/pocketstation-io/relay/internal/session"
)

const opusIngressSubprotocol = "pks-opus-v1"
const opusIngressIdleTimeout = 15 * time.Second
const opusIngressAuthorityInterval = 2 * time.Second

var errSourceCapabilityExpired = errors.New("source capability expired")

// audioIngress is a transport adapter, not a WebRTC publisher or PCM mixer.
// Browser subscriptions still use the existing Relay WebRTC media engine.
func (s *Server) audioIngress(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	shuttingDown := s.shuttingDown
	s.mu.RUnlock()
	if shuttingDown {
		http.Error(w, "relay shutting down", http.StatusServiceUnavailable)
		return
	}
	if !s.handshakeAdmission.TryAcquire() {
		http.Error(w, "media ingress capacity exceeded", http.StatusServiceUnavailable)
		return
	}
	defer s.handshakeAdmission.Release()
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		http.Error(w, "source capability required", http.StatusUnauthorized)
		return
	}
	encoded := strings.TrimPrefix(header, "Bearer ")
	claims, err := s.verifyCapability(encoded, auth.RoleSource)
	if err != nil {
		status := http.StatusUnauthorized
		if errors.Is(err, errAuthorityUnavailable) {
			status = http.StatusServiceUnavailable
		}
		if errors.Is(err, callback.ErrSessionNotFound) {
			status = http.StatusNotFound
		}
		http.Error(w, "source admission rejected", status)
		return
	}
	sessionID := r.PathValue("id")
	busID := r.URL.Query().Get("bus")
	if sessionID != claims.SessionID || !validPublishIdentifier(busID) || !claims.AllowsBus(busID) {
		http.Error(w, "source scope rejected", http.StatusForbidden)
		return
	}
	protocolFound := false
	for _, protocol := range websocket.Subprotocols(r) {
		if protocol == opusIngressSubprotocol {
			protocolFound = true
		}
	}
	if !protocolFound {
		http.Error(w, "pks-opus-v1 required", http.StatusBadRequest)
		return
	}
	if err = s.requireActiveControlSession(r.Context(), sessionID); err != nil {
		http.Error(w, "Session authority unavailable", http.StatusServiceUnavailable)
		return
	}
	// Fence Session creation against shutdown. The network upgrade runs after
	// releasing this lock and must recheck before registering its connection.
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		http.Error(w, "relay shutting down", http.StatusServiceUnavailable)
		return
	}
	relaySession, _, admitted := s.relaySessions.GetOrCreateWithinLimit(sessionID, s.maxRooms)
	if !admitted {
		s.mu.Unlock()
		http.Error(w, "Session capacity exceeded", http.StatusTooManyRequests)
		return
	}
	bus := relaySession.GetOrCreateBus(busID, busRoleFor(busID))
	s.mu.Unlock()
	if bus == nil {
		http.Error(w, "AudioBus capacity exceeded", http.StatusTooManyRequests)
		return
	}
	var ssrcBytes [4]byte
	if _, err = rand.Read(ssrcBytes[:]); err != nil {
		http.Error(w, "source identity unavailable", http.StatusInternalServerError)
		return
	}
	ssrc := binary.BigEndian.Uint32(ssrcBytes[:])
	if ssrc == 0 {
		ssrc = 1
	}
	audioUpgrader := upgrader
	audioUpgrader.Subprotocols = []string{opusIngressSubprotocol}
	audioUpgrader.HandshakeTimeout = 5 * time.Second
	conn, err := audioUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(ingress.MaxMessageBytes)
	source := &opusWebSocketSource{conn: conn, done: make(chan struct{}), bus: bus, expiresAt: claims.ExpiresAt.Time}
	source.packet.Header = rtp.Header{Version: 2, PayloadType: opusPayloadType, SSRC: ssrc}
	id := newID()
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		source.close(websocket.CloseGoingAway, "relay shutting down")
		return
	}
	s.audioIngressConns.Store(id, source)
	s.mu.Unlock()
	defer s.audioIngressConns.Delete(id)
	defer source.close(websocket.CloseNormalClosure, "source ended")
	s.bindControlState(relaySession)
	generation, err := relaySession.AttachSource(busID, busRoleFor(busID), source, func() {
		source.close(websocket.CloseGoingAway, "source replaced")
	})
	if err != nil {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	err = conn.WriteJSON(struct {
		Type       string `json:"type"`
		BusID      string `json:"bus_id"`
		Generation uint64 `json:"source_generation"`
	}{"READY", busID, generation})
	if err != nil {
		return
	}
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		s.monitorAudioIngress(source, relaySession, encoded, claims.ExpiresAt.Time)
	}()
	<-source.done
	<-monitorDone
}

func (s *Server) monitorAudioIngress(source *opusWebSocketSource, relaySession *session.RelaySession, token string, expiresAt time.Time) {
	expiry := time.NewTimer(time.Until(expiresAt))
	defer expiry.Stop()
	authority := time.NewTicker(opusIngressAuthorityInterval)
	defer authority.Stop()
	for {
		select {
		case <-source.done:
			return
		case <-relaySession.Done():
			source.close(websocket.CloseGoingAway, "Session ended")
			return
		case <-expiry.C:
			source.close(websocket.ClosePolicyViolation, "source capability expired")
			return
		case <-authority.C:
			if _, err := s.verifyCapability(token, auth.RoleSource); err != nil {
				source.close(websocket.ClosePolicyViolation, "source authority unavailable")
				return
			}
			select {
			case <-source.done:
				return
			default:
			}
			if err := s.requireActiveControlSession(s.controlSyncContext, relaySession.ID); err != nil {
				source.close(websocket.ClosePolicyViolation, "source placement revoked")
				return
			}
		}
	}
}

// A single existing AudioBus reader owns the fixed frame/packet storage. No
// additional unbounded queue or capture callback exists. The parser and media
// timeline reuse fixed storage; WebSocket IO runs on this network worker.
type opusWebSocketSource struct {
	conn      *websocket.Conn
	done      chan struct{}
	once      sync.Once
	bus       *session.AudioBus
	buffer    [ingress.MaxMessageBytes + 1]byte
	packet    rtp.Packet
	timeline  ingress.Timeline
	expiresAt time.Time
}

func (source *opusWebSocketSource) close(code int, reason string) {
	source.closeBefore(code, reason, time.Now().Add(250*time.Millisecond))
}

func (source *opusWebSocketSource) closeBefore(code int, reason string, deadline time.Time) {
	source.once.Do(func() {
		close(source.done)
		_ = source.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), deadline)
		_ = source.conn.Close()
	})
}

func (source *opusWebSocketSource) ReadRTP() (*rtp.Packet, error) {
	for {
		now := time.Now()
		if !now.Before(source.expiresAt) {
			source.close(websocket.ClosePolicyViolation, "source capability expired")
			return nil, errSourceCapabilityExpired
		}
		deadline := now.Add(opusIngressIdleTimeout)
		if source.expiresAt.Before(deadline) {
			deadline = source.expiresAt
		}
		_ = source.conn.SetReadDeadline(deadline)
		kind, reader, err := source.conn.NextReader()
		if err != nil {
			if !time.Now().Before(source.expiresAt) {
				source.close(websocket.ClosePolicyViolation, "source capability expired")
				return nil, errSourceCapabilityExpired
			}
			source.close(websocket.CloseGoingAway, "source disconnected")
			return nil, err
		}
		if kind != websocket.BinaryMessage {
			source.close(websocket.CloseProtocolError, "binary Opus frame required")
			return nil, ingress.ErrFrame
		}
		n, err := io.ReadFull(reader, source.buffer[:])
		arrivedAt := time.Now()
		if !arrivedAt.Before(source.expiresAt) {
			source.close(websocket.ClosePolicyViolation, "source capability expired")
			return nil, errSourceCapabilityExpired
		}
		if err != io.ErrUnexpectedEOF && err != io.EOF {
			source.close(websocket.CloseProtocolError, "frame size rejected")
			return nil, ingress.ErrFrame
		}
		frame, err := ingress.Parse(source.buffer[:n])
		if err != nil {
			source.close(websocket.CloseProtocolError, "Opus frame rejected")
			return nil, err
		}
		late, err := source.timeline.Admit(frame, arrivedAt)
		if err != nil {
			source.close(websocket.CloseProtocolError, "Opus continuity rejected")
			return nil, err
		}
		if late {
			source.bus.PacketDropCount.Add(1)
			continue
		}
		source.packet.SequenceNumber = uint16(frame.Sequence)
		source.packet.Timestamp = frame.TimestampSamples
		source.packet.Payload = frame.Payload
		return &source.packet, nil
	}
}
