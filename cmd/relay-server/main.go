package main

import (
    "encoding/json"
    "log"
    "net/http"
    "os"
    "time"

    "github.com/google/uuid"
    "github.com/gorilla/websocket"
    "github.com/pocketstation-io/relay/internal/auth"
    "github.com/pocketstation-io/relay/internal/room"
    "github.com/pocketstation-io/relay/internal/signaling"
)

type Server struct { rooms *room.Manager; jwtSecret []byte }

func main() {
    s := &Server{rooms: room.NewManager(), jwtSecret: []byte(getenv("POCKETSTATION_JWT_SECRET", "dev-secret-change-me"))}
    http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request){ _, _ = w.Write([]byte("ok")) })
    http.HandleFunc("/v1/rooms", s.createRoom)
    http.HandleFunc("/v1/signal", s.signal)
    log.Println("relay listening on :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}

func getenv(k,d string) string { if v:=os.Getenv(k); v!="" { return v }; return d }

func (s *Server) createRoom(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost { http.Error(w, "method", http.StatusMethodNotAllowed); return }
    id := uuid.NewString()
    s.rooms.GetOrCreate(id)
    sourceToken, _ := auth.Sign(s.jwtSecret, id, auth.RoleSource, 15*time.Minute)
    listenerToken, _ := auth.Sign(s.jwtSecret, id, auth.RoleListener, 2*time.Hour)
    _ = json.NewEncoder(w).Encode(map[string]string{"room_id": id, "source_token": sourceToken, "listener_token": listenerToken, "qr_url": "/listen?room="+id})
}

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func (s *Server) signal(w http.ResponseWriter, r *http.Request) {
    c, err := upgrader.Upgrade(w,r,nil); if err != nil { return }
    defer c.Close()
    for {
        var msg signaling.ClientMessage
        if err := c.ReadJSON(&msg); err != nil { return }
        switch msg.Type {
        case signaling.TypePublish, signaling.TypeSubscribe:
            claims, err := auth.Verify(s.jwtSecret, msg.Token)
            if err != nil { _ = c.WriteJSON(signaling.ServerMessage{Type: signaling.TypeError, Code:"bad_token", Message:err.Error()}); continue }
            rm := s.rooms.GetOrCreate(claims.RoomID)
            _ = c.WriteJSON(signaling.ServerMessage{Type: signaling.TypeRoomState, SourceActive: false, ListenerCount: rm.ListenerCount(), Codec: "opus"})
            // TODO Phase 1: create Pion PeerConnection, set remote SDP, answer, ICE exchange.
            _ = c.WriteJSON(signaling.ServerMessage{Type: signaling.TypeError, Code:"not_implemented", Message:"WebRTC SDP handling scaffolded; implement in Phase 1 issue"})
        default:
            _ = c.WriteJSON(signaling.ServerMessage{Type: signaling.TypeError, Code:"unknown_type", Message:"unknown message type"})
        }
    }
}
