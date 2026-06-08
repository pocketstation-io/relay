package server

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// startKeepalive sends periodic WebSocket pings on conn and resets the read
// deadline on each incoming pong. It exits when done is closed.
//
// Browsers respond automatically to server pings with a pong. Without this,
// Fly.io's proxy and home NAT devices silently drop idle TCP connections after
// their inactivity timeout (~1 hour).
//
// The goroutine this function spawns exits when done is closed; the caller is
// responsible for closing done (typically by closing a channel with defer).
func startKeepalive(conn *websocket.Conn, wmu *sync.Mutex, done <-chan struct{}) {
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(wsKeepAliveTimeout))
	})
	go func() {
		ticker := time.NewTicker(wsKeepAlivePingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				wmu.Lock()
				err := conn.WriteMessage(websocket.PingMessage, nil)
				wmu.Unlock()
				if err != nil {
					return
				}
			case <-done:
				return
			}
		}
	}()
}
