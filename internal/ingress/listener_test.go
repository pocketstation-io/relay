package ingress

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/stun/v3"
	pionTurn "github.com/pion/turn/v4"
	relayTurn "github.com/pocketstation-io/relay/internal/turn"
)

const proxyHeader = "PROXY TCP4 203.0.113.7 192.0.2.1 41234 443\r\n"

func listenIngress(t *testing.T, limit int, timeout time.Duration) *Listener {
	t.Helper()
	parent, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := newListener(parent, limit, timeout)
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func dialIngress(t *testing.T, l *Listener, prefix string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp4", l.parent.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, prefix); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestGivenProxyHTTPAndTURNWhenSharingOneSocketThenBothProtocolsAndClientIdentityWork(t *testing.T) {
	l := listenIngress(t, 8, time.Second)
	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.RemoteAddr)
	}), ReadHeaderTimeout: time.Second}
	served := make(chan error, 1)
	go func() { served <- httpServer.Serve(l.HTTP()) }()
	t.Cleanup(func() { _ = httpServer.Close(); <-served })
	secret := []byte("loopback-test-only-shared-TURN-secret")
	turnServer, err := relayTurn.Start(relayTurn.ServerConfig{
		PublicIP: net.ParseIP("127.0.0.1"), Secret: secret,
		UDPBindAddress: "127.0.0.1", TCPListener: l.TURN(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(turnServer.Stop)

	httpConn := dialIngress(t, l, proxyHeader+"GET /healthz HTTP/1.1\r\nHost: localhost\r\nFly-Client-IP: 198.51.100.9\r\nConnection: close\r\n\r\n")
	response, err := http.ReadResponse(bufio.NewReader(httpConn), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || string(body) != "203.0.113.7:41234" {
		t.Fatalf("client identity = %q, error = %v", body, err)
	}

	turnConn := dialIngress(t, l, proxyHeader)
	username, password := relayTurn.Credentials(secret, "loopback", time.Minute)
	client, err := pionTurn.NewClient(&pionTurn.ClientConfig{
		Conn: pionTurn.NewSTUNConn(turnConn), TURNServerAddr: l.parent.Addr().String(),
		Username: username, Password: password, RTO: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	if err := client.Listen(); err != nil {
		t.Fatal(err)
	}
	allocation, err := client.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = allocation.Close() })
	peer, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	if err := peer.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	message := []byte("bounded nonaudible TURN transport fixture")
	if _, err := allocation.WriteTo(message, peer.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 128)
	n, address, err := peer.ReadFrom(buffer)
	if err != nil || !bytes.Equal(buffer[:n], message) {
		t.Fatalf("TURN send: %q, %v", buffer[:n], err)
	}
	if _, err := peer.WriteTo(buffer[:n], address); err != nil {
		t.Fatal(err)
	}
	if err := allocation.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	n, _, err = allocation.ReadFrom(buffer)
	if err != nil || !bytes.Equal(buffer[:n], message) {
		t.Fatalf("TURN return: %q, %v", buffer[:n], err)
	}
}

func TestGivenWebSocketUpgradeWhenRoutedThenPersistentSignalingRemainsBidirectional(t *testing.T) {
	l := listenIngress(t, 4, time.Second)
	upgrader := websocket.Upgrader{}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		kind, payload, err := conn.ReadMessage()
		if err == nil {
			_ = conn.WriteMessage(kind, payload)
		}
	}), ReadHeaderTimeout: time.Second}
	served := make(chan error, 1)
	go func() { served <- server.Serve(l.HTTP()) }()
	t.Cleanup(func() { _ = server.Close(); <-served })
	dialer := websocket.Dialer{NetDial: func(_, _ string) (net.Conn, error) {
		return dialIngress(t, l, proxyHeader), nil
	}, HandshakeTimeout: time.Second}
	conn, _, err := dialer.Dial("ws://"+l.parent.Addr().String()+"/v1/signal", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, []byte("signaling fixture")); err != nil {
		t.Fatal(err)
	}
	_, payload, err := conn.ReadMessage()
	if err != nil || string(payload) != "signaling fixture" {
		t.Fatalf("signaling = %q, %v", payload, err)
	}
}

func TestGivenMalformedOrMissingProxyWhenConnectingThenIngressRejectsWithoutRouting(t *testing.T) {
	for _, input := range []string{
		"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
		"PROXY UNKNOWN\r\nGET / HTTP/1.1\r\n",
		"PROXY TCP4 ::1 192.0.2.1 12 443\r\nGET / HTTP/1.1\r\n",
		"PROXY TCP4 192.0.2.1 192.0.2.2 0 443\r\nGET / HTTP/1.1\r\n",
		"PROXY TCP4 192.0.2.1 192.0.2.2 12 65536\r\nGET / HTTP/1.1\r\n",
		"PROXY " + strings.Repeat("x", 130) + "\r\nGET / HTTP/1.1\r\n",
		proxyHeader + "\x00\x01\x00\x00bad!",
	} {
		l := listenIngress(t, 2, 100*time.Millisecond)
		conn := dialIngress(t, l, input)
		if n, err := conn.Read(make([]byte, 1)); err == nil || n != 0 {
			t.Fatalf("malformed ingress was accepted: %q", input)
		}
		_ = l.Close()
	}
}

func TestGivenBoundedIngressWhenConnectionsStallThenTimeoutAndCapacityReleaseSockets(t *testing.T) {
	l := listenIngress(t, 1, 150*time.Millisecond)
	first := dialIngress(t, l, "PROXY ")
	// The first accepted socket must occupy the sole slot before the next dial.
	deadline := time.Now().Add(time.Second)
	for {
		l.mu.Lock()
		count := len(l.conns)
		l.mu.Unlock()
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first connection not accepted")
		}
		time.Sleep(time.Millisecond)
	}
	second := dialIngress(t, l, "")
	if _, err := second.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection limit was not enforced")
	}
	if _, err := first.Read(make([]byte, 1)); err == nil {
		t.Fatal("header deadline was not enforced")
	}
	third := dialIngress(t, l, proxyHeader+"GET / HTTP/1.1\r\n\r\n")
	accepted, err := l.HTTP().Accept()
	if err != nil {
		t.Fatal(err)
	}
	_ = accepted.Close()
	if _, err := third.Read(make([]byte, 1)); err == nil {
		t.Fatal("accepted connection did not close")
	}
}

func TestGivenClosedTURNRouteWhenHTTPConnectsThenSiblingWorksAndShutdownJoinsPendingReads(t *testing.T) {
	l := listenIngress(t, 4, time.Second)
	_ = l.TURN().Close()
	conn := dialIngress(t, l, proxyHeader+"GET / HTTP/1.1\r\n\r\n")
	accepted, err := l.HTTP().Accept()
	if err != nil {
		t.Fatal(err)
	}
	_ = dialIngress(t, l, "PROXY ")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("active HTTP connection survived shutdown")
	}
	if _, err := accepted.Write([]byte("closed")); err == nil {
		t.Fatal("accepted HTTP connection survived shutdown")
	}
	if _, err := l.HTTP().Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed accept = %v", err)
	}
	l.mu.Lock()
	remaining := len(l.conns)
	l.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("%d connections survived shutdown", remaining)
	}
}

func TestGivenFragmentedIPv6ProxyWhenSTUNArrivesThenBytesAndMappedClientArePreserved(t *testing.T) {
	l := listenIngress(t, 2, time.Second)
	conn := dialIngress(t, l, "PROXY TCP6 2001:db8::1 2001:db8::2 51234 443\r")
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	if _, err := conn.Write(append([]byte{'\n'}, request.Raw...)); err != nil {
		t.Fatal(err)
	}
	accepted, err := l.TURN().Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	if accepted.RemoteAddr().String() != "[2001:db8::1]:51234" {
		t.Fatal(accepted.RemoteAddr())
	}
	buffer := make([]byte, len(request.Raw))
	if _, err := io.ReadFull(accepted, buffer); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buffer, request.Raw) {
		t.Fatal("protocol bytes were consumed by routing")
	}
}
