// Package ingress shares a TLS-terminating proxy's TCP service between HTTP
// signaling and TURN. It does not terminate TLS or forward media packets.
package ingress

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxConnections = 128
	headerTimeout  = 5 * time.Second
	maxProxyHeader = 108
)

// Listener owns the parent listener, classification workers and all accepted
// connections. HTTP and TURN own accepted connections until they close them.
// Closing either child leaves the sibling available; Close releases both.
// The parent must receive PROXY v1 from a trusted TLS edge on every connection.
// Do not expose this listener as an untrusted raw TCP service.
type Listener struct {
	parent  net.Listener
	http    *route
	turn    *route
	mu      sync.Mutex
	conns   map[*connection]struct{}
	closed  bool
	wg      sync.WaitGroup
	once    sync.Once
	limit   int
	timeout time.Duration
}

// New takes ownership of parent and starts bounded connection classification.
// Both protocols retain their original bytes and the proxy's actual client IP.
func New(parent net.Listener) *Listener {
	return newListener(parent, maxConnections, headerTimeout)
}

func newListener(parent net.Listener, limit int, timeout time.Duration) *Listener {
	l := &Listener{parent: parent, conns: make(map[*connection]struct{}), limit: limit, timeout: timeout}
	l.http = &route{owner: l, pending: make(chan net.Conn, limit), closed: make(chan struct{})}
	l.turn = &route{owner: l, pending: make(chan net.Conn, limit), closed: make(chan struct{})}
	l.wg.Add(1)
	go l.accept()
	return l
}

func (l *Listener) HTTP() net.Listener { return l.http }
func (l *Listener) TURN() net.Listener { return l.turn }

func (l *Listener) accept() {
	defer l.wg.Done()
	for {
		raw, err := l.parent.Accept()
		if err != nil {
			_ = l.http.Close()
			_ = l.turn.Close()
			return
		}
		conn := &connection{Conn: raw, owner: l, reader: bufio.NewReaderSize(raw, 128)}
		l.mu.Lock()
		if l.closed || len(l.conns) >= l.limit {
			l.mu.Unlock()
			_ = raw.Close()
			continue
		}
		l.conns[conn] = struct{}{}
		l.wg.Add(1)
		l.mu.Unlock()
		go l.classify(conn)
	}
}

func (l *Listener) classify(conn *connection) {
	defer l.wg.Done()
	accepted := false
	defer func() {
		if !accepted {
			_ = conn.Close()
		}
	}()
	if err := conn.SetReadDeadline(time.Now().Add(l.timeout)); err != nil {
		return
	}
	remote, err := readProxyAddress(conn.reader)
	if err != nil {
		return
	}
	conn.remote = remote
	prefix, err := conn.reader.Peek(8)
	if err != nil {
		return
	}
	var target *route
	switch {
	case prefix[0]&0xc0 == 0 && string(prefix[4:8]) == "\x21\x12\xa4\x42":
		target = l.turn
	case prefix[0] >= 'A' && prefix[0] <= 'Z':
		// net/http validates the full method, path, protocol and headers.
		target = l.http
	default:
		return
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	select {
	case <-target.closed:
		return
	default:
	}
	select {
	case target.pending <- conn:
		accepted = true
	default:
		// No connection may wait indefinitely for a stalled protocol owner.
	}
}

// Close is idempotent and joins classification workers after closing sockets.
func (l *Listener) Close() error {
	l.once.Do(func() {
		l.mu.Lock()
		l.closed = true
		connections := make([]*connection, 0, len(l.conns))
		for conn := range l.conns {
			connections = append(connections, conn)
		}
		l.mu.Unlock()
		_ = l.parent.Close()
		_ = l.http.Close()
		_ = l.turn.Close()
		for _, conn := range connections {
			_ = conn.Close()
		}
		l.wg.Wait()
	})
	return nil
}

type route struct {
	owner   *Listener
	pending chan net.Conn
	closed  chan struct{}
	mu      sync.Mutex
	once    sync.Once
}

func (r *route) Addr() net.Addr { return r.owner.parent.Addr() }

func (r *route) Accept() (net.Conn, error) {
	select {
	case <-r.closed:
		return nil, net.ErrClosed
	case conn := <-r.pending:
		select {
		case <-r.closed:
			_ = conn.Close()
			return nil, net.ErrClosed
		default:
			return conn, nil
		}
	}
}

func (r *route) Close() error {
	r.once.Do(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		close(r.closed)
		for {
			select {
			case conn := <-r.pending:
				_ = conn.Close()
			default:
				return
			}
		}
	})
	return nil
}

type connection struct {
	net.Conn
	reader *bufio.Reader
	remote net.Addr
	owner  *Listener
	once   sync.Once
}

func (c *connection) Read(p []byte) (int, error) { return c.reader.Read(p) }
func (c *connection) RemoteAddr() net.Addr       { return c.remote }
func (c *connection) Close() error {
	var err error
	c.once.Do(func() {
		err = c.Conn.Close()
		c.owner.mu.Lock()
		delete(c.owner.conns, c)
		c.owner.mu.Unlock()
	})
	return err
}

var errProxyHeader = errors.New("ingress requires a valid PROXY v1 client address")

// PROXY v1 is bounded to 107 bytes plus CRLF by the protocol. UNKNOWN is
// rejected: a missing client identity must not collapse admission onto the edge.
func readProxyAddress(reader *bufio.Reader) (net.Addr, error) {
	header, err := reader.ReadSlice('\n')
	if err != nil || len(header) > maxProxyHeader || !strings.HasSuffix(string(header), "\r\n") {
		return nil, errProxyHeader
	}
	parts := strings.Split(strings.TrimSuffix(string(header), "\r\n"), " ")
	if len(parts) != 6 || parts[0] != "PROXY" || (parts[1] != "TCP4" && parts[1] != "TCP6") {
		return nil, errProxyHeader
	}
	source, destination := net.ParseIP(parts[2]), net.ParseIP(parts[3])
	if source == nil || destination == nil || (source.To4() != nil) != (parts[1] == "TCP4") || (destination.To4() != nil) != (parts[1] == "TCP4") {
		return nil, errProxyHeader
	}
	port, err := strconv.Atoi(parts[4])
	destinationPort, destinationErr := strconv.Atoi(parts[5])
	if err != nil || destinationErr != nil || port < 1 || port > 65535 || destinationPort < 1 || destinationPort > 65535 {
		return nil, errProxyHeader
	}
	return &net.TCPAddr{IP: source, Port: port}, nil
}

var _ net.Listener = (*route)(nil)
var _ io.Closer = (*Listener)(nil)
