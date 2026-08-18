package callback_test

import (
	"net"
	"net/http"
	"net/http/httptest"
)

// newIPv4Server creates an httptest.Server bound to 127.0.0.1 only.
// httptest.NewServer prefers ::1 (IPv6), which is blocked in some macOS
// sandbox environments. Tests in this package must use this helper.
func newIPv4Server(handler http.Handler) *httptest.Server {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		panic("newIPv4Server: " + err.Error())
	}
	ts := httptest.NewUnstartedServer(handler)
	ts.Listener = l
	ts.Start()
	return ts
}
