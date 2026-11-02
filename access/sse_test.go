package access

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type flushRecorder struct {
	*httptest.ResponseRecorder
	mu      sync.Mutex
	flushed chan struct{}
	text    string
}

func (recorder *flushRecorder) Flush() {
	recorder.mu.Lock()
	if recorder.text == "" {
		recorder.text = recorder.Body.String()
		close(recorder.flushed)
	}
	recorder.mu.Unlock()
}

func TestGivenSSEConnectionWhenOpenedThenRevisionAndFullStateAreEmitted(t *testing.T) {
	store := NewSessionStore(sessionTestSecret)
	created, _ := store.Create()
	request := httptest.NewRequest(http.MethodGet, "/v1/sessions/"+created.ID()+"/events", nil)
	request.Header.Set("Authorization", "Bearer "+created.SourceToken())
	ctx, cancel := context.WithCancel(request.Context())
	request = request.WithContext(ctx)
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder(), flushed: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		testMux(store, HandlerConfig{}).ServeHTTP(recorder, request)
		close(done)
	}()
	select {
	case <-recorder.flushed:
	case <-time.After(time.Second):
		t.Fatal("initial SSE state was not flushed")
	}
	cancel()
	<-done
	recorder.mu.Lock()
	text := recorder.text
	recorder.mu.Unlock()
	if !strings.Contains(text, "id: 1\n") || !strings.Contains(text, "event: session.state\n") || !strings.Contains(text, `"required_buses":["application","microphone"]`) {
		t.Fatalf("unexpected SSE event:\n%s", text)
	}
}
