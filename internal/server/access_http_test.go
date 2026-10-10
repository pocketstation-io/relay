package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/pocketstation-io/relay/internal/server"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func cleanupTestRelay(t *testing.T, relay *server.Server) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = relay.Shutdown(ctx)
	})
}

func subscriberForOwner(t *testing.T, endpoint *httptest.Server, sessionID, owner string) string {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, endpoint.URL+"/v1/sessions/"+sessionID+"/subscribe", bytes.NewBufferString(`{"bus_id":"application"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+owner)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("subscribe status=%d", response.StatusCode)
	}
	var body struct {
		Token string `json:"subscriber_token"`
	}
	if err = json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Token == "" {
		t.Fatal("subscriber credential missing")
	}
	return body.Token
}
