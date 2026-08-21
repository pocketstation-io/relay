package callback

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pocketstation-io/relay/internal/session"
)

const callbackTestSecret = "0123456789abcdef0123456789abcdef"

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestGivenControlStateWhenPushedThenRequestIsAuthenticatedFullReplacement(t *testing.T) {
	client, err := NewClient("https://control.example/base", callbackTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	var captured *http.Request
	var body session.ControlState
	client.http.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		captured = request
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"changed":true}`)),
			Header:     make(http.Header),
		}, nil
	})
	state := session.ControlState{
		ContractVersion: session.RelayStateContractVersion,
		SessionID:       "session-1",
		RelayEpoch:      "relay-1",
		Revision:        7,
		ObservedAt:      time.Now().UTC(),
		Buses: []session.ControlBusState{
			{BusID: "application", Role: "application", SourceActive: true, SourceGeneration: 2},
		},
		Subscriptions: []session.ControlSubscriptionState{{SubscriberID: "receiver-1", BusID: "application"}},
	}
	if err := client.PushState(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if captured.Method != http.MethodPut || captured.URL.Path != "/base/v1/internal/sessions/session-1/relay-state" {
		t.Fatalf("unexpected request %s %s", captured.Method, captured.URL.Path)
	}
	if captured.Header.Get("X-PocketStation-Internal-Secret") != callbackTestSecret {
		t.Fatal("internal authentication header missing")
	}
	if body.Revision != 7 || len(body.Buses) != 1 || len(body.Subscriptions) != 1 {
		t.Fatalf("incomplete state body %#v", body)
	}
}

func TestGivenInvalidCallbackConfigurationWhenClientIsCreatedThenConstructionFailsClosed(t *testing.T) {
	for _, input := range []struct{ url, secret string }{
		{"", callbackTestSecret},
		{"ftp://control.example", callbackTestSecret},
		{"https://control.example?token=x", callbackTestSecret},
		{"https://control.example", "short"},
	} {
		if _, err := NewClient(input.url, input.secret); err == nil {
			t.Fatalf("invalid config accepted: %#v", input)
		}
	}
}

func TestGivenOversizedFailureResponseWhenStateIsPushedThenDeliveryFails(t *testing.T) {
	client, _ := NewClient("https://control.example", callbackTestSecret)
	client.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", maxResponseBodyBytes+1))), Header: make(http.Header)}, nil
	})
	if err := client.PushState(context.Background(), session.ControlState{SessionID: "session-1"}); err == nil {
		t.Fatal("oversized failure response accepted")
	}
}
