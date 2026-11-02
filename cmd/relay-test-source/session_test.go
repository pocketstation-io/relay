package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGivenStandaloneCreateWhenCreatedResponseThenSessionAndSourceCapabilityAreReturned(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/sessions" {
			t.Error("fixture sent an unexpected Session creation request")
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(writer, `{"session_id":"fixture-session","source_token":"fixture-capability"}`)
	}))
	defer server.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	sessionID, sourceToken, err := createSession(server.URL, logger)
	if err != nil {
		t.Fatalf("Session creation failed: %v", err)
	}
	if sessionID != "fixture-session" || sourceToken != "fixture-capability" {
		t.Fatal("Session creation did not return the supplied identity and capability")
	}
}

func TestGivenStandaloneCreateWhenStatusIsNotCreatedThenNoCredentialBodyIsReported(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusBadRequest, http.StatusForbidden, http.StatusInternalServerError} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(status)
				_, _ = io.WriteString(writer, `{"source_token":"fixture-private-capability","error":"fixture-private-body"}`)
			}))
			defer server.Close()
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))

			sessionID, sourceToken, err := createSession(server.URL, logger)
			if err == nil || err.Error() != fmt.Sprintf("unexpected status %d", status) {
				t.Fatal("non-created status did not produce the exact status-only error")
			}
			if sessionID != "" || sourceToken != "" {
				t.Fatal("failed Session creation returned an identity or capability")
			}
		})
	}
}

func TestGivenStandaloneCreateWhenResponseIsInvalidOrOversizedThenOnlyStaticErrorsAreReturned(t *testing.T) {
	valid := `{"session_id":"fixture-session","source_token":"fixture-private-capability"}`
	for _, test := range []struct {
		name  string
		body  string
		error string
	}{
		{"malformed_json", `{"source_token":"fixture-private-capability"`, "decode response: invalid JSON"},
		{"oversized_valid_json", valid + strings.Repeat(" ", 64*1024), "response exceeds 64 KiB"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(writer, test.body)
			}))
			defer server.Close()
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))

			sessionID, sourceToken, err := createSession(server.URL, logger)
			if err == nil || err.Error() != test.error {
				t.Fatal("invalid response did not produce the expected static error")
			}
			if sessionID != "" || sourceToken != "" {
				t.Fatal("invalid Session response returned an identity or capability")
			}
		})
	}
}
