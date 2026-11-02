// Package callback exchanges authenticated RelaySession authority and state
// with the control plane.
package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pocketstation-io/relay/access"
	"github.com/pocketstation-io/relay/access/storage"
	"github.com/pocketstation-io/relay/internal/session"
)

const (
	httpTimeout          = 5 * time.Second
	maxResponseBodyBytes = 4096
)

var (
	ErrInvalidConfiguration = errors.New("invalid control-plane callback configuration")
	ErrWriterFenced         = errors.New("media writer fenced")
	ErrSessionNotFound      = errors.New("control-plane session not found")
)

// Client verifies durable Session authority and sends complete Relay state.
type Client struct {
	baseURL string
	secret  string
	http    http.Client
}

func NewClient(baseURL, secret string) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.Fragment != "" || len(secret) < 32 {
		return nil, ErrInvalidConfiguration
	}
	return &Client{
		baseURL: strings.TrimRight(parsed.String(), "/"),
		secret:  secret,
		http:    http.Client{Timeout: httpTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}, nil
}

// RequireActiveSession verifies that the control plane still authorizes a
// RelaySession before Relay allocates signaling or media state for it. The
// caller supplies the bounded admission context; this request is never made on
// the RTP forwarding path.
func (client *Client) RequireActiveSession(ctx context.Context, sessionID string) error {
	endpoint := client.baseURL + "/v1/internal/sessions/" + url.PathEscape(sessionID) + "/authority"
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-PocketStation-Internal-Secret", client.secret)
	response, err := client.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: authority status %d", ErrSessionNotFound, response.StatusCode)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("control-plane authority status %d", response.StatusCode)
	}
	return nil
}

// PushState replaces the control-plane's Relay-owned state. Retrying the same
// epoch and revision is safe; the receiver acknowledges it without mutation.
func (client *Client) PushState(ctx context.Context, state session.ControlState) error {
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	endpoint := client.baseURL + "/v1/internal/sessions/" + url.PathEscape(state.SessionID) + "/relay-state"
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-PocketStation-Internal-Secret", client.secret)
	response, err := client.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBodyBytes+1))
	if readErr != nil {
		return readErr
	}
	if len(responseBody) > maxResponseBodyBytes {
		return fmt.Errorf("callback response exceeded %d bytes", maxResponseBodyBytes)
	}
	if response.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: callback status %d", ErrSessionNotFound, response.StatusCode)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("control-plane callback status %d", response.StatusCode)
	}
	return nil
}

// Metadata pins verification to the stored Session profile.
func (client *Client) Metadata(ctx context.Context, id string) (access.AuthorityMetadata, error) {
	var result access.AuthorityMetadata
	err := client.exchange(ctx, http.MethodGet, id, "authority", nil, &result)
	if err == nil && result.SessionID != id {
		err = errors.New("authority returned another Session")
	}
	return result, err
}
func (client *Client) AcquireRelayWriter(ctx context.Context, id, epoch string, expected uint64) (uint64, error) {
	var result struct {
		Term uint64 `json:"writer_term"`
	}
	err := client.exchange(ctx, http.MethodPost, id, "relay-writer", map[string]any{"relay_epoch": epoch, "expected_term": expected}, &result)
	if err == nil && result.Term == 0 {
		err = errors.New("authority returned no media term")
	}
	return result.Term, err
}
func (client *Client) PushAccessState(ctx context.Context, state storage.RelayState) error {
	return client.exchange(ctx, http.MethodPut, state.SessionID, "relay-state", state, nil)
}
func (client *Client) exchange(ctx context.Context, method, id, operation string, input, output any) error {
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil {
			return err
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, client.baseURL+"/v1/internal/sessions/"+url.PathEscape(id)+"/"+operation, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("X-PocketStation-Internal-Secret", client.secret)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBodyBytes+1))
	if err != nil {
		return err
	}
	if len(data) > maxResponseBodyBytes {
		return errors.New("authority response too large")
	}
	if response.StatusCode == http.StatusNotFound {
		return ErrSessionNotFound
	}
	if response.StatusCode == http.StatusConflict {
		return ErrWriterFenced
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("authority HTTP %d", response.StatusCode)
	}
	if output != nil {
		return json.Unmarshal(data, output)
	}
	return nil
}

// Ready checks the managed service without guessing or creating a Session.
func (client *Client) Ready(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.baseURL+"/readyz", nil)
	if err != nil {
		return err
	}
	response, err := client.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("managed Session service is not ready")
	}
	return nil
}
