// Package callback sends complete RelaySession state to the control plane.
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

	"github.com/pocketstation-io/relay/internal/session"
)

const (
	httpTimeout          = 5 * time.Second
	maxResponseBodyBytes = 4096
)

var ErrInvalidConfiguration = errors.New("invalid control-plane callback configuration")

// Client sends authenticated full-state replacement requests.
type Client struct {
	baseURL string
	secret  string
	http    http.Client
}

func NewClient(baseURL, secret string) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" || len(secret) < 32 {
		return nil, ErrInvalidConfiguration
	}
	return &Client{
		baseURL: strings.TrimRight(parsed.String(), "/"),
		secret:  secret,
		http:    http.Client{Timeout: httpTimeout},
	}, nil
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
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("control-plane callback status %d", response.StatusCode)
	}
	return nil
}
