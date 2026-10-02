package feedv1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
)

// Client is a standalone consumer for the public feed contract. Its transport
// may be configured for mTLS by the embedding Matter runtime; it never sends a
// principal as an application field or chooses a native route.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func (c Client) Snapshot(ctx context.Context) (SnapshotResponse, error) {
	var response SnapshotResponse
	return response, c.do(ctx, http.MethodGet, "/v1/snapshot", nil, &response, http.StatusOK)
}

func (c Client) Changes(ctx context.Context, cursor Cursor) (ChangesResponse, error) {
	var response ChangesResponse
	path := "/v1/changes?cursor=" + url.QueryEscape(FormatCursor(cursor))
	return response, c.do(ctx, http.MethodGet, path, nil, &response, http.StatusOK, http.StatusConflict)
}

func (c Client) Invoke(ctx context.Context, intent Intent) (Execution, error) {
	var response Execution
	if err := intent.Validate(); err != nil {
		return response, err
	}
	return response, c.do(ctx, http.MethodPost, "/v1/invoke", intent, &response, http.StatusOK)
}

func (c Client) do(ctx context.Context, method, path string, requestValue, responseValue any, expected ...int) error {
	base := strings.TrimRight(c.BaseURL, "/")
	if base == "" || c.HTTPClient == nil {
		return errors.New("matter binding feed client is not configured")
	}
	var body *bytes.Reader
	if requestValue != nil {
		raw, err := json.Marshal(requestValue)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	} else {
		body = bytes.NewReader(nil)
	}
	request, err := http.NewRequestWithContext(ctx, method, base+path, body)
	if err != nil {
		return err
	}
	if requestValue != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	accepted := false
	for _, status := range expected {
		accepted = accepted || response.StatusCode == status
	}
	if !accepted {
		return errors.New("matter binding feed request rejected")
	}
	return decodeStrict(response.Body, responseValue)
}
