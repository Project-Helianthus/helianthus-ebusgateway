// Package releasecheck compares the running add-on version with the newest
// successful public add-on build. It deliberately owns no installation or
// download path.
package releasecheck

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/mod/semver"
)

const (
	addonOwner             = "Project-Helianthus"
	addonRepository        = "helianthus-ha-addon"
	workflowPath           = "build.yml"
	configPath             = "helianthus/config.json"
	defaultAPIBaseURL      = "https://api.github.com"
	defaultRefreshInterval = 6 * time.Hour
	defaultRequestTimeout  = 5 * time.Second
	defaultMaxResponse     = 1 << 20
)

var fullSHA = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)

// Doer is the narrow HTTP dependency used by Checker.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Options configures a bounded public release check. APIBaseURL is injectable
// for deterministic tests; production should leave it empty.
type Options struct {
	CurrentVersion  string
	HTTPClient      Doer
	APIBaseURL      string
	Clock           func() time.Time
	RefreshInterval time.Duration
	RequestTimeout  time.Duration
	MaxResponse     int64
}

// Checker retains the last successful semantic comparison. A failed refresh
// never changes that value, and request-path callers only read this cache.
type Checker struct {
	currentVersion  string
	httpClient      Doer
	apiBaseURL      string
	now             func() time.Time
	refreshInterval time.Duration
	requestTimeout  time.Duration
	maxResponse     int64

	mu            sync.RWMutex
	lastAttempt   time.Time
	refreshing    bool
	hasSuccessful bool
	available     bool
}

// New constructs an inert checker. Call Start from lifecycle composition to
// refresh in the background, or Refresh from a controlled worker/test.
func New(options Options) *Checker {
	apiBaseURL := strings.TrimRight(strings.TrimSpace(options.APIBaseURL), "/")
	if apiBaseURL == "" {
		apiBaseURL = defaultAPIBaseURL
	}
	now := options.Clock
	if now == nil {
		now = time.Now
	}
	refreshInterval := options.RefreshInterval
	if refreshInterval <= 0 {
		refreshInterval = defaultRefreshInterval
	}
	requestTimeout := options.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = defaultRequestTimeout
	}
	maxResponse := options.MaxResponse
	if maxResponse <= 0 {
		maxResponse = defaultMaxResponse
	}
	client := options.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: requestTimeout}
	}
	return &Checker{
		currentVersion:  normalizeVersion(options.CurrentVersion),
		httpClient:      client,
		apiBaseURL:      apiBaseURL,
		now:             now,
		refreshInterval: refreshInterval,
		requestTimeout:  requestTimeout,
		maxResponse:     maxResponse,
	}
}

// Start performs an initial background refresh and then periodically refreshes
// the cache. It never blocks API startup and stops with ctx.
func (c *Checker) Start(ctx context.Context) {
	if c == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		_ = c.Refresh(ctx)
		ticker := time.NewTicker(c.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = c.Refresh(ctx)
			}
		}
	}()
}

// UpdatesAvailable is a cache-only status read. It never starts or waits for a
// network operation and fails closed until a refresh succeeds.
func (c *Checker) UpdatesAvailable() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hasSuccessful && c.available
}

// Refresh discovers a successful build and reads config.json at its immutable
// workflow head SHA. Concurrent or rate-limited calls leave the cache intact.
func (c *Checker) Refresh(ctx context.Context) error {
	if c == nil {
		return errors.New("release checker is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := c.now()
	c.mu.Lock()
	if c.refreshing {
		c.mu.Unlock()
		return nil
	}
	if !c.lastAttempt.IsZero() && now.Sub(c.lastAttempt) < c.refreshInterval {
		c.mu.Unlock()
		return nil
	}
	c.refreshing = true
	c.lastAttempt = now
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.refreshing = false
		c.mu.Unlock()
	}()

	remoteVersion, err := c.discoverVersion(ctx)
	if err != nil {
		return err
	}
	available := newer(c.currentVersion, remoteVersion)
	c.mu.Lock()
	c.available = available
	c.hasSuccessful = true
	c.mu.Unlock()
	return nil
}

func (c *Checker) discoverVersion(parent context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(parent, c.requestTimeout)
	defer cancel()

	workflowURL, err := url.Parse(c.apiBaseURL + "/repos/" + addonOwner + "/" + addonRepository + "/actions/workflows/" + workflowPath + "/runs")
	if err != nil {
		return "", fmt.Errorf("build workflow URL: %w", err)
	}
	query := workflowURL.Query()
	query.Set("branch", "main")
	query.Set("event", "push")
	query.Set("status", "success")
	query.Set("per_page", "1")
	workflowURL.RawQuery = query.Encode()

	var runs workflowRunsResponse
	if err := c.getJSON(ctx, workflowURL.String(), &runs); err != nil {
		return "", fmt.Errorf("read build workflow runs: %w", err)
	}
	if len(runs.WorkflowRuns) != 1 {
		return "", errors.New("successful build response must contain exactly one run")
	}
	run := runs.WorkflowRuns[0]
	if run.Status != "completed" || run.Conclusion != "success" || run.Event != "push" || run.HeadBranch != "main" || !fullSHA.MatchString(run.HeadSHA) {
		return "", errors.New("successful build response failed validation")
	}

	configURL, err := url.Parse(c.apiBaseURL + "/repos/" + addonOwner + "/" + addonRepository + "/contents/" + configPath)
	if err != nil {
		return "", fmt.Errorf("add-on config URL: %w", err)
	}
	configQuery := configURL.Query()
	configQuery.Set("ref", run.HeadSHA)
	configURL.RawQuery = configQuery.Encode()
	var content contentsResponse
	if err := c.getJSON(ctx, configURL.String(), &content); err != nil {
		return "", fmt.Errorf("read add-on config at workflow head: %w", err)
	}
	if content.Type != "file" || content.Name != "config.json" || content.Path != configPath || content.Encoding != "base64" || !fullSHA.MatchString(content.SHA) || content.Content == "" {
		return "", errors.New("add-on config response failed validation")
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(content.Content, "\n", ""))
	if err != nil {
		return "", fmt.Errorf("decode add-on config: %w", err)
	}
	if int64(len(decoded)) > c.maxResponse {
		return "", errors.New("decoded add-on config exceeds response limit")
	}
	var config addonConfig
	if err := decodeSingleJSON(decoded, &config); err != nil {
		return "", fmt.Errorf("decode add-on config: %w", err)
	}
	version := normalizeVersion(config.Version)
	if version == "" {
		return "", errors.New("add-on config version is invalid")
	}
	return version, nil
}

func (c *Checker) getJSON(ctx context.Context, target string, destination any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("User-Agent", "helianthus-ebusgateway-release-status")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	if response == nil || response.Body == nil {
		return errors.New("empty HTTP response")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status %d", response.StatusCode)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || (mediaType != "application/json" && mediaType != "application/vnd.github+json") {
		return errors.New("response is not GitHub JSON")
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, c.maxResponse+1))
	if err != nil {
		return err
	}
	if int64(len(payload)) > c.maxResponse {
		return errors.New("response exceeds limit")
	}
	return decodeSingleJSON(payload, destination)
}

func decodeSingleJSON(payload []byte, destination any) error {
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("response contains multiple JSON values")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("response contains multiple JSON values")
		}
		return err
	}
	return nil
}

func normalizeVersion(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return ""
	}
	if !strings.HasPrefix(version, "v") {
		version = "v" + version
	}
	if !semver.IsValid(version) {
		return ""
	}
	return version
}

func newer(current, remote string) bool {
	return current != "" && remote != "" && semver.Compare(remote, current) > 0
}

type workflowRunsResponse struct {
	WorkflowRuns []workflowRun `json:"workflow_runs"`
}

type workflowRun struct {
	HeadSHA    string `json:"head_sha"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	Event      string `json:"event"`
	HeadBranch string `json:"head_branch"`
}

type contentsResponse struct {
	Type     string `json:"type"`
	Name     string `json:"name"`
	Path     string `json:"path"`
	SHA      string `json:"sha"`
	Encoding string `json:"encoding"`
	Content  string `json:"content"`
}

type addonConfig struct {
	Version string `json:"version"`
}
