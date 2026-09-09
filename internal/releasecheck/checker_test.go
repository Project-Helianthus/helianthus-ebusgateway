package releasecheck

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

const testHeadSHA = "0123456789abcdef0123456789abcdef01234567"

type fakeHTTP struct {
	mu       sync.Mutex
	requests []*http.Request
	respond  func(*http.Request) (*http.Response, error)
}

func (f *fakeHTTP) Do(request *http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request.Clone(request.Context()))
	f.mu.Unlock()
	return f.respond(request)
}

func (f *fakeHTTP) snapshotRequests() []*http.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*http.Request(nil), f.requests...)
}

func testJSONResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/vnd.github+json; charset=utf-8"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func successfulWorkflow(version string) func(*http.Request) (*http.Response, error) {
	return func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/repos/Project-Helianthus/helianthus-ha-addon/actions/workflows/build.yml/runs":
			return testJSONResponse(`{"workflow_runs":[{"head_sha":"` + testHeadSHA + `","status":"completed","conclusion":"success","event":"push","head_branch":"main"}]}`), nil
		case "/repos/Project-Helianthus/helianthus-ha-addon/contents/helianthus/config.json":
			content := base64.StdEncoding.EncodeToString([]byte(`{"version":"` + version + `"}`))
			return testJSONResponse(`{"type":"file","name":"config.json","path":"helianthus/config.json","sha":"` + testHeadSHA + `","encoding":"base64","content":"` + content + `"}`), nil
		default:
			return nil, errors.New("unexpected request " + request.URL.String())
		}
	}
}

func newTestChecker(current string, client Doer, now func() time.Time) *Checker {
	return New(Options{
		CurrentVersion:  current,
		HTTPClient:      client,
		APIBaseURL:      "https://github.example.test",
		Clock:           now,
		RefreshInterval: time.Hour,
		RequestTimeout:  time.Second,
		MaxResponse:     4096,
	})
}

func TestCheckerRefreshUsesNewestSuccessfulPushAndExactWorkflowSHA(t *testing.T) {
	now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
	client := &fakeHTTP{respond: successfulWorkflow("1.2.4")}
	checker := newTestChecker("1.2.3", client, func() time.Time { return now })

	if checker.UpdatesAvailable() {
		t.Fatal("update available before the first successful refresh")
	}
	if err := checker.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if !checker.UpdatesAvailable() {
		t.Fatal("update unavailable after newer successful build")
	}
	requests := client.snapshotRequests()
	if len(requests) != 2 {
		t.Fatalf("requests = %d; want 2", len(requests))
	}
	workflow := requests[0]
	if got := workflow.URL.Query(); got.Get("branch") != "main" || got.Get("event") != "push" || got.Get("status") != "success" || got.Get("per_page") != "1" {
		t.Fatalf("workflow query = %q", workflow.URL.RawQuery)
	}
	config := requests[1]
	if got := config.URL.Query().Get("ref"); got != testHeadSHA {
		t.Fatalf("config ref = %q; want workflow head SHA", got)
	}
	if config.URL.Query().Get("ref") == "main" {
		t.Fatal("config request used moving main")
	}
}

func TestCheckerSemanticComparisonFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name    string
		current string
		remote  string
		want    bool
	}{
		{name: "equal", current: "1.2.3", remote: "1.2.3", want: false},
		{name: "older", current: "1.2.4", remote: "1.2.3", want: false},
		{name: "invalid current", current: "development", remote: "1.2.4", want: false},
		{name: "invalid remote", current: "1.2.3", remote: "release", want: false},
		{name: "newer", current: "1.2.3", remote: "1.2.4", want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeHTTP{respond: successfulWorkflow(test.remote)}
			checker := newTestChecker(test.current, client, time.Now)
			err := checker.Refresh(context.Background())
			if test.remote == "release" && err == nil {
				t.Fatal("invalid remote version accepted")
			}
			if got := checker.UpdatesAvailable(); got != test.want {
				t.Fatalf("UpdatesAvailable() = %v; want %v (refresh err=%v)", got, test.want, err)
			}
		})
	}
}

func TestCheckerRefreshFailureRetainsLastSuccessfulComparison(t *testing.T) {
	now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
	client := &fakeHTTP{respond: successfulWorkflow("1.2.4")}
	checker := newTestChecker("1.2.3", client, func() time.Time { return now })
	if err := checker.Refresh(context.Background()); err != nil || !checker.UpdatesAvailable() {
		t.Fatalf("initial Refresh()/status = %v/%v", err, checker.UpdatesAvailable())
	}
	now = now.Add(time.Hour)
	client.respond = func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") }
	if err := checker.Refresh(context.Background()); err == nil {
		t.Fatal("failed refresh returned nil")
	}
	if !checker.UpdatesAvailable() {
		t.Fatal("failed refresh erased last successful comparison")
	}
}

func TestCheckerValidatesResponsesAndBoundsStatusReads(t *testing.T) {
	client := &fakeHTTP{respond: func(request *http.Request) (*http.Response, error) {
		if strings.Contains(request.URL.Path, "/runs") {
			return testJSONResponse(`{"workflow_runs":[{"head_sha":"not-a-sha","status":"completed","conclusion":"success","event":"push","head_branch":"main"}]}`), nil
		}
		return nil, errors.New("config should not be requested")
	}}
	checker := newTestChecker("1.2.3", client, time.Now)
	if err := checker.Refresh(context.Background()); err == nil {
		t.Fatal("invalid workflow response accepted")
	}
	requests := len(client.snapshotRequests())
	if checker.UpdatesAvailable() {
		t.Fatal("invalid workflow response set update state")
	}
	if got := len(client.snapshotRequests()); got != requests {
		t.Fatalf("cache-only status read made %d additional requests", got-requests)
	}
}

func TestCheckerConcurrentRefreshMakesOneBoundedAttempt(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	client := &fakeHTTP{respond: func(request *http.Request) (*http.Response, error) {
		if strings.Contains(request.URL.Path, "/runs") {
			close(entered)
			<-release
		}
		return successfulWorkflow("1.2.4")(request)
	}}
	checker := newTestChecker("1.2.3", client, time.Now)
	first := make(chan error, 1)
	go func() { first <- checker.Refresh(context.Background()) }()
	<-entered
	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := checker.Refresh(context.Background()); err != nil {
				t.Errorf("concurrent Refresh() error = %v", err)
			}
		}()
	}
	group.Wait()
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first Refresh() error = %v", err)
	}
	if got := len(client.snapshotRequests()); got != 2 {
		t.Fatalf("requests = %d; want one workflow/config pair", got)
	}
}
