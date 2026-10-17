package m2mgraphql

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// unreadableBody fails the test if the handler ever reads it.
type unreadableBody struct{ t *testing.T }

func (b unreadableBody) Read([]byte) (int, error) {
	b.t.Errorf("refused host's request body was read")
	return 0, http.ErrBodyReadAfterClose
}

func serveRaw(h http.Handler, method, path, remote string, body io.Reader) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, body)
	r.RemoteAddr = remote
	response := httptest.NewRecorder()
	h.ServeHTTP(response, r)
	return response
}

func TestM2MRateLimitRefusedHostBodyIsNeverParsed(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	const host = "192.0.2.30:40001"
	// In flight: the one-in-flight guard refuses before the body is touched.
	held := h.admit("remote:192.0.2.30")
	if held == nil {
		t.Fatal("first admission refused")
	}
	response := serveRaw(h, http.MethodPost, route, host, unreadableBody{t})
	assertLimitRefusal(t, response)
	h.release(held)
	// Exhausted: both tokens spent, the next malformed body is never read.
	if got := serveStorageFrom(t, h, host); got != "ok" {
		t.Fatalf("second token=%s want ok", got)
	}
	response = serveRaw(h, http.MethodPost, route, host, unreadableBody{t})
	assertLimitRefusal(t, response)
}

func TestM2MRateLimitMalformedRequestsSpendBudgetAndRelease(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	const host = "[2001:db8::30]:40001"
	codes := []string{}
	for _, body := range []string{`{`, `{"operationName":"x"}`, `{`} {
		response := serveRaw(h, http.MethodPost, route, host, strings.NewReader(body))
		codes = append(codes, errorCode(response.Body.String()))
	}
	// Each early error return released its entry (otherwise the second would
	// hit the in-flight guard) and spent a token (so the third is refused).
	if strings.Join(codes, ",") != "REQUEST_INVALID,REQUEST_INVALID,REQUEST_LIMIT_EXCEEDED" {
		t.Fatalf("malformed request codes=%v", codes)
	}
	if h.rateLimitTableLen(t) != 1 {
		t.Fatalf("rate-limit table=%d want 1", h.rateLimitTableLen(t))
	}
}

func TestM2MRequestBudgetReleasesOnPanic(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	wrapped, err := RequestBudget(h, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("handler failure") }))
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("wrapped handler panic was swallowed")
			}
		}()
		serveRaw(wrapped, http.MethodGet, "/v1/snapshot", "192.0.2.31:1", nil)
	}()
	limit := h.admit("remote:192.0.2.31")
	if limit == nil {
		t.Fatal("panicking request left its budget entry in flight")
	}
	h.release(limit)
}

func TestM2MRequestBudgetRejectsForeignOwner(t *testing.T) {
	if _, err := RequestBudget(http.NotFoundHandler(), http.NotFoundHandler()); err == nil {
		t.Fatal("request budget accepted a handler that owns no budget table")
	}
	var now atomic.Int64
	if _, err := RequestBudget(newRateLimitTableHandler(t, &now), nil); err == nil {
		t.Fatal("request budget accepted a nil wrapped handler")
	}
}

func TestM2MRequestBudgetSharesGraphQLTableAndRefusal(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	entered, unblock := make(chan struct{}), make(chan struct{})
	wrapped, err := RequestBudget(h, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.RemoteAddr == "192.0.2.40:1" {
			close(entered)
			<-unblock
		}
		w.WriteHeader(http.StatusOK)
	}))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() { done <- serveRaw(wrapped, http.MethodGet, "/v1/snapshot", "192.0.2.40:1", nil).Code }()
	<-entered
	routeRefusal := serveRaw(wrapped, http.MethodGet, "/v1/changes", "192.0.2.40:2", nil)
	assertLimitRefusal(t, routeRefusal)
	graphqlRefusal := serveRaw(h, http.MethodPost, route, "192.0.2.40:3", unreadableBody{t})
	assertLimitRefusal(t, graphqlRefusal)
	if routeRefusal.Code != graphqlRefusal.Code || routeRefusal.Body.String() != graphqlRefusal.Body.String() || routeRefusal.Header().Get("Content-Type") != graphqlRefusal.Header().Get("Content-Type") {
		t.Fatalf("wrapped refusal %d %q differs from GraphQL refusal %d %q", routeRefusal.Code, routeRefusal.Body.String(), graphqlRefusal.Code, graphqlRefusal.Body.String())
	}
	if other := serveRaw(wrapped, http.MethodGet, "/v1/snapshot", "192.0.2.41:1", nil); other.Code != http.StatusOK || other.Body.Len() != 0 {
		t.Fatalf("different host refused: %d %s", other.Code, other.Body.String())
	}
	close(unblock)
	if code := <-done; code != http.StatusOK {
		t.Fatalf("admitted request status=%d", code)
	}
}

func assertLimitRefusal(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusOK || errorCode(response.Body.String()) != "REQUEST_LIMIT_EXCEEDED" || !strings.Contains(response.Body.String(), `"path":[]`) {
		t.Fatalf("refusal=%d %s; want neutral REQUEST_LIMIT_EXCEEDED", response.Code, response.Body.String())
	}
}

func errorCode(body string) string {
	for _, code := range []string{"REQUEST_LIMIT_EXCEEDED", "REQUEST_INVALID", "QUERY_REJECTED", "CONTRACT_INCOMPATIBLE", "ASSET_FORBIDDEN", "SOURCE_UNAVAILABLE"} {
		if strings.Contains(body, `"`+code+`"`) {
			return code
		}
	}
	return ""
}
