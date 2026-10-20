package m2mgraphql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestP08ThreeConsumerBurstAndBoundedRefill(t *testing.T) {
	var now atomic.Int64
	calls := map[string]int{}
	provider := func(family string) func(context.Context, string) (json.RawMessage, bool) {
		return func(_ context.Context, asset string) (json.RawMessage, bool) {
			if asset != "asset:"+family {
				t.Fatalf("%s provider received asset %q", family, asset)
			}
			calls[family]++
			return json.RawMessage(`{"snapshot":{},"evaluation":{},"selections":[],"projection":{}}`), true
		}
	}
	h, err := NewHandler(Config{
		AllowedAssets:         map[string]struct{}{"asset:pv": {}, "asset:storage": {}, "asset:evse": {}},
		MonotonicMilliseconds: now.Load,
		SemanticPVCurrent:     provider("pv"), SemanticStorageCurrent: provider("storage"), SemanticEVSECurrent: provider("evse"),
	})
	if err != nil {
		t.Fatal(err)
	}
	reads := []struct{ family, operation, query, root, contract string }{
		{"pv", "SemanticPVCurrent", semanticPVFixedQuery, "semanticPVCurrent", "PUBLIC_GRAPHQL_SEMANTIC_PV_V1"},
		{"storage", "SemanticStorageCurrent", semanticStorageFixedQuery, "semanticStorageCurrent", "PUBLIC_GRAPHQL_SEMANTIC_STORAGE_V1"},
		{"evse", "SemanticEVSECurrent", semanticEVSEFixedQuery, "semanticEVSECurrent", "PUBLIC_GRAPHQL_SEMANTIC_EVSE_V1"},
	}
	read := func(index int, remote string) {
		t.Helper()
		r := reads[index]
		body := fmt.Sprintf(`{"operationName":%q,"query":%q,"variables":{"request":{"contractId":%q,"assetRef":%q}}}`, r.operation, r.query, r.contract, "asset:"+r.family)
		before := calls[r.family]
		response := serveRaw(h, http.MethodPost, route, remote, strings.NewReader(body))
		var decoded struct{ Data map[string]json.RawMessage }
		if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil || response.Code != http.StatusOK || decoded.Data[r.root] == nil || strings.Contains(response.Body.String(), `"errors"`) || calls[r.family] != before+1 {
			t.Fatalf("%s read: calls=%d response=%d %s err=%v", r.family, calls[r.family], response.Code, response.Body.String(), err)
		}
	}
	for i := range reads {
		read(i, fmt.Sprintf("192.0.2.60:%d", 40001+i))
	}
	refused := func() {
		t.Helper()
		assertLimitRefusal(t, serveRaw(h, http.MethodPost, route, "192.0.2.60:40004", unreadableBody{t}))
	}
	refused()
	// Change the host alone: the same source port has an independent budget.
	read(0, "192.0.2.61:40001")
	now.Store(999)
	refused()
	now.Store(1000)
	read(2, "192.0.2.60:40005")
	refused()
	// A long idle refill remains capped at three, below the eviction window.
	now.Store(10_000)
	for i := range reads {
		read(i, "192.0.2.60:40006")
	}
	refused()
}

func TestP08GraphQLAndMatterShareSequentialTokens(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	served := 0
	matter, err := RequestBudget(h, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served++
		w.WriteHeader(http.StatusNoContent)
	}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := serveStorageFrom(t, h, "192.0.2.70:40001"); got != "ok" {
		t.Fatalf("first GraphQL read=%s", got)
	}
	for _, path := range []string{"/matter-binding/v1/snapshot", "/matter-binding/v1/changes?cursor=synthetic:1"} {
		if response := serveRaw(matter, http.MethodGet, path, "192.0.2.70:40002", nil); response.Code != http.StatusNoContent {
			t.Fatalf("Matter read=%d %s", response.Code, response.Body.String())
		}
	}
	assertLimitRefusal(t, serveRaw(h, http.MethodPost, route, "192.0.2.70:40003", unreadableBody{t}))
	assertLimitRefusal(t, serveRaw(matter, http.MethodGet, "/matter-binding/v1/snapshot", "192.0.2.70:40004", nil))
	if served != 2 {
		t.Fatalf("Matter calls=%d want 2", served)
	}
	now.Store(1000)
	if response := serveRaw(matter, http.MethodGet, "/matter-binding/v1/snapshot", "192.0.2.70:40005", nil); response.Code != http.StatusNoContent || served != 3 {
		t.Fatalf("refilled Matter read=%d calls=%d", response.Code, served)
	}
	assertLimitRefusal(t, serveRaw(h, http.MethodPost, route, "192.0.2.70:40006", unreadableBody{t}))
}

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
	// Exhaust all three tokens; the next malformed body is never read.
	for token := 2; token <= 3; token++ {
		if got := serveStorageFrom(t, h, host); got != "ok" {
			t.Fatalf("token %d=%s want ok", token, got)
		}
	}
	response = serveRaw(h, http.MethodPost, route, host, unreadableBody{t})
	assertLimitRefusal(t, response)
}

func TestM2MRateLimitMalformedRequestsSpendBudgetAndRelease(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	const host = "[2001:db8::30]:40001"
	codes := []string{}
	for _, body := range []string{`{`, `{"operationName":"x"}`, `{`, `{`} {
		response := serveRaw(h, http.MethodPost, route, host, strings.NewReader(body))
		codes = append(codes, errorCode(response.Body.String()))
	}
	// Each early error return released its entry (otherwise the second would
	// hit the in-flight guard) and spent a token (so the fourth is refused).
	if strings.Join(codes, ",") != "REQUEST_INVALID,REQUEST_INVALID,REQUEST_INVALID,REQUEST_LIMIT_EXCEEDED" {
		t.Fatalf("malformed request codes=%v", codes)
	}
	if h.rateLimitTableLen(t) != 1 {
		t.Fatalf("rate-limit table=%d want 1", h.rateLimitTableLen(t))
	}
}

func TestM2MRequestBudgetReleasesOnPanic(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	wrapped, err := RequestBudget(h, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("handler failure") }), nil)
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
	if _, err := RequestBudget(http.NotFoundHandler(), http.NotFoundHandler(), nil); err == nil {
		t.Fatal("request budget accepted a handler that owns no budget table")
	}
	var now atomic.Int64
	if _, err := RequestBudget(newRateLimitTableHandler(t, &now), nil, nil); err == nil {
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
	}), nil)
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

func TestM2MRequestBudgetCustomRefusalLeavesGraphQLRefusalUnchanged(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	entered, unblock := make(chan struct{}), make(chan struct{})
	wrapped, err := RequestBudget(h, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.RemoteAddr == "192.0.2.50:1" {
			close(entered)
			<-unblock
		}
		w.WriteHeader(http.StatusOK)
	}), func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"custom":"refused"}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() { done <- serveRaw(wrapped, http.MethodGet, "/v1/snapshot", "192.0.2.50:1", nil).Code }()
	<-entered
	custom := serveRaw(wrapped, http.MethodGet, "/v1/changes", "192.0.2.50:2", nil)
	if custom.Code != http.StatusServiceUnavailable || custom.Body.String() != `{"custom":"refused"}` {
		t.Fatalf("custom refusal=%d %s", custom.Code, custom.Body.String())
	}
	assertLimitRefusal(t, serveRaw(h, http.MethodPost, route, "192.0.2.50:3", unreadableBody{t}))
	if other := serveRaw(wrapped, http.MethodGet, "/v1/snapshot", "192.0.2.51:1", nil); other.Code != http.StatusOK || other.Body.Len() != 0 {
		t.Fatalf("different host refused: %d %s", other.Code, other.Body.String())
	}
	close(unblock)
	<-done
}
