package m2mgraphql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func newRateLimitTableHandler(t *testing.T, now *atomic.Int64) *handler {
	t.Helper()
	built, err := NewHandler(Config{AllowedAssets: map[string]struct{}{"asset:storage-test": {}}, MonotonicMilliseconds: now.Load, SemanticStorageCurrent: func(context.Context, string) (json.RawMessage, bool) {
		return json.RawMessage(`{"snapshot":{},"evaluation":{},"selections":[],"projection":{}}`), true
	}})
	if err != nil {
		t.Fatal(err)
	}
	return built.(*handler)
}

func serveStorageFrom(t *testing.T, h *handler, remote string) string {
	t.Helper()
	request := `{"operationName":"SemanticStorageCurrent","query":` + strconv.Quote(semanticStorageFixedQuery) + `,"variables":{"request":{"contractId":"PUBLIC_GRAPHQL_SEMANTIC_STORAGE_V1","assetRef":"asset:storage-test"}}}`
	r := httptest.NewRequest(http.MethodPost, route, strings.NewReader(request))
	r.RemoteAddr = remote
	response := httptest.NewRecorder()
	h.ServeHTTP(response, r)
	if strings.Contains(response.Body.String(), `"REQUEST_LIMIT_EXCEEDED"`) {
		return "limited"
	}
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"errors"`) {
		t.Fatalf("remote %s response=%d %s", remote, response.Code, response.Body.String())
	}
	return "ok"
}

func (h *handler) rateLimitTableLen(t *testing.T) int {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.limits) != h.lru.Len() {
		t.Fatalf("rate-limit index=%d list=%d", len(h.limits), h.lru.Len())
	}
	return len(h.limits)
}

func (h *handler) hasRateLimitKey(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.limits[key]
	return ok
}

func TestM2MRateLimitTableStaysBoundedUnderRotatingIPv6AndIPv4Hosts(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	// Keys are derived exactly as for network requests; admission is driven
	// directly so the race suite stays fast across 9k rotated hosts.
	admitFrom := func(remote string) {
		r := httptest.NewRequest(http.MethodPost, route, nil)
		r.RemoteAddr = remote
		limit := h.admit(requestRateLimitKey(r))
		if limit == nil {
			// Every rotated address is new and must be admitted, never
			// refused for table pressure.
			t.Fatalf("rotated host %s refused; table pressure must evict, not refuse", remote)
		}
		h.release(limit)
	}
	for i := 0; i < rateLimitTableCapacity+512; i++ {
		admitFrom(fmt.Sprintf("[2001:db8:%x:%x::1]:40001", i>>16, i&0xffff))
		admitFrom(fmt.Sprintf("10.%d.%d.%d:40001", (i>>16)&0xff, (i>>8)&0xff, i&0xff))
		if size := h.rateLimitTableLen(t); size > rateLimitTableCapacity {
			t.Fatalf("rate-limit table=%d exceeds capacity %d", size, rateLimitTableCapacity)
		}
	}
	// The end-to-end path still admits a brand-new host at capacity.
	if got := serveStorageFrom(t, h, "[2001:db8:ffff::1]:40001"); got != "ok" {
		t.Fatalf("new host at capacity=%s; want admitted", got)
	}
	if size := h.rateLimitTableLen(t); size != rateLimitTableCapacity {
		t.Fatalf("rate-limit table=%d want full capacity %d", size, rateLimitTableCapacity)
	}
}

func TestM2MRateLimitIdleEntryIsEvictedAfterIdleWindow(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	serveStorageFrom(t, h, "192.0.2.10:40001")
	now.Store(rateLimitIdleMillis)
	serveStorageFrom(t, h, "192.0.2.11:40001")
	if !h.hasRateLimitKey("remote:192.0.2.10") {
		t.Fatal("entry evicted at exactly the idle window; eviction is for idle > window")
	}
	now.Store(rateLimitIdleMillis + 1)
	serveStorageFrom(t, h, "192.0.2.12:40001")
	if h.hasRateLimitKey("remote:192.0.2.10") {
		t.Fatal("entry idle beyond the window was not evicted")
	}
	if !h.hasRateLimitKey("remote:192.0.2.11") || !h.hasRateLimitKey("remote:192.0.2.12") || h.rateLimitTableLen(t) != 2 {
		t.Fatalf("recently used entries lost; table=%d", h.rateLimitTableLen(t))
	}
}

func TestM2MRateLimitLRUVictimIsLeastRecentlyUsedKey(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	h.limitCapacity = 3
	serveStorageFrom(t, h, "192.0.2.1:40001")
	serveStorageFrom(t, h, "192.0.2.2:40001")
	serveStorageFrom(t, h, "192.0.2.3:40001")
	// Touch .1 (its second token is admitted) so .2 becomes least recently used.
	serveStorageFrom(t, h, "192.0.2.1:40002")
	serveStorageFrom(t, h, "192.0.2.4:40001")
	if h.hasRateLimitKey("remote:192.0.2.2") {
		t.Fatal("least recently used key survived eviction at capacity")
	}
	for _, key := range []string{"remote:192.0.2.1", "remote:192.0.2.3", "remote:192.0.2.4"} {
		if !h.hasRateLimitKey(key) {
			t.Fatalf("recently used %s was evicted instead of the LRU key", key)
		}
	}
	// The surviving host retains its remaining third token, not a fresh budget.
	if got := serveStorageFrom(t, h, "192.0.2.1:40003"); got != "ok" {
		t.Fatalf("surviving host third token=%s want ok", got)
	}
	if got := serveStorageFrom(t, h, "192.0.2.1:40004"); got != "limited" {
		t.Fatalf("surviving host budget=%s want limited", got)
	}
}

func TestM2MRateLimitEvictedHostReturnsWithFreshBudget(t *testing.T) {
	// LRU eviction of a released entry can only widen a budget, never deny one.
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	h.limitCapacity = 2
	got := []string{serveStorageFrom(t, h, "[2001:db8::1]:1"), serveStorageFrom(t, h, "[2001:db8::1]:2"), serveStorageFrom(t, h, "[2001:db8::1]:3"), serveStorageFrom(t, h, "[2001:db8::1]:4")}
	serveStorageFrom(t, h, "[2001:db8::2]:1")
	serveStorageFrom(t, h, "[2001:db8::3]:1") // evicts 2001:db8::1
	got = append(got, serveStorageFrom(t, h, "[2001:db8::1]:5"))
	want := []string{"ok", "ok", "ok", "limited", "ok"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("evicted host admission=%v want=%v", got, want)
	}
}

func TestM2MRateLimitReleaseDoesNotClearRecreatedEntry(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	stale := h.admit("remote:192.0.2.1")
	if stale == nil {
		t.Fatal("first admission refused")
	}
	// In-flight entries are never evicted by policy; drop the entry directly
	// to exercise release's pointer guard against a recreated entry.
	h.mu.Lock()
	h.removeLocked(h.limits["remote:192.0.2.1"])
	h.mu.Unlock()
	fresh := h.admit("remote:192.0.2.1")
	if fresh == nil || fresh == stale {
		t.Fatal("dropped key was not recreated with a fresh entry")
	}
	h.release(stale)
	if h.admit("remote:192.0.2.1") != nil {
		t.Fatal("stale release cleared the in-flight mark of the recreated entry")
	}
	h.release(fresh)
	if h.admit("remote:192.0.2.1") == nil {
		t.Fatal("released entry with a remaining token was refused")
	}
}

func TestM2MRateLimitInFlightEntrySurvivesRotatingHostChurn(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	const busy = "[2001:db8:beef::1]:40001"
	r := httptest.NewRequest(http.MethodPost, route, nil)
	r.RemoteAddr = busy
	busyKey := requestRateLimitKey(r)
	held := h.admit(busyKey)
	if held == nil {
		t.Fatal("first request from the busy host refused")
	}
	// Rotate enough fresh hosts to cycle the whole table twice over, and run
	// one churn pass past the idle window, while the busy request is running.
	for i := 0; i < 2*rateLimitTableCapacity+512; i++ {
		if i == rateLimitTableCapacity {
			now.Store(rateLimitIdleMillis + 1)
		}
		if got := serveStorageFrom(t, h, fmt.Sprintf("10.%d.%d.%d:40001", (i>>16)&0xff, (i>>8)&0xff, i&0xff)); got != "ok" {
			t.Fatalf("rotated host %d=%s; want admitted", i, got)
		}
		if size := h.rateLimitTableLen(t); size > rateLimitTableCapacity+1 {
			t.Fatalf("rate-limit table=%d exceeds capacity %d plus one in-flight entry", size, rateLimitTableCapacity)
		}
	}
	if !h.hasRateLimitKey(busyKey) {
		t.Fatal("in-flight entry was evicted under host churn")
	}
	// The busy host still holds a token, so only the in-flight guard can
	// reject this second concurrent request.
	if got := serveStorageFrom(t, h, "[2001:db8:beef::1]:40002"); got != "limited" {
		t.Fatalf("second concurrent request from busy host=%s; want limited", got)
	}
	h.release(held)
	if got := serveStorageFrom(t, h, "[2001:db8:beef::1]:40003"); got != "ok" {
		t.Fatalf("busy host after release=%s; want admitted", got)
	}
}

func TestM2MRateLimitTableOverflowsOnlyByInFlightEntries(t *testing.T) {
	var now atomic.Int64
	h := newRateLimitTableHandler(t, &now)
	h.limitCapacity = 3
	var held []*requestLimit
	for i := 1; i <= 5; i++ {
		limit := h.admit(fmt.Sprintf("remote:192.0.2.%d", i))
		if limit == nil {
			t.Fatalf("host %d refused with %d in-flight entries; want admitted", i, len(held))
		}
		held = append(held, limit)
	}
	// Capacity 3 entries are in flight; the two extra hosts were admitted and
	// the table holds capacity plus the in-flight overflow.
	if size := h.rateLimitTableLen(t); size != h.limitCapacity+2 {
		t.Fatalf("rate-limit table=%d want capacity %d plus 2 in-flight overflow", size, h.limitCapacity)
	}
	for _, limit := range held {
		h.release(limit)
	}
	// The next new key trims the released entries back to capacity.
	serveStorageFrom(t, h, "192.0.2.6:40001")
	if size := h.rateLimitTableLen(t); size != h.limitCapacity {
		t.Fatalf("rate-limit table after release=%d want capacity %d", size, h.limitCapacity)
	}
	// Past the idle window the released entries go and the table drops below capacity.
	now.Store(rateLimitIdleMillis + 1)
	serveStorageFrom(t, h, "192.0.2.7:40001")
	if size := h.rateLimitTableLen(t); size >= h.limitCapacity {
		t.Fatalf("rate-limit table after idle window=%d want below capacity %d", size, h.limitCapacity)
	}
}
