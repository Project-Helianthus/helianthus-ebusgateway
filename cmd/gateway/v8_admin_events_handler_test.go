package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// v8_admin_events_handler_test.go pins the HTTP contract for the
// /debug/v8/admin-events endpoint added by F-NEW-26 (2026-05-21).

// TestHandleV8AdminEvents_NilClassifier_ReturnsEmptyResponse pins
// the nil-safe degradation contract: when no classifier is
// registered in v8AdminEventsCurrentClassifier (non-adapter-direct
// transport, or run() not yet completed), the handler returns
// `{"events": [], "dropped": 0}` with HTTP 200. Tooling that
// probes this endpoint unconditionally must NOT see a 404.
func TestHandleV8AdminEvents_NilClassifier_ReturnsEmptyResponse(t *testing.T) {
	// Restore the original pointer after the test so subsequent
	// tests that depend on a registered classifier (none today,
	// but defensively) are not affected.
	orig := v8AdminEventsCurrentClassifier.Load()
	t.Cleanup(func() { v8AdminEventsCurrentClassifier.Store(orig) })

	v8AdminEventsCurrentClassifier.Store(nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/v8/admin-events", nil)
	handleV8AdminEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 (nil-classifier must not 404)", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q; want application/json", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q; want no-store (drain is destructive)", cc)
	}

	var resp struct {
		Events  []map[string]any `json:"events"`
		Dropped uint64           `json:"dropped"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v\nbody=%s", err, rec.Body.String())
	}
	if len(resp.Events) != 0 {
		t.Errorf("events length = %d; want 0 (nil classifier)", len(resp.Events))
	}
	if resp.Dropped != 0 {
		t.Errorf("dropped = %d; want 0", resp.Dropped)
	}
}

// TestHandleV8AdminEvents_DrainsClassifierEvents pins the
// happy-path contract: a registered classifier with buffered
// admin events returns those events JSON-encoded, drains the
// buffer, and a subsequent GET returns the empty envelope.
func TestHandleV8AdminEvents_DrainsClassifierEvents(t *testing.T) {
	orig := v8AdminEventsCurrentClassifier.Load()
	t.Cleanup(func() { v8AdminEventsCurrentClassifier.Store(orig) })

	c := v8classifier.New(v8classifier.ModeShadow)
	v8AdminEventsCurrentClassifier.Store(c)

	// Trigger an AA-injection drop to populate the ring buffer.
	now := time.Date(2026, 5, 21, 9, 30, 0, 0, time.UTC)
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted, Data: 0x71,
	}, now)
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0xAA,
	}, now)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/v8/admin-events", nil)
	handleV8AdminEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rec.Code)
	}

	var resp struct {
		Events []struct {
			At         time.Time `json:"at"`
			Kind       string    `json:"kind"`
			FSMState   string    `json:"fsm_state"`
			Byte       string    `json:"byte"`
			WasEscaped bool      `json:"was_escaped"`
		} `json:"events"`
		Dropped uint64 `json:"dropped"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v\nbody=%s", err, rec.Body.String())
	}
	if len(resp.Events) != 1 {
		t.Fatalf("events length = %d; want 1 (drop did not emit admin event)", len(resp.Events))
	}
	ev := resp.Events[0]
	if ev.Kind != "aa_injection_drop" {
		t.Errorf("event kind = %q; want %q", ev.Kind, "aa_injection_drop")
	}
	if ev.Byte != "0xAA" {
		t.Errorf("event byte = %q; want %q", ev.Byte, "0xAA")
	}
	if ev.WasEscaped {
		t.Errorf("event was_escaped = true; want false (raw wire 0xAA)")
	}
	if !ev.At.Equal(now) {
		t.Errorf("event at = %v; want %v (timestamp must round-trip JSON)", ev.At, now)
	}

	// Second GET must return empty — the first drained.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/debug/v8/admin-events", nil)
	handleV8AdminEvents(rec2, req2)

	var resp2 struct {
		Events  []map[string]any `json:"events"`
		Dropped uint64           `json:"dropped"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("second GET invalid JSON: %v", err)
	}
	if len(resp2.Events) != 0 {
		t.Errorf("second GET events length = %d; want 0 (first GET should have drained)", len(resp2.Events))
	}
}

// TestHandleV8AdminEvents_OnlyGetAllowed pins that POST/PUT/etc
// respond 405 — the drain is state-mutating; only GET is the
// documented action.
func TestHandleV8AdminEvents_OnlyGetAllowed(t *testing.T) {
	orig := v8AdminEventsCurrentClassifier.Load()
	t.Cleanup(func() { v8AdminEventsCurrentClassifier.Store(orig) })

	for _, method := range []string{
		http.MethodPost,
		http.MethodPut,
		http.MethodDelete,
		http.MethodPatch,
	} {
		t.Run(method, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(method, "/debug/v8/admin-events", nil)
			handleV8AdminEvents(rec, req)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s: status = %d; want 405", method, rec.Code)
			}
			if allow := rec.Header().Get("Allow"); allow != "GET" {
				t.Errorf("%s: Allow = %q; want %q", method, allow, "GET")
			}
		})
	}
}

// TestHandleV8AdminEvents_PeekDoesNotDrain pins the
// `?peek=true` non-destructive contract added to close
// Codex round-1 LOW on PR #657 (GET-drain footgun). Two
// successive peek requests return the same events; a
// subsequent drain (no query param) still observes them.
// Cache-Control header switches from `no-store` (drain) to
// `max-age=1` (peek) so caching proxies treat the two modes
// correctly.
func TestHandleV8AdminEvents_PeekDoesNotDrain(t *testing.T) {
	orig := v8AdminEventsCurrentClassifier.Load()
	t.Cleanup(func() { v8AdminEventsCurrentClassifier.Store(orig) })

	c := v8classifier.New(v8classifier.ModeShadow)
	v8AdminEventsCurrentClassifier.Store(c)

	// Populate the ring with one event.
	now := time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC)
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted, Data: 0x71,
	}, now)
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0xAA,
	}, now)

	// First peek.
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/debug/v8/admin-events?peek=true", nil)
	handleV8AdminEvents(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("peek status = %d; want 200", rec1.Code)
	}
	if cc := rec1.Header().Get("Cache-Control"); cc != "max-age=1" {
		t.Errorf("peek Cache-Control = %q; want %q (peek is idempotent — short cache OK)",
			cc, "max-age=1")
	}

	var resp1 struct {
		Events []struct {
			Kind string `json:"kind"`
			Byte string `json:"byte"`
		} `json:"events"`
	}
	if err := json.Unmarshal(rec1.Body.Bytes(), &resp1); err != nil {
		t.Fatalf("first peek invalid JSON: %v", err)
	}
	if len(resp1.Events) != 1 {
		t.Fatalf("first peek events = %d; want 1", len(resp1.Events))
	}

	// Second peek returns the SAME event.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/debug/v8/admin-events?peek=true", nil)
	handleV8AdminEvents(rec2, req2)

	var resp2 struct {
		Events []struct {
			Kind string `json:"kind"`
			Byte string `json:"byte"`
		} `json:"events"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("second peek invalid JSON: %v", err)
	}
	if len(resp2.Events) != 1 {
		t.Fatalf("second peek events = %d; want 1 (peek must NOT drain)", len(resp2.Events))
	}

	// Drain (no peek param) finally consumes.
	rec3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "/debug/v8/admin-events", nil)
	handleV8AdminEvents(rec3, req3)
	if cc := rec3.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("drain Cache-Control = %q; want %q (drain is destructive)", cc, "no-store")
	}

	// Post-drain peek sees empty.
	rec4 := httptest.NewRecorder()
	req4 := httptest.NewRequest(http.MethodGet, "/debug/v8/admin-events?peek=true", nil)
	handleV8AdminEvents(rec4, req4)
	var resp4 struct {
		Events []struct{} `json:"events"`
	}
	if err := json.Unmarshal(rec4.Body.Bytes(), &resp4); err != nil {
		t.Fatalf("post-drain peek invalid JSON: %v", err)
	}
	if len(resp4.Events) != 0 {
		t.Errorf("post-drain peek events = %d; want 0 (drain failed)", len(resp4.Events))
	}
}

// TestV8AdminEventsCurrentClassifier_AtomicSwap pins the
// indirection contract used by the handler: the package-level
// atomic.Pointer can be swapped at test re-entry without
// affecting the registered handler. Mirrors the contract added
// for v8RolloutExpvarCurrent in PR #655.
func TestV8AdminEventsCurrentClassifier_AtomicSwap(t *testing.T) {
	orig := v8AdminEventsCurrentClassifier.Load()
	t.Cleanup(func() { v8AdminEventsCurrentClassifier.Store(orig) })

	v8AdminEventsCurrentClassifier.Store(nil)
	if got := v8AdminEventsCurrentClassifier.Load(); got != nil {
		t.Errorf("after Store(nil) Load() = %v; want nil", got)
	}

	c := v8classifier.New(v8classifier.ModeShadow)
	v8AdminEventsCurrentClassifier.Store(c)
	if got := v8AdminEventsCurrentClassifier.Load(); got != c {
		t.Errorf("after Store(c) Load() = %p; want %p", got, c)
	}
}
