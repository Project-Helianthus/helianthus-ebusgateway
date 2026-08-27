package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

var (
	buildVersion                              = "dev"
	buildID                                   = "unknown"
	wireObserveFirstObserversFn               = wireObserveFirstObservers
	startDiscoveryScanLoopFn                  = startDiscoveryScanLoopWithClassifier
	startVaillantSemanticPollingFn            = startVaillantSemanticPolling
	attachPassiveShadowProducerFn             = (*vaillantSemanticPoller).AttachPassiveShadowProducer
	startPassiveTransactionReconstructor      = ebusgateway.StartPassiveTransactionReconstructor
	startBroadcastListenerWithReconstructorFn = ebusgateway.StartBroadcastListenerWithReconstructor
	startHTTPServerFn                         func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error)
	admissionStabilityRefreshDelay            = time.Duration(ebusgateway.StartupAdmissionStateMinStabilitySecondsDefault)*time.Second + 200*time.Millisecond
	instanceGUIDPattern                       = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

	// v8RolloutExpvarCurrent holds the currently-active bus +
	// classifier references that back the five helianthus_round9_* /
	// helianthus_payload_aa_* / helianthus_v8_shadow_* expvars
	// mirrored onto /debug/vars.
	//
	// Why a process-global atomic and not a plain sync.Once-captured
	// closure: expvar.Publish panics on duplicate names, so the
	// publishes themselves must be one-shot. But the test harness re-
	// enters run() with a fresh *Gateway / *protocol.Bus and a fresh
	// *adaptermux.Mux per scenario. A naive sync.Once that captures
	// the first bus would leave /debug/vars reading stale counters
	// from the dead first gateway while /metrics serves the live
	// counters from the new BusObservabilityStore provider — a
	// genuine surface-inconsistency bug flagged in PR #655 round-1
	// review.
	//
	// Indirection: publish the expvar.Funcs ONCE with closures that
	// dereference v8RolloutExpvarCurrent.Load() at every scrape.
	// run() updates the pointer on each invocation; old gateways
	// stop being scraped because nothing holds their bus reference
	// any more.
	v8RolloutExpvarPublishOnce sync.Once
	v8RolloutExpvarCurrent     atomic.Pointer[v8RolloutExpvarSource]

	// v8AdminEventsCurrentClassifier is the indirection layer for
	// the /debug/v8/admin-events HTTP surface. Same rationale as
	// v8RolloutExpvarCurrent: the handler is registered ONCE at
	// startHTTPServer and dereferences this atomic.Pointer at
	// request time. run() updates the pointer on each invocation
	// (test re-entry safe). When the active transport isn't
	// adapter-direct, classifier is nil; the handler returns an
	// empty events array + dropped=0 in that case so the contract
	// degrades cleanly without 404-ing.
	v8AdminEventsCurrentClassifier atomic.Pointer[v8classifier.Classifier]
	v8AdminEventsCurrentProvider   atomic.Pointer[v8AdminClassifierProvider]
)

// v8RolloutExpvarSource is the indirection layer for the five
// helianthus_*_total expvar surfaces. Nil-classifier is intentional —
// non-adapter-direct transports run with classifier=nil and rely on
// v8classifier.Classifier.ShadowWouldHaveDroppedTotal()'s nil-receiver
// contract (returns 0). bus must not be nil: gateway.Bus is always
// non-nil after a successful ebusgateway.New(), and the wiring site
// guards on it.
type v8RolloutExpvarSource struct {
	bus               *protocol.Bus
	shadowDropCountFn func() uint64
}

type v8AdminClassifierProvider struct {
	withClassifier func(func(*v8classifier.Classifier))
}

func withCurrentV8AdminClassifier(callback func(*v8classifier.Classifier)) {
	if callback == nil {
		return
	}
	// Direct pointer is retained as a test/backward-compatible injection seam.
	// Production clears it and installs the generation-aware provider below.
	if classifier := v8AdminEventsCurrentClassifier.Load(); classifier != nil {
		callback(classifier)
		return
	}
	provider := v8AdminEventsCurrentProvider.Load()
	if provider == nil || provider.withClassifier == nil {
		return
	}
	provider.withClassifier(callback)
}

type runtimeWatchObserver struct {
	primary  ebusgateway.WatchObserver
	fallback ebusgateway.WatchObserver
}

func (observer *runtimeWatchObserver) Observe(key ebusgateway.WatchKey) ebusgateway.WatchObservation {
	if key == nil {
		return ebusgateway.WatchObservation{State: ebusgateway.WatchObservationStateCatalogMiss}
	}
	if observer != nil && observer.primary != nil {
		observation := observer.primary.Observe(key)
		if observation.State != ebusgateway.WatchObservationStateCatalogMiss {
			return observation
		}
	}
	if observer != nil && observer.fallback != nil {
		return observer.fallback.Observe(key)
	}
	return ebusgateway.WatchObservation{State: ebusgateway.WatchObservationStateCatalogMiss}
}

type v8AdminEventsResponse struct {
	Events  []v8AdminEventJSON `json:"events"`
	Dropped uint64             `json:"dropped"`
}

// v8AdminEventJSON is the per-event wire shape returned by
// /debug/v8/admin-events. Mirrors v8classifier.ClassifierAdminEvent
// but encodes the enums as stable string labels.
type v8AdminEventJSON struct {
	// At is the monotonic-clock observation time of the event.
	// Encoded as RFC3339Nano for human-readable parsing without
	// loss of resolution.
	At time.Time `json:"at"`

	// Kind is the v8classifier.AdminEventKind label (e.g.
	// "protocol_fault", "aa_injection_drop").
	Kind string `json:"kind"`

	// FSMState is the telegram_fsm.State label at the time of
	// the event (e.g. "MASTER_HEADER", "MASTER_PAYLOAD").
	FSMState string `json:"fsm_state"`

	// Byte is the wire byte that triggered the event, encoded
	// as a 0x-prefixed two-digit hex string for human
	// readability (e.g. "0xAA"). Zero-valued for non-byte
	// events (queue overflow, echo timeouts).
	Byte string `json:"byte"`

	// WasEscaped reports whether the byte arrived as an
	// escape-decoded payload (true) or as a raw wire byte
	// (false). The shadow→enforce promotion gate uses this to
	// distinguish true-positive AA-injection (raw wire 0xAA
	// mid-frame) from data-byte 0xAA that arrived legitimately
	// via the ENS 0xA9 escape sequence.
	WasEscaped bool `json:"was_escaped"`
}

// handleV8AdminEvents returns the active v8 classifier's admin
// event ring buffer as JSON.
//
// **Default GET drains the ring** (destructive). Each successful
// GET returns the events accumulated since the last drain and
// EMPTIES the buffer. This is the documented contract for
// long-running poller tooling. Operators MUST poll at a steady
// cadence (every few seconds in production); a stuck consumer
// drops the OLDEST events FIFO and the `dropped` field surfaces
// the saturation.
//
// **`?peek=true` returns events WITHOUT draining** (non-
// destructive). For ad-hoc operator inspection (`curl | jq`,
// browser visit, dashboards, health checks) that should NOT
// consume the long-running poller's evidence stream. The full
// ring contents are returned every time — the operator is
// responsible for de-duplicating against prior peeks. Codex
// round-1 LOW on PR #657 — the GET-drain footgun when
// concurrent consumers share the HTTP surface.
//
// Nil-safe: when no classifier is registered (non-adapter-direct
// transport, or run() not yet completed), returns
// `{"events": [], "dropped": 0}` so probing tooling sees a
// well-formed response on every transport rather than 404-ing.
//
// Only GET is accepted — POST/etc respond 405 to avoid surprising
// side effects.
func handleV8AdminEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	peekOnly := r.URL.Query().Get("peek") == "true"

	resp := v8AdminEventsResponse{
		Events: []v8AdminEventJSON{},
	}

	withCurrentV8AdminClassifier(func(classifier *v8classifier.Classifier) {
		var events []v8classifier.ClassifierAdminEvent
		var dropped uint64
		if peekOnly {
			// Non-destructive ATOMIC read: events + dropped come
			// from a single mutex acquire (Codex round-2 MEDIUM
			// on PR #657 — separate calls would race against a
			// concurrent drain/overflow emit). The classifier
			// exposes the destructive variant via DrainAdminEvents.
			events, dropped = classifier.PeekAdminEvents()
		} else {
			events, dropped = classifier.DrainAdminEvents()
		}
		resp.Dropped = dropped
		if len(events) > 0 {
			resp.Events = make([]v8AdminEventJSON, len(events))
			for i, ev := range events {
				resp.Events[i] = v8AdminEventJSON{
					At:         ev.At,
					Kind:       ev.Kind.String(),
					FSMState:   ev.FSMState.String(),
					Byte:       fmt.Sprintf("0x%02X", ev.Byte),
					WasEscaped: ev.WasEscaped,
				}
			}
		}
	})

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if peekOnly {
		// Peek is idempotent. `private` keeps shared
		// intermediary caches (CDNs, reverse proxies) from
		// storing the response on behalf of unrelated
		// operators — this is a debug surface, not public
		// content. `max-age=1` keeps tooling that polls every
		// few seconds from seeing stale data while still
		// allowing the browser's own cache to debounce
		// duplicate-refresh storms. Codex round-2 LOW on
		// PR #657.
		w.Header().Set("Cache-Control", "private, max-age=1")
	} else {
		// Drain is destructive, the same response is never
		// valid twice. Tools that cache the response would
		// silently mask new events.
		w.Header().Set("Cache-Control", "no-store")
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("/debug/v8/admin-events: encode error: %v", err)
	}
}
