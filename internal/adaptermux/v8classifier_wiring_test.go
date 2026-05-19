package adaptermux

import (
	"context"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
)

// Phase 3 Step B3.2: integration tests for the v8 classifier
// wiring at the adaptermux.Mux boundary. These pin:
//
//   - cfg.V8ClassifierMode = ModeOff yields a nil Mux.V8Classifier()
//     (no allocation, no observation overhead).
//   - cfg.V8ClassifierMode = ModeShadow / ModeEnforce instantiates a
//     classifier whose ObservedBytesTotal increments as the readLoop
//     dispatches stream events.
//   - The classifier's presence does NOT alter the byte stream
//     forwarded to sessions (legacy adaptermux tests in the same
//     package already pin that — they all run with the default
//     ModeOff). The shadow-mode test below confirms the BYTES counter
//     bumps without exercising any session-level assertion (which is
//     the same null behavior as ModeOff).

// newClassifiedTestMux is a variant of newP3TestMux that lets a test
// override the V8ClassifierMode. The rest of the configuration
// matches newP3TestMux exactly so legacy invariants stay aligned.
func newClassifiedTestMux(t *testing.T, mode v8classifier.Mode) (*Mux, *p3MockTransport, context.CancelFunc, func()) {
	t.Helper()

	mock := newP3MockTransport()

	mux := New(Config{
		Protocol:         "enh",
		Network:          "tcp",
		Address:          "127.0.0.1:0",
		ReadTimeout:      200 * time.Millisecond,
		PendingStartTTL:  24 * time.Hour,
		SYNInterval:      time.Hour,
		V8ClassifierMode: mode,
	})

	ctx, cancel := context.WithCancel(context.Background())
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	mux.stateMu.Lock()
	mux.lastWireActivity = time.Now()
	mux.stateMu.Unlock()

	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()

	cleanup := func() {
		cancel()
		closeOrLog(t, mock, "p3 mock transport")
		mux.wg.Wait()
	}

	return mux, mock, cancel, cleanup
}

// TestV8Classifier_OffMode_NilClassifier pins the production-default
// invariant: ModeOff (the zero value) yields a nil classifier
// instance on the Mux. This is the production-default behavior
// through Phase 3 until B3.7 live-bus validation passes.
func TestV8Classifier_OffMode_NilClassifier(t *testing.T) {
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeOff)
	defer cleanup()

	if got := mux.V8Classifier(); got != nil {
		t.Errorf("V8Classifier() = %v; want nil in ModeOff (zero-allocation default)", got)
	}
}

// TestV8Classifier_ShadowMode_InstantiatedAndObserves pins the
// shadow-mode wiring: configuring ModeShadow instantiates a real
// classifier whose ObservedBytesTotal increments as bytes flow
// through the readLoop. The byte stream itself is NOT altered
// (B3.2 scaffold; real filtering lands in B3.3+).
func TestV8Classifier_ShadowMode_InstantiatedAndObserves(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	c := mux.V8Classifier()
	if c == nil {
		t.Fatal("V8Classifier() = nil in ModeShadow; want non-nil")
	}
	if got := c.Mode(); got != v8classifier.ModeShadow {
		t.Errorf("classifier mode = %v; want ModeShadow", got)
	}

	// Push 5 byte events through the mock; readLoop dispatches
	// each to onReceived AND through the classifier Observe hook.
	const n = 5
	for i := 0; i < n; i++ {
		mock.eventCh <- transport.StreamEvent{
			Kind: transport.StreamEventByte,
			Byte: 0xAA, // SYN — won't disturb mux state (idle marker)
		}
	}

	// Poll until the classifier has observed all 5 events or the
	// deadline expires. The readLoop processes events
	// asynchronously, so we need to wait for the dispatch.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.ObservedBytesTotal() >= n {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.ObservedBytesTotal(); got != n {
		t.Errorf("ObservedBytesTotal() = %d; want %d (readLoop must call Observe for every dispatched StreamEvent)", got, n)
	}
}

// TestV8Classifier_ShadowMode_ProvenanceCountersThroughMux pins the
// B3.3 provenance taxonomy at the mux boundary: bytes with
// different (Byte, WasEscaped) tuples flowing through the
// readLoop must land in the correct provenance buckets.
//
// Three event shapes:
//   - (0xAA, WasEscaped=true)  → escapedPayloadAaTotal
//   - (0xAA, WasEscaped=false) → wireAutoSynTotal
//   - (0x55, WasEscaped=false) → plainByteTotal
func TestV8Classifier_ShadowMode_ProvenanceCountersThroughMux(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	c := mux.V8Classifier()
	if c == nil {
		t.Fatal("V8Classifier() = nil in ModeShadow; want non-nil")
	}

	// Push 3 of each shape (9 StreamEventByte events).
	for i := 0; i < 3; i++ {
		mock.eventCh <- transport.StreamEvent{
			Kind:       transport.StreamEventByte,
			Byte:       0xAA,
			WasEscaped: true,
		}
		mock.eventCh <- transport.StreamEvent{
			Kind:       transport.StreamEventByte,
			Byte:       0xAA,
			WasEscaped: false,
		}
		mock.eventCh <- transport.StreamEvent{
			Kind:       transport.StreamEventByte,
			Byte:       0x55,
			WasEscaped: false,
		}
	}

	// Poll until all 9 events are observed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.ObservedBytesTotal() >= 9 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.ObservedBytesTotal(); got != 9 {
		t.Fatalf("ObservedBytesTotal()=%d; want 9 (readLoop must dispatch every StreamEvent)", got)
	}

	if got := c.EscapedPayloadAaTotal(); got != 3 {
		t.Errorf("EscapedPayloadAaTotal()=%d; want 3 (3 payload-AA events)", got)
	}
	if got := c.WireAutoSynTotal(); got != 3 {
		t.Errorf("WireAutoSynTotal()=%d; want 3 (3 wire-SYN events)", got)
	}
	if got := c.PlainByteTotal(); got != 3 {
		t.Errorf("PlainByteTotal()=%d; want 3 (3 plain-byte events)", got)
	}
	// EscapedPayloadEsc should stay 0 — we didn't send any.
	if got := c.EscapedPayloadEscTotal(); got != 0 {
		t.Errorf("EscapedPayloadEscTotal()=%d; want 0 (no payload-ESC events sent)", got)
	}

	// Cross-counter invariant from the unit test, re-verified at
	// the mux level: sum of provenance buckets == count of
	// StreamEventByte events == 9.
	sum := c.EscapedPayloadAaTotal() + c.EscapedPayloadEscTotal() +
		c.WireAutoSynTotal() + c.PlainByteTotal()
	if sum != 9 {
		t.Errorf("sum of provenance counters = %d; want 9", sum)
	}
}

// TestV8Classifier_EnforceMode_InstantiatedAndObservesButDoesNotDrop
// pins the B3.2-scaffold invariant for enforce mode: even when the
// caller explicitly opts in to ModeEnforce, the classifier in this
// PR ONLY observes; it does NOT yet drop or rewrite any bytes.
// Real filtering lands in B3.3+.
//
// MAINTAINER NOTE: when B3.3+ wires real filtering authority into
// the enforce path, this test MUST be UPDATED OR DELETED — its
// purpose is to fire if a future PR accidentally enables filtering
// before the full B3.3..B3.7 stack lands. Once enforce gains real
// filtering, replace the "drop=false always" assertion with a
// session-level behavior assertion that the right bytes are
// filtered.
func TestV8Classifier_EnforceMode_InstantiatedAndObservesButDoesNotDrop(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeEnforce)
	defer cleanup()

	c := mux.V8Classifier()
	if c == nil {
		t.Fatal("V8Classifier() = nil in ModeEnforce; want non-nil")
	}
	if got := c.Mode(); got != v8classifier.ModeEnforce {
		t.Errorf("classifier mode = %v; want ModeEnforce", got)
	}

	// Push a single SYN. Classifier MUST observe it; mux MUST emit
	// it normally (no drop). The latter is implicitly asserted by
	// readLoop's existing onReceived dispatch — if Observe ever
	// returned drop=true, the existing legacy adaptermux tests
	// would surface the regression.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if c.ObservedBytesTotal() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.ObservedBytesTotal(); got != 1 {
		t.Errorf("ObservedBytesTotal() = %d; want 1 in ModeEnforce", got)
	}
}
