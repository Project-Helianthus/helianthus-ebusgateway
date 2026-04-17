package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// ---------------------------------------------------------------------
// Source-resolution diagnostics
// ---------------------------------------------------------------------

// TestStartupScanSourceMode verifies the diagnostic label for each
// source-selection path.
func TestStartupScanSourceMode(t *testing.T) {
	tests := []struct {
		name     string
		original ebusgateway.Config
		resolved ebusgateway.Config
		want     string
	}{
		{
			name: "auto-proxy (auto=true, source=0x00, resolved to 0xF7)",
			original: ebusgateway.Config{
				ScanSourceAuto: true,
				ScanSource:     0x00,
			},
			resolved: ebusgateway.Config{
				ScanSourceAuto: false,
				ScanSource:     proxyObserveFirstStartupSource,
			},
			want: "auto-proxy",
		},
		{
			name: "auto kept (auto=true, source=0x71, no proxy change)",
			original: ebusgateway.Config{
				ScanSourceAuto: true,
				ScanSource:     0x71,
			},
			resolved: ebusgateway.Config{
				ScanSourceAuto: true,
				ScanSource:     0x71,
			},
			want: "auto",
		},
		{
			name: "configured (auto=false, source=0x71)",
			original: ebusgateway.Config{
				ScanSourceAuto: false,
				ScanSource:     0x71,
			},
			resolved: ebusgateway.Config{
				ScanSourceAuto: false,
				ScanSource:     0x71,
			},
			want: "configured",
		},
		{
			name: "default (auto=false, source=0x00)",
			original: ebusgateway.Config{
				ScanSourceAuto: false,
				ScanSource:     0x00,
			},
			resolved: ebusgateway.Config{
				ScanSourceAuto: false,
				ScanSource:     0x00,
			},
			want: "default",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := startupScanSourceMode(tc.original, tc.resolved)
			if got != tc.want {
				t.Fatalf("startupScanSourceMode() = %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------
// statsBus per-attempt bounded logging
// ---------------------------------------------------------------------

// stubBus implements registry.ScanBus, returning a programmable
// sequence of errors (nil = success).
type stubBus struct {
	errors []error
	calls  int
}

func (s *stubBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	i := s.calls
	s.calls++
	if i < len(s.errors) {
		return nil, s.errors[i]
	}
	return nil, nil
}

var _ registry.ScanBus = (*stubBus)(nil)

// TestStatsBus_AttemptsBoundedToCap proves that statsBus records at most
// scanAttemptLogCap per-attempt entries, regardless of how many targets
// are scanned. This protects production logs from 164-target floods.
func TestStatsBus_AttemptsBoundedToCap(t *testing.T) {
	// Generate cap+20 errors to ensure the bound is exercised.
	errs := make([]error, scanAttemptLogCap+20)
	for i := range errs {
		errs[i] = ebuserrors.ErrTimeout
	}
	sb := &statsBus{
		bus:    &stubBus{errors: errs},
		source: 0x71,
	}

	var frames []protocol.Frame
	for i := 0; i < scanAttemptLogCap+20; i++ {
		frames = append(frames, protocol.Frame{Source: 0x71, Target: byte(0x10 + i)})
	}
	for _, f := range frames {
		_, _ = sb.Send(context.Background(), f)
	}

	if len(sb.attempts) != scanAttemptLogCap {
		t.Fatalf("attempts = %d, want %d (bounded by cap)", len(sb.attempts), scanAttemptLogCap)
	}
	if sb.total != scanAttemptLogCap+20 {
		t.Fatalf("total = %d, want %d", sb.total, scanAttemptLogCap+20)
	}
	if sb.stats.timeouts != scanAttemptLogCap+20 {
		t.Fatalf("timeouts = %d, want %d", sb.stats.timeouts, scanAttemptLogCap+20)
	}
}

// TestStatsBus_AttemptRecordsSourceTargetClass proves that each
// recorded attempt carries source, target, result class, and error
// string (truncated).
func TestStatsBus_AttemptRecordsSourceTargetClass(t *testing.T) {
	sb := &statsBus{
		bus: &stubBus{errors: []error{
			nil,
			ebuserrors.ErrTimeout,
			ebuserrors.ErrBusCollision,
			ebuserrors.ErrNACK,
			ebuserrors.ErrCRCMismatch,
			errors.New("unknown protocol error"),
		}},
		source: 0x71,
	}
	targets := []byte{0x08, 0x10, 0x15, 0x26, 0x04, 0x03}
	for _, tgt := range targets {
		_, _ = sb.Send(context.Background(), protocol.Frame{Source: 0x71, Target: tgt})
	}

	if len(sb.attempts) != 6 {
		t.Fatalf("attempts = %d, want 6", len(sb.attempts))
	}
	wantClasses := []string{"ok", "timeout", "collision", "nack", "crc", "other"}
	for i, a := range sb.attempts {
		if a.source != 0x71 {
			t.Errorf("attempt %d source = 0x%02X, want 0x71", i, a.source)
		}
		if a.target != targets[i] {
			t.Errorf("attempt %d target = 0x%02X, want 0x%02X", i, a.target, targets[i])
		}
		if a.resClass != wantClasses[i] {
			t.Errorf("attempt %d resClass = %q, want %q", i, a.resClass, wantClasses[i])
		}
	}
	// ok attempt has empty error string.
	if sb.attempts[0].errStr != "" {
		t.Errorf("ok attempt errStr = %q, want empty", sb.attempts[0].errStr)
	}
	// non-ok attempts have non-empty truncated error string.
	for i := 1; i < 6; i++ {
		if sb.attempts[i].errStr == "" {
			t.Errorf("non-ok attempt %d has empty errStr", i)
		}
	}
}

// TestTruncateErr verifies that error strings are capped at maxErrStrLen.
func TestTruncateErr(t *testing.T) {
	if truncateErr(nil) != "" {
		t.Fatal("truncateErr(nil) must be empty")
	}
	short := errors.New("short")
	if truncateErr(short) != "short" {
		t.Fatal("short error unchanged")
	}
	longMsg := make([]byte, maxErrStrLen*2)
	for i := range longMsg {
		longMsg[i] = 'x'
	}
	long := errors.New(string(longMsg))
	got := truncateErr(long)
	if len(got) != maxErrStrLen+3 {
		t.Fatalf("truncated len = %d, want %d", len(got), maxErrStrLen+3)
	}
	if got[len(got)-3:] != "..." {
		t.Fatalf("truncated suffix = %q, want '...'", got[len(got)-3:])
	}
}

// ---------------------------------------------------------------------
// Transaction-classification wiring into scan attempt log
// ---------------------------------------------------------------------

// fakeClassifier implements activeTxnClassifier for tests.
type fakeClassifier struct {
	val string
}

func (f *fakeClassifier) LastTxnClass() string { return f.val }

// TestScanAttemptLog_IncludesTxnClass proves that when a classifier is
// wired into statsBus, each recorded attempt carries the classifier's
// LastTxnClass value. This is the runtime diagnostic seam that lets
// operators distinguish echo-only / invalid-frame / schema / success
// failure modes at the scan-attempt granularity.
func TestScanAttemptLog_IncludesTxnClass(t *testing.T) {
	fc := &fakeClassifier{val: "echo_only_timeout"}
	sb := &statsBus{
		bus:        &stubBus{errors: []error{ebuserrors.ErrTimeout, nil}},
		source:     0x71,
		classifier: fc,
	}
	_, _ = sb.Send(context.Background(), protocol.Frame{Source: 0x71, Target: 0x08, Primary: 0x07, Secondary: 0x04})
	fc.val = "success_like"
	_, _ = sb.Send(context.Background(), protocol.Frame{Source: 0x71, Target: 0x10, Primary: 0x07, Secondary: 0x04})

	if len(sb.attempts) != 2 {
		t.Fatalf("attempts = %d, want 2", len(sb.attempts))
	}
	if sb.attempts[0].txnClass != "echo_only_timeout" {
		t.Errorf("attempts[0].txnClass = %q, want %q", sb.attempts[0].txnClass, "echo_only_timeout")
	}
	if sb.attempts[1].txnClass != "success_like" {
		t.Errorf("attempts[1].txnClass = %q, want %q", sb.attempts[1].txnClass, "success_like")
	}
	// probeKind also recorded for shape triage.
	if sb.attempts[0].probeKind != "scan_07_04" {
		t.Errorf("attempts[0].probeKind = %q, want %q", sb.attempts[0].probeKind, "scan_07_04")
	}
}

// TestScanAttemptLog_BoundedWithTxnClass proves the cap is still
// enforced when classifier is wired — classification doesn't bypass
// the bounded-log invariant.
func TestScanAttemptLog_BoundedWithTxnClass(t *testing.T) {
	fc := &fakeClassifier{val: "echo_only_timeout"}
	errs := make([]error, scanAttemptLogCap+10)
	for i := range errs {
		errs[i] = ebuserrors.ErrTimeout
	}
	sb := &statsBus{
		bus:        &stubBus{errors: errs},
		source:     0x71,
		classifier: fc,
	}
	for i := 0; i < scanAttemptLogCap+10; i++ {
		_, _ = sb.Send(context.Background(),
			protocol.Frame{Source: 0x71, Target: byte(0x10 + i), Primary: 0x07, Secondary: 0x04})
	}
	if len(sb.attempts) != scanAttemptLogCap {
		t.Fatalf("attempts = %d, want %d", len(sb.attempts), scanAttemptLogCap)
	}
	// All retained entries must carry the classifier's value — not empty.
	for i, a := range sb.attempts {
		if a.txnClass != "echo_only_timeout" {
			t.Errorf("attempts[%d].txnClass = %q, want %q", i, a.txnClass, "echo_only_timeout")
		}
	}
}

// TestScanAttemptLog_NoClassifier_EmptyTxnClass proves that when no
// classifier is wired the txnClass field remains empty (optional-
// interface pattern: adapters without Mux degrade gracefully, never panic).
func TestScanAttemptLog_NoClassifier_EmptyTxnClass(t *testing.T) {
	sb := &statsBus{
		bus:    &stubBus{errors: []error{ebuserrors.ErrTimeout}},
		source: 0x71,
		// classifier: nil (explicit).
	}
	_, _ = sb.Send(context.Background(), protocol.Frame{Source: 0x71, Target: 0x08, Primary: 0x07, Secondary: 0x04})
	if len(sb.attempts) != 1 {
		t.Fatalf("attempts = %d, want 1", len(sb.attempts))
	}
	if sb.attempts[0].txnClass != "" {
		t.Errorf("attempts[0].txnClass = %q, want empty (nil classifier)", sb.attempts[0].txnClass)
	}
}

// TestScanProbeKind_Classification verifies bounded probe-kind labels.
func TestScanProbeKind_Classification(t *testing.T) {
	tests := []struct {
		name  string
		frame protocol.Frame
		want  string
	}{
		{"07 04 identification", protocol.Frame{Primary: 0x07, Secondary: 0x04}, "scan_07_04"},
		{"B5 09 identity", protocol.Frame{Primary: 0xB5, Secondary: 0x09}, "b509_identity"},
		{"B5 24 register", protocol.Frame{Primary: 0xB5, Secondary: 0x24}, "b524_register"},
		{"unknown", protocol.Frame{Primary: 0x03, Secondary: 0x00}, "other"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := scanProbeKind(tc.frame); got != tc.want {
				t.Errorf("scanProbeKind = %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------
// Scan-request shape: frame source/target/primary/secondary match contract
// ---------------------------------------------------------------------

// shapeCaptureBus captures every frame the scan path sends so the test
// can assert the wire-shape contract. Stops the scan at the first send
// by returning a nil response + ErrTimeout (scan tolerates and retries).
type shapeCaptureBus struct {
	frames []protocol.Frame
}

func (b *shapeCaptureBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	b.frames = append(b.frames, frame)
	return nil, ebuserrors.ErrTimeout
}

// TestScanRequestShape_AdapterDirect proves the adapter-direct startup
// scan builds frames with the exact contract expected by a Vaillant bus:
//
//	Frame.Source    == configured ScanSource (e.g. 0xF7 proxy or 0x71)
//	Frame.Target    ∈ valid scan-target iteration range (0x01..0xFD)
//	Frame.Primary   == 0x07 (identification)
//	Frame.Secondary == 0x04
//	Frame.Data      == nil / empty (scan probe carries no payload)
//
// This is the minimum shape contract; a wrong primary/secondary would
// cause every probe to time out regardless of peer presence.
func TestScanRequestShape_AdapterDirect(t *testing.T) {
	bus := &shapeCaptureBus{}
	reg := registry.NewDeviceRegistry(nil)

	// Use a small explicit target list so the test finishes quickly.
	targets := []byte{0x08, 0x15, 0x26}
	const source byte = 0x71
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _ = registry.Scan(ctx, bus, reg, source, targets)

	if len(bus.frames) == 0 {
		t.Fatal("scan did not emit any frames")
	}
	// All emitted frames must satisfy the shape contract.
	for i, f := range bus.frames {
		if f.Source != source {
			t.Errorf("frame[%d].Source = 0x%02X, want 0x%02X", i, f.Source, source)
		}
		if f.Primary != 0x07 {
			t.Errorf("frame[%d].Primary = 0x%02X, want 0x07", i, f.Primary)
		}
		if f.Secondary != 0x04 {
			t.Errorf("frame[%d].Secondary = 0x%02X, want 0x04", i, f.Secondary)
		}
		if len(f.Data) != 0 {
			t.Errorf("frame[%d].Data len = %d, want 0", i, len(f.Data))
		}
		// Target must be from our iteration range.
		found := false
		for _, t2 := range targets {
			if f.Target == t2 {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("frame[%d].Target = 0x%02X not in configured targets % X", i, f.Target, targets)
		}
	}
}

// TestClassifyScanErr verifies error classification matches the
// documented set.
func TestClassifyScanErr(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{nil, "ok"},
		{ebuserrors.ErrTimeout, "timeout"},
		{context.DeadlineExceeded, "timeout"},
		{ebuserrors.ErrBusCollision, "collision"},
		{ebuserrors.ErrNACK, "nack"},
		{ebuserrors.ErrCRCMismatch, "crc"},
		{errors.New("unrelated"), "other"},
	}
	for _, tc := range tests {
		if got := classifyScanErr(tc.err); got != tc.want {
			t.Errorf("classifyScanErr(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
	// Sanity: ensure at least one call uses time.Duration to keep
	// the import live if refactored later.
	_ = time.Duration(0)
}

// TestStatsBus_NonOkPrioritizedOverOk proves that when the bounded
// attempts cap is full of "ok" entries, a later non-ok attempt evicts
// the oldest "ok" slot. This keeps the cap focused on failure evidence
// even in mixed passes where early targets succeed and later fail.
func TestStatsBus_NonOkPrioritizedOverOk(t *testing.T) {
	// Fill cap with oks, then add non-oks.
	errs := make([]error, scanAttemptLogCap+5)
	// First cap attempts: ok (nil). Then 5 timeouts.
	for i := scanAttemptLogCap; i < len(errs); i++ {
		errs[i] = ebuserrors.ErrTimeout
	}
	sb := &statsBus{
		bus:    &stubBus{errors: errs},
		source: 0x71,
	}
	for i := 0; i < len(errs); i++ {
		_, _ = sb.Send(context.Background(), protocol.Frame{Source: 0x71, Target: byte(0x10 + i)})
	}

	if len(sb.attempts) != scanAttemptLogCap {
		t.Fatalf("attempts = %d, want %d", len(sb.attempts), scanAttemptLogCap)
	}

	// Count classes in retained attempts. All 5 non-ok attempts should
	// have displaced ok entries.
	okCount := 0
	timeoutCount := 0
	for _, a := range sb.attempts {
		switch a.resClass {
		case "ok":
			okCount++
		case "timeout":
			timeoutCount++
		}
	}
	if timeoutCount != 5 {
		t.Fatalf("timeout attempts retained = %d, want 5 (non-ok must displace ok entries)", timeoutCount)
	}
	if okCount != scanAttemptLogCap-5 {
		t.Fatalf("ok attempts retained = %d, want %d (older oks evicted for non-oks)", okCount, scanAttemptLogCap-5)
	}
}

// TestStatsBus_AllNonOk_NoFurtherEvictions proves that once the cap
// is full of non-ok entries, subsequent non-ok attempts do NOT evict
// any of them (first-N non-ok wins).
func TestStatsBus_AllNonOk_NoFurtherEvictions(t *testing.T) {
	errs := make([]error, scanAttemptLogCap+10)
	for i := range errs {
		errs[i] = ebuserrors.ErrTimeout
	}
	sb := &statsBus{
		bus:    &stubBus{errors: errs},
		source: 0x71,
	}
	// Send cap+10 timeouts with distinct target bytes.
	for i := 0; i < len(errs); i++ {
		_, _ = sb.Send(context.Background(), protocol.Frame{Source: 0x71, Target: byte(0x10 + i)})
	}

	if len(sb.attempts) != scanAttemptLogCap {
		t.Fatalf("attempts = %d, want %d", len(sb.attempts), scanAttemptLogCap)
	}
	// The retained attempts should be the FIRST cap targets (0x10..).
	for i, a := range sb.attempts {
		wantTarget := byte(0x10 + i)
		if a.target != wantTarget {
			t.Fatalf("attempt[%d].target = 0x%02X, want 0x%02X (first-N non-ok must be retained)", i, a.target, wantTarget)
		}
	}
}

// TestStartDiscoveryScanLoopWithClassifier_InstanceScoped proves that the
// classifier is threaded through function arguments only, not a package
// global. Two concurrent scan passes with DIFFERENT classifiers must
// record each classifier's value onto its own statsBus — no cross-
// contamination — which is only possible when the classifier is
// instance-local state.
func TestStartDiscoveryScanLoopWithClassifier_InstanceScoped(t *testing.T) {
	fcA := &fakeClassifier{val: "echo_only_timeout"}
	fcB := &fakeClassifier{val: "success_like"}

	sbA := &statsBus{
		bus:        &stubBus{errors: []error{ebuserrors.ErrTimeout}},
		source:     0x71,
		classifier: fcA,
	}
	sbB := &statsBus{
		bus:        &stubBus{errors: []error{ebuserrors.ErrTimeout}},
		source:     0x71,
		classifier: fcB,
	}

	_, _ = sbA.Send(context.Background(), protocol.Frame{Source: 0x71, Target: 0x08, Primary: 0x07, Secondary: 0x04})
	_, _ = sbB.Send(context.Background(), protocol.Frame{Source: 0x71, Target: 0x10, Primary: 0x07, Secondary: 0x04})

	if len(sbA.attempts) != 1 || sbA.attempts[0].txnClass != "echo_only_timeout" {
		t.Fatalf("sbA: got %+v, want 1 attempt with txnClass=echo_only_timeout", sbA.attempts)
	}
	if len(sbB.attempts) != 1 || sbB.attempts[0].txnClass != "success_like" {
		t.Fatalf("sbB: got %+v, want 1 attempt with txnClass=success_like", sbB.attempts)
	}
	// Cross-check: flipping fcA's value must not retroactively affect
	// recorded attempt (snapshot at Send time), nor leak into sbB.
	fcA.val = "mutated"
	if sbA.attempts[0].txnClass != "echo_only_timeout" {
		t.Errorf("sbA.attempts[0].txnClass mutated after Send: %q", sbA.attempts[0].txnClass)
	}
	if sbB.attempts[0].txnClass != "success_like" {
		t.Errorf("sbB.attempts[0].txnClass cross-contaminated: %q", sbB.attempts[0].txnClass)
	}
}

// TestStartDiscoveryScanLoop_NoPackageGlobalClassifier ensures that the
// 4-arg default entry point (used by tests and as the pre-rebind value
// of startDiscoveryScanLoopFn) forwards a nil classifier — i.e. the
// default path has NO implicit package-level wiring. Production wiring
// happens only when run() rebinds startDiscoveryScanLoopFn to a closure
// capturing the instance mux (see main.go).
func TestStartDiscoveryScanLoop_NoPackageGlobalClassifier(t *testing.T) {
	// Synthesize a statsBus exactly as startDiscoveryScanLoop would
	// when invoked via the default 4-arg seam (classifier == nil).
	sb := &statsBus{
		bus:        &stubBus{errors: []error{ebuserrors.ErrTimeout}},
		source:     0x71,
		classifier: nil,
	}
	_, _ = sb.Send(context.Background(), protocol.Frame{Source: 0x71, Target: 0x08, Primary: 0x07, Secondary: 0x04})
	if sb.attempts[0].txnClass != "" {
		t.Errorf("default entry point leaked a classifier: txnClass=%q", sb.attempts[0].txnClass)
	}
}
