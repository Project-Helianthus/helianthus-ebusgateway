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
