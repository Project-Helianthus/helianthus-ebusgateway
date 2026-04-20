package nm_runtime_test

// M4c2 responder runtime tests — catalog-driven FF 03/04/05/06 emit path.
//
// Scope (decision doc @ 567a6798 §5, §6.2):
//   - Runtime construction MUST fail with ErrResponderTransportUnavailable
//     when the active transport does not satisfy ResponderTransport OR when
//     the capability signal reports scope=none.
//   - Per-inbound emit MUST consult execution_policy.Check with
//     CallerSystemNMRuntime before any byte hits the wire.
//   - Dispatch MUST route through ResponderTransport.SendResponderBytes.

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/execution_policy"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/nm_runtime"
	ebusgoxport "github.com/Project-Helianthus/helianthus-ebusgo/transport"
	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

// fakeResponderTransport implements ebusgoxport.ResponderTransport for
// runtime-construction tests. Calls to SendResponderBytes increment calls
// and record the payload.
type fakeResponderTransport struct {
	calls     int64
	lastBytes []byte
	err       error
}

func (f *fakeResponderTransport) SendResponderBytes(payload []byte) (int, error) {
	atomic.AddInt64(&f.calls, 1)
	if f.err != nil {
		return 0, f.err
	}
	b := make([]byte, len(payload))
	copy(b, payload)
	f.lastBytes = b
	return len(payload), nil
}

// Compile-time check: fakeResponderTransport satisfies the interface.
var _ ebusgoxport.ResponderTransport = (*fakeResponderTransport)(nil)

func loadTestCatalog(t *testing.T) ebusstd.Catalog {
	t.Helper()
	return ebusstd.MustEmbeddedCatalog()
}

// TestResponderRuntime_ConstructionRequiresTransport asserts nil transport
// fails construction with ErrResponderTransportUnavailable (mirrors
// ErrEmitterRequired fail-fast pattern in nm_runtime.NewRuntime).
func TestResponderRuntime_ConstructionRequiresTransport(t *testing.T) {
	cat := loadTestCatalog(t)
	_, err := nm_runtime.NewResponderRuntime(cat, nil)
	if err == nil {
		t.Fatal("expected ErrResponderTransportUnavailable on nil transport")
	}
	if !errors.Is(err, execution_policy.ErrResponderTransportUnavailable) {
		t.Fatalf("err=%v, want ErrResponderTransportUnavailable", err)
	}
}

// TestResponderRuntime_ConstructionSucceedsOnENHLike asserts a transport
// that satisfies ResponderTransport constructs cleanly.
func TestResponderRuntime_ConstructionSucceedsOnENHLike(t *testing.T) {
	cat := loadTestCatalog(t)
	tp := &fakeResponderTransport{}
	rt, err := nm_runtime.NewResponderRuntime(cat, tp)
	if err != nil {
		t.Fatalf("construction failed: %v", err)
	}
	if rt == nil {
		t.Fatal("runtime is nil")
	}
}

// TestResponderRuntime_PolicyGateEnforced asserts an emit for a
// catalog-unknown identity is rejected WITHOUT any bytes hitting the wire.
// Uses an identity that will not match the 14-axis whitelist.
func TestResponderRuntime_PolicyGateEnforced(t *testing.T) {
	cat := loadTestCatalog(t)
	tp := &fakeResponderTransport{}
	rt, err := nm_runtime.NewResponderRuntime(cat, tp)
	if err != nil {
		t.Fatalf("construction: %v", err)
	}
	// Fabricate a command with a mutating safety class that is NOT in the
	// whitelist. Any non-FF-03..06 row with SafetyMutating qualifies.
	cmd := ebusstd.Command{
		ID: "synthetic.unauthorized",
		Identity: ebusstd.IdentityKey{
			Namespace: "ebus_standard",
		},
		SafetyClass: ebusstd.SafetyMutating,
	}
	err = rt.EmitResponder(cmd, []byte{0x00})
	if err == nil {
		t.Fatal("expected policy denial for non-whitelisted mutating row")
	}
	if !execution_policy.IsDenied(err) {
		t.Fatalf("err=%v, want ErrSafetyClassDenied", err)
	}
	if atomic.LoadInt64(&tp.calls) != 0 {
		t.Fatalf("bytes hit wire despite policy denial: %d calls", tp.calls)
	}
}

// TestResponderRuntime_DispatchesViaSendResponderBytes asserts the happy-path
// dispatch reaches the ResponderTransport. Uses a read_only_safe shim so
// the policy gate accepts without depending on whitelist contents.
func TestResponderRuntime_DispatchesViaSendResponderBytes(t *testing.T) {
	cat := loadTestCatalog(t)
	tp := &fakeResponderTransport{}
	rt, err := nm_runtime.NewResponderRuntime(cat, tp)
	if err != nil {
		t.Fatalf("construction: %v", err)
	}
	cmd := ebusstd.Command{
		ID:          "synthetic.responder_probe",
		Identity:    ebusstd.IdentityKey{Namespace: "ebus_standard"},
		SafetyClass: ebusstd.SafetyReadOnlySafe,
	}
	payload := []byte{0xAA, 0xBB, 0xCC}
	if err := rt.EmitResponder(cmd, payload); err != nil {
		t.Fatalf("EmitResponder: %v", err)
	}
	if got := atomic.LoadInt64(&tp.calls); got != 1 {
		t.Fatalf("SendResponderBytes calls=%d, want 1", got)
	}
	if len(tp.lastBytes) != len(payload) {
		t.Fatalf("dispatched %d bytes, want %d", len(tp.lastBytes), len(payload))
	}
}
