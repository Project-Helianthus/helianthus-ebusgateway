package nm_runtime

// M4c2 — catalog-driven responder runtime (FF 03/04/05/06).
//
// Scope per decision doc @ 567a6798 §5:
//   - Per-inbound emit consults execution_policy.Check with caller
//     CallerSystemNMRuntime before any byte hits the wire.
//   - Dispatches via transport.ResponderTransport.SendResponderBytes
//     (bypasses arbitration — the caller has already observed the
//     initiator header).
//   - Construction fails fast with ErrResponderTransportUnavailable if
//     the transport does not satisfy ResponderTransport; mirrors
//     NewRuntime's ErrEmitterRequired fail-fast pattern.
//
// The FSM that decodes inbound frames and sequences ACK/response/final-ACK
// lives in github.com/Project-Helianthus/helianthus-ebusgo/protocol/responder.
// M4c2 scope is the CATALOG + POLICY + DISPATCH glue on the gateway side;
// the wire state machine remains owned by ebusgo.

import (
	"fmt"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/execution_policy"
	ebusgoxport "github.com/Project-Helianthus/helianthus-ebusgo/transport"
	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

// ResponderRuntime is the gateway-side responder emit runtime for
// FF 03/04/05/06. It is catalog-driven: no hand-rolled opcode tables
// survive this boundary.
type ResponderRuntime struct {
	catalog   ebusstd.Catalog
	transport ebusgoxport.ResponderTransport
}

// NewResponderRuntime constructs a responder runtime bound to the
// catalog and a transport that MUST satisfy ResponderTransport.
// Returns ErrResponderTransportUnavailable when transport is nil (the
// runtime has no fallback path — fail-fast at construction per
// §5 "transport-capability pre-gate").
//
// Note: we accept the interface type directly so a nil interface check
// correctly detects both (*T)(nil) and an unset interface. Callers in
// main.go only invoke this when transport selection has produced a
// concrete ENH/ENS transport; ebusd-tcp deployments must NOT call this
// (the main-bootstrap branch omits runtime construction entirely and
// populates the capability provider with scope=none instead).
func NewResponderRuntime(cat ebusstd.Catalog, tp ebusgoxport.ResponderTransport) (*ResponderRuntime, error) {
	if tp == nil {
		return nil, fmt.Errorf("nm_runtime: %w", execution_policy.ErrResponderTransportUnavailable)
	}
	return &ResponderRuntime{catalog: cat, transport: tp}, nil
}

// EmitResponder dispatches a responder-direction emission for cmd with
// payload. The flow is:
//  1. Consult execution_policy.Check with CallerSystemNMRuntime. On
//     denial, the error satisfies errors.Is(err, ErrSafetyClassDenied)
//     and NO bytes hit the wire.
//  2. On accept, dispatch via ResponderTransport.SendResponderBytes.
//
// The catalog parameter is carried on the runtime for future extension
// (per-inbound identity-key match against the embedded catalog); at
// M4c2 scope the caller is already expected to present a resolved
// ebusstd.Command from the inbound-frame decoder.
func (r *ResponderRuntime) EmitResponder(cmd ebusstd.Command, payload []byte) error {
	if err := execution_policy.Check(cmd, execution_policy.CallerSystemNMRuntime); err != nil {
		return err
	}
	if _, err := r.transport.SendResponderBytes(payload); err != nil {
		return fmt.Errorf("nm_runtime: responder send: %w", err)
	}
	return nil
}
