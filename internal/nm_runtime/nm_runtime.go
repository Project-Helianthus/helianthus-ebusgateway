// Package nm_runtime is the catalog-driven Network-Management emit path.
//
// Per canonical plan §8, the gateway's NM runtime consumes catalog metadata
// for 0xFF command emit (FF 00 = reset_status broadcast, FF 02 =
// failure_message broadcast). After M4, zero hand-coded FF 00 / FF 02
// handlers survive; this package is the single emit boundary.
//
// CATALOG-DRIVEN INVARIANT (issue #505 r3106832675): the EmitEvent constants
// MUST be the EXACT service_variant strings emitted by the embedded ebusreg
// catalog YAML. The integration regression test
// nm_runtime_catalog_integration_test.go loads the embedded catalog and
// asserts every EmitEvent resolves to a real catalog command — drift is
// caught at test time, not at runtime against ErrNoCatalogEntry.
//
// Responder emit (FF 03 / FF 04 / FF 05 / FF 06) is NOT implemented — that
// scope is gated on M4b2 go/no-go (see canonical plan §8) and landed under
// M4c.
package nm_runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/execution_policy"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/rpc_source"
	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

// EmitEvent names the NM emit events supported in M4b first-delivery. The
// underlying string MUST match the catalog `service_variant` literal exactly
// (see ebusreg@30aa69a catalog/ebus_standard/catalog.yaml).
type EmitEvent string

// Supported emit events. Values are catalog `service_variant` strings.
const (
	EventResetStatus EmitEvent = "reset_status"    // FF 00
	EventFailure     EmitEvent = "failure_message" // FF 02
)

// ErrNoCatalogEntry is returned when the catalog has no command matching
// the requested emit event's 14-tuple identity.
var ErrNoCatalogEntry = errors.New("nm_runtime: no catalog entry for emit event")

// ErrEmitterRequired is returned by NewRuntime when the caller passes a nil
// Emitter. The NM runtime has no fallback transport — a nil emitter at
// construction is always a wiring bug, and fail-fast at construction is
// preferred over a lazy panic on the first Emit call (issue #505
// r3106859312).
var ErrEmitterRequired = errors.New("nm_runtime: emitter is required")

// Emitter is the transport-facing interface used by the NM runtime to push
// catalog-driven frames onto the bus. Implementations MUST use the gateway
// initiator source (rpc_source.Gateway).
type Emitter interface {
	// EmitBroadcast sends a broadcast frame with PB/SB and payload from
	// the gateway initiator source. The implementation MUST call
	// rpc_source.Enforce on the source byte it uses.
	EmitBroadcast(ctx context.Context, source, pb, sb byte, payload []byte) error
}

// Runtime is the catalog-driven NM emit runtime. It owns no hand-coded
// FF 00 / FF 02 tables; all emit decisions are driven by the catalog +
// shared execution_policy module.
type Runtime struct {
	catalog ebusstd.Catalog
	emitter Emitter
}

// NewRuntime constructs a runtime bound to the catalog and emitter. It
// returns ErrEmitterRequired when emitter is nil — the NM runtime has no
// valid mode of operation without a transport, and lazy-failing at the
// first Emit call would only surface the wiring bug much later (and as a
// nil-pointer panic). Fail-fast at construction per operator preference.
func NewRuntime(cat ebusstd.Catalog, emitter Emitter) (*Runtime, error) {
	if emitter == nil {
		return nil, ErrEmitterRequired
	}
	return &Runtime{catalog: cat, emitter: emitter}, nil
}

// Emit dispatches the named NM emit event through the catalog + policy
// gate. The policy gate is consulted with caller=system_nm_runtime so the
// compile-time whitelist in execution_policy applies.
func (r *Runtime) Emit(ctx context.Context, event EmitEvent, payload []byte) error {
	if err := rpc_source.Enforce(rpc_source.Gateway); err != nil {
		return err // defensive: Gateway is const, never non-113
	}
	cmd, ok := r.findEmit(event)
	if !ok {
		return fmt.Errorf("event=%q: %w", event, ErrNoCatalogEntry)
	}
	if err := execution_policy.Check(cmd, execution_policy.CallerSystemNMRuntime); err != nil {
		return err
	}
	pb := cmd.Identity.PBValue()
	sb := cmd.Identity.SBValue()
	return r.emitter.EmitBroadcast(ctx, rpc_source.Gateway, pb, sb, payload)
}

// findEmit selects the catalog command matching the broadcast emit event.
// Some catalog service_variants appear on multiple commands (e.g. request +
// response rows of the same query); for emit-broadcast the originator/
// broadcast/request row is the only valid match, so we filter on those axes
// in addition to the service_variant string.
func (r *Runtime) findEmit(event EmitEvent) (ebusstd.Command, bool) {
	target := string(event)
	for _, svc := range r.catalog.Services {
		for _, cmd := range svc.Commands {
			id := cmd.Identity
			if id.ServiceVariant != target {
				continue
			}
			if id.Direction != ebusstd.DirectionRequest {
				continue
			}
			if id.RequestOrResponseRole != ebusstd.RoleOriginator {
				continue
			}
			if id.BroadcastOrAddressed != ebusstd.AddressedBroadcast {
				continue
			}
			return cmd, true
		}
	}
	return ebusstd.Command{}, false
}
