// Package b503session implements the live-monitor session FSM for the
// Vaillant B503 extended-register protocol as specified in
// helianthus-docs-ebus/protocols/vaillant/ebus-vaillant-B503.md sections 6,
// 7.1.1, and 7.4.
//
// The package is deliberately self-contained: no dependencies outside the
// Go standard library, no coupling to adaptermux internals. The exported
// API centers on a Manager that gates the single live-monitor ownership
// slot using a dedicated liveMonitorMu distinct from the B524 readMu.
package b503session

// State is the public FSM state of the live-monitor session.
//
// The internal expired state is used by the refresh-once policy but is
// NEVER returned by any public accessor: State() resolves expired into
// Active or Disabled before returning (spec §7.1.1).
type State int

const (
	// Idle is the initial and terminal state; no owner holds the gate.
	Idle State = iota
	// Enabling is a transient state during Idle->Active transition.
	Enabling
	// Active means the session holds the ownership gate and is accepting reads.
	Active
	// Disabled is a terminal post-failure state prior to returning to Idle.
	Disabled
	// expired is INTERNAL ONLY. It is set while a refresh-once policy runs
	// and is never leaked via State().
	expired
)

// String returns a stable human-readable label. The internal expired state
// maps to "Expired" for test-only diagnostics; production code paths must
// not observe it through State().
func (s State) String() string {
	switch s {
	case Idle:
		return "Idle"
	case Enabling:
		return "Enabling"
	case Active:
		return "Active"
	case Disabled:
		return "Disabled"
	case expired:
		return "Expired"
	default:
		return "Unknown"
	}
}
