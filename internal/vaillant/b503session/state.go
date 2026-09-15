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
// Refreshing is a stable transitional state for the refresh-once policy. It
// keeps ownership visible without claiming the session is active while the
// transport incarnation is being re-homed (spec §7.1.1).
type State int

const (
	// Idle is the initial and terminal state; no owner holds the gate.
	Idle State = iota
	// Enabling is a transient state during Idle->Active transition.
	Enabling
	// Active means the session holds the ownership gate and is accepting reads.
	Active
	// Refreshing means an epoch refresh holds the ownership gate. Operations are
	// unavailable until it reaches Active or releases to Idle.
	Refreshing
	// Disabled is a terminal post-failure state prior to returning to Idle.
	Disabled
)

// String returns a stable human-readable label.
func (s State) String() string {
	switch s {
	case Idle:
		return "Idle"
	case Enabling:
		return "Enabling"
	case Active:
		return "Active"
	case Refreshing:
		return "Refreshing"
	case Disabled:
		return "Disabled"
	default:
		return "Unknown"
	}
}
