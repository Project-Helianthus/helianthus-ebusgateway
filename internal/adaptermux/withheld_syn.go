package adaptermux

import "time"

// enforceOwnershipLimitsOnWithheldSYN applies the ownership limits to a wire
// SYN the classifier withheld in enforce mode. readLoop only, after the drop
// and before it moves on to the next item; the SYN itself stays withheld.
//
// The idle-gap release (ExternalSessionSYNGrace) and the ownership cap are
// otherwise evaluated in the SYN and receive handlers, which a withheld SYN
// never reaches, and readLoop's quiet-bus cap check runs only on a read
// timeout, which never happens while SYN keeps arriving. Without this, an
// external owner that stopped in mid-telegram would hold the bus until some
// other traffic arrived.
//
// The owner is released when it has held the bus longer than its ownership
// limit (any owner), or when it is an external session whose last wire
// activity is at least ExternalSessionSYNGrace old. An owner with wire activity
// inside the grace keeps the bus. The gateway's idle-release grace is not
// applied here. Every release fences selection until SYN, so the existing
// recovery resets both wire trackers at the next SYN (no owner write) or at the
// second consecutive SYN (a live telegram). Lock order: stateMu, then arb.mu.
func (m *Mux) enforceOwnershipLimitsOnWithheldSYN() {
	m.stateMu.Lock()
	ownerID, _, hasOwner := m.arb.owner()
	if !hasOwner || m.busOwned.IsZero() {
		m.stateMu.Unlock()
		return
	}
	owned := time.Since(m.busOwned)
	limit := m.ownershipDurationLimit(ownerID)
	idle := time.Since(m.lastWireActivity)
	grace := m.cfg.ExternalSessionSYNGrace
	capReached := owned > limit
	idleReached := ownerID != gatewaySessionID && idle >= grace
	if !capReached && !idleReached {
		m.stateMu.Unlock()
		return
	}
	m.arb.releaseOwnership(ownerID)
	cancelWatchdog := false
	if ownerID == gatewaySessionID {
		m.gatewayEcho.reset()
		if m.gatewayTxnActive {
			m.gatewayTxnActive = false
			m.recordGatewayInactive(ReasonMaxOwnership)
			m.discardStaleActiveBytesLocked()
			cancelWatchdog = true
		}
	}
	m.fenceArbitrationUntilSYNLocked()
	m.stateMu.Unlock()

	if cancelWatchdog {
		m.cancelGatewayWatchdog()
	}
	remote := m.sessionRemoteAddrOrUnknown(ownerID)
	if capReached {
		m.logger.Printf("adaptermux: ownership released for session %d remote=%s (classifier withheld SYN: maximum ownership %v reached)",
			ownerID, remote, limit)
		return
	}
	m.logger.Printf("adaptermux: ownership released for session %d remote=%s (classifier withheld SYN: idle gap %v >= grace %v)",
		ownerID, remote, idle.Round(time.Millisecond), grace)
}
