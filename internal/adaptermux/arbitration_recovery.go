package adaptermux

import (
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Arbitration state machine (all mux state is guarded by stateMu):
//
//	idle -> selected/wire: selectBidsAtSYN captures the bidder and local losers;
//	wire -> attribution: STARTED/FAILED consumes that selection exactly once;
//	attribution -> owned: only a matching, live, deliverable STARTED commits;
//	wire/attribution -> fenced: cancel, expiry, mismatch, loss, transport error,
//	  or an undeliverable win. Cancellation/expiry retain the loser set until
//	  attribution or observed SYN, and absorb any still-outstanding result;
//	fenced -> idle: the next logical structural SYN reconciles both trackers
//	  before filtering and clears the regrant fence. The independent absorb
//	  and blocking-call barriers must also finish before another selection;
//	any -> reset: reset/disconnect/close retire selection, losers and barriers.
//
// The classifier may still be tracking the abandoned result's header. Its
// single producer must accept the recovery SYN before applying the ordinary
// mid-telegram AA filter; otherwise the fence can never reach the SYN handler.
// Raw-only AA and escaped payload AA cannot complete this transition. A retired
// selection remains recognizable until the next selection or lifecycle boundary,
// so a response arriving even after absorb timeout cannot strand the classifier.
type arbitrationRecovery uint8

const (
	arbitrationRecoveryNone arbitrationRecovery = iota
	arbitrationRecoveryAtSYN
	arbitrationRecoveryReset
)

func (m *Mux) fenceArbitrationUntilSYNLocked() {
	m.retiredSelection = true
	m.deferRegrantUntilNextSyn = true
	if m.arbitrationRecovery != arbitrationRecoveryReset {
		m.arbitrationRecovery = arbitrationRecoveryAtSYN
	}
}

// reconcileArbitrationClassifier runs only on readLoop, before Observe and
// raw delivery. Timers/cancellation request recovery under stateMu; they never
// mutate the single-producer classifier themselves. Lifecycle resets apply
// before any new item, while an aborted arbitration waits for its logical SYN.
func (m *Mux) reconcileArbitrationClassifier(event transport.StreamEvent) {
	isSYN := event.Kind == transport.StreamEventWireSyn ||
		(event.Kind == transport.StreamEventByte && event.Byte == protocol.SymbolSyn && !event.WasEscaped)
	m.stateMu.Lock()
	reset := m.arbitrationRecovery == arbitrationRecoveryReset ||
		(m.arbitrationRecovery == arbitrationRecoveryAtSYN && isSYN)
	if reset {
		m.phase.reset(wirePhaseIdle)
		m.arbitrationRecovery = arbitrationRecoveryNone
	}
	m.stateMu.Unlock()
	if reset {
		m.v8.ResetArbitrationTracking()
	}
}
