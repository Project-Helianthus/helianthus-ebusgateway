package adaptermux

import (
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol/telegram_fsm"
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
//
// A response naming a real foreign winner is different: that winner's telegram
// follows, and in enforce mode an AA inside it stays subject to the filter. The
// fence then ends at the telegram's own terminator, or at a second consecutive
// AA for a telegram the winner abandoned, since idle repeats SYN and an injected
// AA is followed by telegram bytes.
//
// The same holds when the telegram is the granted owner's own: if the owner
// cancels or disconnects after writing its request, the wire-phase tracker
// still follows the response, so the first SYN of an AtSYN fence is judged like
// a foreign winner's. Lifecycle resets (reset, reconnect, close) end the stream
// itself and always reset.
type arbitrationRecovery uint8

const (
	arbitrationRecoveryNone arbitrationRecovery = iota
	arbitrationRecoveryAtSYN
	arbitrationRecoveryReset
	arbitrationRecoveryForeign        // winner's telegram expected; no AA seen
	arbitrationRecoveryForeignAfterAA // the previous item was an unescaped AA
)

func (m *Mux) fenceArbitrationUntilSYNLocked() {
	m.retiredSelection = true
	m.deferRegrantUntilNextSyn = true
	if m.arbitrationRecovery == arbitrationRecoveryNone {
		m.arbitrationRecovery = arbitrationRecoveryAtSYN
	}
}

// expectForeignTelegram upgrades a fence set by the response just consumed when
// that response named a real foreign winner. A retired win of the selected
// address and a phantom FAILED have no telegram to protect, and only enforce
// mode filters, so those keep recovering at the next SYN. readLoop only.
func (m *Mux) expectForeignTelegram(winner byte) {
	if m.v8 == nil || m.v8.Mode() != v8classifier.ModeEnforce {
		return
	}
	if m.cfg.IsKnownInitiatorByte != nil && !m.cfg.IsKnownInitiatorByte(winner) {
		return
	}
	m.stateMu.Lock()
	if m.arbitrationRecovery == arbitrationRecoveryAtSYN && winner != m.localLoserSource {
		m.arbitrationRecovery = arbitrationRecoveryForeign
	}
	m.stateMu.Unlock()
}

// reconcileArbitrationClassifier runs only on readLoop, before Observe and
// raw delivery. Timers/cancellation request recovery under stateMu; they never
// mutate the single-producer classifier themselves. Lifecycle resets apply
// before any new item, while an aborted arbitration waits for its logical SYN.
func (m *Mux) reconcileArbitrationClassifier(event transport.StreamEvent) {
	isSYN := event.Kind == transport.StreamEventWireSyn ||
		(event.Kind == transport.StreamEventByte && event.Byte == protocol.SymbolSyn && !event.WasEscaped)
	m.stateMu.Lock()
	// A fence can be raised by any path that ends an ownership, including
	// one whose telegram is still arriving (an owner that wrote its request
	// and then cancelled or disconnected). The first SYN is then ambiguous
	// like a foreign winner's: it is either the telegram's terminator or an
	// injected AA inside it, so it follows the same recovery.
	if m.arbitrationRecovery == arbitrationRecoveryAtSYN && isSYN && m.phase.telegramLive() {
		m.arbitrationRecovery = arbitrationRecoveryForeign
	}
	reset := m.arbitrationRecovery == arbitrationRecoveryReset ||
		(m.arbitrationRecovery == arbitrationRecoveryAtSYN && isSYN)
	switch m.arbitrationRecovery {
	case arbitrationRecoveryForeign, arbitrationRecoveryForeignAfterAA:
		// An aborted classifier filters nothing and only a reset revives it.
		reset = isSYN && (m.arbitrationRecovery == arbitrationRecoveryForeignAfterAA ||
			m.v8.FSMInternalState() == telegram_fsm.StateAborted)
		if isSYN {
			m.arbitrationRecovery = arbitrationRecoveryForeignAfterAA
		} else {
			m.arbitrationRecovery = arbitrationRecoveryForeign
		}
	}
	if reset {
		m.phase.reset(wirePhaseIdle)
		m.arbitrationRecovery = arbitrationRecoveryNone
	}
	m.stateMu.Unlock()
	if reset {
		m.v8.ResetArbitrationTracking()
	}
}
