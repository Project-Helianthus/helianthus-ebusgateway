package adaptermux

import (
	"errors"
	"fmt"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Raw-symbol continuity at the mux upstream boundary (issue #1003, contract
// docs/architecture/mux-raw-symbol-continuity.md).
//
// The mux consumes two views of the same upstream stream:
//
//   - the raw view: every RECEIVED wire symbol the upstream observed, in
//     order, including an escape lead before its suffix exists. External
//     ENH sessions are fed from this view only, one RECEIVED per wire
//     symbol, so a logical 0xA9 or 0xAA reaches them as the two observed
//     symbols and a raw-symbol client can wait for each individual echo;
//   - the logical view: decoded bytes with escape provenance (WasEscaped).
//     Wire-phase tracking, passive reconstruction, the gateway active path
//     and the arbitration lifecycle consume this view, unchanged.
//
// The raw view is never regenerated from logical bytes. An upstream that
// cannot supply it keeps the previous logical session delivery.

// upstreamRawSymbolReader is the optional upstream capability that supplies
// the raw view (helianthus-ebusgo transport.RawSymbolStreamReader). It is
// declared here with existing types so the mux detects the capability at
// run time. Each call returns one ordered item: either an observed RECEIVED
// wire symbol (hasSymbol) together with the logical item it completed (ev,
// zero Kind when it completed none), or a control item (hasSymbol false).
type upstreamRawSymbolReader interface {
	ReadEventWithRawSymbol() (ev transport.StreamEvent, symbol byte, hasSymbol bool, err error)
}

// upstreamItem is one ordered item read at the mux upstream boundary.
type upstreamItem struct {
	// event is the logical or control item. Its Kind is zero for a raw
	// symbol that completed no logical item (an escape lead, a 0xAA
	// absorbed inside a pending pair, or a symbol discarded by a decode
	// fault).
	event transport.StreamEvent
	// rawSymbol is the observed wire symbol when hasRaw is set.
	rawSymbol byte
	hasRaw    bool
}

// readUpstreamItem reads the next ordered item from the adapter transport,
// preferring the raw view when the upstream supplies it.
func (m *Mux) readUpstreamItem() (upstreamItem, error) {
	m.connMu.Lock()
	tr := m.upstream
	m.connMu.Unlock()

	if tr == nil {
		return upstreamItem{}, errors.New("adaptermux: not connected")
	}
	if reader, ok := tr.(upstreamRawSymbolReader); ok {
		ev, symbol, hasSymbol, err := reader.ReadEventWithRawSymbol()
		if err != nil {
			return upstreamItem{}, err
		}
		return upstreamItem{event: ev, rawSymbol: symbol, hasRaw: hasSymbol}, nil
	}
	ev, err := m.readUpstream()
	if err != nil {
		return upstreamItem{}, err
	}
	return upstreamItem{event: ev}, nil
}

// upstreamSuppliesRawSymbols reports whether tr provides the raw view, which
// is what makes an escape lead's own echo observable to the mux.
func upstreamSuppliesRawSymbols(tr transport.RawTransport) bool {
	_, ok := tr.(upstreamRawSymbolReader)
	return ok
}

// onRawSymbol delivers one observed RECEIVED wire symbol that passed the
// session-view filter, before its logical item (if any) is processed. It runs
// on the readLoop goroutine, so session delivery keeps exact upstream order
// relative to every other item the readLoop delivers, including a granted
// session's STARTED. The gateway escape-pair verifier is fed earlier, from
// every observed symbol, by readLoop itself.
func (m *Mux) onRawSymbol(symbol byte, logical transport.StreamEvent) {
	if logical.Kind == 0 && symbol != protocol.SymbolSyn {
		// A raw-only non-SYN symbol (typically an escape lead) is wire
		// activity even though no logical byte completed yet. A raw-only
		// 0xAA can be an absorbed injection inside a pending pair; it never
		// refreshes the activity clock, matching the logical SYN rule.
		m.stateMu.Lock()
		m.lastWireActivity = time.Now()
		m.stateMu.Unlock()
	}
	m.deliverRawToSessions(symbol)
}

// deliverRawToSessions delivers one observed wire symbol to every external
// session, owner included (F-18 echo contract), as one ENH RECEIVED.
func (m *Mux) deliverRawToSessions(symbol byte) {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()
	for _, sess := range m.sessions {
		sess.deliverReceived(symbol)
	}
}

// Write normalization (independent of receive normalization).
//
// ENH and ENS SEND requests each transmit exactly one raw wire symbol; the
// adapter performs no escape encoding on the send path (a transparent
// adapter echoes each SEND data byte as one RECEIVED, and raw-symbol ENH
// clients escape before SEND and wait for each symbol's echo). External
// sessions therefore pass their raw SEND symbols through unchanged: a client
// that sends 0xA9 and then 0x00 produces exactly those two wire symbols.
//
// The gateway active path writes logical bytes (protocol.Bus applies no
// escaping when the path reports BytesAreUnescaped). The mux encodes those
// writes itself: a payload 0xA9 becomes the raw symbols 0xA9 0x00, a payload
// 0xAA becomes 0xA9 0x01, and only a write explicitly marked structural via
// SignalNextWriteIsStructuralSyn goes out as a raw 0xAA SYN. The decision
// never consults the upstream's receive-side flag on its own: the active
// path advertises logical writes only when the upstream SEND contract is
// raw-symbol (activeWritesAreLogical) AND its receive side is logical,
// because protocol.Bus learns both directions from one EscapeAware flag.

// upstreamSendIsRawSymbol reports the upstream SEND contract: true for the
// ENH/ENS adapter protocols, whose SEND request transmits one raw symbol.
func (m *Mux) upstreamSendIsRawSymbol() bool {
	switch m.cfg.Protocol {
	case "enh", "ens", "":
		return true
	default:
		return false
	}
}

// activeReadsAreLogical reports the receive-side normalization of the active
// path: the upstream delivers decoded logical bytes with escape provenance.
func (m *Mux) activeReadsAreLogical() bool {
	return m.bytesAreUnescaped()
}

// activeWritesAreLogical reports the write-side normalization of the active
// path: the mux accepts logical bytes from protocol.Bus and encodes reserved
// payload values into raw SEND symbols itself.
func (m *Mux) activeWritesAreLogical() bool {
	return m.upstreamSendIsRawSymbol()
}

// activePathIsLogical is what the active path advertises to protocol.Bus via
// BytesAreUnescaped, and therefore exactly when the mux must encode gateway
// writes: protocol.Bus escapes nothing in that mode and unescapes nothing.
func (m *Mux) activePathIsLogical() bool {
	return m.activeReadsAreLogical() && m.activeWritesAreLogical()
}

// gatewaySendSymbols encodes one logical gateway write into the raw wire
// symbols its SEND requests transmit. structural marks the end-of-message
// SYN; any other 0xAA, and every 0xA9, is payload and is escaped.
func gatewaySendSymbols(data byte, structural bool) []byte {
	switch {
	case data == protocol.SymbolSyn && structural:
		return []byte{protocol.SymbolSyn}
	case data == protocol.SymbolEscape:
		return []byte{protocol.SymbolEscape, 0x00}
	case data == protocol.SymbolSyn:
		return []byte{protocol.SymbolEscape, 0x01}
	default:
		return []byte{data}
	}
}

// gatewayLeadEchoTimeout bounds how long the gateway write path waits for the
// raw echo of an escape lead before it fails the write. It matches the
// active-path read bound protocol.Bus applies to every echo.
const gatewayLeadEchoTimeout = activeChanTimeout

// gatewayLeadWait is the single outstanding raw-echo expectation for the
// escape pair of a gateway write: the lead's echo, then the suffix's echo as
// the symbol immediately following it. It is published through
// Mux.gatewayLeadWait (an atomic pointer); whoever swaps it out resolves it,
// exactly once.
type gatewayLeadWait struct {
	lead   byte
	suffix byte
	result chan gatewayLeadOutcome // capacity 1, resolved at most once
	// leadSeen is set when the lead's echo was observed; from then on the
	// very next raw symbol decides the pair. Read and written only by the
	// readLoop goroutine (observeGatewayLeadEcho).
	leadSeen bool
}

type gatewayLeadOutcome struct {
	observed   byte
	suffixStep bool  // observed answered the suffix (else the lead)
	err        error // non-nil when a lifecycle boundary retired the wait
}

// armGatewayLeadWait registers the raw-echo expectation for a gateway escape
// pair. The caller arms before writing the lead, so the echo cannot be
// observed before the expectation exists. Gateway writes are serialized by
// sendLoop, so at most one wait is outstanding.
func (m *Mux) armGatewayLeadWait(lead, suffix byte) *gatewayLeadWait {
	w := &gatewayLeadWait{lead: lead, suffix: suffix, result: make(chan gatewayLeadOutcome, 1)}
	m.gatewayLeadWait.Store(w)
	return w
}

// retireGatewayLeadWait fences the current ownership period for escape-pair
// suffix commits and resolves an outstanding wait with err, at a lifecycle
// boundary (adapter reset, reconnect, close). The fence is taken under the
// lifecycle epoch the commits check under the arbitrator lock, so a commit
// that checks after the boundary writes nothing; the fence itself never waits
// for a commit write in flight, so Close can always reach the upstream close.
// A retired pair can never be acknowledged by a symbol observed after it.
func (m *Mux) retireGatewayLeadWait(err error) {
	m.arb.fenceLifecycle()
	if w := m.gatewayLeadWait.Swap(nil); w != nil {
		w.result <- gatewayLeadOutcome{err: err}
	}
}

// observeGatewayLeadEcho advances an outstanding gateway escape-pair wait
// with the next raw symbol.
//
// Lead step: an unsolicited structural SYN (raw 0xAA that completed a logical
// wire SYN) can never satisfy a payload escape lead; it is mid-write bus
// noise exactly as the logical P10.2 gate treats it, so the wait continues.
// The lead's echo moves the wait to the suffix step; any other symbol
// resolves it as a lead mismatch.
//
// Suffix step: the suffix's echo must be the symbol immediately following
// the lead's echo. Whatever that symbol is resolves the wait, so a malformed
// suffix fails the pair and a later well-formed pair (for example A9 FF A9 00)
// can never acknowledge it.
func (m *Mux) observeGatewayLeadEcho(symbol byte, logical transport.StreamEvent) {
	w := m.gatewayLeadWait.Load()
	if w == nil {
		return
	}
	if !w.leadSeen {
		if logical.Kind == transport.StreamEventWireSyn ||
			(logical.Kind == transport.StreamEventByte && logical.Byte == protocol.SymbolSyn && !logical.WasEscaped) {
			return
		}
		if symbol == w.lead {
			w.leadSeen = true
			return
		}
		if m.gatewayLeadWait.CompareAndSwap(w, nil) {
			w.result <- gatewayLeadOutcome{observed: symbol}
		}
		return
	}
	if m.gatewayLeadWait.CompareAndSwap(w, nil) {
		w.result <- gatewayLeadOutcome{observed: symbol, suffixStep: true}
	}
}

// awaitGatewayLeadEcho waits until the raw echoes of the escape pair the
// gateway just wrote decided it. It returns nil only when the lead's echo was
// followed immediately by the suffix's echo.
func (m *Mux) awaitGatewayLeadEcho(w *gatewayLeadWait) error {
	timer := time.NewTimer(gatewayLeadEchoTimeout)
	defer timer.Stop()
	select {
	case out := <-w.result:
		if out.err != nil {
			return out.err
		}
		if !out.suffixStep {
			return fmt.Errorf("adaptermux: escape lead echo mismatch (wrote 0x%02X, observed 0x%02X): %w",
				w.lead, out.observed, ebuserrors.ErrBusCollision)
		}
		if out.observed != w.suffix {
			return fmt.Errorf("adaptermux: escape suffix echo mismatch (wrote 0x%02X 0x%02X, observed 0x%02X 0x%02X): %w",
				w.lead, w.suffix, w.lead, out.observed, ebuserrors.ErrBusCollision)
		}
		return nil
	case <-timer.C:
		m.dropGatewayLeadWait(w)
		return fmt.Errorf("adaptermux: escape pair 0x%02X 0x%02X echo not observed: %w", w.lead, w.suffix, ebuserrors.ErrTimeout)
	case <-m.ctx.Done():
		m.dropGatewayLeadWait(w)
		return fmt.Errorf("adaptermux: %w", m.ctx.Err())
	}
}

// dropGatewayLeadWait clears w if it is still the outstanding wait.
func (m *Mux) dropGatewayLeadWait(w *gatewayLeadWait) {
	m.gatewayLeadWait.CompareAndSwap(w, nil)
}

// writeSymbols transmits the raw symbols of one requested write and
// verifies every symbol. A single symbol (every external SEND, and every
// gateway byte that needs no escaping) is verified by the existing echo
// paths.
//
// An escape pair is sent as two SEND requests back to back, with no host
// round trip between them. The upstream decoder abandons a pending lead
// after its escape-pending cap (32 ms of host time, frame-atomic-visibility
// v8 §1.3); waiting for the lead's echo before sending the suffix would let
// an adapter round trip near that cap split a correct pair into a dropped
// lead and a plain suffix.
//
// The suffix belongs to the ownership period the lead was sent in. It is
// written inside arbitrator.commitWhileOwnershipCurrent, which holds the
// arbitrator lock across the check and the write; every ownership
// transition and every lifecycle fence (reset, reconnect, close) takes that
// lock. A boundary is therefore either ordered before the check, and the
// suffix is never transmitted (the write fails once), or it waits until the
// suffix write returned. Nothing can retire the pair between the check and
// the write.
//
// Verification covers both symbols: with the raw view the first eligible
// raw symbol after the write must be the lead and the symbol immediately
// after it must be the suffix (anything else fails the write as a
// collision); the logical echo protocol.Bus then checks carries
// WasEscaped=true. Without the raw view the logical echo verifies the pair.
func (m *Mux) writeSymbols(tr transport.RawTransport, symbols []byte) error {
	if len(symbols) == 1 {
		_, err := tr.Write(symbols)
		return err
	}
	lead, suffix := symbols[0], symbols[1]
	tok, owned := m.arb.currentOwnershipToken(gatewaySessionID)
	if !owned {
		return errGatewayLeadEcho{err: errNotBusOwner}
	}
	var wait *gatewayLeadWait
	if upstreamSuppliesRawSymbols(tr) {
		wait = m.armGatewayLeadWait(lead, suffix)
	}
	if hook := m.gatewayLeadCommitHook; hook != nil {
		hook()
	}
	// The lead is committed under the same ownership check as the suffix, so
	// a boundary that landed between the token and the arm cannot let an
	// unowned pair onto the bus.
	leadErr := m.arb.commitWhileOwnershipCurrent(tok, func() error {
		_, err := tr.Write([]byte{lead})
		return err
	})
	if leadErr != nil {
		if wait != nil {
			m.dropGatewayLeadWait(wait)
		}
		switch {
		case errors.Is(leadErr, errOwnershipLifecycleFenced):
			return errGatewayLeadEcho{err: fmt.Errorf("adaptermux: escape lead withheld after a lifecycle boundary: %w", ebuserrors.ErrAdapterReset)}
		case errors.Is(leadErr, errOwnershipChanged):
			return errGatewayLeadEcho{err: fmt.Errorf("adaptermux: escape lead withheld after ownership ended: %w", errNotBusOwner)}
		default:
			return leadErr
		}
	}
	if hook := m.gatewaySuffixCommitHook; hook != nil {
		hook()
	}
	var writeErr error
	commitErr := m.arb.commitWhileOwnershipCurrent(tok, func() error {
		_, writeErr = tr.Write([]byte{suffix})
		return writeErr
	})
	if commitErr != nil {
		if wait != nil {
			m.dropGatewayLeadWait(wait)
		}
		switch {
		case errors.Is(commitErr, errOwnershipLifecycleFenced):
			return errGatewayLeadEcho{err: fmt.Errorf("adaptermux: escape suffix withheld after a lifecycle boundary: %w", ebuserrors.ErrAdapterReset)}
		case errors.Is(commitErr, errOwnershipChanged):
			return errGatewayLeadEcho{err: fmt.Errorf("adaptermux: escape suffix withheld after ownership ended: %w", errNotBusOwner)}
		default:
			return commitErr
		}
	}
	if wait != nil {
		if err := m.awaitGatewayLeadEcho(wait); err != nil {
			return errGatewayLeadEcho{err: err}
		}
	}
	return nil
}

// errGatewayLeadEcho marks a failed escape-lead verification so doSend can
// return it to protocol.Bus unchanged (its class drives retry handling)
// instead of reclassifying it as an adapter write failure.
type errGatewayLeadEcho struct{ err error }

func (e errGatewayLeadEcho) Error() string { return e.err.Error() }
func (e errGatewayLeadEcho) Unwrap() error { return e.err }

// enterWriteCollisionRecovery moves a gateway transaction whose escape pair
// failed its echo verification with a bus collision into the bounded
// collision recovery lifecycle. The transaction stays active so the two SYN
// symbols protocol.Bus waits for are delivered, and the recovery token
// becomes visible; Retry or Abandon from the bus then releases the grant.
// Without this the write rollback would clear the transaction and starve the
// SYN wait until the request context ends.
func (m *Mux) enterWriteCollisionRecovery() {
	m.stateMu.Lock()
	entered := false
	if m.gatewayTxnActive && !m.activeTxn.firstByteSuspectArbLoss.Load() {
		m.activeTxn.firstByteSuspectArbLoss.Store(true)
		m.collisionResyncSYNs = 0
		m.gatewayEcho.reset()
		entered = true
	}
	m.stateMu.Unlock()
	if entered {
		m.cancelGatewayWatchdog()
	}
}
