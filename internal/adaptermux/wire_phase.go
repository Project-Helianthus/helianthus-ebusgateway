// Package adaptermux embeds adapter multiplexing logic directly in the
// gateway, allowing a single ENH/ENS connection to the adapter hardware
// with demuxed active (owner) and passive (observer) paths.
package adaptermux

import "github.com/Project-Helianthus/helianthus-ebusgo/protocol"

// wirePhase tracks the byte-level eBUS transaction state on the bus.
// The tracker identifies frame boundaries, arbitration windows, and
// echo suppression boundaries by counting bytes in request and response
// phases.
//
// Adapted from the proxy's minimal direct-mode phase tracker (proxy M3).
type wirePhase uint8

const (
	wirePhaseIdle             wirePhase = iota // bus idle, arbitration open
	wirePhaseCollectRequest                    // owner sending request bytes
	wirePhaseWaitCmdAck                        // waiting for target ACK/NACK
	wirePhaseWaitResponseLen                   // waiting for response LEN byte
	wirePhaseWaitResponseBody                  // counting response body + CRC
	wirePhaseWaitResponseAck                   // waiting for initiator final ACK
	// wirePhaseWaitTerminalSyn (F-21, batch-20, 2026-05-14) is entered
	// at every structural terminal of a master frame — broadcast ACK,
	// i2i ACK, response success ACK, response double-NACK, CMD double-
	// NACK. Pre-F-21 each of those byte positions immediately fired
	// wirePhaseEventTransactionDone (or wirePhaseEventCmdNACK) and the
	// mux released ownership at the SAME observation. That premature
	// release rejected external sessions' (ebusd's) trailing-SYN
	// ENH_REQ_SEND with "session N SEND 0xAA rejected — session does
	// not own bus" because the wire round-trip between the gateway
	// observing M_ACK and ebusd sending its terminal SYN crosses the
	// ownership-release boundary. F-21 defers the terminal event one
	// byte later: at the SYN observation the wire-phase tracker fires
	// TransactionDone via advanceWaitTerminalSyn, and onSYNLocked
	// releases ownership atomically with the SYN — by then ebusd's
	// session.handleSend has already passed the owner check and
	// forwarded the SYN to the adapter.
	//
	// Gateway session 0 is structurally unaffected: gateway-owned
	// non-SYN bytes do NOT go through advanceWithProvenance (mux.go
	// gates the call on !gateway-owned), so the tracker never enters
	// any of these terminal sites under gateway ownership; the
	// gateway's trailing SYN flows through the SYNIdle path with its
	// own IdleReleaseGrace.
	wirePhaseWaitTerminalSyn
)

// isSYNTimeoutBoundary reports whether receiving a SYN in this phase
// indicates a timeout condition (transaction abandoned by bus timeout).
func (p wirePhase) isSYNTimeoutBoundary() bool {
	switch p {
	case wirePhaseWaitCmdAck, wirePhaseWaitResponseLen, wirePhaseWaitResponseBody, wirePhaseWaitResponseAck:
		return true
	default:
		return false
	}
}

// wirePhaseTracker tracks the byte-level eBUS transaction structure.
// It follows the eBUS initiator-target transaction protocol:
//
//	SRC DST PB SB LEN DATA[0..LEN-1] CRC → ACK → LEN DATA[0..LEN-1] CRC → ACK
//
// The tracker does NOT validate CRC or interpret field values.
// It only counts bytes to know when phase transitions occur.
type wirePhaseTracker struct {
	phase wirePhase

	// Request phase counters.
	requestBytesSeen  int  // bytes received since CollectRequest start
	requestDataLength int  // LEN field value (-1 until captured)
	requestSrc        byte // initiator address (byte 1)
	requestDst        byte // target address (byte 2)

	// AM2: CMD NACK retry — eBUS spec allows one retry without re-arbitration.
	cmdRetried bool

	// AM3: Response NACK retry — limit to one retry.
	responseRetried bool

	// Response phase counter.
	responseBytesRemain int // countdown: LEN + 1 (data + CRC)
}

// reset clears all tracking state and sets the phase.
func (t *wirePhaseTracker) reset(phase wirePhase) {
	t.phase = phase
	t.requestBytesSeen = 0
	t.requestDataLength = -1
	t.requestSrc = 0
	t.requestDst = 0
	t.responseBytesRemain = 0
	t.cmdRetried = false
	t.responseRetried = false
}

// wirePhaseEvent describes what happened after advancing the tracker.
type wirePhaseEvent uint8

const (
	wirePhaseEventNone            wirePhaseEvent = iota // no transition
	wirePhaseEventRequestComplete                       // request phase done, waiting ACK
	wirePhaseEventCmdACK                                // target ACK'd, response expected
	wirePhaseEventCmdNACK                               // target NACK'd, transaction failed
	wirePhaseEventResponseDone                          // response fully received, waiting final ACK
	wirePhaseEventTransactionDone                       // final ACK received, transaction complete
	wirePhaseEventCmdNACKRetry                          // AM2: target NACK'd, retrying without re-arbitration
	wirePhaseEventSYNIdle                               // SYN received, bus idle
	wirePhaseEventSYNTimeout                            // SYN during wait phase (timeout)
)

// advance processes a received symbol and returns what event occurred.
// This must be called for every byte received from the adapter.
// Value-only SYN classification: any `symbol == protocol.SymbolSyn`
// is treated as a wire SYN. Callers with provenance information
// (post-F-23) should use advanceWithProvenance instead so an
// escape-decoded payload 0xAA is not misclassified as SYN.
func (t *wirePhaseTracker) advance(symbol byte) wirePhaseEvent {
	return t.advanceWithProvenance(symbol, symbol == protocol.SymbolSyn)
}

// advanceWithProvenance is the F-23-aware (batch-19, Codex bot on
// PR-2) variant of advance. Callers pass `isWireSyn` explicitly
// instead of letting the tracker key on the byte value; this lets
// escape-decoded payload 0xAA (wasEscaped=true at the upstream
// transport, isWireSyn=false here) flow through as data rather
// than triggering SYN-as-frame-terminator semantics. The mux's
// onReceived path uses this; legacy plain-transport callers and
// tests use the value-only advance() wrapper above.
func (t *wirePhaseTracker) advanceWithProvenance(symbol byte, isWireSyn bool) wirePhaseEvent {
	// F-21 (batch-20): wirePhaseWaitTerminalSyn is the deferred-terminal
	// state entered by every structural terminal site (broadcast ACK,
	// i2i ACK, response success ACK, response double-NACK, CMD double-
	// NACK). Dispatch ANY byte arriving in that state directly to
	// advanceWaitTerminalSyn BEFORE the generic isWireSyn handler runs.
	// Without this intercept, the trailing SYN would be classified by
	// the SYN-fallthrough branch below: WaitTerminalSyn is intentionally
	// NOT in isSYNTimeoutBoundary's wait-set, so the fallthrough returns
	// wirePhaseEventSYNIdle — which routes through the SYNIdle/idle-
	// grace release path instead of firing TransactionDone. F-21 needs
	// TransactionDone specifically so onSYNLocked's terminal release
	// branch (no grace, immediate) fires; SYNIdle would impose
	// IdleReleaseGrace and reorder relative to external session
	// follow-up writes.
	if t.phase == wirePhaseWaitTerminalSyn {
		return t.advanceWaitTerminalSyn(symbol)
	}
	// SYN resets the tracker. If we were in a wait phase, it's a timeout.
	if isWireSyn {
		if t.phase.isSYNTimeoutBoundary() {
			t.reset(wirePhaseIdle)
			return wirePhaseEventSYNTimeout
		}
		if t.phase == wirePhaseCollectRequest {
			// SYN during request collection. Distinguish between:
			// - Fresh grant (requestBytesSeen <= 1): only the pre-loaded
			//   SRC from ArbitrationSendsSource, no real bytes transmitted
			//   yet. This SYN is normal inter-transaction bus idle traffic
			//   arriving before the gateway starts sending. Treat as idle.
			// - Active request (requestBytesSeen > 1): real bytes are on
			//   the wire but the request wasn't completed. Treat as timeout.
			if t.requestBytesSeen <= 1 {
				t.reset(wirePhaseIdle)
				return wirePhaseEventSYNIdle
			}
			t.reset(wirePhaseIdle)
			return wirePhaseEventSYNTimeout
		}
		t.reset(wirePhaseIdle)
		return wirePhaseEventSYNIdle
	}

	switch t.phase {
	case wirePhaseIdle:
		// Non-SYN byte in idle — should not happen on a well-behaved bus.
		// Ignore and stay idle.
		return wirePhaseEventNone

	case wirePhaseCollectRequest:
		return t.advanceCollectRequest(symbol)

	case wirePhaseWaitCmdAck:
		return t.advanceWaitCmdAck(symbol)

	case wirePhaseWaitResponseLen:
		return t.advanceWaitResponseLen(symbol)

	case wirePhaseWaitResponseBody:
		return t.advanceWaitResponseBody(symbol)

	case wirePhaseWaitResponseAck:
		return t.advanceWaitResponseAck(symbol)
	}

	return wirePhaseEventNone
}

func (t *wirePhaseTracker) advanceCollectRequest(symbol byte) wirePhaseEvent {
	t.requestBytesSeen++

	switch t.requestBytesSeen {
	case 1:
		t.requestSrc = symbol
	case 2:
		t.requestDst = symbol
	case 5:
		t.requestDataLength = int(symbol)
	}

	// Request is complete when we've seen: SRC + DST + PB + SB + LEN + DATA[LEN] + CRC
	// Total = 5 (header) + requestDataLength (data) + 1 (CRC)
	if t.requestDataLength >= 0 && t.requestBytesSeen >= 6+t.requestDataLength {
		t.phase = wirePhaseWaitCmdAck
		return wirePhaseEventRequestComplete
	}

	return wirePhaseEventNone
}

func (t *wirePhaseTracker) advanceWaitCmdAck(symbol byte) wirePhaseEvent {
	if symbol == protocol.SymbolAck {
		// Target ACK'd. Check if this is a broadcast (no response expected).
		if t.requestDst == protocol.AddressBroadcast {
			// F-21 (batch-20): defer TransactionDone to trailing SYN.
			// Broadcast frames still have a SYN terminator; releasing
			// ownership at ACK position rejected ebusd's trailing SYN
			// ENH_REQ_SEND on external-session paths.
			t.phase = wirePhaseWaitTerminalSyn
			return wirePhaseEventNone
		}
		// AM1 fix: initiator-to-initiator (i2i) frames have no response
		// phase. Initiator-capable addresses cannot be targets in a
		// request-response transaction (eBUS spec). Transition to done
		// instead of waiting for a response that will never arrive.
		if protocol.IsInitiatorCapableAddress(t.requestDst) {
			// F-21 (batch-20): defer TransactionDone to trailing SYN —
			// same rationale as the broadcast branch above.
			t.phase = wirePhaseWaitTerminalSyn
			return wirePhaseEventNone
		}
		t.phase = wirePhaseWaitResponseLen
		return wirePhaseEventCmdACK
	}

	// AM21: require exact NACK symbol. Any other byte is a protocol
	// error -- reset to idle without retry.
	if symbol == protocol.SymbolNack {
		// AM2: eBUS spec allows one retry without re-arbitration after
		// a CMD NACK. Re-enter CollectRequest to track the retransmitted
		// request bytes. SRC/DST will be recaptured from the retransmit.
		if !t.cmdRetried {
			t.cmdRetried = true
			t.requestBytesSeen = 0
			t.requestDataLength = -1
			t.phase = wirePhaseCollectRequest
			return wirePhaseEventCmdNACKRetry
		}
		// F-21 (batch-20): defer the terminal event to trailing SYN.
		// Returning wirePhaseEventNone instead of wirePhaseEventCmdNACK
		// here is a deliberate deviation from the F-21 prompt: the
		// non-SYN release block at mux.go:1825-1846 fires on BOTH
		// TransactionDone AND CmdNACK, so returning CmdNACK at this
		// byte position would still release ownership prematurely and
		// reject ebusd's trailing SYN. The diagnostic distinction
		// (ReasonCmdNACK vs ReasonTransactionDone) is irrelevant on
		// external paths (recordGatewayInactive only fires for
		// gateway-owned txns, and gateway txns don't enter this branch
		// because gateway-owned non-SYN bytes bypass the wire-phase
		// tracker).
		t.phase = wirePhaseWaitTerminalSyn
		return wirePhaseEventNone
	}

	// Neither ACK nor NACK -- garbled byte, protocol error.
	// F-21 NOTE: this defensive path is intentionally NOT redirected to
	// wirePhaseWaitTerminalSyn. The bus is in a degraded state at this
	// point; the initiator may or may not emit a trailing SYN. Falling
	// through to Idle keeps the existing non-SYN CmdNACK release
	// semantics for the protocol-error case; the eventual SYN (whether
	// from the initiator or the bus's natural idle SYN burst) will
	// arrive in Idle phase and fire SYNIdle through the generic
	// fallthrough.
	t.reset(wirePhaseIdle)
	return wirePhaseEventCmdNACK
}

func (t *wirePhaseTracker) advanceWaitResponseLen(symbol byte) wirePhaseEvent {
	// This byte is the response LEN field.
	// We need to receive LEN data bytes + 1 CRC byte.
	t.responseBytesRemain = int(symbol) + 1
	t.phase = wirePhaseWaitResponseBody
	return wirePhaseEventNone
}

func (t *wirePhaseTracker) advanceWaitResponseBody(symbol byte) wirePhaseEvent {
	t.responseBytesRemain--
	if t.responseBytesRemain <= 0 {
		t.phase = wirePhaseWaitResponseAck
		return wirePhaseEventResponseDone
	}
	return wirePhaseEventNone
}

func (t *wirePhaseTracker) advanceWaitResponseAck(symbol byte) wirePhaseEvent {
	// SYN is handled by the caller before reaching this point.
	if symbol == protocol.SymbolNack {
		// AM3: limit response NACK retries to one. The eBUS spec allows
		// a single response retry. A second NACK ends the transaction.
		if !t.responseRetried {
			t.responseRetried = true
			// NACK: the initiator rejected the response CRC. The target
			// will retry the response (LEN DATA CRC). Transition to
			// WaitResponseLen instead of idle to keep phase tracking
			// active during the retry -- otherwise, third-party traffic
			// arriving during idle interleaves with the retried response
			// bytes in activeCh.
			t.phase = wirePhaseWaitResponseLen
			return wirePhaseEventNone
		}
		// F-21 (batch-20): second NACK is a structural terminal. Defer
		// TransactionDone to trailing SYN.
		t.phase = wirePhaseWaitTerminalSyn
		return wirePhaseEventNone
	}
	// F-21 (batch-20): ACK (0x00) or any other non-SYN symbol is the
	// successful structural terminal. Defer TransactionDone to trailing
	// SYN. The trailing SYN will be the ebusd-emitted 0xAA arriving via
	// session.handleSend forwarded to the adapter; under F-21 deferral
	// ownership remains with the external session through this round-
	// trip, so the SYN send is accepted instead of rejected.
	t.phase = wirePhaseWaitTerminalSyn
	return wirePhaseEventNone
}

// advanceWaitTerminalSyn handles the byte that arrives while the tracker
// is waiting for the trailing SYN of a fully-acknowledged (or terminally-
// NACKed / broadcast / i2i-ACKed) master frame. F-21 (batch-20).
//
// Expected: the byte IS the trailing wire SYN (0xAA). When it arrives,
// fire TransactionDone so onSYNLocked's terminal-SYN release branch
// drops ownership atomically with the SYN observation. By this point
// the external owner's session.handleSend has already forwarded its
// SYN to the adapter — the owner check passed because the tracker
// (and the mux) still considered the session the owner during the
// wire round-trip.
//
// Defensive: any non-SYN byte arriving in this state is a protocol
// anomaly (the initiator emitted something other than 0xAA after a
// structural terminal). Still fire TransactionDone so the mux can
// release ownership and the bus can recover; the next-frame parser
// will treat the next byte as either junk (Layer 1 resync) or as a
// fresh SRC. Without this defensive fire, a misbehaving initiator
// could leave the tracker in WaitTerminalSyn indefinitely (the bus's
// natural idle SYN burst would eventually rescue it, but explicit
// recovery is cheaper than waiting on bus-idle physics).
func (t *wirePhaseTracker) advanceWaitTerminalSyn(symbol byte) wirePhaseEvent {
	_ = symbol
	t.reset(wirePhaseIdle)
	return wirePhaseEventTransactionDone
}

// startRequest initializes the tracker for a new request phase.
// Called when the multiplexer grants bus ownership to a session.
func (t *wirePhaseTracker) startRequest() {
	t.reset(wirePhaseCollectRequest)
}

// startRequestWithSource initializes the tracker with the SRC byte
// pre-loaded. Use this when ArbitrationSendsSource() is true — the
// adapter firmware already placed the initiator byte on the wire
// during arbitration (StreamEventStarted), so onReceived never sees
// it. Without this, the tracker is off-by-one: DST is captured as
// SRC, PB as DST, and the LEN field is read from a data byte,
// causing premature WaitCmdAck transitions and false CmdNACK events.
func (t *wirePhaseTracker) startRequestWithSource(src byte) {
	t.reset(wirePhaseCollectRequest)
	t.requestBytesSeen = 1
	t.requestSrc = src
}

// isIdle reports whether the bus is in the idle state.
func (t *wirePhaseTracker) isIdle() bool {
	return t.phase == wirePhaseIdle
}

// initiator returns the SRC address captured during request collection.
// Only valid after at least one request byte has been seen.
func (t *wirePhaseTracker) initiator() byte {
	return t.requestSrc
}

// target returns the DST address captured during request collection.
// Only valid after at least two request bytes have been seen.
func (t *wirePhaseTracker) target() byte {
	return t.requestDst
}
