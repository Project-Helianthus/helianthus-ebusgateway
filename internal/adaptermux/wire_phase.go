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
			t.reset(wirePhaseIdle)
			return wirePhaseEventTransactionDone
		}
		// AM1 fix: initiator-to-initiator (i2i) frames have no response
		// phase. Initiator-capable addresses cannot be targets in a
		// request-response transaction (eBUS spec). Transition to done
		// instead of waiting for a response that will never arrive.
		if protocol.IsInitiatorCapableAddress(t.requestDst) {
			t.reset(wirePhaseIdle)
			return wirePhaseEventTransactionDone
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
		t.reset(wirePhaseIdle)
		return wirePhaseEventCmdNACK
	}

	// Neither ACK nor NACK -- garbled byte, protocol error.
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
		// Second NACK -- transaction considered complete (response failed).
		t.reset(wirePhaseIdle)
		return wirePhaseEventTransactionDone
	}
	// ACK (0x00) or any other non-SYN symbol: transaction complete.
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
