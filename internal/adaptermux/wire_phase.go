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
	wirePhaseEventSYNIdle                               // SYN received, bus idle
	wirePhaseEventSYNTimeout                            // SYN during wait phase (timeout)
)

// advance processes a received symbol and returns what event occurred.
// This must be called for every byte received from the adapter.
func (t *wirePhaseTracker) advance(symbol byte) wirePhaseEvent {
	// SYN resets the tracker. If we were in a wait phase, it's a timeout.
	if symbol == protocol.SymbolSyn {
		if t.phase.isSYNTimeoutBoundary() {
			t.reset(wirePhaseIdle)
			return wirePhaseEventSYNTimeout
		}
		if t.phase == wirePhaseCollectRequest {
			// SYN during request collection — incomplete request.
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
		t.phase = wirePhaseWaitResponseLen
		return wirePhaseEventCmdACK
	}

	// NACK or unexpected byte — transaction failed.
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

func (t *wirePhaseTracker) advanceWaitResponseAck(_ byte) wirePhaseEvent {
	// Any non-SYN symbol in WaitResponseAck phase = final ACK.
	// (SYN is handled by the caller before reaching this point.)
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
