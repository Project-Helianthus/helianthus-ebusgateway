package adaptermux

import (
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// A wire SYN that reaches onSYNLocked runs through named steps under
// stateMu: the opening step clears the fence and the recovery and delivers an
// owed resynchronization SYN; the echo step classifies the SYN against the
// gateway's expected echo and flushes the echo queue; the terminator step
// delivers the gateway's frame terminator; three release steps release the
// owner on a completed transaction, on a SYN timeout and on an idle SYN after
// its grace; the pre-echo step records the suppression of a SYN that cannot
// be a terminator; and the diagnostics step classifies the SYN for the
// forensic counters and the SYN ring. Each step reads what it observes at the
// point the handler always read it, derives the typed state and takes its
// decision from decideSYNBoundary, which starts from decideMuxTransition.
// The steps apply their effects in the order the handler always used. An
// effect whose outcome a later decision reads comes back as a second input
// under the same lock: the terminator write's outcome and the cleared
// gateway transaction re-enter the input of the later steps, and an idle SYN
// past its grace is presented as the idle-grace event before the owner is
// released.

// synBoundaryEvent is the transition event of a wire SYN.
var synBoundaryEvent = muxEvent{kind: muxEventSYN, data: protocol.SymbolSyn}

// synBoundaryIdleGraceEvent is the idle-grace event an idle SYN past the
// owner's grace becomes.
var synBoundaryIdleGraceEvent = muxEvent{kind: muxEventIdleGrace, data: protocol.SymbolSyn}

// synBoundaryInput is what the SYN boundary observes under stateMu besides
// the typed state. The arrival fields are read before any step changes the
// gateway transaction; the other fields are read again by each step at the
// point the handler always read them.
type synBoundaryInput struct {
	// phaseEvent is the wire-phase event the tracker resolved the SYN to.
	phaseEvent wirePhaseEvent
	// arrivedGatewayOwned: the gateway owned the bus when the SYN arrived.
	arrivedGatewayOwned bool
	// txnActiveBefore and deliveredBefore: at arrival the gateway
	// transaction was active and had delivered a byte to the active path.
	txnActiveBefore bool
	deliveredBefore bool
	// The gateway's expected-echo queue at arrival, before the flush: an
	// echo is pending, that echo is the structural terminator SYN, and the
	// queue was just drained between two writes.
	echoPending      bool
	echoTerminator   bool
	queueJustDrained bool
	// txnActive and delivered are the same two facts where the step decides.
	txnActive bool
	delivered bool
	// terminatorDelivered: the terminator SYN reached the active path.
	terminatorDelivered bool
	// wireGap is the time since the last wire activity; grantAge the time
	// since the grant; idleElapsed the owner's idle clock: since the grant
	// for the gateway, since the last wire activity for a session.
	wireGap     time.Duration
	grantAge    time.Duration
	idleElapsed time.Duration
	// idleGrace and sessionGrace are the configured idle release grace and
	// external session SYN grace.
	idleGrace    time.Duration
	sessionGrace time.Duration
	// externalPendingRead: externalPending was read; externalPending: an
	// external session has a START pending.
	externalPendingRead bool
	externalPending     bool
}

// synRelease is the ownership release a SYN decides.
type synRelease uint8

const (
	synReleaseNone synRelease = iota
	// synReleaseTransactionDone: the tracker confirmed a structural terminal.
	synReleaseTransactionDone
	// synReleaseTimeout: a SYN during a wait phase.
	synReleaseTimeout
	// synReleaseIdleGrace: an idle SYN after the owner's grace, or with an
	// external START pending while the gateway owns the bus.
	synReleaseIdleGrace
)

// synBoundaryDecision is the decision of decideSYNBoundary.
type synBoundaryDecision struct {
	// clearRecovery: the SYN ends a recovery other than the reset recovery.
	clearRecovery bool
	// deliverOwedResync: an owed resynchronization SYN goes to the active
	// path.
	deliverOwedResync bool
	// midWrite and betweenWrites classify the SYN against the gateway's
	// expected echo; keepEchoQueue: the SYN is mid-frame noise of a live
	// gateway transaction, so the echo queue is not flushed.
	midWrite      bool
	betweenWrites bool
	keepEchoQueue bool
	// deliverTerminator: the SYN is the gateway's frame terminator.
	deliverTerminator bool
	// release is the ownership release; logGap: an external SYN timeout
	// past its grace is logged first; askExternalPending: the gateway's
	// idle release needs to know whether an external START is pending;
	// externalBypass: the release skips the gateway's grace for it;
	// graceUsed is the grace the idle release compared against.
	release            synRelease
	logGap             bool
	askExternalPending bool
	externalBypass     bool
	graceUsed          time.Duration
	// The teardown of the released owner: the reason that ends the gateway
	// transaction (ReasonNone keeps it), clearing the inter-write sentinel,
	// cancelling the gateway echo watchdog and completing the out-of-step
	// run of a session.
	endTxn         ActiveTxnInactiveReason
	clearSentinel  bool
	cancelWatchdog bool
	stepCompleted  bool
	// preEchoSuppressed: the SYN is withheld from the active path;
	// betweenWritesSuppressed: the between-writes window was the reason.
	preEchoSuppressed       bool
	betweenWritesSuppressed bool
	// The diagnostics: the grant-window and inter-write-empty populations,
	// the transport window check, the SYN ring entry and whether the SYN
	// reached the active path.
	grantWindowSYN       bool
	interWriteEmptySYN   bool
	checkTransportWindow bool
	recordDiag           bool
	synDelivered         bool
}

// decideSYNBoundary is the SYN boundary's part of the transition function.
// decideMuxTransition decides whether the event can occur in the state; for
// a wire SYN this decides every step of the boundary, and for the idle-grace
// event the release and its teardown.
func decideSYNBoundary(s muxState, ev muxEvent, in synBoundaryInput) synBoundaryDecision {
	var d synBoundaryDecision
	if decideMuxTransition(s, ev).invalid != "" {
		return d
	}
	owner, owned := s.ownerParty()
	gatewayOwned := owned && owner == gatewaySessionID
	switch ev.kind {
	case muxEventIdleGrace:
		d.release = synReleaseIdleGrace
		decideSYNTeardown(&d, gatewayOwned, in.txnActive)
		return d
	case muxEventSYN:
	default:
		return d
	}
	d.clearRecovery = s.recovery != arbitrationRecoveryReset
	d.deliverOwedResync = s.resyncOwed > 0 && !in.txnActiveBefore
	decideSYNEcho(&d, in)
	decideSYNTerminator(&d, gatewayOwned, in)
	if owned {
		switch in.phaseEvent {
		case wirePhaseEventTransactionDone:
			d.release = synReleaseTransactionDone
		case wirePhaseEventSYNTimeout:
			decideSYNTimeoutRelease(&d, owner, in)
		case wirePhaseEventSYNIdle:
			decideIdleSYNRelease(&d, owner, in)
		}
		if d.release == synReleaseTransactionDone || d.release == synReleaseTimeout {
			decideSYNTeardown(&d, gatewayOwned, in.txnActive)
		}
	}
	decideSYNPreEcho(&d, gatewayOwned, in)
	decideSYNDiag(&d, in)
	return d
}

// decideSYNEcho classifies the SYN against the gateway's expected echo.
func decideSYNEcho(d *synBoundaryDecision, in synBoundaryInput) {
	// P10.2 — peek the gateway echo tracker BEFORE flushOnSYN clears it.
	// The next-expected-echo byte discriminates the *only* unsafe case
	// (mid-write race) from all the legitimate ones. Using the queue
	// head as the discriminator preserves backward-compat with the
	// pre-P10.2 terminator semantics for the common shape (queue empty
	// at SYN time — gateway between writes or in response-read) while
	// closing the specific bug where a buffered/colliding SYN arrives
	// while the gateway is still awaiting the echo of a non-SYN body
	// byte:
	//
	//   - hasPending=true  && nextExpected == SymbolSyn: bus.Send just
	//     wrote the terminator SYN (sendEndOfMessage path); the wire
	//     echo IS the legitimate terminator. Allow terminator gate.
	//   - hasPending=false: queue is empty. Either the gateway has
	//     consumed all its echoes (between request and response, or
	//     during response read), or it has never written. A SYN here
	//     was the assumed-terminator under pre-P10.2 semantics. Allow
	//     terminator gate (preserves all existing tests / lifecycle
	//     contracts; legacy behavior from PR #502).
	//   - hasPending=true  && nextExpected != SymbolSyn: gateway is
	//     mid-write (waiting for echo of a request body byte). This
	//     wire SYN CANNOT be a terminator — eBUS protocol guarantees
	//     no foreign SYN appears mid-frame after arbitration. SUPPRESS
	//     activeCh delivery, idle release, and flushOnSYN. This is the
	//     P10.2-specific behavior change that closes the 671
	//     pre_echo_syn events observed in production.
	//
	// Codex P10.2 review: idle release MUST be gated on the suppression
	// decision — otherwise a mid-frame noise SYN that arrives after
	// IdleReleaseGrace elapses (slow txn) would still abort the
	// legitimate transaction even though we suppressed activeCh delivery.
	// F-NEW-28 (2026-05-21, round-9 layer-correct fix): peek the
	// next-expected echo's STRUCTURAL flag in addition to its byte
	// value. The flag distinguishes:
	//
	//   (a) `(SymbolSyn, structural=true)`  — the gateway just wrote
	//        a structural terminator SYN; the legitimate wire echo
	//        is raw 0xAA (WasEscaped=false). A wire AUTO-SYN here is
	//        either the legitimate terminator echo OR (more rarely)
	//        a pre-terminator-echo idle SYN; both flow paths are
	//        already handled by the terminator-delivery gate at
	//        line 2614.
	//
	//   (b) `(SymbolSyn, structural=false)` — the gateway just wrote
	//        a PAYLOAD 0xAA; the legitimate wire echo is logical 0xAA
	//        with WasEscaped=true (the adapter wire-encoded our byte
	//        as `A9 01`). A wire AUTO-SYN here is INTERFERENCE
	//        (round-9 invariant violation) — it MUST be suppressed
	//        from activeCh delivery to keep `sendRawWithEcho`'s
	//        round-9 absorb path at bus.go:1204 cold under v8 enforce
	//        (HelianthusRound9FiredUnderProxy I8).
	//
	//   (c) `(non-SymbolSyn, *)`              — non-SYN expected echo;
	//        a wire SYN here is mid-frame noise. Same suppression as
	//        before.
	//
	// Pre-F-NEW-28 the predicate only checked the byte value, so
	// case (b) was incorrectly NOT suppressed — that was the leak
	// (production: ~4/min residual under v8 enforce). With the
	// structural flag the predicate now treats case (b) the same as
	// case (c): midWriteSyn=true → wire SYN suppressed.
	d.midWrite = in.echoPending && !in.echoTerminator

	// batch-26 round-7 — Attack 3 closure (inter-write empty-queue SYN
	// leak). The peek-based P10.2 gate (midWriteSyn) cannot suppress a
	// wire SYN that arrives during the brief window between matchEcho
	// consuming echo K and sendLoop's recordSent of byte K+1 — at that
	// instant peekNextExpected returns hasPending=false (the queue is
	// empty), so midWriteSyn is false even though the gateway is
	// objectively still mid-frame between body writes.
	//
	// queueJustDrained (echo_tracker) is set by matchEcho when a
	// NON-SYN echo consumption empties the queue, and cleared on the
	// next bounding event:
	//   - recordSent: a new write arms the queue again.
	//   - markRequestStart: a fresh txn boundary.
	//   - flushOnSYN: a wire SYN observed legitimately closing the txn.
	//   - matchEcho consuming SymbolSyn: terminator echo observed.
	// The flag is true ONLY in the brief window between echo K's read
	// and byte K+1's write — exactly where Attack 3 fires.
	//
	// This replaces the round-6 bytesDeliveredToActive>0 broadening
	// (reverted in batch-25) which leaked across txn boundaries (only
	// reset at recordGatewayGrant), causing legitimate post-txn echo
	// windows to be misclassified as wire intrusions under load and
	// collapsing throughput ~65%. queueJustDrained is bounded by
	// recordSent/markRequestStart/flushOnSYN — it CANNOT carry over to
	// a new transaction.
	d.betweenWrites = in.queueJustDrained && !in.echoPending

	// P10.2: skip flush when this SYN is mid-write OR between-writes
	// noise. The expected-echo queue (and the inter-write sentinel)
	// must survive so the next real echo still matches.
	d.keepEchoQueue = in.arrivedGatewayOwned && in.txnActiveBefore &&
		(d.midWrite || d.betweenWrites)
}

// decideSYNTerminator decides whether the SYN is the gateway's frame
// terminator.
func decideSYNTerminator(d *synBoundaryDecision, gatewayOwned bool, in synBoundaryInput) {
	// Runtime-soak P0 + lifecycle correctness + Codex PR #502 P1:
	// clear gatewayTxnActive on SYN during gateway ownership ONLY if
	// at least one real adapter byte has already been enqueued on
	// activeCh for this txn (bytesDeliveredToActive > 0). That counter
	// is incremented on successful activeCh sends in the readLoop byte-
	// delivery path (SYN-branch deliverToActive and non-SYN inline
	// delivery) — so it rises precisely when the mux has observed the
	// first post-grant adapter byte AND routed it to the active path.
	//
	// Why this counter and not bytesRead / bytesWritten:
	//   - bytesRead lags (consumer side): incremented by
	//     activeTransport.ReadByte/ReadEvent on CONSUMPTION, which is
	//     strictly after activeCh enqueue. Under production timing
	//     readLoop is a tight loop while bus.Send.sendRawWithEcho has
	//     multiple channel hops between receiving a grant and reading.
	//     Gating on bytesRead would over-suppress legitimate terminator
	//     SYNs at end of reply whenever the consumer is slower than
	//     readLoop — that is the normal case.
	//   - bytesWritten leads (initiator side): incremented by
	//     activeTransport.Write BEFORE the byte reaches the adapter and
	//     BEFORE any echo has been processed. On a busy/idle-chatter
	//     link, a buffered idle SYN can arrive between Write returning
	//     and the echo being enqueued on activeCh; gating on bytesWritten
	//     would treat that SYN as a terminator, clear gatewayTxnActive,
	//     and deliver 0xAA to the active path — sendRawWithEcho then
	//     sees SYN instead of the expected echo (echo_mismatch / timeout).
	//   - bytesDeliveredToActive is the precise midpoint: it goes from 0
	//     to ≥1 at the exact instant the first real adapter byte has
	//     been enqueued on activeCh. Before that point any SYN is
	//     pre-echo idle-buffer noise (suppress); after that point any
	//     SYN is either a response byte (delivered via the non-SYN path)
	//     or the legitimate frame terminator (delivered here).
	//
	// Normal transactions (including broadcast) produce
	// bytesDeliveredToActive>0 via echoes of the gateway's own writes,
	// so the trailing SYN correctly clears. Genuine aborts (no writes,
	// no reads) are caught by MaxOwnershipDuration, ActiveWriteError,
	// ActiveReadTimeout, context cancel, reset, or reconnect — per the
	// lifecycle contract.
	//
	// P10.2: terminator gate gains an additional guard `!midWriteSyn`.
	// Without this guard, mid-write SYNs (stale buffered SYN from before
	// grant interleaved into mid-frame echo flow, wire collision,
	// foreign-initiator glitch) would be delivered to activeCh while
	// bus.Send is waiting for echo of a non-SYN data byte → 0xAA in
	// echo position → echo_mismatch with subclass=pre_echo_syn (671
	// events observed in production soak before this fix; ~16% of all
	// echo_mismatch).
	//
	// batch-25 hotfix: reverted batch-24 round-6 broadening to
	// `!preEchoMidFrameSuppress`. The wider guard caused legitimate
	// queue-empty terminator paths to be misclassified as wire
	// intrusions under load, blocking ownership unwind and collapsing
	// gateway throughput (~65% drop in passive frame rate). See the
	// preEchoMidFrameSuppress derivation above.
	d.deliverTerminator = gatewayOwned && in.txnActive && in.delivered && !d.keepEchoQueue
}

// decideSYNTimeoutRelease decides the release on a SYN timeout.
func decideSYNTimeoutRelease(d *synBoundaryDecision, owner uint64, in synBoundaryInput) {
	// F-10v2 (EBUSD-VERIFICATION-2026-05-11-batch3.md): branch the
	// SYN-timeout release on owner identity.
	//
	//   - Gateway-owned: SYN during WaitCmdAck/CollectRequest genuinely
	//     means the gateway's B524 directed-read transaction died. The
	//     gateway's protocol pattern has no legitimate inter-byte gap,
	//     so SYN-timeout is a reliable signal. Release immediately.
	//   - External-session-owned: an external client (e.g. ebusd
	//     running a broadcast scan to 0xFE) legitimately produces
	//     inter-responder-response gaps that the wire-phase machine
	//     reports as SYN-timeout. Treat those as soft idle and only
	//     release if the gap exceeds ExternalSessionSYNGrace (default
	//     2s). Without this branch, the mux yanked ownership mid-
	//     frame from ebusd, producing the read-timeout cascade
	//     observed in the batch-3 trace (80 SYN-timeout releases vs
	//     0 for the gateway in the same window).
	if owner == gatewaySessionID {
		d.release = synReleaseTimeout
		return
	}
	if in.wireGap >= in.sessionGrace {
		d.release, d.logGap = synReleaseTimeout, true
	}
	// Otherwise: hold ownership. The external session's
	// protocol is still mid-frame from its own perspective;
	// the next legitimate adapter event will either advance
	// the wire phase or trigger MaxOwnershipDuration.
}

// decideIdleSYNRelease decides the release on an idle SYN.
func decideIdleSYNRelease(d *synBoundaryDecision, owner uint64, in synBoundaryInput) {
	// P10.2 — gate idle release ONLY for live mid-frame races, NOT for
	// stuck-write timeouts. The distinguishing signal:
	//   - bytesDeliveredToActive > 0 + midWriteSyn: the gateway has
	//     live echo activity AND a noise SYN arrived mid-write
	//     (queue head non-SYN). Aborting the txn would discard real
	//     progress. Suppress idle release.
	//   - bytesDeliveredToActive == 0 + midWriteSyn: the gateway
	//     wrote bytes but NO echo ever arrived. The bus is
	//     stuck/unresponsive. The IdleReleaseGrace mechanism is
	//     exactly designed to bail out here. Allow idle release.
	//     (Used by TestRegression_StartedButNoResponse to model
	//     production unresponsive-target scenarios.)
	//
	// batch-26 round-7 (Codex r2 defect 3 + r3 P1): the idle release
	// gate must honor BOTH midWriteSyn AND betweenWritesSyn during the
	// IdleReleaseGrace window so the F-26 external-bidder bypass
	// (below) does NOT release ownership mid-write under contention.
	//
	// Initial fix (r2 defect 3) narrowed suppressIdleRelease to
	// midWriteSyn-only on the theory that the sentinel could stick
	// indefinitely if the gateway stalled between bytes. Codex r3 P1
	// caught the missed implication: when an Attack-3 SYN arrives in
	// the queueJustDrained window AND an external session has a
	// pending START, the F-26 bypass fires SYNIdle release on the
	// gateway in the middle of its inter-write window, aborting the
	// live transaction before recordSent(K+1) runs.
	//
	// Correct invariant: suppress idle release whenever gateway is
	// mid-frame (midWriteSyn OR betweenWritesSyn) — BUT bound the
	// betweenWritesSyn case by IdleReleaseGrace so a pathological
	// stall doesn't pin ownership forever. midWriteSyn has its own
	// bound via bus.Send's activeChanTimeout, whose expiry releases the
	// grant at once (issue #1401); the sentinel-only case relies on grace.
	//
	// When grace expires for the between-writes case, the existing
	// gateway-owner release branch below clears the sentinel via
	// gatewayEcho.ClearQueueJustDrained() — so the next SYN
	// observation IS a legitimate idle terminator.
	betweenWritesWithinGrace := d.betweenWrites && in.grantAge <= in.idleGrace
	suppressIdleRelease := in.arrivedGatewayOwned && in.txnActive && in.delivered &&
		(d.midWrite || betweenWritesWithinGrace)
	if suppressIdleRelease {
		return
	}
	// Two thresholds:
	//   - Gateway owner: 200 ms IdleReleaseGrace measured from
	//     grant (legacy behavior — correct for the gateway's tight
	//     directed-read protocol).
	//   - External owner: ExternalSessionSYNGrace measured from
	//     the most recent wire activity (NOT from grant). This
	//     closes the bypass Codex P1 round-1 identified: after
	//     wirePhaseTracker.advance() resets the tracker to idle,
	//     subsequent SYNs are classified as SYNIdle rather than
	//     SYNTimeout, so the SYN-timeout branch above is never
	//     re-entered. Without this gap-based external grace, the
	//     200ms IdleReleaseGrace would still tear down ebusd's
	//     scan after the first 200ms. (Codex P1 + P2 on PR #621.)
	if owner != gatewaySessionID {
		d.graceUsed = in.sessionGrace
		if in.idleElapsed >= in.sessionGrace {
			d.release = synReleaseIdleGrace
		}
		return
	}
	d.graceUsed = in.idleGrace
	// F-26 (batch-23, iter3, 2026-05-14): bypass IdleReleaseGrace
	// when an external session has a pending START. The grace
	// period exists to absorb mid-transaction wire stalls on the
	// gateway's own protocol (e.g. slow B524 responses). It does
	// NOT need to apply when an external bidder is waiting,
	// because:
	//
	//   1. Once the gateway's transaction has produced its
	//      trailing SYN, the phaseEvent here is SYNIdle —
	//      meaning the wire-phase tracker has already
	//      committed the gateway's transaction as complete.
	//      Releasing immediately doesn't truncate any in-flight
	//      work.
	//
	//   2. Pre-F-26, the grace combined with the gateway's
	//      back-to-back poll cadence produced 595 ms median
	//      / 4.2 s p95 START-to-STARTED latency on ebusd —
	//      far past its arbitration-wait timeout, triggering
	//      the `arbitration won in invalid state` cascade.
	//      Wire-byte trace evidence:
	//      /tmp/iter3-enh-stream.txt, latency analysis in
	//      /tmp/measure_start_latency.py.
	//
	//   3. The fix is gated on `arb.hasExternalPending()` so
	//      steady-state gateway-only operation is unaffected
	//      (the grace still fires when no external bidder is
	//      waiting).
	switch {
	case in.idleElapsed > in.idleGrace:
		d.release = synReleaseIdleGrace
	case !in.externalPendingRead:
		d.askExternalPending = true
	case in.externalPending:
		d.release, d.externalBypass = synReleaseIdleGrace, true
	}
}

// decideSYNTeardown decides what the release of the owner tears down; no
// release tears nothing down.
func decideSYNTeardown(d *synBoundaryDecision, gatewayOwned, txnActive bool) {
	if d.release == synReleaseNone {
		return
	}
	if gatewayOwned && txnActive {
		switch d.release {
		case synReleaseTransactionDone:
			d.endTxn = ReasonTransactionDone
		case synReleaseTimeout:
			d.endTxn, d.cancelWatchdog = ReasonSYNTimeout, true
		case synReleaseIdleGrace:
			d.clearSentinel, d.endTxn, d.cancelWatchdog = true, ReasonSYNIdle, true
		}
	}
	if d.release == synReleaseTransactionDone && !gatewayOwned {
		d.stepCompleted = true
	}
}

// decideSYNPreEcho decides whether the SYN is withheld from the active path.
func decideSYNPreEcho(d *synBoundaryDecision, gatewayOwned bool, in synBoundaryInput) {
	// Pre-echo SYN suppression decision (echo_mismatch fix).
	//
	// P10.2 — combines the original "pre-first-echo" suppression
	// (bytesDeliveredToActive == 0 — buffered idle SYN that pre-dates
	// any post-grant byte) with the new "mid-write-race" suppression
	// (gateway owns + txn active + queue head is non-SYN). Both cases
	// describe a SYN that cannot be a legitimate terminator and that
	// would corrupt sendRawWithEcho if delivered to activeCh.
	//
	// Codex PR #502 P1 originally defined the pre-first-echo gate using
	// bytesDeliveredToActive (chosen for its precise mid-point semantics
	// between bytesWritten leading and bytesRead lagging). P10.2 keeps
	// that branch and adds the mid-write branch as an OR.
	//
	// Counter (synSuppressedPreEcho) preserved for backward-compat with
	// existing diag tests; semantically it now covers a superset.
	preFirstEchoSyn := gatewayOwned && in.txnActive && !in.delivered
	d.preEchoSuppressed = preFirstEchoSyn || d.keepEchoQueue
	// batch-26 round-7 — Attack 3 subset counter. Increment specifically
	// when the betweenWritesSyn branch (queueJustDrained=true,
	// hasPendingEcho=false) was the path that triggered suppression. Two
	// guard conditions:
	//   1. preEchoMidFrameSuppress fired (txn active, owner, etc).
	//   2. betweenWritesSyn is the *reason* it fired (not just a
	//      coincident midWriteSyn path — those two are mutually
	//      exclusive because midWriteSyn requires hasPendingEcho=true
	//      and betweenWritesSyn requires hasPendingEcho=false, but the
	//      explicit guard documents intent and survives any future
	//      refactor of the suppression derivation).
	d.betweenWritesSuppressed = d.keepEchoQueue && d.betweenWrites && !d.midWrite
}

// decideSYNDiag decides the forensic classification of the SYN.
func decideSYNDiag(d *synBoundaryDecision, in synBoundaryInput) {
	// batch-21 diagnostic: classify which suppression gap a SYN passes
	// through at the wire-SYN observation point. Forensic only — no
	// behavior change. Counters are incremented under stateMu (held by
	// the caller of onSYNLocked) and read OBSERVATION-TIME snapshots:
	//
	//   - wasGatewayOwned: derived from the function's hasOwner+ownerID
	//     parameters, which the caller pinned at SYN arrival.
	//   - gwActiveBefore: snapshotted at function entry (line ~2035)
	//     before the terminator/idle/timeout branches mutate
	//     m.gatewayTxnActive. CRITICAL: reading m.gatewayTxnActive at
	//     this site would misclassify a legitimate terminator SYN
	//     (which clears m.gatewayTxnActive at line ~2160) as Attack 1.
	//     Codex pre-push review caught this.
	//   - hasPendingEcho: snapshotted at function entry (line ~2068)
	//     before flushOnSYN clears the echo queue.
	//   - bytesDeliveredBefore: snapshotted at function entry (line
	//     ~2036) before the terminator branch increments the counter
	//     for its own delivery.
	//
	// Three mutually-exclusive populations are counted (a fourth case
	// — "fully covered by existing P10.2 suppression" — is implicit in
	// preEchoSuppressed=true above and isn't recounted here to avoid
	// double-counting against echo_mismatch):
	//
	//   Attack 1 (synSeenDuringGrantWindow): gateway owns the bus but
	//     the txn-active flag is still false — the brief window between
	//     completeArbitrationGrant() and the first activeTransport.Write
	//     calling recordSent. preEchoMidFrameSuppress requires
	//     gatewayTxnActive=true so a SYN here bypasses the gate.
	//
	//   Attack 3 (synSeenWhileInterWriteEmpty): txn is active, at
	//     least one byte has been delivered (so we're past the
	//     pre-first-echo branch), but the echo queue is currently empty
	//     (peekNextExpected returns hasPending=false). This is the
	//     "between two writes" window where matchEcho consumed echo K
	//     and sendLoop hasn't yet armed echo K+1. The peek-based gate
	//     can't suppress because there's no expected head to compare to.
	//
	//     IMPORTANT (Codex pre-push review, 2026-05-15): at the SYN
	//     observation point this condition CANNOT be cleanly separated
	//     from a legitimate end-of-transaction terminator SYN. Both
	//     match {gwActive=true, hasPendingEcho=false,
	//     bytesDeliveredBefore>0}. The forensic interpretation:
	//
	//       leak_rate ≈ synSeenWhileInterWriteEmpty − grantsTotal
	//
	//     where grantsTotal is the per-window transaction count (each
	//     transaction has one legitimate terminator SYN). Operator
	//     compares the residual to ebus_errors_total{class=
	//     echo_mismatch,subclass=pre_echo_syn} rate. If the residual
	//     matches the leak rate, Attack 3 is confirmed. The metric
	//     help text in bus_observability_store.go documents this
	//     subtraction explicitly.
	if in.arrivedGatewayOwned {
		d.grantWindowSYN = !in.txnActiveBefore
		d.interWriteEmptySYN = in.txnActiveBefore && !in.echoPending && in.deliveredBefore
		d.checkTransportWindow = true
	}

	// SYN-path diagnostics: record only when gateway owned the bus at
	// SYN arrival OR at the instant one of the branches above just
	// cleared gatewayTxnActive (so we see the transition). The caller's
	// activePathExpectsBytes() check (== gatewayTxnActive AFTER this
	// function returns) determines whether the SYN will be delivered to
	// activeCh — if gwActiveAfter is false, onSYNLocked consumed this
	// SYN as an end-of-txn terminator and the Send consumer on activeCh
	// does NOT see it.
	d.recordDiag = in.arrivedGatewayOwned || in.txnActiveBefore
	// synDelivered is true iff the SYN actually reaches activeCh.
	// That happens in exactly two cases:
	//   (a) onSYNLocked delivered the SYN inline as the frame terminator
	//       (terminatorDelivered, bytesRead>0 branch, PR #502 E2E fix), OR
	//   (b) the txn is still active after onSYNLocked AND this SYN is
	//       NOT pre-echo noise (gwActiveAfter && !preEchoSuppressed).
	d.synDelivered = in.terminatorDelivered || (in.txnActive && !d.preEchoSuppressed)
}

// synBoundary carries one wire SYN through the steps of onSYNLocked.
type synBoundary struct {
	// ownerID and hasOwner are the owner the caller pinned when the SYN
	// arrived; hasOwner is cleared once a completed transaction released
	// that owner.
	ownerID  uint64
	hasOwner bool
	now      time.Time
	in       synBoundaryInput
	// The return values of onSYNLocked.
	passiveEvents         []PassiveEvent
	preEchoSuppressed     bool
	cancelGatewayWatchdog bool
}

// synBoundaryStateLocked derives the typed state the SYN boundary decides
// on. The owner is the one the caller pinned when the SYN arrived, cleared
// once a step released it, so the view takes neither the arbiter lock nor
// the echo tracker: the queued bids and the deferred gateway loser are not
// read (an unowned bus with queued bids is muxIdle here), and the expected
// echo is observed by the echo step instead. Caller holds stateMu.
func (m *Mux) synBoundaryStateLocked(ownerID uint64, hasOwner bool) muxState {
	s := muxState{
		recovery:     m.arbitrationRecovery,
		absorb:       m.pendingStartAbsorb,
		blocking:     m.blockingArbActive,
		held:         m.step.held,
		resyncOwed:   m.collisionResyncOwed,
		retired:      m.retiredSelection,
		telegramLive: m.phase.telegramLive(),
	}
	if s.retired {
		s.retiredSource = m.localLoserSource
	}
	if p := m.pendingStart; p != nil {
		s.start = muxSelection{pending: true, party: p.sessionID, initiator: p.initiator}
	}
	switch {
	case hasOwner && ownerID == gatewaySessionID && m.gatewayTxnActive:
		s.phase = muxGatewayTransaction
	case hasOwner && ownerID == gatewaySessionID:
		s.phase = muxGatewayGrace
	case hasOwner:
		s.phase = muxSessionOwned
		s.owner = ownerID
	case s.start.pending && m.pendingStart.blockingArb:
		s.phase = muxStartBlocking
	case s.start.pending:
		s.phase = muxStartAsync
	case m.deferRegrantUntilNextSyn:
		s.phase = muxFenced
	default:
		s.phase = muxIdle
	}
	return s
}

// decideSYNBoundaryLocked takes the decision of a step over the typed state
// and the input observed so far. Caller holds stateMu.
func (m *Mux) decideSYNBoundaryLocked(b *synBoundary, ev muxEvent) synBoundaryDecision {
	return decideSYNBoundary(m.synBoundaryStateLocked(b.ownerID, b.hasOwner), ev, b.in)
}

// onSYNLocked handles a SYN symbol. Caller holds stateMu.
// Returns buffered passive events, whether tryGrant should be called,
// and whether this SYN is pre-echo noise that the caller must suppress
// from activeCh delivery (see echo_mismatch root-cause comment at the
// call site).
// Codex round-3 MUST FIX #1 on PR #647: returns an additional
// `cancelGatewayWatchdog` bool. When true, the caller MUST invoke
// m.cancelGatewayWatchdog() after releasing stateMu — set by the
// SYN-timeout / SYN-idle teardown branches inside onSYNLocked
// when they transition the gateway out of an active txn, so the
// echo watchdog cannot fire stale after the txn is gone.
func (m *Mux) onSYNLocked(phaseEvent wirePhaseEvent, ownerID uint64, hasOwner bool, now time.Time) ([]PassiveEvent, bool, bool, bool) {
	b := synBoundary{ownerID: ownerID, hasOwner: hasOwner, now: now}
	b.in.phaseEvent = phaseEvent
	m.openSYNBoundaryLocked(&b)
	m.classifySYNAgainstEchoLocked(&b)
	m.deliverSYNTerminatorLocked(&b)
	m.releaseOnTransactionDoneLocked(&b)
	m.releaseOnSYNTimeoutLocked(&b)
	m.releaseOnIdleSYNLocked(&b)
	m.suppressPreEchoSYNLocked(&b)
	m.recordSYNBoundaryDiagLocked(&b)

	shouldTryGrant := m.arb.hasPending()
	// When onSYNLocked itself delivered the terminator, signal the caller
	// via the returned shouldTryGrant alone is insufficient — the caller
	// also needs to skip its own deliverToActive(symbol) call because
	// activePathExpectsBytes() is now false. That already happens
	// naturally: activeExpects is re-read AFTER onSYNLocked returns (see
	// onReceived), so with gatewayTxnActive cleared it becomes false and
	// the caller's deliverToActive is skipped. No double-delivery.
	return b.passiveEvents, shouldTryGrant, b.preEchoSuppressed, b.cancelGatewayWatchdog
}

// openSYNBoundaryLocked is the opening step: a SYN clears the fence and a
// recovery other than the reset recovery, and delivers an owed
// resynchronization SYN. Caller holds stateMu.
func (m *Mux) openSYNBoundaryLocked(b *synBoundary) {
	state := m.synBoundaryStateLocked(b.ownerID, b.hasOwner)
	owner, owned := state.ownerParty()
	b.in.arrivedGatewayOwned = owned && owner == gatewaySessionID
	b.in.txnActiveBefore = m.gatewayTxnActive
	b.in.txnActive = b.in.txnActiveBefore
	d := decideSYNBoundary(state, synBoundaryEvent, b.in)

	m.deferRegrantUntilNextSyn = false
	if d.clearRecovery {
		m.arbitrationRecovery = arbitrationRecoveryNone
	}

	// Issue #1401: a confirmed arbitration loss released the gateway at
	// once, but protocol.Bus still waits for
	// protocol.CollisionRetryResyncSYNCount wire SYNs before it re-bids.
	// Deliver exactly the owed number to activeCh although the gateway no
	// longer owns the bus, so the retry is not starved until its request
	// deadline. Surplus SYNs are not delivered; the window closes early on
	// the collision lifecycle completion or the gateway's next bid.
	if d.deliverOwedResync {
		select {
		case m.activeCh <- activeEvent{kind: activeEventByte, b: protocol.SymbolSyn}:
			m.collisionResyncOwed--
		default:
			m.activeTxn.terminatorDropOnFullCh.Add(1)
			m.logger.Printf("adaptermux: active channel full, dropping owed collision resync SYN")
		}
	}
}

// classifySYNAgainstEchoLocked is the echo step: it snapshots the gateway
// transaction at arrival, observes the gateway's expected echo and flushes
// the echo queue unless the SYN is mid-frame noise. Caller holds stateMu.
func (m *Mux) classifySYNAgainstEchoLocked(b *synBoundary) {
	// SYN-path diagnostics (bounded). Capture gwActiveBefore snapshot
	// under stateMu so the ring entry matches the decision the SYN
	// branches below make. Only record when gateway owns the bus at
	// SYN arrival — that's the hypothesis-relevant window (final-SYN
	// echo potentially consumed by onSYNLocked before Send sees the
	// frame terminator).
	// batch-21 diagnostic: snapshot the delivery counter at observation
	// time as well. The terminator-delivery branch increments
	// bytesDeliveredToActive at line ~2155 mid-function; the Attack 3
	// gate must classify against the SYN-arrival state, not the post-
	// mutation state, otherwise a terminator SYN whose own delivery
	// pushed the count from 0→1 would falsely satisfy the >0 gate.
	b.in.deliveredBefore = m.activeTxn.bytesDeliveredToActive.Load() > 0

	// The expected echo is peeked before flushOnSYN clears it; see
	// decideSYNEcho for why its head and structural flag decide.
	next, structural, pending := m.gatewayEcho.peekNextExpectedWithFlags()
	b.in.echoPending = pending
	b.in.echoTerminator = pending && next == protocol.SymbolSyn && structural
	b.in.queueJustDrained = m.gatewayEcho.IsQueueJustDrained()
	d := m.decideSYNBoundaryLocked(b, synBoundaryEvent)

	// batch-25 hotfix — reverted batch-24 round-6 broadening (grantSyn /
	// interWriteSyn). Live Prometheus history showed the broader gate
	// over-suppressed legitimate gateway echoes: per-frame echo_mismatch
	// ratio CLIMBED from 5-11% (pre-investigation) to 12-23% on the
	// round-6 binary, while absolute counts dropped only because gateway
	// throughput collapsed (passive frame rate 1.0-1.3 c/s →
	// 0.0-0.5 c/s, ~65% drop). Post-revert (binary injected
	// 2026-05-18T05:00 UTC) ratio dropped back to 7.21% and frame rate
	// recovered to 1.119 c/s within 7 minutes. See
	// _work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-18-batch25.md.
	//
	// batch-26 round-7 — re-introduces inter-write suppression via the
	// tightly-bounded queueJustDrained sentinel above (not the
	// cross-txn bytesDeliveredToActive heuristic).
	//
	// Flush gateway echo tracker at SYN boundary. Flushed bytes are
	// confirmed gateway self-traffic — do NOT emit to passive path
	// (passive is third-party only). They are delivered to external
	// sessions via deliverSYNToSessions + deliverToSessions elsewhere.
	if !d.keepEchoQueue {
		m.gatewayEcho.flushOnSYN()
	}
}

// deliverSYNTerminatorLocked is the terminator step: the gateway's frame
// terminator goes to the active path and ends its transaction. The outcome
// of the write re-enters the input the later steps decide on. Caller holds
// stateMu.
func (m *Mux) deliverSYNTerminatorLocked(b *synBoundary) {
	b.in.txnActive = m.gatewayTxnActive
	b.in.delivered = m.activeTxn.bytesDeliveredToActive.Load() > 0
	d := m.decideSYNBoundaryLocked(b, synBoundaryEvent)
	if !d.deliverTerminator {
		return
	}
	// PR #502 E2E fix: this SYN is the legitimate frame terminator.
	// The bus.Send consumer on activeCh needs to see it to complete the
	// response frame — without delivery, Send hangs until read-timeout.
	// Deliver the SYN byte to activeCh BEFORE clearing gatewayTxnActive
	// so activePathExpectsBytes() is still true at enqueue time. The
	// send is non-blocking: if activeCh is full we bump a diagnostic
	// counter and still clear (blocking would deadlock under stateMu).
	// Use ReasonSYNTerminator (not ReasonSYNIdle) to distinguish a
	// successful terminator delivery from the abandoned-grant SYN-idle
	// path in onSYNLocked's idle-release branch below.
	select {
	case m.activeCh <- activeEvent{kind: activeEventByte, b: protocol.SymbolSyn}:
		b.in.terminatorDelivered = true
		// Count the terminator SYN — it is a real adapter byte
		// enqueued while the gateway owned the bus, AM-NEW-41 spec.
		m.activeTxn.bytesDeliveredToActive.Add(1)
	default:
		m.activeTxn.terminatorDropOnFullCh.Add(1)
		m.logger.Printf("adaptermux: active channel full, dropping SYN terminator")
	}
	m.gatewayTxnActive = false
	m.recordGatewayInactive(ReasonSYNTerminator)
	// Codex GitHub-bot P1 review on PR #650: the normal
	// gateway-txn completion path (SYN terminator echo) must
	// cancel the watchdog. Without this, every successful
	// gateway transaction in ModeShadow / ModeEnforce leaves
	// the timer armed; a subsequent quiet period (no new
	// writes to re-arm) fires the soft/hard deadline and
	// emits a false-positive admin event for a transaction
	// that already completed successfully. Mirror of the
	// SYN-timeout / SYN-idle cancel signaling — set the flag,
	// caller cancels after stateMu unlock.
	b.cancelGatewayWatchdog = true

	// Note: external session echo tracker flush is done AFTER stateMu.Unlock()
	// by the caller (flushSessionEchoTrackersOnSYN) to avoid ABBA deadlock
	// with doSend which acquires sessionsMu→stateMu.
}

// releaseOnTransactionDoneLocked releases the owner once the wire-phase
// tracker confirmed the structural terminal. Caller holds stateMu.
func (m *Mux) releaseOnTransactionDoneLocked(b *synBoundary) {
	// F-21 (batch-20, 2026-05-14): release ownership when the wire-phase
	// tracker resolves this SYN as wirePhaseEventTransactionDone. That
	// event fires only from advanceWaitTerminalSyn — meaning the tracker
	// was in wirePhaseWaitTerminalSyn before this byte, which means the
	// PREVIOUS observation was a structural terminal (broadcast ACK,
	// i2i ACK, response success ACK, response double-NACK, CMD double-
	// NACK). Pre-F-21 the release fired at that previous-byte position
	// via the non-SYN release block (mux.go release for
	// wirePhaseEventTransactionDone / wirePhaseEventCmdNACK), which
	// dropped ownership before external sessions could forward their
	// trailing-SYN ENH_REQ_SEND through session.handleSend's owner
	// check. F-21 defers the release one wire byte: by the time we
	// observe the trailing SYN echo here, ebusd's terminal SYN send
	// has already passed handleSend (ownership was still with the
	// external session) and has been forwarded to the adapter.
	//
	// No grace period — the tracker already confirmed the structural
	// terminal, so the SYN here is unambiguous. This branch is
	// orthogonal to the SYNTimeout / SYNIdle paths below (they check
	// different phaseEvent values).
	//
	// Gateway session 0: the gateway-owned SYN takes the SYNIdle
	// branch (set above at the start of onReceived for gateway-owned
	// bus); the tracker never enters WaitTerminalSyn under gateway
	// ownership, so this branch never fires for the gateway and the
	// existing SYNIdle / IdleReleaseGrace contract is preserved.
	b.in.txnActive = m.gatewayTxnActive
	d := m.decideSYNBoundaryLocked(b, synBoundaryEvent)
	if d.release != synReleaseTransactionDone {
		return
	}
	m.logger.Printf("adaptermux: ownership released for session %d remote=%s (terminal SYN)",
		b.ownerID, m.sessionRemoteAddrOrUnknown(b.ownerID))
	m.arb.releaseOwnership(b.ownerID)
	if d.endTxn != ReasonNone {
		m.gatewayTxnActive = false
		m.recordGatewayInactive(d.endTxn)
	}
	if d.stepCompleted {
		m.stepCompletedLocked()
	}
	b.hasOwner = false
}

// releaseOnSYNTimeoutLocked releases the owner on a SYN during a wait phase:
// the gateway at once, an external session only past its SYN grace. Caller
// holds stateMu.
func (m *Mux) releaseOnSYNTimeoutLocked(b *synBoundary) {
	// Measure the actual inter-byte gap, not time since
	// grant. m.lastWireActivity is bumped by every non-SYN
	// adapter byte while ownership is held, so an external
	// transaction with live wire traffic resets the clock
	// each time. A 5-second-long ebusd scan with regular
	// inter-responder bytes will never trip this branch
	// based on grant age alone. (Codex P2 round-1 on PR
	// #621.)
	b.in.wireGap = time.Since(m.lastWireActivity)
	b.in.sessionGrace = m.cfg.ExternalSessionSYNGrace
	b.in.txnActive = m.gatewayTxnActive
	d := m.decideSYNBoundaryLocked(b, synBoundaryEvent)
	if d.release != synReleaseTimeout {
		return
	}
	if d.logGap {
		m.logger.Printf("adaptermux: external session %d remote=%s SYN-timeout idle-gap %v ≥ grace %v — releasing",
			b.ownerID, m.sessionRemoteAddrOrUnknown(b.ownerID), b.in.wireGap, m.cfg.ExternalSessionSYNGrace)
	}
	m.logger.Printf("adaptermux: ownership released for session %d remote=%s (SYN timeout)",
		b.ownerID, m.sessionRemoteAddrOrUnknown(b.ownerID))
	m.arb.releaseOwnership(b.ownerID)
	if d.endTxn != ReasonNone {
		m.gatewayTxnActive = false
		m.recordGatewayInactive(d.endTxn)
		// Codex round-3 MUST FIX #1 on PR #647: signal
		// the caller (onReceived SYN branch at line ~2035)
		// to cancel the gateway echo watchdog after
		// releasing stateMu. The teardown path released
		// gateway ownership; an armed watchdog must not
		// emit a stale soft/hard event for the lost txn.
		if d.cancelWatchdog {
			b.cancelGatewayWatchdog = true
		}
	}
}

// releaseOnIdleSYNLocked releases the owner on an idle SYN after its grace,
// or the gateway at once while an external START is pending, unless the SYN
// is mid-frame noise of a live gateway transaction. An idle SYN past the
// grace is presented as the idle-grace event, whose validity and teardown
// the transition function decides. Caller holds stateMu.
func (m *Mux) releaseOnIdleSYNLocked(b *synBoundary) {
	b.in.grantAge = time.Since(m.busOwned)
	b.in.idleGrace = m.cfg.IdleReleaseGrace
	b.in.txnActive = m.gatewayTxnActive
	b.in.delivered = m.activeTxn.bytesDeliveredToActive.Load() > 0
	if b.ownerID == gatewaySessionID {
		b.in.idleElapsed = time.Since(m.busOwned)
	} else {
		b.in.idleElapsed = time.Since(m.lastWireActivity)
	}
	d := m.decideSYNBoundaryLocked(b, synBoundaryEvent)
	if d.askExternalPending {
		b.in.externalPendingRead = true
		b.in.externalPending = m.arb.hasExternalPending()
		d = m.decideSYNBoundaryLocked(b, synBoundaryEvent)
	}
	if d.release != synReleaseIdleGrace {
		return
	}
	release := m.decideSYNBoundaryLocked(b, synBoundaryIdleGraceEvent)
	if release.release != synReleaseIdleGrace {
		return
	}
	if d.externalBypass {
		m.logger.Printf("adaptermux: ownership released for session %d remote=%s (external START pending: elapsed %v skipping grace %v)",
			b.ownerID, m.sessionRemoteAddrOrUnknown(b.ownerID), b.in.idleElapsed, d.graceUsed)
	} else {
		m.logger.Printf("adaptermux: ownership released for session %d remote=%s (idle grace expired: %v ≥ %v)",
			b.ownerID, m.sessionRemoteAddrOrUnknown(b.ownerID), b.in.idleElapsed, d.graceUsed)
	}
	m.arb.releaseOwnership(b.ownerID)
	// Codex: clear gatewayTxnActive here too — the "SYN-before-read"
	// guard above only clears when bytesRead > 0, so an abandoned
	// grant (no write, no read) keeps the flag true until idle
	// grace releases ownership. Without this clear, ownership is
	// gone but activePathExpectsBytes() still returns true,
	// routing third-party traffic into activeCh indefinitely.
	if release.endTxn != ReasonNone {
		// batch-26 round-7 (Codex r2 defect 3 — MEDIUM): clear
		// the inter-write sentinel along with txn teardown. The
		// sentinel was set by matchEcho consuming a non-SYN
		// echo, and would otherwise persist into the next grant
		// because it is only cleared by recordSent /
		// markRequestStart / flushOnSYN. Idle release is the
		// fourth bounding event in spirit — once we tore down
		// the txn due to IdleReleaseGrace, the inter-write
		// window is over by definition.
		if release.clearSentinel {
			m.gatewayEcho.ClearQueueJustDrained()
		}
		m.gatewayTxnActive = false
		m.recordGatewayInactive(release.endTxn)
		// Codex round-3 MUST FIX #1 on PR #647: same as
		// SYN timeout above — flag the caller to cancel
		// the watchdog after stateMu unlock so a stale
		// timer fire cannot emit an admin event for the
		// torn-down txn.
		if release.cancelWatchdog {
			b.cancelGatewayWatchdog = true
		}
	}
}

// suppressPreEchoSYNLocked is the pre-echo step: the SYN goes to the passive
// path and a SYN that cannot be a terminator is counted as withheld from the
// active path. Caller holds stateMu.
func (m *Mux) suppressPreEchoSYNLocked(b *synBoundary) {
	// SYN is always visible on passive path.
	b.passiveEvents = append(b.passiveEvents, PassiveEvent{
		Kind: PassiveEventSymbol, Symbol: protocol.SymbolSyn, ObservedAt: b.now,
	})

	b.in.txnActive = m.gatewayTxnActive
	b.in.delivered = m.activeTxn.bytesDeliveredToActive.Load() > 0
	d := m.decideSYNBoundaryLocked(b, synBoundaryEvent)
	b.preEchoSuppressed = d.preEchoSuppressed
	if d.preEchoSuppressed {
		m.activeTxn.synSuppressedPreEcho.Add(1)
	}
	if d.betweenWritesSuppressed {
		m.activeTxn.synSuppressedBetweenWrites.Add(1)
	}
}

// recordSYNBoundaryDiagLocked is the diagnostics step: the forensic
// populations, the transport window check and the SYN ring entry. Caller
// holds stateMu.
func (m *Mux) recordSYNBoundaryDiagLocked(b *synBoundary) {
	b.in.txnActive = m.gatewayTxnActive
	d := m.decideSYNBoundaryLocked(b, synBoundaryEvent)
	if d.grantWindowSYN {
		m.activeTxn.synSeenDuringGrantWindow.Add(1)
	}
	if d.interWriteEmptySYN {
		m.activeTxn.synSeenWhileInterWriteEmpty.Add(1)
	}
	if d.checkTransportWindow {
		// batch-22 round-3 Attack 2 observation: orthogonal to the
		// gwActiveBefore / interWriteEmpty branches above (this
		// counter can fire alongside either, OR independently if
		// neither). Increment when the upstream ENH transport's
		// postGrantPreEcho window has closed via expiry in the
		// current gateway transaction. Compares the lifetime
		// counter against the per-txn baseline captured at
		// recordGatewayGrant. Forensic only — no behavior change.
		//
		// Use postGrantWindowExpiredCountSafe() which acquires
		// connMu internally — m.upstream is guarded by connMu and
		// can be replaced concurrently by reconnect; reading it
		// under stateMu alone would race (Codex pre-push review).
		current, hasReporter := m.postGrantWindowExpiredCountSafe()
		if hasReporter && current > m.activeTxn.transportExpiredAtTxnStart.Load() {
			m.activeTxn.synSeenAfterTransportWindowExpired.Add(1)
		}
	}
	if d.recordDiag {
		m.recordSynDiagLocked(b.ownerID, b.in.txnActiveBefore, b.in.txnActive, d.synDelivered)
	}
}
