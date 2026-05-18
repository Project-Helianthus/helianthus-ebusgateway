package adaptermux

import (
	"sync/atomic"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// TxnClass is the terminal classification of a gateway-owned transaction.
// Computed at inactive-time from write/read prefix capture and lifecycle
// counters. Bounded set — used for runtime diagnostics and test assertions.
type TxnClass string

const (
	TxnClassUnknown             TxnClass = ""
	TxnClassEchoOnlyTimeout     TxnClass = "echo_only_timeout"
	TxnClassNonEchoInvalidFrame TxnClass = "non_echo_invalid_frame"
	TxnClassCandidateNoParse    TxnClass = "candidate_no_parse"
	TxnClassSchemaError         TxnClass = "schema_error"
	TxnClassSuccessLike         TxnClass = "success_like"
)

// txnPrefixCap bounds how many leading bytes of write and read traffic are
// captured per gateway transaction. 8 is enough to characterize classification
// (source + QQ + PB + SB + NN + two data + ACK-like) without unbounded memory.
const txnPrefixCap = 8

// ActiveTxnInactiveReason identifies why a gateway active transaction
// transitioned to inactive. Bounded set of reasons — used for runtime
// diagnostics and regression tests.
type ActiveTxnInactiveReason string

const (
	ReasonNone            ActiveTxnInactiveReason = ""
	ReasonTransactionDone ActiveTxnInactiveReason = "transaction_done"
	ReasonCmdNACK         ActiveTxnInactiveReason = "cmd_nack"
	ReasonSYNIdle         ActiveTxnInactiveReason = "syn_idle"
	// ReasonSYNTerminator is recorded when a SYN arrives mid-transaction
	// (bytesRead>0) and is treated as the legitimate frame terminator.
	// Distinguished from ReasonSYNIdle (which historically covered both
	// terminator and abandoned-grant cases) because, for a legitimate
	// terminator, the SYN byte IS delivered to activeCh so the bus.Send
	// consumer observes the terminator. See onSYNLocked.
	ReasonSYNTerminator     ActiveTxnInactiveReason = "syn_terminator"
	ReasonSYNTimeout        ActiveTxnInactiveReason = "syn_timeout"
	ReasonMaxOwnership      ActiveTxnInactiveReason = "max_ownership"
	ReasonReset             ActiveTxnInactiveReason = "reset"
	ReasonReconnect         ActiveTxnInactiveReason = "reconnect"
	ReasonActiveWriteError  ActiveTxnInactiveReason = "active_write_error"
	ReasonActiveReadTimeout ActiveTxnInactiveReason = "active_read_timeout"
	ReasonContextCancel     ActiveTxnInactiveReason = "context_cancel"
)

// activeTxnDiag captures per-transaction diagnostic data for a gateway
// active transaction. Protected by stateMu when mutated via the mux.
// Counters in ActiveTxnSnapshot are atomic to permit lock-free reads
// from the hot path (active_path.go Write/Read).
type activeTxnDiag struct {
	id           uint64    // monotonic transaction id (set at grant)
	initiator    byte      // source/initiator byte for this grant
	grantedAt    time.Time // timestamp of the grant
	inactiveAt   time.Time // timestamp of the inactive transition
	inactiveReas ActiveTxnInactiveReason
	bytesWritten atomic.Uint64 // incremented by activeTransport.Write on success
	bytesRead    atomic.Uint64 // incremented by activeTransport.ReadByte/ReadEvent success
	// bytesDeliveredToActive is the precise "at least one real adapter byte
	// has been enqueued on activeCh during this gateway-owned txn" signal.
	// Incremented by the readLoop byte-delivery path AFTER a successful
	// non-blocking send to activeCh while gatewayTxnActive is true. Unlike
	// bytesRead (lags — consumer side) and bytesWritten (leads — initiator
	// side before echo returns), this is the correct gate for "the pre-echo
	// window has ended". Used by onSYNLocked to decide whether a SYN is a
	// legitimate frame terminator or pre-echo idle-buffer noise. Codex PR
	// #502 P1 — superseded bytesWritten as the terminator/suppression gate.
	bytesDeliveredToActive atomic.Uint64
	drainedOnGrant         int // count of stale bytes drained just before this grant

	// totals across the mux lifetime (never reset)
	grantsTotal    atomic.Uint64
	writeErrTotal  atomic.Uint64
	readTimeoutTot atomic.Uint64
	// afterInactive counts any active delivery attempts observed after
	// the current transaction was marked inactive (should be zero under
	// the lifecycle-correct policy; non-zero indicates a regression).
	afterInactive atomic.Uint64
	// terminatorDropOnFullCh counts the number of SYN terminators that
	// the onSYNLocked path tried to deliver to activeCh but could not
	// because the channel was full. Expected to be zero in normal
	// operation (activeCh has capacity 4096 and the reader drains it
	// promptly during bus.Send). Non-zero indicates runtime backpressure.
	terminatorDropOnFullCh atomic.Uint64

	// interWriteDrainTotal counts bytes drained from activeCh in
	// sendLoop's pre-recordSent critical section (P12). Each drained
	// byte was a stale entry sitting in activeCh during the inter-write
	// queue-empty window — i.e., between matchEcho consuming byte K's
	// echo and sendLoop arming byte K+1. Without the drain those bytes
	// would have been read by bus.Send.sendRawWithEcho on the next
	// write and fired echo_mismatch (post_grant_ack on stale 0x00,
	// pre_echo_syn on stale 0xAA). Non-zero is normal under busy bus
	// load; persistent zero post-deploy means the upstream pipeline
	// is fully drained between writes (clean).
	interWriteDrainTotal atomic.Uint64

	// synSuppressedPreEcho counts SYN bytes that arrived during gateway
	// ownership with gatewayTxnActive=true and were classified as
	// non-terminator noise (NOT a legitimate frame-end SYN echo). These
	// SYNs MUST NOT be delivered to activeCh — doing so would race the
	// real echo byte and cause sendRawWithEcho to observe SYN (0xAA)
	// in place of the expected echo, emitting echo_mismatch.
	//
	// P10.2 — the gate now uses `gatewayEcho.peekNextExpected()` to
	// classify SYNs that arrive during an active gateway txn:
	//   - hasPending=true && nextExpected==SymbolSyn → legitimate
	//     terminator (bus.Send wrote SYN, awaiting its echo); NOT
	//     suppressed.
	//   - hasPending=true && nextExpected!=SymbolSyn → mid-write
	//     noise (gateway awaiting echo of a non-SYN body byte);
	//     SUPPRESSED.
	//   - hasPending=false (queue empty): the gate is OPEN —
	//     `midWriteSyn=false`, so the terminator branch fires per
	//     the legacy PR #502 contract whenever
	//     bytesDeliveredToActive>0 (the gateway has consumed all its
	//     echoes — typical end-of-frame shape, including tests that
	//     omit the explicit terminator-write step). The pre-first-
	//     echo branch (separate gate, bytesDeliveredToActive==0)
	//     suppresses queue-empty SYNs that arrive before the first
	//     adapter byte has been delivered to activeCh.
	//
	// Original semantics (P0): pre-first-echo case where
	// bytesDeliveredToActive==0. P10.2 generalizes — that case still
	// suppresses (no echoes yet, so any pending entry is non-SYN by
	// construction in normal request flow), but additionally suppresses
	// late mid-frame SYN races and response-phase noise. 671 pre_echo_syn
	// events observed pre-P10.2 (~16% of all echo_mismatch); expected to
	// drop to floor (single-digit per hour) after deploy.
	//
	// Expected to be non-zero under normal adapter-direct operation
	// (typically 1–3 suppressed SYNs per transaction).
	synSuppressedPreEcho atomic.Uint64

	// batch-21 diagnostic counters — disambiguate three competing
	// hypotheses about residual `pre_echo_syn` echo_mismatch events
	// (~11/min as of 2026-05-15). Forensic only; no behavior change.
	// Reference: _work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-15-batch21.md.
	//
	// synSeenDuringGrantWindow counts SYN observations where the gateway
	// is the owner but gatewayTxnActive is still false (the brief
	// window between completeArbitrationGrant and the first
	// recordSent). preEchoMidFrameSuppress requires gatewayTxnActive=
	// true, so any SYN in this window bypasses the suppression. If this
	// counter dominates the residual echo_mismatch rate, Attack 1
	// (gatewayTxnActive grant-lifecycle gap) is confirmed.
	synSeenDuringGrantWindow atomic.Uint64

	// synSeenWhileInterWriteEmpty counts SYN observations where the
	// gateway owns the bus, gatewayTxnActive is true, the echo queue
	// is empty (peekNextExpected returns hasPending=false) AND we've
	// already delivered at least one byte to active. P10.2's peek-based
	// gate requires hasPendingEcho=true to suppress; the brief inter-
	// write window between echo K being consumed and recordSent(K+1)
	// arming the next expected echo leaves the queue empty. If this
	// counter dominates, Attack 3 (queue-empty inter-write window) is
	// confirmed.
	synSeenWhileInterWriteEmpty atomic.Uint64

	// synSeenAfterTransportWindowExpired (batch-22 round-3 Attack 2
	// instrumentation, 2026-05-17) counts SYN observations where
	// the upstream ENH transport's postGrantPreEcho window has
	// closed via DEADLINE EXPIRY in the current gateway transaction
	// (vs. closed normally via first-real-echo arrival). The check
	// compares transport.PostGrantWindowExpiredCount() against
	// `transportExpiredAtTxnStart` (captured at recordGatewayGrant)
	// so the delta reflects only THIS transaction's expiry events.
	//
	// Per batch-22: events here are leak candidates beyond what
	// Attack 1 (grant-window race) and Attack 3 (inter-write empty)
	// instrumentation surfaced — they target the ~54% of the
	// pre_echo_syn residual unaccounted-for after round-2.
	//
	// Implemented via the helianthus-ebusgo
	// transport.PostGrantWindowExpiredReporter optional interface;
	// the gateway type-asserts m.upstream against it. Transports
	// that don't implement the interface (plain TCP/UDP, ebusd_tcp)
	// leave the counter at zero — diagnostic degrades cleanly.
	synSeenAfterTransportWindowExpired atomic.Uint64

	// synSuppressedBetweenWrites (batch-26 round-7) counts wire SYNs
	// that the betweenWritesSyn branch of preEchoMidFrameSuppress
	// suppressed. The branch fires when the gateway echo tracker
	// reports queueJustDrained=true AND the expected-echo queue is
	// currently empty — i.e., the brief window between matchEcho
	// consuming echo K of a non-SYN body byte and sendLoop's
	// recordSent arming byte K+1.
	//
	// Forensic semantics: synSuppressedPreEcho is the SUM of all
	// suppression paths (pre-first-echo + midWriteSyn +
	// betweenWritesSyn); this counter is the subset specifically
	// attributable to the Attack 3 (inter-write empty-queue) fix.
	// Operator dashboards: subtract this from synSeenWhileInterWriteEmpty
	// to get the residual that was NOT closed by round-7.
	synSuppressedBetweenWrites atomic.Uint64

	// transportExpiredAtTxnStart is the snapshot of
	// transport.PostGrantWindowExpiredCount() taken at the moment
	// the current gateway transaction was granted (in
	// recordGatewayGrant). Distinguishes "the window expired in
	// THIS txn" from "the window has expired in some past txn."
	// Atomic so the SYN-observation site (under stateMu) and the
	// txn-start site can both interact lock-free.
	transportExpiredAtTxnStart atomic.Uint64

	// --- Transaction-shape diagnostics (bounded) ---
	// Captured under stateMu via recordWritePrefix/recordReadPrefix.
	writePrefix    [txnPrefixCap]byte
	writePrefixLen int
	readPrefix     [txnPrefixCap]byte
	readPrefixLen  int
	echoCursor     int
	// Byte-class counters (atomic; hot-path increments in onReceived
	// and Write paths).
	echoLike   atomic.Uint64
	nonEcho    atomic.Uint64
	synMarkers atomic.Uint64
	// Terminal classification computed at recordGatewayInactive time.
	txnClass TxnClass
	// lastClass persists the most recently classified txn's class so it
	// can be surfaced through ActiveTxnSnapshot/LastTxnClass even after
	// recordGatewayGrant resets txnClass for the next grant.
	lastClass TxnClass
}

// ActiveTxnSnapshot is an immutable snapshot of the current (or last)
// gateway active transaction, suitable for logging and test assertions.
type ActiveTxnSnapshot struct {
	ID             uint64
	Initiator      byte
	GrantedAt      time.Time
	InactiveAt     time.Time
	InactiveReason ActiveTxnInactiveReason
	BytesWritten   uint64
	BytesRead      uint64
	DrainedOnGrant int
	Active         bool // whether txn is currently active (gatewayTxnActive)
	GrantsTotal    uint64
	WriteErrTotal  uint64
	ReadTimeoutTot uint64
	AfterInactive  uint64
	// TerminatorDropOnFullCh mirrors activeTxnDiag.terminatorDropOnFullCh.
	TerminatorDropOnFullCh uint64
	// SynSuppressedPreEcho mirrors activeTxnDiag.synSuppressedPreEcho.
	SynSuppressedPreEcho uint64
	// SynSeenDuringGrantWindow mirrors
	// activeTxnDiag.synSeenDuringGrantWindow (batch-21 diagnostic).
	// Counts SYNs observed while gateway owned bus but
	// gatewayTxnActive=false. Operator measures rate vs echo_mismatch
	// to identify Attack 1 dominance.
	SynSeenDuringGrantWindow uint64
	// SynSeenWhileInterWriteEmpty mirrors
	// activeTxnDiag.synSeenWhileInterWriteEmpty (batch-21 diagnostic).
	// Counts SYNs observed during gateway-owned active txn while the
	// echo queue is empty AND at least one byte has been delivered.
	// Operator measures rate vs echo_mismatch to identify Attack 3
	// dominance.
	SynSeenWhileInterWriteEmpty uint64
	// SynSeenAfterTransportWindowExpired mirrors
	// activeTxnDiag.synSeenAfterTransportWindowExpired (batch-22
	// round-3 Attack 2 instrumentation). Counts SYNs observed while
	// gateway owned bus AND the upstream transport's postGrantPreEcho
	// window has already closed via deadline-expiry in the current
	// transaction. Targets the ~54% pre_echo_syn residual unexplained
	// by Attack 1 + Attack 3 instrumentation.
	SynSeenAfterTransportWindowExpired uint64
	// SynSuppressedBetweenWrites mirrors
	// activeTxnDiag.synSuppressedBetweenWrites (batch-26 round-7).
	// Counts wire SYNs the betweenWritesSyn (queueJustDrained) gate
	// suppressed — the Attack 3 (inter-write empty-queue) fix.
	// Subset of SynSuppressedPreEcho.
	SynSuppressedBetweenWrites uint64
	// InterWriteDrainTotal mirrors activeTxnDiag.interWriteDrainTotal
	// (P12). Bytes drained from activeCh in sendLoop's pre-recordSent
	// section. Non-zero is normal; persistent zero indicates the
	// upstream pipeline is fully drained between writes.
	InterWriteDrainTotal uint64
	// EchoQueueOverflowResets mirrors gatewayEcho.totalOverflowResets
	// (P10.2.1). Non-zero indicates the gateway-write loop produced
	// more than 256 unacknowledged writes, triggering a queue reset.
	// Real eBUS frames are <30 bytes; reaching 256 implies a
	// pathological condition (TCP backpressure, paused readLoop,
	// runaway write loop) and warrants operator investigation.
	EchoQueueOverflowResets uint64
	// BytesDeliveredToActive mirrors activeTxnDiag.bytesDeliveredToActive.
	BytesDeliveredToActive uint64
	// AbsorbResetTotal mirrors Mux.absorbResetTotal. F-22
	// (batch-19, 2026-05-13): counts every fail-safe firing of
	// armPendingStartAbsorbLocked where the absorb counter was
	// reset to zero because the expected stale STARTED/FAILED
	// never arrived. Replaces the prior transport-reconnect side
	// effect: closing the upstream ENH connection on each timeout
	// severed external sessions (ebusd) and triggered cascade
	// RequestStart failures (batch-19 measured 13 reconnects /
	// 90 min producing 263 cascade failures). Non-zero is
	// expected on a busy bus; a sustained climb on an otherwise
	// quiet bus warrants investigation.
	AbsorbResetTotal uint64

	// Transaction-shape diagnostics (bounded).
	WritePrefix []byte
	ReadPrefix  []byte
	EchoLike    uint64
	NonEcho     uint64
	SynMarkers  uint64
	// TxnClass is the terminal classification of the current transaction
	// if inactive; otherwise TxnClassUnknown. LastTxnClass carries the
	// most recent classified class across grants.
	TxnClass     TxnClass
	LastTxnClass TxnClass
}

// ActiveTxnSnapshot returns a copy of the current active-transaction
// diagnostics. Safe to call from any goroutine.
//
// Codex-R9: atomic counters are loaded UNDER stateMu to keep the
// snapshot in the same transaction epoch as the struct fields.
// Without this, recordGatewayGrant (which resets bytesWritten/bytesRead
// under stateMu before the next caller returns) could interleave
// between the struct copy and the atomic loads, producing mixed-epoch
// results (e.g. ID from txn N with bytesRead from txn N+1).
func (m *Mux) ActiveTxnSnapshot() ActiveTxnSnapshot {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	wp := make([]byte, m.activeTxn.writePrefixLen)
	copy(wp, m.activeTxn.writePrefix[:m.activeTxn.writePrefixLen])
	rp := make([]byte, m.activeTxn.readPrefixLen)
	copy(rp, m.activeTxn.readPrefix[:m.activeTxn.readPrefixLen])
	return ActiveTxnSnapshot{
		ID:                          m.activeTxn.id,
		Initiator:                   m.activeTxn.initiator,
		GrantedAt:                   m.activeTxn.grantedAt,
		InactiveAt:                  m.activeTxn.inactiveAt,
		InactiveReason:              m.activeTxn.inactiveReas,
		DrainedOnGrant:              m.activeTxn.drainedOnGrant,
		Active:                      m.gatewayTxnActive,
		BytesWritten:                m.activeTxn.bytesWritten.Load(),
		BytesRead:                   m.activeTxn.bytesRead.Load(),
		GrantsTotal:                 m.activeTxn.grantsTotal.Load(),
		WriteErrTotal:               m.activeTxn.writeErrTotal.Load(),
		ReadTimeoutTot:              m.activeTxn.readTimeoutTot.Load(),
		AfterInactive:               m.activeTxn.afterInactive.Load(),
		TerminatorDropOnFullCh:      m.activeTxn.terminatorDropOnFullCh.Load(),
		SynSuppressedPreEcho:               m.activeTxn.synSuppressedPreEcho.Load(),
		SynSeenDuringGrantWindow:           m.activeTxn.synSeenDuringGrantWindow.Load(),
		SynSeenWhileInterWriteEmpty:        m.activeTxn.synSeenWhileInterWriteEmpty.Load(),
		SynSeenAfterTransportWindowExpired: m.activeTxn.synSeenAfterTransportWindowExpired.Load(),
		SynSuppressedBetweenWrites:         m.activeTxn.synSuppressedBetweenWrites.Load(),
		InterWriteDrainTotal:        m.activeTxn.interWriteDrainTotal.Load(),
		EchoQueueOverflowResets:     m.gatewayEcho.overflowResets(),
		BytesDeliveredToActive:      m.activeTxn.bytesDeliveredToActive.Load(),
		AbsorbResetTotal:            m.absorbResetTotal.Load(),
		WritePrefix:                 wp,
		ReadPrefix:                  rp,
		EchoLike:                    m.activeTxn.echoLike.Load(),
		NonEcho:                     m.activeTxn.nonEcho.Load(),
		SynMarkers:                  m.activeTxn.synMarkers.Load(),
		TxnClass:                    m.activeTxn.txnClass,
		LastTxnClass:                m.activeTxn.lastClass,
	}
}

// LastTxnClass returns the terminal classification of the most recently
// completed gateway transaction. If no transaction has completed yet, the
// returned value is TxnClassUnknown. Safe to call from any goroutine.
//
// This is the lightweight accessor used by optional
// ActiveTxnClassifier consumers (e.g. statsBus) that only need the final
// class string without the full snapshot.
func (m *Mux) LastTxnClass() string {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	// While a transaction is still active the live txnClass is empty;
	// fall back to the last completed class so diagnostics remain useful.
	if m.activeTxn.txnClass != TxnClassUnknown {
		return string(m.activeTxn.txnClass)
	}
	return string(m.activeTxn.lastClass)
}

// recordGatewayGrant marks the start of a new gateway active transaction.
// Caller must hold stateMu. `drained` is the number of stale bytes drained
// from activeCh as part of the grant.
func (m *Mux) recordGatewayGrant(initiator byte, drained int) {
	m.activeTxn.id++
	m.activeTxn.initiator = initiator
	m.activeTxn.grantedAt = time.Now()
	m.activeTxn.inactiveAt = time.Time{}
	m.activeTxn.inactiveReas = ReasonNone
	m.activeTxn.bytesWritten.Store(0)
	m.activeTxn.bytesRead.Store(0)
	m.activeTxn.bytesDeliveredToActive.Store(0)
	m.activeTxn.drainedOnGrant = drained
	m.activeTxn.grantsTotal.Add(1)
	// batch-22 round-3 Attack 2: snapshot the upstream transport's
	// post-grant-window expiry counter so the SYN observation site
	// can compute the per-txn delta. postGrantWindowExpiredCountSafe
	// acquires connMu internally (m.upstream is connMu-guarded and
	// can be replaced concurrently by reconnect; reading it without
	// connMu would race — Codex pre-push review). Transports that
	// don't implement PostGrantWindowExpiredReporter leave the
	// snapshot at 0 (and the diagnostic counter stays at 0).
	current, _ := m.postGrantWindowExpiredCountSafe()
	m.activeTxn.transportExpiredAtTxnStart.Store(current)
	// Reset per-txn shape diagnostics.
	m.activeTxn.writePrefixLen = 0
	m.activeTxn.readPrefixLen = 0
	m.activeTxn.echoCursor = 0
	m.activeTxn.writePrefix = [txnPrefixCap]byte{}
	m.activeTxn.readPrefix = [txnPrefixCap]byte{}
	m.activeTxn.echoLike.Store(0)
	m.activeTxn.nonEcho.Store(0)
	m.activeTxn.synMarkers.Store(0)
	m.activeTxn.txnClass = TxnClassUnknown
	m.logger.Printf(
		"adaptermux: activeTxn grant id=%d initiator=0x%02X drained=%d",
		m.activeTxn.id, initiator, drained,
	)
}

// markActiveReadTimeout clears gatewayTxnActive with ReasonActiveReadTimeout.
// Called from activeTransport.ReadByte/ReadEvent when the read times out.
// Acquires stateMu internally. Caller must NOT hold stateMu.
func (m *Mux) markActiveReadTimeout() {
	m.stateMu.Lock()
	if m.gatewayTxnActive {
		m.gatewayTxnActive = false
		m.recordGatewayInactive(ReasonActiveReadTimeout)
	}
	m.stateMu.Unlock()
}

// markActiveWriteError clears gatewayTxnActive with ReasonActiveWriteError.
// Called from activeTransport.Write when sendLoop returns an error.
// Acquires stateMu internally. Caller must NOT hold stateMu.
func (m *Mux) markActiveWriteError() {
	m.stateMu.Lock()
	if m.gatewayTxnActive {
		m.gatewayTxnActive = false
		m.recordGatewayInactive(ReasonActiveWriteError)
	}
	m.stateMu.Unlock()
}

// markActiveContextCancel clears gatewayTxnActive with ReasonContextCancel.
// Called from activeTransport Read/Write when ctx.Done() fires.
// Acquires stateMu internally. Caller must NOT hold stateMu.
func (m *Mux) markActiveContextCancel() {
	m.stateMu.Lock()
	if m.gatewayTxnActive {
		m.gatewayTxnActive = false
		m.recordGatewayInactive(ReasonContextCancel)
	}
	m.stateMu.Unlock()
}

// recordGatewayInactive marks the gateway transaction as inactive with
// the given reason. Caller must hold stateMu. Idempotent: a second call
// with a different reason does not override the first (the first reason
// is the truth; subsequent cleanup is not the root cause).
func (m *Mux) recordGatewayInactive(reason ActiveTxnInactiveReason) {
	if m.activeTxn.inactiveReas != ReasonNone {
		return
	}
	m.activeTxn.inactiveAt = time.Now()
	m.activeTxn.inactiveReas = reason
	class := m.classifyTxnLocked(reason)
	m.activeTxn.txnClass = class
	m.activeTxn.lastClass = class
	wp := m.activeTxn.writePrefix[:m.activeTxn.writePrefixLen]
	rp := m.activeTxn.readPrefix[:m.activeTxn.readPrefixLen]
	m.logger.Printf(
		"adaptermux: activeTxn inactive id=%d reason=%s class=%s writes=%d reads=%d echoLike=%d nonEcho=%d synMarkers=%d writePrefix=% X readPrefix=% X dur=%s",
		m.activeTxn.id, reason, class,
		m.activeTxn.bytesWritten.Load(), m.activeTxn.bytesRead.Load(),
		m.activeTxn.echoLike.Load(), m.activeTxn.nonEcho.Load(), m.activeTxn.synMarkers.Load(),
		wp, rp,
		time.Since(m.activeTxn.grantedAt),
	)
}

// recordWritePrefix captures the first txnPrefixCap bytes the gateway
// writes during the current transaction. Caller must hold stateMu.
func (m *Mux) recordWritePrefix(b byte) {
	if m.activeTxn.writePrefixLen < txnPrefixCap {
		m.activeTxn.writePrefix[m.activeTxn.writePrefixLen] = b
		m.activeTxn.writePrefixLen++
	}
}

// recordReadPrefixAndClassify captures the first txnPrefixCap bytes the
// gateway reads during the current transaction and updates byte-class
// counters. Caller must hold stateMu.
func (m *Mux) recordReadPrefixAndClassify(b byte) {
	if b == protocol.SymbolSyn {
		m.activeTxn.synMarkers.Add(1)
	} else if m.activeTxn.echoCursor < m.activeTxn.writePrefixLen &&
		m.activeTxn.writePrefix[m.activeTxn.echoCursor] == b {
		// Echo matching follows the next expected echoed write byte,
		// not the absolute read position. Leading SYN chatter must not
		// permanently misalign a later real echo.
		m.activeTxn.echoLike.Add(1)
		m.activeTxn.echoCursor++
	} else {
		m.activeTxn.nonEcho.Add(1)
		if m.activeTxn.echoCursor < m.activeTxn.writePrefixLen {
			m.activeTxn.echoCursor++
		}
	}
	if m.activeTxn.readPrefixLen < txnPrefixCap {
		m.activeTxn.readPrefix[m.activeTxn.readPrefixLen] = b
		m.activeTxn.readPrefixLen++
	}
}

// classifyTxnLocked computes the terminal TxnClass from captured prefixes
// and counters. Caller must hold stateMu. Pure function modulo struct
// fields — no side effects.
//
// Decision order (first matching wins):
//   - SuccessLike: TransactionDone or CmdNACK reason (lifecycle-complete)
//   - SuccessLike: reads include a SYN marker AND non-echo byte count is
//     positive AND reads >= writes (plausible response + terminator)
//   - EchoOnlyTimeout: no-response shape (including SYN-terminated echo-only
//     traffic) AND reads all matched writes position-wise (no nonEcho bytes seen)
//   - NonEchoInvalidFrame: got nonEcho bytes but no SYN and no success
//     (incoherent bytes on the wire; not a framed response)
//   - CandidateNoParse: got nonEcho bytes AND a SYN but still timed out
//     (looks like a response but upper layer didn't produce a frame)
//   - SchemaError: reserved for payload-schema failure path; decided by
//     caller via classifyTxnWithSchemaError (not computed here)
//   - Unknown: insufficient signal
func (m *Mux) classifyTxnLocked(reason ActiveTxnInactiveReason) TxnClass {
	hasWritePrefix := m.activeTxn.writePrefixLen > 0
	echo := m.activeTxn.echoLike.Load()
	nonEcho := m.activeTxn.nonEcho.Load()
	syns := m.activeTxn.synMarkers.Load()

	// Lifecycle-complete reasons imply the phase tracker observed a
	// full frame or an explicit NACK. Treat as success-like for the
	// purpose of distinguishing from silent timeouts. (CmdNACK is not
	// a "successful" read, but the target DID respond — different from
	// echo-only timeout.)
	if reason == ReasonTransactionDone || reason == ReasonCmdNACK {
		return TxnClassSuccessLike
	}

	isTimeoutLike := reason == ReasonActiveReadTimeout ||
		reason == ReasonSYNTerminator ||
		reason == ReasonSYNIdle ||
		reason == ReasonSYNTimeout ||
		reason == ReasonMaxOwnership ||
		reason == ReasonActiveWriteError ||
		reason == ReasonContextCancel ||
		reason == ReasonReset ||
		reason == ReasonReconnect

	// SuccessLike heuristic: saw non-echo response bytes and a SYN
	// terminator after at least part of the gateway write stream was
	// echoed back. Without an echo signal, stale/foreign traffic can
	// be incorrectly blessed as the gateway's successful transaction.
	if nonEcho > 0 && syns > 0 && echo > 0 && hasWritePrefix {
		return TxnClassSuccessLike
	}

	if !isTimeoutLike {
		return TxnClassUnknown
	}

	// Timeout-like paths past this point.
	if hasWritePrefix && nonEcho == 0 && echo >= 1 {
		// Every read matched the write prefix position-wise: echo-only.
		return TxnClassEchoOnlyTimeout
	}
	if hasWritePrefix && m.activeTxn.readPrefixLen == 0 {
		// Didn't even see our own echo back — treat as echo-only
		// (lowest-confidence class, but still more informative than
		// "unknown" for an operator triaging the soak log).
		return TxnClassEchoOnlyTimeout
	}
	if nonEcho > 0 && syns == 0 {
		return TxnClassNonEchoInvalidFrame
	}
	if nonEcho > 0 && syns > 0 {
		return TxnClassCandidateNoParse
	}
	return TxnClassUnknown
}

// synDiagRingCap bounds the SYN-path diagnostics ring. 16 entries is
// enough to characterize the last ~16 SYN events that arrived while the
// gateway owned the bus (the window where a missed final-SYN echo would
// strand a Send consumer). Bounded — never grows.
const synDiagRingCap = 16

// SynDiagEntry records a single SYN-on-gateway-ownership event. The
// fields answer: at SYN arrival, did the mux consider the txn active?
// What was the last byte we wrote (to correlate with a real response)?
// How many bytes had the active path read already? Did we deliver the
// SYN to activeCh for the Send consumer, or did onSYNLocked consume it
// as an end-of-txn terminator? Which inactive reason (if any) was set?
type SynDiagEntry struct {
	ObservedAt           time.Time
	TxnID                uint64
	OwnerID              uint64
	GatewayOwned         bool
	GwActiveBefore       bool
	GwActiveAfter        bool
	LastWrittenByte      byte
	HasLastWrittenByte   bool
	BytesRead            uint64
	SynDeliveredToActive bool
	InactiveReason       ActiveTxnInactiveReason
}

// synDiagRing is a bounded ring of SynDiagEntry. Wraps once it reaches
// synDiagRingCap — oldest entries are overwritten. Access is guarded by
// Mux.stateMu.
type synDiagRing struct {
	entries [synDiagRingCap]SynDiagEntry
	head    int // next write slot
	count   int // number of valid entries (<=synDiagRingCap)
}

// push appends entry, overwriting the oldest slot once full.
func (r *synDiagRing) push(e SynDiagEntry) {
	r.entries[r.head] = e
	r.head = (r.head + 1) % synDiagRingCap
	if r.count < synDiagRingCap {
		r.count++
	}
}

// snapshot returns a slice of entries in chronological order (oldest
// first). Safe to call with Mux.stateMu held.
func (r *synDiagRing) snapshot() []SynDiagEntry {
	if r.count == 0 {
		return nil
	}
	out := make([]SynDiagEntry, 0, r.count)
	start := (r.head - r.count + synDiagRingCap) % synDiagRingCap
	for i := 0; i < r.count; i++ {
		out = append(out, r.entries[(start+i)%synDiagRingCap])
	}
	return out
}

// SynDiagSnapshot returns a chronological snapshot of recent SYN-on-
// gateway-ownership events. Bounded to synDiagRingCap entries. Safe to
// call from any goroutine.
//
// Used by tests and by runtime soak triage to confirm or exclude the
// hypothesis that the trailing SYN of a gateway transaction is consumed
// by onSYNLocked before the Send consumer sees it as a frame terminator.
func (m *Mux) SynDiagSnapshot() []SynDiagEntry {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.synDiag.snapshot()
}

// ActiveTxnSnapshotForScan returns a txnID-carrying snapshot of the
// current (or most recent) gateway transaction, bounded to the first
// txnPrefixCap write/read bytes and the lightweight class string. This
// is the richer seam used by statsBus when the injected classifier
// implements activeTxnSnapshotter — it lets each scan attempt log carry
// the specific txn ID the bus.Send call actually produced, eliminating
// the attribution race where LastTxnClass() could return a later txn's
// class if a second grant completed between Send return and the log
// write.
//
// Returned prefix slices are freshly allocated copies — safe to hold
// across calls. Safe to call from any goroutine.
func (m *Mux) ActiveTxnSnapshotForScan() (id uint64, writePrefix []byte, readPrefix []byte, class string) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	wp := make([]byte, m.activeTxn.writePrefixLen)
	copy(wp, m.activeTxn.writePrefix[:m.activeTxn.writePrefixLen])
	rp := make([]byte, m.activeTxn.readPrefixLen)
	copy(rp, m.activeTxn.readPrefix[:m.activeTxn.readPrefixLen])
	cls := m.activeTxn.txnClass
	if cls == TxnClassUnknown {
		cls = m.activeTxn.lastClass
	}
	return m.activeTxn.id, wp, rp, string(cls)
}

// recordSynDiagLocked pushes a SYN-on-gateway-ownership entry into the
// ring. Caller must hold stateMu. Only call when gateway owns the bus at
// SYN arrival (the hypothesis-relevant window).
func (m *Mux) recordSynDiagLocked(
	ownerID uint64,
	gwActiveBefore bool,
	gwActiveAfter bool,
	synDelivered bool,
) {
	var last byte
	hasLast := false
	if n := m.activeTxn.writePrefixLen; n > 0 {
		last = m.activeTxn.writePrefix[n-1]
		hasLast = true
	}
	m.synDiag.push(SynDiagEntry{
		ObservedAt:           time.Now(),
		TxnID:                m.activeTxn.id,
		OwnerID:              ownerID,
		GatewayOwned:         true,
		GwActiveBefore:       gwActiveBefore,
		GwActiveAfter:        gwActiveAfter,
		LastWrittenByte:      last,
		HasLastWrittenByte:   hasLast,
		BytesRead:            m.activeTxn.bytesRead.Load(),
		SynDeliveredToActive: synDelivered,
		InactiveReason:       m.activeTxn.inactiveReas,
	})
}

// MarkSchemaError flags the most recent (or current) gateway transaction
// as having failed because the payload did not match the expected
// schema. Called by the gateway-side parse path when a candidate frame
// was received but schema validation failed. Safe to call after the txn
// has already been marked inactive — it overrides the recorded class
// only in that case (schema error is a post-terminal classification
// refinement).
func (m *Mux) MarkSchemaError() {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	// Only promote to SchemaError if the prior class was a plausible
	// response (CandidateNoParse or SuccessLike). Don't overwrite
	// EchoOnlyTimeout — that is physically incompatible with a
	// schema error.
	switch m.activeTxn.txnClass {
	case TxnClassCandidateNoParse, TxnClassSuccessLike, TxnClassUnknown:
		m.activeTxn.txnClass = TxnClassSchemaError
	}
	switch m.activeTxn.lastClass {
	case TxnClassCandidateNoParse, TxnClassSuccessLike, TxnClassUnknown:
		m.activeTxn.lastClass = TxnClassSchemaError
	}
}
