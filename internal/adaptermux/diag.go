package adaptermux

import (
	"sync/atomic"
	"time"
)

// ActiveTxnInactiveReason identifies why a gateway active transaction
// transitioned to inactive. Bounded set of reasons — used for runtime
// diagnostics and regression tests.
type ActiveTxnInactiveReason string

const (
	ReasonNone              ActiveTxnInactiveReason = ""
	ReasonTransactionDone   ActiveTxnInactiveReason = "transaction_done"
	ReasonCmdNACK           ActiveTxnInactiveReason = "cmd_nack"
	ReasonSYNIdle           ActiveTxnInactiveReason = "syn_idle"
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
	id            uint64    // monotonic transaction id (set at grant)
	initiator     byte      // source/initiator byte for this grant
	grantedAt     time.Time // timestamp of the grant
	inactiveAt    time.Time // timestamp of the inactive transition
	inactiveReas  ActiveTxnInactiveReason
	bytesWritten  atomic.Uint64 // incremented by activeTransport.Write on success
	bytesRead     atomic.Uint64 // incremented by activeTransport.ReadByte/ReadEvent success
	drainedOnGrant int          // count of stale bytes drained just before this grant

	// totals across the mux lifetime (never reset)
	grantsTotal    atomic.Uint64
	writeErrTotal  atomic.Uint64
	readTimeoutTot atomic.Uint64
	// afterInactive counts any active delivery attempts observed after
	// the current transaction was marked inactive (should be zero under
	// the lifecycle-correct policy; non-zero indicates a regression).
	afterInactive atomic.Uint64
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
}

// ActiveTxnSnapshot returns a copy of the current active-transaction
// diagnostics. Safe to call from any goroutine.
func (m *Mux) ActiveTxnSnapshot() ActiveTxnSnapshot {
	m.stateMu.Lock()
	snap := ActiveTxnSnapshot{
		ID:             m.activeTxn.id,
		Initiator:      m.activeTxn.initiator,
		GrantedAt:      m.activeTxn.grantedAt,
		InactiveAt:     m.activeTxn.inactiveAt,
		InactiveReason: m.activeTxn.inactiveReas,
		DrainedOnGrant: m.activeTxn.drainedOnGrant,
		Active:         m.gatewayTxnActive,
	}
	m.stateMu.Unlock()
	// atomic counters read lock-free
	snap.BytesWritten = m.activeTxn.bytesWritten.Load()
	snap.BytesRead = m.activeTxn.bytesRead.Load()
	snap.GrantsTotal = m.activeTxn.grantsTotal.Load()
	snap.WriteErrTotal = m.activeTxn.writeErrTotal.Load()
	snap.ReadTimeoutTot = m.activeTxn.readTimeoutTot.Load()
	snap.AfterInactive = m.activeTxn.afterInactive.Load()
	return snap
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
	m.activeTxn.drainedOnGrant = drained
	m.activeTxn.grantsTotal.Add(1)
	m.logger.Printf(
		"adaptermux: activeTxn grant id=%d initiator=0x%02X drained=%d",
		m.activeTxn.id, initiator, drained,
	)
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
	m.logger.Printf(
		"adaptermux: activeTxn inactive id=%d reason=%s writes=%d reads=%d dur=%s",
		m.activeTxn.id, reason,
		m.activeTxn.bytesWritten.Load(), m.activeTxn.bytesRead.Load(),
		time.Since(m.activeTxn.grantedAt),
	)
}
