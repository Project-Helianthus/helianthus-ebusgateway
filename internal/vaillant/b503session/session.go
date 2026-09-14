package b503session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// TransportKey identifies a specific adapter incarnation on the bus.
// TransportEpoch advances on every successful transport reconnect so the
// session FSM can detect that its owning incarnation is gone.
type TransportKey struct {
	AdapterInstanceID string
	TransportEpoch    uint64
}

// SessionKey is the (transport_key, issuer_token) tuple returned by
// Enable. A caller holding the full SessionKey may Disable; only the
// transport_key is required for Read (spec §6.2).
type SessionKey struct {
	Transport   TransportKey
	IssuerToken string
}

// RefreshFunc is invoked during epoch-advance handling. It MUST return
// one of:
//   - (newTransportKey, nil)           refresh succeeded; session re-homes.
//   - (TransportKey{}, ErrTransportDown) transport went down during refresh.
//   - (TransportKey{}, other-error)    refresh failed for another reason;
//     session becomes permanently busy
//     until the next Enable.
type RefreshFunc func(ctx context.Context) (TransportKey, error)

// NativeResult is the exact device-side wire outcome relevant to the B503
// lifecycle. ACK and NAK are evidence about the native exchange only; neither
// proves that device-side session state has settled.
type NativeResult uint8

const (
	NativeAmbiguous NativeResult = iota
	NativeACK
	NativeNAK
)

// DispatchOutcome records the conservative wire boundary for one native B503
// operation. Emitted becomes true as soon as bus.Send is entered; it does not
// claim that the target received or accepted the frame.
type DispatchOutcome struct {
	Response  []byte
	Emitted   bool
	Native    NativeResult
	Transport TransportKey
	Err       error
}

// DispatchFunc emits exactly one target-bound native operation.
type DispatchFunc func(context.Context, byte) DispatchOutcome

// CleanupObligationSnapshot is read-only evidence for deterministic tests and
// diagnostics. It carries no caller authority.
type CleanupObligationSnapshot struct {
	Target                  byte
	GatewayCleanupAttemptID string
	TransportEpoch          uint64
	OriginEmitted           bool
	OriginNative            NativeResult
	OriginErr               error
	LastAttemptEpoch        uint64
	LastEmitted             bool
	LastNative              NativeResult
	LastErr                 error
}

type cleanupObligation struct {
	CleanupObligationSnapshot
	sourceOperationID string
}

// OperationKind identifies the native lifecycle operation currently admitted
// by Manager.
type OperationKind uint8

const (
	OperationEnable OperationKind = iota + 1
	OperationRead
	OperationDisable
)

// PendingOperationSnapshot is non-authoritative diagnostic evidence. The
// operation ID is process-local and grants no caller authority.
type PendingOperationSnapshot struct {
	OperationID string
	Kind        OperationKind
	Target      byte
	Transport   TransportKey
	Emitted     bool
}

type pendingOperation struct {
	PendingOperationSnapshot
	token string
}

// Manager is the single-owner live-monitor session FSM.
//
// Two mutexes cooperate:
//
//   - mu (liveMonitorMu): the ownership gate. Acquired on the
//     Idle->Enabling transition; released exactly once on entry to
//     Disabled from a held-owner state (Enabling, Active, or Refreshing).
//     This is distinct from the B524 readMu used by the poller. The
//     lock lifecycle is tracked by mutexHeld so cleanup paths never
//     double-release.
//
//   - stateMu: field-level protection for state, transport, activeToken,
//     idleTimer, and mutexHeld. All public methods acquire stateMu for
//     the duration of their FSM inspection/mutation; they acquire or
//     release mu as the FSM dictates.
//
// This separation avoids the deadlock trap of having the idle-timer
// callback try to re-enter a mutex that is semantically held by a
// different logical agent (the session owner).
type Manager struct {
	mu sync.Mutex // liveMonitorMu — ownership gate, distinct from B524 readMu.

	stateMu      sync.Mutex
	state        State
	transport    TransportKey
	activeToken  string
	idleTimeout  time.Duration
	idleTimer    *time.Timer
	idleTimerGen uint64 // generation counter — stale callback guard
	refresh      RefreshFunc
	mutexHeld    bool
	// refreshFailed sticks to true when a refresh returned a non-nil,
	// non-ErrTransportDown error. Subsequent Reads then surface
	// ErrSessionBusy until ResetForRestart or a new Enable. It is
	// cleared by Enable and ResetForRestart.
	refreshFailed bool
	// lastRefreshTransportDown is set when OnEpochAdvance's refresh returned
	// ErrTransportDown; cleared on Enable / ResetForRestart. See spec §7.1
	// and plan AD14 for the resolver-layer contract this enables.
	lastRefreshTransportDown bool

	activeTarget     byte
	pendingOperation *pendingOperation
	cleanup          *cleanupObligation
	cleanupDispatch  DispatchFunc
	observedEpoch    uint64
	pendingEpoch     uint64
	qualified        map[byte]struct{}
	restarted        bool
	restartFences    map[byte]struct{}
}

// StatusSnapshot is one coherent, public observation of the live-monitor
// session. State and Owned are captured from that same observation.
//
// Callers that render or serialize both values must use StatusSnapshot rather
// than combining State and IsOwned, because a lifecycle transition may occur
// between separate calls to those accessors.
type StatusSnapshot struct {
	State State
	Owned bool
}

// New constructs a Manager bound to the initial transport incarnation.
// idleTimeout is the Active->Disabled auto-disable window (spec §7.6;
// production value 30s). refresh is invoked during epoch-advance handling.
func New(transport TransportKey, idleTimeout time.Duration, refresh RefreshFunc) *Manager {
	return &Manager{
		state:         Idle,
		transport:     transport,
		idleTimeout:   idleTimeout,
		refresh:       refresh,
		observedEpoch: transport.TransportEpoch,
		qualified:     make(map[byte]struct{}),
		restartFences: make(map[byte]struct{}),
	}
}

// SetCleanupDispatcher installs the process-local native disable hook used by
// idle expiry and ambiguous enable cleanup. It does not itself emit I/O.
func (m *Manager) SetCleanupDispatcher(dispatch DispatchFunc) {
	m.stateMu.Lock()
	m.cleanupDispatch = dispatch
	m.stateMu.Unlock()
}

// MarkQualifiedTarget records a finite registry-qualified B503 target. After
// ResetForRestart, newly enumerated qualified targets inherit the restart
// UNKNOWN fence until separately authorized recovery outside public v1.
func (m *Manager) MarkQualifiedTarget(target byte) {
	m.stateMu.Lock()
	m.qualified[target] = struct{}{}
	if m.restarted {
		m.restartFences[target] = struct{}{}
	}
	m.stateMu.Unlock()
}

// TargetBlocked reports whether cleanup or restart recovery makes target
// unavailable. This is deliberately target-specific.
func (m *Manager) TargetBlocked(target byte) bool {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if _, fenced := m.restartFences[target]; fenced {
		return true
	}
	// The cleanup write remains target-bound, but the physical live-monitor
	// slot is single-owner per transport. Any retained cleanup therefore makes
	// every target on that transport unavailable for a new claim.
	return m.cleanup != nil
}

// CleanupObligation returns a copy of the current process-local obligation.
func (m *Manager) CleanupObligation() (CleanupObligationSnapshot, bool) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.cleanup == nil {
		return CleanupObligationSnapshot{}, false
	}
	return m.cleanup.CleanupObligationSnapshot, true
}

// PendingOperation returns the current typed target-bound operation, if any.
func (m *Manager) PendingOperation() (PendingOperationSnapshot, bool) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.pendingOperation == nil {
		return PendingOperationSnapshot{}, false
	}
	return m.pendingOperation.PendingOperationSnapshot, true
}

// DispatchEpoch is the latest transport epoch observed by lifecycle routing.
// Dispatchers compare this value with request-side issue metadata so a stale
// completion cannot be rescued by a still-bound old owner key.
func (m *Manager) DispatchEpoch() uint64 {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.observedEpoch
}

// Enable transitions Idle -> Enabling -> Active and returns a SessionKey
// with a freshly-minted 16-byte hex issuer_token. Returns ErrSessionBusy
// if the FSM is not Idle.
//
// This lower-level transition remains available to tests that isolate the
// owner gate. Production B503 paths use EnableOperation so native ACK/NAK and
// emission state own the transition.
func (m *Manager) Enable(ctx context.Context) (SessionKey, error) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	if m.state != Idle {
		return SessionKey{}, ErrSessionBusy
	}

	// Idle -> Enabling -> Active, atomically from the caller's perspective.
	m.state = Enabling
	// Acquire ownership gate. The mutex is held logically by the session,
	// not by this goroutine; we release it on entry to Disabled.
	m.mu.Lock()
	m.mutexHeld = true

	token, err := newIssuerToken()
	if err != nil {
		// Roll back: release gate, revert state.
		m.mu.Unlock()
		m.mutexHeld = false
		m.state = Idle
		return SessionKey{}, err
	}
	m.activeToken = token
	m.activeTarget = 0
	m.state = Active
	m.refreshFailed = false
	m.lastRefreshTransportDown = false
	m.armIdleTimerLocked()

	return SessionKey{Transport: m.transport, IssuerToken: token}, nil
}

// EnableOperation owns the target-bound enable handshake. A pre-emission
// cancellation returns to Idle with no cleanup. Every emitted outcome creates
// a process-local cleanup obligation because native ACK/NAK does not prove the
// device-side session state settled. An exact ACK may activate the caller
// handle, while every emitted failure releases caller ownership and retains
// cleanup under UNKNOWN.
func (m *Manager) EnableOperation(ctx context.Context, target byte, dispatch DispatchFunc) (SessionKey, error) {
	m.MarkQualifiedTarget(target)
	m.stateMu.Lock()
	if _, fenced := m.restartFences[target]; fenced {
		m.stateMu.Unlock()
		return SessionKey{}, ErrCleanupPending
	}
	if m.cleanup != nil {
		m.stateMu.Unlock()
		return SessionKey{}, ErrCleanupPending
	}
	if m.state != Idle {
		m.stateMu.Unlock()
		return SessionKey{}, ErrSessionBusy
	}
	m.state = Enabling
	m.mu.Lock()
	m.mutexHeld = true
	token, err := newIssuerToken()
	if err != nil {
		m.releaseOwnerLocked()
		m.state = Idle
		m.stateMu.Unlock()
		return SessionKey{}, err
	}
	opID := newCleanupAttemptID()
	pending := &pendingOperation{
		PendingOperationSnapshot: PendingOperationSnapshot{
			OperationID: opID, Kind: OperationEnable, Target: target, Transport: m.transport,
		},
		token: token,
	}
	m.pendingOperation = pending
	m.activeToken = token
	m.activeTarget = target
	m.refreshFailed = false
	m.lastRefreshTransportDown = false
	m.stateMu.Unlock()

	outcome := dispatchOnce(ctx, target, dispatch)

	m.stateMu.Lock()
	if m.pendingOperation != pending {
		m.stateMu.Unlock()
		if outcome.Err != nil {
			return SessionKey{}, outcome.Err
		}
		return SessionKey{}, ErrTransportDown
	}
	pending.Emitted = outcome.Emitted
	m.pendingOperation = nil
	advanced := m.pendingEpoch > pending.Transport.TransportEpoch
	if advanced {
		m.transport.TransportEpoch = m.pendingEpoch
	}
	if m.state != Enabling {
		if !outcome.Emitted {
			m.clearCleanupForOperationLocked(opID)
			if m.cleanup == nil {
				m.state = Idle
			}
		} else if m.cleanup != nil && m.cleanup.sourceOperationID == opID {
			m.cleanup.OriginEmitted = true
			m.cleanup.OriginNative = outcome.Native
			m.cleanup.OriginErr = outcome.Err
		}
		m.stateMu.Unlock()
		if outcome.Err != nil {
			return SessionKey{}, outcome.Err
		}
		return SessionKey{}, ErrTransportDown
	}

	if !outcome.Emitted {
		m.releaseOwnerLocked()
		m.state = Idle
		m.stateMu.Unlock()
		return SessionKey{}, outcome.Err
	}

	// Once the enable enters bus.Send, retain a fresh target/epoch-bound
	// cleanup obligation regardless of ACK, NAK, or ambiguous completion.
	m.createCleanupLocked(target, m.transport.TransportEpoch, opID)
	m.cleanup.OriginEmitted = outcome.Emitted
	m.cleanup.OriginNative = outcome.Native
	m.cleanup.OriginErr = outcome.Err
	if outcome.Native == NativeACK && outcome.Err == nil && !advanced {
		m.state = Active
		m.armIdleTimerLocked()
		key := SessionKey{Transport: m.transport, IssuerToken: token}
		m.stateMu.Unlock()
		return key, nil
	}
	m.releaseOwnerLocked()
	m.state = Disabled
	epoch := m.transport.TransportEpoch
	m.stateMu.Unlock()
	m.attemptCleanup(ctx, epoch)
	if outcome.Err != nil {
		return SessionKey{}, outcome.Err
	}
	return SessionKey{}, ErrTransportDown
}

// ReadOperation validates the target-bound owner, performs at most one epoch
// refresh when required, and dispatches the admitted read exactly once.
func (m *Manager) ReadOperation(ctx context.Context, target byte, dispatch DispatchFunc) ([]byte, error) {
	m.MarkQualifiedTarget(target)
	pending, err := m.beginOwnerOperation(ctx, target, nil, OperationRead)
	if err != nil {
		return nil, err
	}
	outcome := dispatchOnce(ctx, target, dispatch)
	m.stateMu.Lock()
	if m.pendingOperation != pending {
		m.stateMu.Unlock()
		if outcome.Err != nil {
			return nil, outcome.Err
		}
		return nil, ErrTransportDown
	}
	pending.Emitted = outcome.Emitted
	m.pendingOperation = nil
	if outcome.Err != nil {
		if errors.Is(outcome.Err, ErrTransportDown) {
			epoch := m.transport.TransportEpoch
			m.toDisabledLocked()
			m.createCleanupLocked(target, epoch, pending.OperationID)
			m.lastRefreshTransportDown = true
		}
		m.stateMu.Unlock()
		return nil, outcome.Err
	}
	if outcome.Native != NativeACK {
		m.stateMu.Unlock()
		return nil, ErrTransportDown
	}
	if m.state != Active || m.transport != pending.Transport || m.activeTarget != target {
		m.stateMu.Unlock()
		return nil, ErrTransportDown
	}
	m.armIdleTimerLocked()
	m.stateMu.Unlock()
	return outcome.Response, nil
}

// DisableOperation owns the explicit native disable. The owner is released on
// entry to internal Disabled. Its exact native outcome is recorded, but ACK or
// NAK alone never clears the cleanup obligation or makes re-Enable admissible.
func (m *Manager) DisableOperation(ctx context.Context, key SessionKey, target byte, dispatch DispatchFunc) error {
	m.MarkQualifiedTarget(target)
	pending, err := m.beginOwnerOperation(ctx, target, &key, OperationDisable)
	if err != nil {
		return err
	}
	m.stateMu.Lock()
	if m.pendingOperation != pending || m.state != Active || !m.mutexHeld {
		m.stateMu.Unlock()
		return ErrTransportDown
	}
	attemptID := newCleanupAttemptID()
	epoch := m.transport.TransportEpoch
	m.releaseOwnerLocked()
	m.state = Disabled
	m.createCleanupLocked(target, epoch, attemptID)
	attemptID = m.cleanup.GatewayCleanupAttemptID
	m.cleanup.LastAttemptEpoch = epoch
	m.stateMu.Unlock()

	outcome := dispatchOnce(ctx, target, dispatch)
	m.stateMu.Lock()
	if m.pendingOperation == pending {
		pending.Emitted = outcome.Emitted
		m.pendingOperation = nil
	}
	m.stateMu.Unlock()
	m.recordCleanupOutcome(attemptID, epoch, outcome)
	return outcome.Err
}

// Disable transitions Active -> Disabled -> Idle. Requires a full
// SessionKey match. ErrWrongToken for issuer_token mismatch; ErrNotActive
// if the FSM is not currently Active.
//
// This lower-level transition remains available to tests that isolate the
// owner gate. Production paths use DisableOperation so a native disable is
// emitted and evidenced.
func (m *Manager) Disable(key SessionKey) error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	if m.state == Refreshing {
		return ErrSessionBusy
	}
	if m.state != Active {
		return ErrNotActive
	}
	if key.Transport != m.transport {
		return ErrWrongToken
	}
	if key.IssuerToken != m.activeToken {
		return ErrWrongToken
	}
	m.toDisabledLocked()
	m.toIdleLocked()
	return nil
}

// Read checks that the FSM is Active for the provided transport and
// resets the idle timer. issuer_token is intentionally not part of the
// Read contract (spec §6.2).
//
// This lower-level transition remains available to tests that isolate the
// owner gate. Production paths use ReadOperation so refresh and the triggering
// dispatch remain atomic.
func (m *Manager) Read(transport TransportKey) error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	if m.refreshFailed || m.state == Refreshing {
		return ErrSessionBusy
	}
	if m.state != Active {
		return ErrNotActive
	}
	if transport != m.transport {
		return ErrNotActive
	}
	m.armIdleTimerLocked()
	return nil
}

// State returns the public FSM state. Refreshing is visible while an epoch
// refresh holds the gate, so a caller never observes Disabled with ownership.
func (m *Manager) State() State {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.state == Disabled && (m.cleanup != nil || m.restarted) {
		return Idle
	}
	return m.state
}

// IsOwned reports whether the session gate is currently held by any
// owner — true when FSM is Enabling, Active, or Refreshing. Resolver-layer code (e.g.
// capability probe in mcp/vaillant_b503.go) uses this to classify the
// surface as SESSION_BUSY during the full held-owner window, not just
// when State() reports Active/Enabling. Without this, a slow refresh
// could publish AVAILABLE while a concurrent Enable would still return
// ErrSessionBusy.
func (m *Manager) IsOwned() bool {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.mutexHeld
}

// StatusSnapshot returns the public state and ownership gate from one stateMu
// critical section. This is the stable observation used by MCP and
// GraphQL session-status surfaces.
func (m *Manager) StatusSnapshot() StatusSnapshot {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	state := m.state
	if state == Disabled && (m.cleanup != nil || m.restarted) {
		state = Idle
	}
	return StatusSnapshot{
		State: state,
		Owned: m.mutexHeld,
	}
}

// TransportKey returns the manager's current transport key. Exposed so
// resolver-layer callers (mcp/vaillant_b503.go) can pass the correct key to
// Read/Disable after OnEpochAdvance has re-homed the session.
func (m *Manager) TransportKey() TransportKey {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.transport
}

// LastRefreshTransportDown reports whether the most recent OnEpochAdvance
// resolved with ErrTransportDown. Callers use this to distinguish
// transport-down teardown from ordinary Idle state when surfacing a public
// error code (spec §7.1 / plan AD14). Cleared by Enable and ResetForRestart.
func (m *Manager) LastRefreshTransportDown() bool {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.lastRefreshTransportDown
}

// OnTransportDisconnect performs owner-conditional cleanup (spec §7.4).
// If the FSM holds the ownership gate (Enabling/Active/Refreshing), releases
// it and transitions to Disabled -> Idle. Otherwise no-op.
func (m *Manager) OnTransportDisconnect() {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if !m.mutexHeld {
		return
	}
	target := m.activeTarget
	sourceID := ""
	if m.pendingOperation != nil && m.pendingOperation.Kind == OperationEnable {
		target = m.pendingOperation.Target
		sourceID = m.pendingOperation.OperationID
	}
	epoch := m.transport.TransportEpoch
	m.toDisabledLocked()
	m.createCleanupLocked(target, epoch, sourceID)
}

// OnEpochAdvance records the latest transport epoch. A surviving owner is
// rebound only when its next admitted ReadOperation or current-owner
// DisableOperation invokes refreshOwner. With no owner, retained cleanup is
// fenced without emitting any recovery write; reconnect and restart cannot
// establish settlement or reconstruct caller ownership.
func (m *Manager) OnEpochAdvance(_ context.Context, newEpoch uint64) {
	m.stateMu.Lock()
	if newEpoch <= m.observedEpoch {
		m.stateMu.Unlock()
		return
	}
	m.observedEpoch = newEpoch
	m.lastRefreshTransportDown = false
	if m.mutexHeld {
		// A surviving owner is rebound only by its next admitted READ or
		// current-owner DISABLE. ENABLE is never a refresh trigger.
		m.pendingEpoch = newEpoch
		m.stateMu.Unlock()
		return
	}
	m.transport.TransportEpoch = newEpoch
	m.pendingEpoch = 0
	m.stateMu.Unlock()
}

// ResetForRestart destroys caller handles and process-local cleanup, releases
// a held gate, and fences known or supplied qualified targets. It emits no
// native operation. The stable public session observation is Idle/owned=false
// while the internal state remains Disabled until out-of-band recovery.
func (m *Manager) ResetForRestart(qualifiedTargets ...byte) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
	m.idleTimerGen++
	if m.mutexHeld {
		m.mu.Unlock()
		m.mutexHeld = false
	}
	m.activeToken = ""
	m.activeTarget = 0
	m.pendingOperation = nil
	m.cleanup = nil
	m.refreshFailed = false
	m.lastRefreshTransportDown = false
	m.pendingEpoch = 0
	m.state = Disabled
	m.restarted = true
	for target := range m.qualified {
		m.restartFences[target] = struct{}{}
	}
	for _, target := range qualifiedTargets {
		m.qualified[target] = struct{}{}
		m.restartFences[target] = struct{}{}
	}
}

// --- internal helpers (all require stateMu held) ---

// toDisabledLocked transitions to Disabled from any held-owner state and
// releases the ownership gate exactly once. Bumps idleTimerGen so any
// in-flight stale callback is invalidated.
func (m *Manager) toDisabledLocked() {
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
	m.idleTimerGen++
	m.state = Disabled
	if m.mutexHeld {
		m.mu.Unlock()
		m.mutexHeld = false
	}
	m.activeToken = ""
	m.activeTarget = 0
}

// toIdleLocked finalises Disabled -> Idle.
func (m *Manager) toIdleLocked() {
	if m.cleanup == nil {
		m.state = Idle
	}
}

// armIdleTimerLocked (re)starts the idle-timeout timer. Safe to call
// repeatedly; any previously-running timer is stopped first. Each call
// increments idleTimerGen so an in-flight stale callback (stopped timer
// whose goroutine was already running at Stop() time) can detect its
// generation is no longer current and exit without touching the FSM.
func (m *Manager) armIdleTimerLocked() {
	if m.idleTimer != nil {
		m.idleTimer.Stop()
	}
	if m.idleTimeout <= 0 {
		return
	}
	m.idleTimerGen++
	gen := m.idleTimerGen
	m.idleTimer = time.AfterFunc(m.idleTimeout, func() { m.idleTimerFired(gen) })
}

// idleTimerFired is the timer callback. It acquires stateMu itself; it
// MUST NOT be called with stateMu held. If `gen` does not match the
// current idleTimerGen, this callback belongs to a previously-stopped
// timer whose goroutine was already running when Stop() was called — it
// MUST return without side effects to avoid prematurely disabling a
// rearmed active session.
func (m *Manager) idleTimerFired(gen uint64) {
	m.stateMu.Lock()
	if gen != m.idleTimerGen {
		m.stateMu.Unlock()
		return // stale callback from a stopped-then-rearmed timer
	}
	if m.state != Active {
		m.stateMu.Unlock()
		return
	}
	target := m.activeTarget
	epoch := m.transport.TransportEpoch
	attemptID := newCleanupAttemptID()
	m.toDisabledLocked()
	m.createCleanupLocked(target, epoch, attemptID)
	m.stateMu.Unlock()
	m.attemptCleanup(context.Background(), epoch)
}

func (m *Manager) beginOwnerOperation(ctx context.Context, target byte, key *SessionKey, kind OperationKind) (*pendingOperation, error) {
	m.stateMu.Lock()
	if m.state == Refreshing {
		m.stateMu.Unlock()
		return nil, ErrSessionBusy
	}
	if !m.mutexHeld || m.state != Active {
		m.stateMu.Unlock()
		return nil, ErrNotActive
	}
	if m.pendingOperation != nil {
		m.stateMu.Unlock()
		return nil, ErrSessionBusy
	}
	if m.activeTarget != target {
		m.stateMu.Unlock()
		return nil, ErrTargetMismatch
	}
	if key != nil && (key.Transport != m.transport || key.IssuerToken != m.activeToken) {
		m.stateMu.Unlock()
		return nil, ErrWrongToken
	}
	pending := &pendingOperation{PendingOperationSnapshot: PendingOperationSnapshot{
		OperationID: newCleanupAttemptID(), Kind: kind, Target: target, Transport: m.transport,
	}}
	m.pendingOperation = pending
	if m.pendingEpoch <= m.transport.TransportEpoch {
		m.stateMu.Unlock()
		return pending, nil
	}
	attemptedEpoch := m.pendingEpoch
	m.state = Refreshing
	refresh := m.refresh
	m.stateMu.Unlock()

	var (
		newTransport TransportKey
		err          error
	)
	if refresh == nil {
		err = ErrTransportDown
	} else {
		newTransport, err = refresh(ctx)
	}

	m.stateMu.Lock()
	if m.pendingOperation != pending || !m.mutexHeld || m.state != Refreshing {
		if m.pendingOperation == pending {
			m.pendingOperation = nil
		}
		m.stateMu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, ErrTransportDown
	}
	if err == nil && newTransport.TransportEpoch >= attemptedEpoch {
		m.transport = newTransport
		m.observedEpoch = newTransport.TransportEpoch
		m.pendingEpoch = 0
		m.state = Active
		pending.Transport = newTransport
		if m.cleanup != nil {
			m.cleanup.TransportEpoch = newTransport.TransportEpoch
		}
		m.armIdleTimerLocked()
		m.stateMu.Unlock()
		return pending, nil
	}
	if err == nil {
		err = ErrTransportDown
	}
	if errors.Is(err, ErrTransportDown) {
		m.lastRefreshTransportDown = true
	} else {
		m.refreshFailed = true
	}
	m.transport.TransportEpoch = attemptedEpoch
	m.pendingEpoch = 0
	m.toDisabledLocked()
	m.createCleanupLocked(target, attemptedEpoch, "")
	m.pendingOperation = nil
	m.stateMu.Unlock()
	return nil, err
}

func dispatchOnce(ctx context.Context, target byte, dispatch DispatchFunc) DispatchOutcome {
	if ctx == nil {
		ctx = context.Background()
	}
	if dispatch == nil {
		return DispatchOutcome{Err: ErrTransportDown}
	}
	return dispatch(ctx, target)
}

func (m *Manager) releaseOwnerLocked() {
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
	m.idleTimerGen++
	if m.mutexHeld {
		m.mu.Unlock()
		m.mutexHeld = false
	}
	m.activeToken = ""
	m.activeTarget = 0
}

func (m *Manager) createCleanupLocked(target byte, epoch uint64, sourceOperationID string) {
	if m.cleanup != nil {
		return
	}
	m.cleanup = &cleanupObligation{
		CleanupObligationSnapshot: CleanupObligationSnapshot{
			Target: target, GatewayCleanupAttemptID: newCleanupAttemptID(), TransportEpoch: epoch,
		},
		sourceOperationID: sourceOperationID,
	}
	m.state = Disabled
}

func (m *Manager) clearCleanupForOperationLocked(operationID string) {
	if m.cleanup != nil && m.cleanup.sourceOperationID == operationID {
		m.cleanup = nil
	}
}

func (m *Manager) attemptCleanup(ctx context.Context, epoch uint64) {
	m.stateMu.Lock()
	if m.cleanup == nil || m.restarted || m.cleanup.LastAttemptEpoch == epoch || m.cleanupDispatch == nil {
		m.stateMu.Unlock()
		return
	}
	attemptID := m.cleanup.GatewayCleanupAttemptID
	target := m.cleanup.Target
	m.cleanup.LastAttemptEpoch = epoch
	m.cleanup.TransportEpoch = epoch
	dispatch := m.cleanupDispatch
	m.stateMu.Unlock()
	outcome := dispatchOnce(ctx, target, dispatch)
	m.recordCleanupOutcome(attemptID, epoch, outcome)
}

// recordCleanupOutcome retains exact native evidence without treating ACK or
// NAK as proof that the device-side session settled.
func (m *Manager) recordCleanupOutcome(attemptID string, epoch uint64, outcome DispatchOutcome) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.cleanup == nil || m.cleanup.GatewayCleanupAttemptID != attemptID || m.cleanup.LastAttemptEpoch != epoch {
		return
	}
	m.cleanup.LastEmitted = outcome.Emitted
	m.cleanup.LastNative = outcome.Native
	m.cleanup.LastErr = outcome.Err
}

var fallbackAttemptID atomic.Uint64

func newCleanupAttemptID() string {
	if id, err := newIssuerToken(); err == nil {
		return id
	}
	return fmt.Sprintf("fallback-%d", fallbackAttemptID.Add(1))
}

// newIssuerToken returns 16 random bytes hex-encoded (32 chars).
func newIssuerToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
