package b503session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
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

// Manager is the single-owner live-monitor session FSM.
//
// Two mutexes cooperate:
//
//   - mu (liveMonitorMu): the ownership gate. Acquired on the
//     Idle->Enabling transition; released exactly once on entry to
//     Disabled from a held-owner state (Enabling, Active, or expired).
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

	stateMu     sync.Mutex
	state       State
	transport   TransportKey
	activeToken string
	idleTimeout time.Duration
	idleTimer   *time.Timer
	refresh     RefreshFunc
	mutexHeld   bool
	// refreshFailed sticks to true when a refresh returned a non-nil,
	// non-ErrTransportDown error. Subsequent Reads then surface
	// ErrSessionBusy until ResetForRestart or a new Enable. It is
	// cleared by Enable and ResetForRestart.
	refreshFailed bool
	// lastRefreshTransportDown is set when OnEpochAdvance's refresh returned
	// ErrTransportDown; cleared on Enable / ResetForRestart. See spec §7.1
	// and plan AD14 for the resolver-layer contract this enables.
	lastRefreshTransportDown bool
}

// New constructs a Manager bound to the initial transport incarnation.
// idleTimeout is the Active->Disabled auto-disable window (spec §7.6;
// production value 30s). refresh is invoked during epoch-advance handling.
func New(transport TransportKey, idleTimeout time.Duration, refresh RefreshFunc) *Manager {
	return &Manager{
		state:       Idle,
		transport:   transport,
		idleTimeout: idleTimeout,
		refresh:     refresh,
	}
}

// Enable transitions Idle -> Enabling -> Active and returns a SessionKey
// with a freshly-minted 16-byte hex issuer_token. Returns ErrSessionBusy
// if the FSM is not Idle.
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
	m.state = Active
	m.refreshFailed = false
	m.lastRefreshTransportDown = false
	m.armIdleTimerLocked()

	return SessionKey{Transport: m.transport, IssuerToken: token}, nil
}

// Disable transitions Active -> Disabled -> Idle. Requires a full
// SessionKey match. ErrWrongToken for issuer_token mismatch; ErrNotActive
// if the FSM is not currently Active.
func (m *Manager) Disable(key SessionKey) error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

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
func (m *Manager) Read(transport TransportKey) error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	if m.refreshFailed {
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

// State returns the public FSM state. The internal expired state is
// never returned here: if the FSM is transiently in expired (e.g. a
// refresh is in-flight) it is normalised to Active or Disabled based on
// the current refreshFailed flag.
func (m *Manager) State() State {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	s := m.state
	if s == expired {
		// Should be unreachable in steady state — OnEpochAdvance resolves
		// expired before returning control. Surface a conservative Disabled.
		return Disabled
	}
	return s
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
// If the FSM holds the ownership gate (Enabling/Active/expired), releases
// it and transitions to Disabled -> Idle. Otherwise no-op.
func (m *Manager) OnTransportDisconnect() {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if !m.mutexHeld {
		return
	}
	m.toDisabledLocked()
	m.toIdleLocked()
}

// OnEpochAdvance handles a transport reconnect whose epoch has advanced.
// If no session is held, updates the tracked transport and returns.
// Otherwise: state -> expired, invokes refresh once.
//
//   - refresh success -> state -> Active, transport updated.
//   - ErrTransportDown -> state -> Disabled -> Idle, gate released.
//   - any other error -> state -> Disabled -> Idle, gate released, and
//     refreshFailed latches true so subsequent Reads return ErrSessionBusy
//     until the next Enable.
//
// This consumes exactly one retry budget; there is no recursion.
func (m *Manager) OnEpochAdvance(ctx context.Context, newEpoch uint64) {
	m.stateMu.Lock()
	if !m.mutexHeld {
		// No owner: just update transport.
		m.transport.TransportEpoch = newEpoch
		m.stateMu.Unlock()
		return
	}
	// Owner held. Transition to expired and release the state lock for the
	// duration of the refresh so observers (State(), Read()) don't block.
	m.state = expired
	refresh := m.refresh
	m.stateMu.Unlock()

	var (
		newTK TransportKey
		err   error
	)
	if refresh != nil {
		newTK, err = refresh(ctx)
	} else {
		err = ErrTransportDown
	}

	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if err == nil {
		// Re-home on the new incarnation; keep the same issuer_token.
		m.transport = newTK
		m.state = Active
		m.armIdleTimerLocked()
		return
	}
	// Refresh failed: release gate and disable.
	if errors.Is(err, ErrTransportDown) {
		m.lastRefreshTransportDown = true
	} else {
		m.refreshFailed = true
	}
	m.toDisabledLocked()
	m.toIdleLocked()
	_ = newEpoch // already consumed via refresh func; keep for debug.
}

// ResetForRestart simulates a gateway restart. Destroys all in-memory
// state, releases the gate if held, and returns the FSM to Idle. Any
// previously-issued SessionKey becomes invalid.
func (m *Manager) ResetForRestart() {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
	if m.mutexHeld {
		m.mu.Unlock()
		m.mutexHeld = false
	}
	m.activeToken = ""
	m.refreshFailed = false
	m.lastRefreshTransportDown = false
	m.state = Idle
}

// --- internal helpers (all require stateMu held) ---

// toDisabledLocked transitions to Disabled from any held-owner state and
// releases the ownership gate exactly once.
func (m *Manager) toDisabledLocked() {
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
	m.state = Disabled
	if m.mutexHeld {
		m.mu.Unlock()
		m.mutexHeld = false
	}
	m.activeToken = ""
}

// toIdleLocked finalises Disabled -> Idle.
func (m *Manager) toIdleLocked() {
	m.state = Idle
}

// armIdleTimerLocked (re)starts the idle-timeout timer. Safe to call
// repeatedly; any previously-running timer is stopped first.
func (m *Manager) armIdleTimerLocked() {
	if m.idleTimer != nil {
		m.idleTimer.Stop()
	}
	if m.idleTimeout <= 0 {
		return
	}
	m.idleTimer = time.AfterFunc(m.idleTimeout, m.idleTimerFired)
}

// idleTimerFired is the timer callback. It acquires stateMu itself; it
// MUST NOT be called with stateMu held.
func (m *Manager) idleTimerFired() {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.state != Active {
		return
	}
	m.toDisabledLocked()
	m.toIdleLocked()
}

// newIssuerToken returns 16 random bytes hex-encoded (32 chars).
func newIssuerToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
