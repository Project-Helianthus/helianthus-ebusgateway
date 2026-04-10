package adaptermux

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Config configures the adapter multiplexer.
type Config struct {
	// Protocol is "enh" or "ens".
	Protocol string

	// Network and Address specify the adapter endpoint (e.g., "tcp", "boiler.local:9999").
	Network string
	Address string

	// Timeouts for the adapter connection.
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// ReconnectInitialDelay is the initial delay before reconnecting
	// after an adapter disconnect. Default: 1s.
	ReconnectInitialDelay time.Duration

	// ReconnectMaxDelay is the maximum reconnection backoff. Default: 30s.
	ReconnectMaxDelay time.Duration

	// MaxOwnershipDuration is the hard limit on continuous bus ownership.
	// Default: 2s (from proxy convention).
	MaxOwnershipDuration time.Duration

	// IdleReleaseGrace is the grace period after bus acquisition before
	// idle SYN can release ownership. Default: 50ms.
	IdleReleaseGrace time.Duration

	// Logger for multiplexer events. If nil, log.Default() is used.
	Logger *log.Logger
}

func (c *Config) defaults() {
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 200 * time.Millisecond
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 2 * time.Second
	}
	if c.ReconnectInitialDelay == 0 {
		c.ReconnectInitialDelay = 1 * time.Second
	}
	if c.ReconnectMaxDelay == 0 {
		c.ReconnectMaxDelay = 30 * time.Second
	}
	if c.MaxOwnershipDuration == 0 {
		c.MaxOwnershipDuration = 2 * time.Second
	}
	if c.IdleReleaseGrace == 0 {
		c.IdleReleaseGrace = 50 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
}

// PassiveEvent is delivered to the passive path callback when a
// third-party symbol is received from the bus.
type PassiveEvent struct {
	Kind       PassiveEventKind
	Symbol     byte
	ObservedAt time.Time
}

// PassiveEventKind categorizes passive events.
type PassiveEventKind uint8

const (
	PassiveEventSymbol       PassiveEventKind = iota + 1 // third-party bus symbol
	PassiveEventConnected                                // adapter connection established
	PassiveEventDisconnected                             // adapter connection lost
	PassiveEventReset                                    // adapter RESETTED
)

// arbitrationRequester is the non-blocking START interface.
// ENHTransport.RequestStart only acquires writeMu (not readMu),
// so readLoop continues receiving bus bytes during arbitration.
type arbitrationRequester interface {
	RequestStart(initiator byte) error
}

// pendingStartState tracks an in-flight START request sent to the
// adapter but not yet confirmed via STARTED/FAILED response.
// Protected by stateMu.
type pendingStartState struct {
	sessionID uint64
	initiator byte
	notify    chan startResult
}

// Mux is the adapter multiplexer. It owns a single ENH/ENS connection
// to the adapter hardware and provides:
//   - Active path: RawTransport for gateway.Bus
//   - Passive path: filtered symbol callback (third-party only)
//   - Session management: external ENH clients (ebusd)
type Mux struct {
	cfg    Config
	logger *log.Logger

	// Adapter connection (guarded by connMu for reconnection).
	connMu           sync.Mutex
	conn             net.Conn
	upstream         transport.RawTransport
	upstreamFeatures atomic.Uint32 // features byte from upstream INIT handshake

	// Multiplexer state (guarded by stateMu).
	stateMu      sync.Mutex
	phase        wirePhaseTracker
	arb          *arbitrator
	busOwned     time.Time          // when current owner acquired the bus
	busDirty     bool               // owner has sent bytes since acquiring
	pendingStart       *pendingStartState // in-flight START awaiting STARTED/FAILED
	pendingStartAbsorb int               // stale adapter responses to absorb (FAILED/STARTED from cancelled requests)

	// Gateway echo tracker (for the internal active path).
	// Protected by stateMu.
	gatewayEcho *echoTracker

	// External sessions.
	sessionsMu sync.Mutex // write lock — protects map AND session echo trackers
	sessions   map[uint64]*session
	sessionSeq atomic.Uint64

	// Active path channels.
	activeSendCh chan sendRequest
	activeCh     chan activeEvent // capacity 4096: unified byte+error channel (FIFO ordered)

	// Passive path callback (set via SetPassiveCallback).
	// The callback must NOT call back into Mux methods (re-entrancy hazard).
	passiveMu       sync.Mutex
	passiveCallback func(PassiveEvent)

	// Lifecycle.
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// activeEventKind tags active-path channel events.
type activeEventKind uint8

const (
	activeEventByte  activeEventKind = iota // data byte from adapter
	activeEventError                        // error (reset, disconnect)
)

// activeEvent is a tagged union carried on the single activeCh channel.
// Merging bytes and errors into one channel guarantees FIFO ordering:
// handleReset drains the channel and enqueues the reset event before
// readLoop resumes enqueuing bytes, so the consumer sees events in
// exact enqueue order — no Go-select non-determinism.
type activeEvent struct {
	kind activeEventKind
	b    byte  // valid when kind == activeEventByte
	err  error // valid when kind == activeEventError
}

// Sentinel errors returned by doSend for error classification.
// Host-side errors (ownership, connectivity, adapter write) are distinct
// from bus errors so callers can deliver the correct ENH response code
// (ENHResErrorHost vs ENHResErrorEBUS).
var (
	errNotBusOwner  = errors.New("adaptermux: session is not bus owner")
	errNotConnected = errors.New("adaptermux: not connected")
	errAdapterWrite = errors.New("adaptermux: adapter write failed")
)

// sendRequest is a request from the active path or an external session
// to send a byte to the adapter.
type sendRequest struct {
	sessionID uint64
	data      byte
	result    chan error
}

// New creates a new adapter multiplexer with the given configuration.
// Call Start() to connect to the adapter and begin processing.
func New(cfg Config) *Mux {
	cfg.defaults()
	return &Mux{
		cfg:          cfg,
		logger:       cfg.Logger,
		arb:          newArbitrator(),
		gatewayEcho:  newEchoTracker(),
		sessions:     make(map[uint64]*session),
		activeSendCh: make(chan sendRequest, 16),
		activeCh:     make(chan activeEvent, 4096), // unified byte+error channel (4096: survives ~16s of bus traffic during arbitration waits)
	}
}

// Start connects to the adapter and begins the multiplexer loop.
// The context controls the multiplexer lifetime.
func (m *Mux) Start(ctx context.Context) error {
	m.ctx, m.cancel = context.WithCancel(ctx)

	if err := m.connect(); err != nil {
		return fmt.Errorf("adaptermux: initial connect: %w", err)
	}

	m.wg.Add(1)
	go m.readLoop() // goroutine: reads bytes from adapter, dispatches to consumers

	m.wg.Add(1)
	go m.sendLoop() // goroutine: processes send requests from active path + sessions

	m.emitPassive(PassiveEvent{
		Kind:       PassiveEventConnected,
		ObservedAt: time.Now(),
	})

	return nil
}

// Close shuts down the multiplexer and releases all resources.
// Safe to call multiple times — subsequent calls return the first error.
func (m *Mux) Close() error {
	m.closeOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}

		// Cancel in-flight pending START if any.
		m.stateMu.Lock()
		pendingToCancel := m.pendingStart
		m.pendingStart = nil
		m.pendingStartAbsorb = 0
		m.stateMu.Unlock()
		if pendingToCancel != nil {
			pendingToCancel.notify <- startResult{granted: false, initiator: pendingToCancel.initiator, err: errors.New("adaptermux: closed")}
		}

		// Fail all pending arbitration requests.
		m.arb.failAllPending(errors.New("adaptermux: closed"))

		// Collect sessions under lock, close outside lock to avoid holding
		// sessionsMu during potentially blocking session.close() (5s timeout).
		m.sessionsMu.Lock()
		toClose := make([]*session, 0, len(m.sessions))
		for id, sess := range m.sessions {
			toClose = append(toClose, sess)
			delete(m.sessions, id)
		}
		m.sessionsMu.Unlock()

		for _, sess := range toClose {
			m.arb.removeSession(sess.id)
			sess.close()
		}

		// Close adapter connection.
		m.connMu.Lock()
		if m.conn != nil {
			m.closeErr = m.conn.Close()
		}
		m.connMu.Unlock()

		m.wg.Wait()
	})
	return m.closeErr
}

// ActiveTransport returns a RawTransport for the gateway's active path.
// This wraps the multiplexer's internal channels to provide a blocking
// byte-level interface compatible with gateway.Bus.
func (m *Mux) ActiveTransport() transport.RawTransport {
	return &activeTransport{mux: m}
}

// SetPassiveCallback sets the callback for passive path events.
// The callback receives only third-party bus traffic (no self-echo).
// The callback must NOT call back into Mux methods (re-entrancy hazard).
// Must be called before Start() or during initialization.
func (m *Mux) SetPassiveCallback(fn func(PassiveEvent)) {
	m.passiveMu.Lock()
	defer m.passiveMu.Unlock()
	m.passiveCallback = fn
}

// connect dials the adapter and performs the INIT handshake.
func (m *Mux) connect() error {
	m.connMu.Lock()
	defer m.connMu.Unlock()

	dialer := net.Dialer{Timeout: m.cfg.DialTimeout}
	conn, err := dialer.DialContext(m.ctx, m.cfg.Network, m.cfg.Address)
	if err != nil {
		return fmt.Errorf("dial %s/%s: %w", m.cfg.Network, m.cfg.Address, err)
	}

	// Set TCP options (log failures instead of discarding).
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			m.logger.Printf("adaptermux: SetNoDelay: %v", err)
		}
		if err := tcpConn.SetKeepAlive(true); err != nil {
			m.logger.Printf("adaptermux: SetKeepAlive: %v", err)
		}
		if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
			m.logger.Printf("adaptermux: SetKeepAlivePeriod: %v", err)
		}
	}

	// Create ENH/ENS transport.
	var tr transport.RawTransport
	switch m.cfg.Protocol {
	case "ens":
		tr = transport.NewENSTransport(conn, m.cfg.ReadTimeout, m.cfg.WriteTimeout)
	case "enh", "":
		tr = transport.NewENHTransport(conn, m.cfg.ReadTimeout, m.cfg.WriteTimeout)
	default:
		_ = conn.Close()
		return fmt.Errorf("adaptermux: unsupported protocol %q (expected \"enh\" or \"ens\")", m.cfg.Protocol)
	}

	m.conn = conn
	m.upstream = tr

	// Perform INIT handshake — fatal if transport implements Init.
	// Store the negotiated features byte from the upstream RESETTED
	// response for session INIT reply fidelity.
	const requestedFeatures byte = 0x01
	if initer, ok := tr.(interface{ Init(byte) (byte, error) }); ok {
		features, err := initer.Init(requestedFeatures)
		if err != nil {
			_ = conn.Close()
			return fmt.Errorf("adaptermux: INIT handshake failed: %w", err)
		}
		m.upstreamFeatures.Store(uint32(features))
		m.logger.Printf("adaptermux: INIT handshake succeeded, upstream features=0x%02X", features)
	}

	return nil
}

// reconnect tears down the current connection and re-establishes it.
// Called from the read loop on adapter disconnect.
func (m *Mux) reconnect() error {
	m.emitPassive(PassiveEvent{
		Kind:       PassiveEventDisconnected,
		ObservedAt: time.Now(),
	})

	// Reset multiplexer state.
	m.stateMu.Lock()
	m.phase.reset(wirePhaseIdle)
	m.arb.forceRelease()
	m.gatewayEcho.reset()
	m.busDirty = false
	pendingToCancel := m.pendingStart
	m.pendingStart = nil
	m.pendingStartAbsorb = 0
	m.stateMu.Unlock()

	// Cancel in-flight pending START if any.
	if pendingToCancel != nil {
		pendingToCancel.notify <- startResult{granted: false, initiator: pendingToCancel.initiator, err: errors.New("adaptermux: adapter disconnected")}
	}

	// Reset external session echo trackers (HIGH-6 fix).
	m.sessionsMu.Lock()
	for _, sess := range m.sessions {
		sess.echoTracker.reset()
	}
	m.sessionsMu.Unlock()

	// Fail pending arbitration.
	m.arb.failAllPending(errors.New("adaptermux: adapter disconnected"))

	// Notify active path of disconnect so gateway.Bus sees the reset
	// boundary (Codex P2 #3058767932). Drain stale bytes first.
	m.drainActiveCh()
	select {
	case m.activeCh <- activeEvent{kind: activeEventError, err: ebuserrors.ErrAdapterReset}:
	default:
		m.logger.Printf("adaptermux: active channel full, dropping disconnect notification")
	}

	// Close old connection.
	m.connMu.Lock()
	if m.conn != nil {
		if err := m.conn.Close(); err != nil {
			m.logger.Printf("adaptermux: old conn close: %v", err)
		}
	}
	m.connMu.Unlock()

	// Reconnection loop with exponential backoff.
	delay := m.cfg.ReconnectInitialDelay
	for {
		timer := time.NewTimer(delay)
		select {
		case <-m.ctx.Done():
			timer.Stop()
			return m.ctx.Err()
		case <-timer.C:
		}

		if err := m.connect(); err != nil {
			m.logger.Printf("adaptermux: reconnect failed: %v", err)
			delay = delay * 2
			if delay > m.cfg.ReconnectMaxDelay {
				delay = m.cfg.ReconnectMaxDelay
			}
			continue
		}

		m.logger.Printf("adaptermux: reconnected to %s/%s", m.cfg.Network, m.cfg.Address)
		m.emitPassive(PassiveEvent{
			Kind:       PassiveEventConnected,
			ObservedAt: time.Now(),
		})

		// Broadcast RESETTED to external sessions.
		m.broadcastResetToSessions()

		return nil
	}
}

// readLoop reads bytes from the adapter and dispatches them to
// active path, passive path, and external sessions.
func (m *Mux) readLoop() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}

		event, err := m.readUpstream()
		if err != nil {
			if m.ctx.Err() != nil {
				return // context cancelled, shutting down
			}
			if errors.Is(err, ebuserrors.ErrAdapterReset) {
				m.handleReset()
				continue
			}
			// Read timeouts on a quiet bus are normal (no data within
			// ReadTimeout window) — treat as idle, not disconnect.
			// Matches passive_bus_tap.go behavior (Codex P2 #3059211395).
			if errors.Is(err, ebuserrors.ErrTimeout) || isNetTimeout(err) {
				// Enforce ownership timeout on quiet bus — onReceived
				// won't run to check MaxOwnershipDuration.
				m.stateMu.Lock()
				ownerID, _, hasOwner := m.arb.owner()
				if hasOwner && !m.busOwned.IsZero() &&
					time.Since(m.busOwned) > m.cfg.MaxOwnershipDuration {
					m.arb.releaseOwnership(ownerID)
					m.gatewayEcho.reset()
				}
				m.stateMu.Unlock()

				// On a quiet bus no SYN or transaction-complete events
				// reach onReceived, so tryGrantAndStart is never called.
				// Drain pending START requests here to prevent stalls.
				if m.arb.hasPending() {
					m.tryGrantAndStart()
				}
				continue
			}
			m.logger.Printf("adaptermux: read error: %v", err)
			if reconnErr := m.reconnect(); reconnErr != nil {
				m.logger.Printf("adaptermux: reconnect gave up: %v", reconnErr)
				return
			}
			continue
		}

		switch event.Kind {
		case transport.StreamEventStarted:
			m.logger.Printf("adaptermux: readLoop got StreamEventStarted data=0x%02X", event.Data)
			m.handleArbitrationResponse(true, event.Data)
			continue
		case transport.StreamEventFailed:
			m.logger.Printf("adaptermux: readLoop got StreamEventFailed data=0x%02X", event.Data)
			m.handleArbitrationResponse(false, event.Data)
			continue
		case transport.StreamEventReset:
			m.handleReset()
			continue
		case transport.StreamEventByte:
			m.onReceived(event.Byte)
		default:
			m.onReceived(event.Byte)
		}
	}
}

// readUpstream reads a stream event from the adapter transport.
func (m *Mux) readUpstream() (transport.StreamEvent, error) {
	m.connMu.Lock()
	tr := m.upstream
	m.connMu.Unlock()

	if tr == nil {
		return transport.StreamEvent{}, errors.New("adaptermux: not connected")
	}

	if reader, ok := tr.(transport.StreamEventReader); ok {
		return reader.ReadEvent()
	}

	b, err := tr.ReadByte()
	if err != nil {
		return transport.StreamEvent{}, err
	}
	return transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}, nil
}

// onReceived processes a byte received from the adapter.
//
// Lock ordering: stateMu acquired first, released before any callback
// invocation or session delivery to avoid re-entrancy deadlocks.
func (m *Mux) onReceived(symbol byte) {
	now := time.Now()

	// --- Phase 1: state update under stateMu ---
	m.stateMu.Lock()

	phaseEvent := m.phase.advance(symbol)

	ownerID, _, hasOwner := m.arb.owner()
	if hasOwner && !m.busOwned.IsZero() &&
		time.Since(m.busOwned) > m.cfg.MaxOwnershipDuration {
		m.arb.releaseOwnership(ownerID)
		m.gatewayEcho.reset()
		hasOwner = false
	}

	// Collect passive events under lock, emit after unlock (Issue#3 fix).
	var passiveEvents []PassiveEvent
	var shouldTryGrant bool

	if symbol == protocol.SymbolSyn {
		passiveEvents, shouldTryGrant = m.onSYNLocked(phaseEvent, ownerID, hasOwner, now)
		m.stateMu.Unlock()

		// --- Phase 2: deliver outside all locks ---
		// Flush external session echo trackers. Called outside stateMu to
		// avoid ABBA deadlock with doSend (sessionsMu→stateMu).
		// The flushed bytes are NOT emitted to passive — they were already
		// delivered live in onReceived (they're third-party from gateway's
		// perspective). Re-emitting would produce duplicates (Codex P1).
		m.flushSessionEchoTrackers()

		m.deliverToActive(symbol)
		for _, pe := range passiveEvents {
			m.emitPassive(pe)
		}
		m.deliverSYNToSessions(now)
		if shouldTryGrant {
			m.tryGrantAndStart()
		}
		return
	}

	// Non-SYN byte: check gateway echo suppression.
	isGatewayEcho := false
	if hasOwner && ownerID == gatewaySessionID {
		result, flushed := m.gatewayEcho.matchEcho(symbol)
		switch result {
		case echoMatchSuppressed:
			isGatewayEcho = true
		case echoMatchFlushed:
			for _, b := range flushed {
				passiveEvents = append(passiveEvents, PassiveEvent{
					Kind: PassiveEventSymbol, Symbol: b, ObservedAt: now,
				})
			}
		}
	}

	m.stateMu.Unlock()

	// --- Phase 2: deliver outside all locks ---
	m.deliverToActive(symbol)

	if !isGatewayEcho {
		passiveEvents = append(passiveEvents, PassiveEvent{
			Kind: PassiveEventSymbol, Symbol: symbol, ObservedAt: now,
		})
	}
	for _, pe := range passiveEvents {
		m.emitPassive(pe)
	}

	m.deliverToSessions(symbol, ownerID, hasOwner, now)

	if phaseEvent == wirePhaseEventTransactionDone ||
		phaseEvent == wirePhaseEventCmdNACK {
		m.arb.releaseOwnership(ownerID)
		m.tryGrantAndStart()
	}
}

// onSYNLocked handles a SYN symbol. Caller holds stateMu.
// Returns buffered passive events and whether tryGrant should be called.
func (m *Mux) onSYNLocked(phaseEvent wirePhaseEvent, ownerID uint64, hasOwner bool, now time.Time) ([]PassiveEvent, bool) {
	var passiveEvents []PassiveEvent

	// Flush gateway echo tracker at SYN boundary. Flushed bytes are
	// confirmed gateway self-traffic — do NOT emit to passive path
	// (passive is third-party only). They are delivered to external
	// sessions via deliverSYNToSessions + deliverToSessions elsewhere.
	m.gatewayEcho.flushOnSYN()

	// Note: external session echo tracker flush is done AFTER stateMu.Unlock()
	// by the caller (flushSessionEchoTrackersOnSYN) to avoid ABBA deadlock
	// with doSend which acquires sessionsMu→stateMu.

	// Release ownership if SYN timeout.
	if phaseEvent == wirePhaseEventSYNTimeout && hasOwner {
		m.arb.releaseOwnership(ownerID)
	}

	// Release ownership on idle SYN after grace period.
	if phaseEvent == wirePhaseEventSYNIdle && hasOwner {
		if !m.busDirty || time.Since(m.busOwned) > m.cfg.IdleReleaseGrace {
			m.arb.releaseOwnership(ownerID)
		}
	}

	// SYN is always visible on passive path.
	passiveEvents = append(passiveEvents, PassiveEvent{
		Kind: PassiveEventSymbol, Symbol: protocol.SymbolSyn, ObservedAt: now,
	})

	shouldTryGrant := m.arb.hasPending()
	return passiveEvents, shouldTryGrant
}

// handleReset handles an adapter RESETTED event.
func (m *Mux) handleReset() {
	now := time.Now()

	m.stateMu.Lock()
	m.phase.reset(wirePhaseIdle)
	m.gatewayEcho.reset()
	m.busDirty = false
	pendingToCancel := m.pendingStart
	m.pendingStart = nil
	m.pendingStartAbsorb = 0 // reset clears adapter state; no stale responses will arrive
	m.stateMu.Unlock()

	// Cancel in-flight pending START if any.
	if pendingToCancel != nil {
		pendingToCancel.notify <- startResult{granted: false, initiator: pendingToCancel.initiator, err: errors.New("adaptermux: adapter reset")}
	}

	// Reset external session echo trackers.
	m.sessionsMu.Lock()
	for _, sess := range m.sessions {
		sess.echoTracker.reset()
	}
	m.sessionsMu.Unlock()

	m.arb.forceRelease()
	m.arb.failAllPending(errors.New("adaptermux: adapter reset"))

	// Drain stale bytes from active channel before enqueuing the reset
	// error. Because activeCh is a single FIFO channel, the consumer is
	// guaranteed to see the reset before any post-reset bytes that
	// readLoop enqueues after this function returns.
	m.drainActiveCh()

	// Enqueue reset error into the unified channel (non-blocking to
	// prevent readLoop deadlock).
	select {
	case m.activeCh <- activeEvent{kind: activeEventError, err: ebuserrors.ErrAdapterReset}:
	default:
		m.logger.Printf("adaptermux: active channel full, dropping adapter reset notification")
	}

	m.emitPassive(PassiveEvent{Kind: PassiveEventReset, ObservedAt: now})
	m.broadcastResetToSessions()

	// Schedule delayed re-INIT after in-band RESETTED (200ms stabilization).
	// Without re-INIT the upstream transport stays in reset state and
	// ownership cannot be re-acquired until a physical TCP disconnect
	// triggers reconnect(). The 200ms delay is from proxy convention.
	m.wg.Add(1)
	go func() { // goroutine: delayed re-INIT after adapter RESETTED
		defer m.wg.Done()
		select {
		case <-time.After(200 * time.Millisecond):
		case <-m.ctx.Done():
			return
		}
		m.connMu.Lock()
		tr := m.upstream
		m.connMu.Unlock()
		if initer, ok := tr.(interface{ Init(byte) (byte, error) }); ok {
			reqFeatures := byte(m.upstreamFeatures.Load())
			if reqFeatures == 0 {
				reqFeatures = 0x01
			}
			features, err := initer.Init(reqFeatures)
			if err != nil {
				m.logger.Printf("adaptermux: re-INIT after RESETTED failed: %v", err)
			} else {
				m.upstreamFeatures.Store(uint32(features))
				m.logger.Printf("adaptermux: re-INIT after RESETTED succeeded, features=0x%02X", features)
			}
		}
	}()
}

// drainActiveCh discards all buffered events from the active channel.
// Called before reset/reconnect to ensure consumers don't see stale
// pre-boundary bytes after a reset event.
func (m *Mux) drainActiveCh() {
	for {
		select {
		case <-m.activeCh:
		default:
			return
		}
	}
}

// flushSessionEchoTrackers flushes echo trackers for all external sessions
// at a SYN boundary. The flushed bytes are discarded — they were already
// delivered live to passive consumers in onReceived. This call only resets
// tracker state for the next transaction cycle. Must be called WITHOUT
// stateMu held to avoid ABBA deadlock with doSend.
func (m *Mux) flushSessionEchoTrackers() {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()
	for _, sess := range m.sessions {
		sess.echoTracker.flushOnSYN()
	}
}

// deliverToActive sends a byte to the active path channel.
// Logs on overflow instead of silently dropping (CRITICAL-1 fix).
func (m *Mux) deliverToActive(symbol byte) {
	select {
	case m.activeCh <- activeEvent{kind: activeEventByte, b: symbol}:
	default:
		m.logger.Printf("adaptermux: active channel full, dropping byte 0x%02X", symbol)
	}
}

// tryGrantAndStart attempts to grant bus ownership and forward the
// START request to the adapter using RequestStart (non-blocking).
//
// P3 rearchitecture: instead of calling StartArbitration (which holds
// readMu and blocks readLoop), we call RequestStart (which only holds
// writeMu) and register a pendingStart. The adapter's STARTED/FAILED
// response is handled asynchronously by readLoop via
// handleArbitrationResponse. This ensures passive observers continue
// to receive bus bytes during gateway arbitration.
//
// Guard: only one pending START at a time. If pendingStart is non-nil,
// this method is a no-op — the next tryGrantAndStart will fire after
// the current one resolves.
func (m *Mux) tryGrantAndStart() {
	// P1 fix (#3062924968): serialize the pendingStart guard, the
	// arb.tryGrant() dequeue, and the pendingStart assignment in a
	// single stateMu critical section.  Without this, two concurrent
	// callers (readLoop + RemoveSession goroutine) can both pass the
	// nil-guard, dequeue different requests, and one overwrites the
	// other's pendingStart — leaving the first waiter without a
	// terminal result.
	//
	// Lock order: stateMu → arb.mu (tryGrant acquires arb.mu
	// internally).  No path holds arb.mu then acquires stateMu,
	// so this is ABBA-safe.
	m.stateMu.Lock()
	if m.pendingStart != nil {
		m.logger.Printf("adaptermux: tryGrantAndStart skipped — pendingStart already set for session %d", m.pendingStart.sessionID)
		m.stateMu.Unlock()
		return
	}

	sessionID, initiator, notify, granted := m.arb.tryGrant()
	if !granted {
		m.stateMu.Unlock()
		return
	}

	m.pendingStart = &pendingStartState{
		sessionID: sessionID,
		initiator: initiator,
		notify:    notify,
	}
	m.stateMu.Unlock()

	// Forward START to adapter via non-blocking RequestStart.
	m.connMu.Lock()
	tr := m.upstream
	m.connMu.Unlock()

	if requester, ok := tr.(arbitrationRequester); ok {
		m.logger.Printf("adaptermux: RequestStart(0x%02X) sent for session %d", initiator, sessionID)
		if err := requester.RequestStart(initiator); err != nil {
			m.logger.Printf("adaptermux: RequestStart failed for session %d: %v", sessionID, err)
			// P1 fix: only send failure if we still own the pending slot.
			// cancelPendingStart (session disconnect / cancel on another
			// goroutine) may have already cleared m.pendingStart and sent
			// on notify while RequestStart was in progress. A second send
			// on the cap-1 channel would block forever, pinning readLoop.
			m.stateMu.Lock()
			if m.pendingStart != nil && m.pendingStart.notify == notify {
				m.pendingStart = nil
				m.stateMu.Unlock()
				notify <- startResult{granted: false, initiator: initiator, err: err}
			} else {
				// RequestStart failed AND pending was already cancelled.
				// Since the adapter never received the START, no stale
				// response will arrive — decrement absorb counter so the
				// next real arbitration response is not incorrectly consumed.
				if m.pendingStartAbsorb > 0 {
					m.pendingStartAbsorb--
				}
				m.stateMu.Unlock()
			}
			return
		}
	} else if starter, ok := tr.(interface {
		StartArbitration(byte) error
	}); ok {
		// Fallback for transports that only implement the blocking
		// StartArbitration (e.g. ENS or test mocks without RequestStart).
		if err := starter.StartArbitration(initiator); err != nil {
			m.logger.Printf("adaptermux: START arbitration failed for session %d: %v", sessionID, err)
			// P1 fix (#3063005909): only send failure if we still own
			// the pending slot.  cancelPendingStart may have cleared
			// m.pendingStart and already sent a failure on notify while
			// StartArbitration was blocking.  A second send on the
			// cap-1 channel would block the caller indefinitely.
			m.stateMu.Lock()
			if m.pendingStart != nil && m.pendingStart.notify == notify {
				m.pendingStart = nil
				m.stateMu.Unlock()
				notify <- startResult{granted: false, initiator: initiator, err: err}
			} else {
				if m.pendingStartAbsorb > 0 {
					m.pendingStartAbsorb--
				}
				m.stateMu.Unlock()
			}
			return
		}
		// Blocking path: adapter already confirmed — handle inline.
		// P1 fix (#3063005909): verify ownership before completing.
		// cancelPendingStart may have run while StartArbitration was
		// blocking, clearing pendingStart and sending a failure on
		// notify.  Completing here would double-send on the cap-1
		// channel and re-grant the bus to a cancelled session.
		m.stateMu.Lock()
		if m.pendingStart == nil || m.pendingStart.notify != notify {
			// Already cancelled — don't double-send.
			if m.pendingStartAbsorb > 0 {
				m.pendingStartAbsorb--
			}
			m.stateMu.Unlock()
			return
		}
		m.pendingStart = nil
		m.stateMu.Unlock()
		m.completeArbitrationGrant(sessionID, initiator, notify)
		return
	} else {
		m.logger.Printf("adaptermux: transport does not support arbitration")
		m.stateMu.Lock()
		if m.pendingStart != nil && m.pendingStart.notify == notify {
			m.pendingStart = nil
		}
		m.stateMu.Unlock()
		notify <- startResult{granted: false, initiator: initiator, err: errors.New("adaptermux: transport does not support arbitration")}
	}
}

// completeArbitrationGrant finalizes a successful arbitration grant.
// Used by the blocking StartArbitration fallback path and by
// handleArbitrationResponse on STARTED.
func (m *Mux) completeArbitrationGrant(sessionID uint64, initiator byte, notify chan startResult) {
	// Verify session is still alive before confirming ownership
	// (P2 TOCTOU fix: liveness check + confirmOwnership under same lock).
	if sessionID != gatewaySessionID {
		m.sessionsMu.Lock()
		_, alive := m.sessions[sessionID]
		if alive {
			m.arb.confirmOwnership(sessionID, initiator)
		}
		m.sessionsMu.Unlock()
		if !alive {
			m.logger.Printf("adaptermux: session %d disconnected during START, discarding grant", sessionID)
			notify <- startResult{granted: false, initiator: initiator, err: errors.New("session disconnected")}
			return
		}
	} else {
		m.arb.confirmOwnership(sessionID, initiator)
	}

	m.stateMu.Lock()
	m.phase.startRequest()
	m.busDirty = false
	m.busOwned = time.Now()
	if sessionID == gatewaySessionID {
		m.gatewayEcho.markRequestStart()
	}
	m.stateMu.Unlock()

	// Mark external session echo tracker for request start.
	if sessionID != gatewaySessionID {
		m.sessionsMu.Lock()
		if sess, ok := m.sessions[sessionID]; ok {
			sess.echoTracker.markRequestStart()
		}
		m.sessionsMu.Unlock()
	}

	// Notify requester of success.
	notify <- startResult{granted: true, initiator: initiator}
}

// handleArbitrationResponse processes a STARTED or FAILED event from
// the adapter, resolving the pending START registered by tryGrantAndStart.
func (m *Mux) handleArbitrationResponse(started bool, data byte) {
	m.logger.Printf("adaptermux: arbitration response started=%v data=0x%02X", started, data)
	m.stateMu.Lock()

	// Absorb stale responses from cancelled RequestStart calls.
	// cancelPendingStart increments pendingStartAbsorb when it clears a
	// pending request, because the adapter will still deliver STARTED or
	// FAILED for that old RequestStart. Without this, a stale FAILED
	// (which carries the WINNER's address, not the loser's, making
	// initiator matching impossible) would incorrectly fail a newer
	// pending request.
	if m.pendingStartAbsorb > 0 {
		m.pendingStartAbsorb--
		m.stateMu.Unlock()
		kind := "FAILED"
		if started {
			kind = "STARTED"
		}
		m.logger.Printf("adaptermux: absorbed stale %s from cancelled request (data=0x%02X)", kind, data)
		return
	}

	pending := m.pendingStart
	if pending == nil {
		m.stateMu.Unlock()
		m.logger.Printf("adaptermux: stale arbitration response (no pending START)")
		return
	}

	if started {
		// Validate the STARTED response belongs to our pending request.
		// A stale STARTED from a cancelled request can arrive after a new
		// request is pending — do not grant ownership in that case.
		if data != pending.initiator {
			// Do NOT clear pendingStart — our real response hasn't arrived yet.
			m.stateMu.Unlock()
			m.logger.Printf("adaptermux: stale STARTED for initiator 0x%02X (pending 0x%02X), ignoring", data, pending.initiator)
			return
		}
		m.pendingStart = nil
		m.stateMu.Unlock()
		m.completeArbitrationGrant(pending.sessionID, pending.initiator, pending.notify)
	} else {
		m.pendingStart = nil
		m.stateMu.Unlock()
		pending.notify <- startResult{granted: false, initiator: data}
	}

	// After resolving, check if more requests are pending.
	if m.arb.hasPending() {
		m.tryGrantAndStart()
	}
}

// cancelPendingStart cancels an in-flight pending START if it belongs
// to the given session. Called by handleStart on SYN cancel (P1 fix:
// START cancel must also clear pending adapter-level START).
func (m *Mux) cancelPendingStart(sessionID uint64) {
	m.stateMu.Lock()
	if m.pendingStart != nil && m.pendingStart.sessionID == sessionID {
		pending := m.pendingStart
		m.pendingStart = nil
		// The adapter will still send STARTED or FAILED for the cancelled
		// RequestStart. Increment absorb counter so handleArbitrationResponse
		// discards that stale response instead of failing a newer request.
		m.pendingStartAbsorb++
		m.stateMu.Unlock()
		pending.notify <- startResult{granted: false, initiator: pending.initiator}
		return
	}
	m.stateMu.Unlock()
}

// sendLoop processes send requests from the active path and external sessions.
func (m *Mux) sendLoop() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case req := <-m.activeSendCh:
			err := m.doSend(req.sessionID, req.data)
			req.result <- err
		}
	}
}

// doSend writes a byte to the adapter for the given session.
func (m *Mux) doSend(sessionID uint64, data byte) error {
	if !m.arb.isOwner(sessionID) {
		return errNotBusOwner
	}

	m.connMu.Lock()
	tr := m.upstream
	m.connMu.Unlock()

	if tr == nil {
		return errNotConnected
	}

	// Record echo expectation.
	if sessionID == gatewaySessionID {
		m.stateMu.Lock()
		m.gatewayEcho.recordSent(data)
		m.busDirty = true
		m.stateMu.Unlock()
	} else {
		m.sessionsMu.Lock()
		if sess, ok := m.sessions[sessionID]; ok {
			sess.echoTracker.recordSent(data)
		}
		m.sessionsMu.Unlock()
		m.stateMu.Lock()
		m.busDirty = true
		m.stateMu.Unlock()
	}

	_, err := tr.Write([]byte{data})
	if err != nil {
		// Rollback echo expectation on send failure.
		if sessionID == gatewaySessionID {
			m.stateMu.Lock()
			m.gatewayEcho.rollbackSent()
			m.stateMu.Unlock()
		} else {
			m.sessionsMu.Lock()
			if sess, ok := m.sessions[sessionID]; ok {
				sess.echoTracker.rollbackSent()
			}
			m.sessionsMu.Unlock()
		}
		return fmt.Errorf("%w: %v", errAdapterWrite, err)
	}

	return nil
}

// emitPassive delivers a passive event to the callback.
// Must be called WITHOUT stateMu held to avoid re-entrancy deadlock.
func (m *Mux) emitPassive(event PassiveEvent) {
	m.passiveMu.Lock()
	fn := m.passiveCallback
	m.passiveMu.Unlock()
	if fn != nil {
		fn(event)
	}
}

// nextSessionID generates a unique session ID for external sessions.
func (m *Mux) nextSessionID() uint64 {
	return m.sessionSeq.Add(1)
}

// deliverToSessions delivers a non-SYN byte to external sessions with
// per-session echo suppression. Uses sessionsMu write lock because
// matchEcho mutates the echo tracker (HIGH-7 data race fix).
func (m *Mux) deliverToSessions(symbol byte, currentOwner uint64, hasOwner bool, now time.Time) {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()

	for _, sess := range m.sessions {
		if hasOwner && sess.id == currentOwner {
			result, flushed := sess.echoTracker.matchEcho(symbol)
			if result == echoMatchSuppressed {
				continue
			}
			// Deliver flushed bytes on mismatch (MEDIUM-2 fix).
			if result == echoMatchFlushed {
				for _, b := range flushed {
					sess.deliverReceived(b)
				}
			}
		}
		sess.deliverReceived(symbol)
	}
}

// deliverSYNToSessions delivers a SYN to all external sessions.
func (m *Mux) deliverSYNToSessions(now time.Time) {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()

	for _, sess := range m.sessions {
		sess.deliverReceived(protocol.SymbolSyn)
	}
}

// broadcastResetToSessions sends a RESETTED event to all external sessions.
// Carries upstreamFeatures so external clients see consistent feature
// signaling on reset boundaries (not 0x00).
func (m *Mux) broadcastResetToSessions() {
	features := byte(m.upstreamFeatures.Load())

	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()

	for _, sess := range m.sessions {
		sess.deliverReset(features)
	}
}

// isNetTimeout reports whether err is a net.Error timeout (read deadline
// exceeded). These are transient idle conditions on a quiet bus, not
// connection failures.
func isNetTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
