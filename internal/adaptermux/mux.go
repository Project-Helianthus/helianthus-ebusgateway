package adaptermux

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

const infoCacheInterRequestDelay = 200 * time.Millisecond

const (
	infoBootstrapIDsEnv = "HELIANTHUS_INFO_BOOTSTRAP_IDS"
	infoDelayMSEnv      = "HELIANTHUS_INFO_DELAY_MS"
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
	// Default: 10s.
	MaxOwnershipDuration time.Duration

	// IdleReleaseGrace is the grace period after bus acquisition before
	// idle SYN can release ownership. Default: 200ms.
	IdleReleaseGrace time.Duration

	// StartDeadline is the maximum time to wait for the adapter to
	// respond with STARTED/FAILED after a START request. Default: 5s.
	// If the adapter does not respond within this duration, the pending
	// start is cleared and the session is notified of failure (AM8).
	StartDeadline time.Duration

	// BlackholeThreshold is the duration of consecutive read timeouts
	// (after the bus was previously active) before triggering a TCP
	// blackhole reconnect. Default: 30s.
	//
	// The previous implementation counted timeout iterations
	// (consecutiveTimeouts > 150) which coupled the threshold to the
	// ReadTimeout value. A duration-based check decouples the two:
	// changing ReadTimeout no longer silently changes when blackhole
	// detection fires.
	BlackholeThreshold time.Duration

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
		// 10s is strictly larger than any request timeout (5s for B524
		// probes, 2s semantic default) to prevent the ownership guard
		// from firing before a legitimate request completes.
		c.MaxOwnershipDuration = 10 * time.Second
	}
	if c.IdleReleaseGrace == 0 {
		// 200ms is enough for any eBUS transaction to complete (~100ms for
		// the longest B524 frame) while releasing promptly after each scan
		// probe. The wire phase is not advanced during gateway ownership,
		// so there's no premature idle/WaitCmdAck issue.
		c.IdleReleaseGrace = 200 * time.Millisecond
	}
	if c.StartDeadline <= 0 {
		c.StartDeadline = 5 * time.Second
	}
	if c.BlackholeThreshold <= 0 {
		c.BlackholeThreshold = 30 * time.Second
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
	sessionID   uint64
	initiator   byte
	notify      chan startResult
	deadline    *time.Timer // AM8: fires if adapter doesn't respond before cfg.StartDeadline
	blockingArb bool        // true when using blocking StartArbitration fallback
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
	stateMu            sync.Mutex
	phase              wirePhaseTracker
	arb                *arbitrator
	busOwned           time.Time          // when current owner acquired the bus
	pendingStart       *pendingStartState // in-flight START awaiting STARTED/FAILED
	pendingStartAbsorb int                // stale adapter responses to absorb (FAILED/STARTED from cancelled requests)
	// Blocking StartArbitration tracking. blockingArbGen is monotonically
	// increasing (never reset to 0 or reused) — reconnect/handleReset
	// bump it forward so any stale goroutine's captured gen no longer
	// matches. blockingArbActive indicates whether any blocking goroutine
	// is currently considered in-flight (owns the transport). Only a
	// goroutine whose gen matches the current blockingArbGen may clear
	// blockingArbActive; stale goroutines from prior generations cannot.
	blockingArbGen    uint64 // monotonic generation counter, never reused
	blockingArbActive bool   // true while a blocking goroutine owns the transport

	// gatewayTxnActive reports whether bus.Send is actively consuming
	// activeCh for a gateway transaction. Separate from ownership:
	// ownership can outlive the transaction up to IdleReleaseGrace.
	// Only when gatewayTxnActive is true are bytes delivered to
	// activeCh. Set in completeArbitrationGrant for the gateway,
	// cleared when ownership is released (transaction complete,
	// NACK, SYN timeout, or idle grace expired).
	gatewayTxnActive bool

	// activeTxn is the diagnostics snapshot for the current/last gateway
	// transaction. Updated under stateMu. Exposed via ActiveTxnSnapshot()
	// for tests and production observability. Bounded — never grows.
	activeTxn activeTxnDiag

	// synDiag is a bounded ring of SYN events observed while gateway owns
	// the bus. Used to confirm/exclude the "final SYN echo consumed by
	// onSYNLocked before Send sees the terminator" hypothesis. Protected
	// by stateMu. Bounded by synDiagRingCap — never grows.
	synDiag synDiagRing

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

	// INFO cache — populated once after INIT, served to active path
	// and sessions without hitting the upstream transport's readMu.
	infoCacheMu sync.RWMutex
	infoCache   map[transport.AdapterInfoID][]byte

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
	// closing is set to true at the start of Close() before m.wg.Wait().
	// Gates tryGrantAndStart's wg.Add(1) for the blocking-path goroutine
	// to prevent a WaitGroup misuse panic when a timer callback /
	// readLoop / RemoveSession races with shutdown.
	closing atomic.Bool
	// closeMu serializes the check-of-closing-and-wg.Add critical section
	// in tryGrantAndStart's blocking path with Close()'s closing.Store(true)
	// that precedes m.wg.Wait(). Codex PR #502 P2: without this mutex,
	// tryGrantAndStart can observe closing=false, then Close sets closing
	// and reaches Wait, and the stale path still calls wg.Add(1) — panic
	// ("sync: WaitGroup misuse") or leak. The mutex is narrow and only
	// held across the guard read and the wg.Add/goroutine launch.
	closeMu sync.Mutex
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
		infoCache:    make(map[transport.AdapterInfoID][]byte),
		activeSendCh: make(chan sendRequest, 256),  // AM49: increased from 16 for burst tolerance
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
		// C1 / PR #502 P2: signal shutdown BEFORE any Wait(). tryGrantAndStart
		// checks this flag to avoid m.wg.Add(1) on the blocking-path goroutine
		// while m.wg.Wait() is running — that would panic with "sync:
		// WaitGroup misuse: Add called concurrently with Wait".
		//
		// The store is performed under closeMu to serialize with
		// tryGrantAndStart's gate-then-Add critical section. Any goroutine
		// that saw closing=false before we took closeMu has already run
		// wg.Add(1) by the time we release the mutex; any goroutine that
		// takes closeMu after us sees closing=true and skips the Add.
		// closeMu is released BEFORE m.wg.Wait() to avoid blocking
		// tryGrantAndStart callers that would otherwise queue behind Wait.
		m.closeMu.Lock()
		m.closing.Store(true)
		m.closeMu.Unlock()
		if m.cancel != nil {
			m.cancel()
		}

		// Cancel in-flight pending START if any.
		m.stateMu.Lock()
		pendingToCancel := m.pendingStart
		m.pendingStart = nil
		m.pendingStartAbsorb = 0
		if pendingToCancel != nil && pendingToCancel.deadline != nil {
			pendingToCancel.deadline.Stop() // AM8: cancel deadline timer
		}
		m.stateMu.Unlock()
		if pendingToCancel != nil {
			// AM53: guarded send to avoid blocking Close if nobody reads notify.
			select {
			case pendingToCancel.notify <- startResult{granted: false, initiator: pendingToCancel.initiator, err: errors.New("adaptermux: closed")}:
			case <-time.After(1 * time.Second):
				m.logger.Printf("adaptermux: Close: timed out sending to pendingStart.notify (AM53)")
			}
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

		// AM51: close sessions concurrently to avoid O(N * 5s) shutdown.
		var sessWg sync.WaitGroup
		for _, sess := range toClose {
			m.arb.removeSession(sess.id)
			sessWg.Add(1)
			go func(s *session) {
				defer sessWg.Done()
				s.close()
			}(sess)
		}
		sessWg.Wait()

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

	// Create ENH/ENS transport in two phases:
	// Phase 1: 2s read timeout for INIT + INFO queries (adapter needs ~200ms).
	// Phase 2: replace with cfg.ReadTimeout for the readLoop idle tick.
	//
	// The parser-state concern is theoretical: RequestInfo holds readMu
	// exclusively and the readLoop hasn't started, so no bus bytes arrive
	// during the setup phase. After INFO completes, the parser is clean.
	newTransport := func(readTimeout time.Duration) transport.RawTransport {
		switch m.cfg.Protocol {
		case "ens":
			return transport.NewENSTransport(conn, readTimeout, m.cfg.WriteTimeout)
		case "enh", "":
			return transport.NewENHTransport(conn, readTimeout, m.cfg.WriteTimeout)
		default:
			return nil
		}
	}

	const infoTimeout = 2 * time.Second
	setupTr := newTransport(infoTimeout)
	if setupTr == nil {
		_ = conn.Close()
		return fmt.Errorf("adaptermux: unsupported protocol %q (expected \"enh\" or \"ens\")", m.cfg.Protocol)
	}

	m.conn = conn
	m.upstream = setupTr

	// Perform INIT handshake — fatal if transport implements Init.
	// Retry up to 5 times with increasing stabilization delays between
	// retries (200ms, 400ms, 600ms, 800ms).
	// The adapter's eBUS transceiver needs time after TCP accept to
	// initialize; features=0x00 on first attempt indicates the adapter
	// wasn't ready. The standalone proxy had a similar 200ms delay.
	const requestedFeatures byte = 0x01
	if initer, ok := setupTr.(interface{ Init(byte) (byte, error) }); ok {
		var features byte
		var initErr error
		initOK := false
		for attempt := 0; attempt < 5; attempt++ {
			if attempt > 0 {
				// Exponential-ish backoff: 200ms, 400ms, 600ms, 800ms
				backoff := time.Duration(200*(attempt)) * time.Millisecond
				select {
				case <-time.After(backoff):
				case <-m.ctx.Done():
					_ = conn.Close()
					m.conn = nil     // AM32: clear stale reference on ctx cancel
					m.upstream = nil // AM32: clear stale transport on ctx cancel
					return m.ctx.Err()
				}
			}
			features, initErr = initer.Init(requestedFeatures)
			if initErr != nil {
				m.logger.Printf("adaptermux: INIT attempt %d failed: %v (retrying)", attempt+1, initErr)
				continue
			}
			if features&requestedFeatures == requestedFeatures {
				initOK = true
				break // adapter confirmed requested features
			}
			// features=0x00 means adapter wasn't ready yet — retry
			m.logger.Printf("adaptermux: INIT attempt %d: features=0x%02X (want 0x%02X), retrying", attempt+1, features, requestedFeatures)
		}
		if !initOK && initErr == nil {
			// All attempts returned features=0x00 but no error.
			// Accept degraded mode — the adapter may not support features.
			m.logger.Printf("adaptermux: INIT: adapter does not confirm features=0x%02X, proceeding with features=0x%02X", requestedFeatures, features)
		} else if initErr != nil && !initOK {
			_ = conn.Close()
			m.conn = nil     // AM34: clear stale reference after INIT failure
			m.upstream = nil // AM34: clear stale transport after INIT failure
			return fmt.Errorf("adaptermux: INIT handshake failed after 5 attempts: %w", initErr)
		}
		m.upstreamFeatures.Store(uint32(features))
		if initOK {
			m.logger.Printf("adaptermux: INIT handshake succeeded, upstream features=0x%02X", features)
		} else {
			m.logger.Printf("adaptermux: INIT handshake degraded, upstream features=0x%02X", features)
		}

		// Post-INIT stabilization: give the adapter time to settle
		// before sending INFO queries. The adapter sends a spontaneous
		// RESETTED after processing INIT; without this delay, the
		// RESETTED arrives during the INFO exchange and corrupts it.
		// 500ms is empirically sufficient (200ms was not enough).
		select {
		case <-time.After(500 * time.Millisecond):
		case <-m.ctx.Done():
			_ = conn.Close()
			m.conn = nil     // AM32: clear stale reference on ctx cancel
			m.upstream = nil // AM32: clear stale transport on ctx cancel
			return m.ctx.Err()
		}
	}

	// Populate INFO cache with the 2s-timeout transport.
	if err := m.populateInfoCache(setupTr); err != nil {
		_ = conn.Close()
		m.conn = nil     // clear stale reference on ctx cancel
		m.upstream = nil // clear stale transport on ctx cancel
		return err
	}

	// Phase 2: replace with cfg.ReadTimeout transport for readLoop.
	// RequestInfo needs the longer setup timeout, but steady-state
	// readLoop timing drives quiet-bus arbitration progress and must
	// continue honoring cfg.ReadTimeout.
	m.upstream = newTransport(m.cfg.ReadTimeout)

	return nil
}

// populateInfoCache queries the upstream transport for INFO metadata
// and stores responses in the mux-level cache. Sessions and the active
// path read from the cache instead of touching the upstream transport,
// avoiding readMu contention during normal operation.
//
// Called from connect() after a successful INIT handshake (connMu held
// by caller). The upstream transport must support InfoRequester; if it
// does not, the cache is cleared and CachedInfo returns an error.
//
// XR_INFO_CACHE_SNAPSHOT / AM29: INFO cache is intentionally a startup
// snapshot. The cache is repopulated on each reconnect via connect() and is
// warmed before the adapter is exposed to downstream consumers.
//
// Design rationale: the ENH transport's RequestInfo holds readMu
// exclusively. During normal operation, readLoop also holds readMu to
// receive bus bytes. Refreshing INFO on-demand would either require
// pausing readLoop (breaking observer continuity) or a second TCP
// connection (doubling adapter load). Neither is acceptable. The
// startup-snapshot model accepts stale telemetry in exchange for
// zero readMu contention during steady state.
//
// INFO version (0x00) is always queried first. The startup cache stores the
// initial identity/configuration snapshot plus the first telemetry sample
// (temperature/supply_voltage/bus_voltage) before the transport is handed to
// consumers. reset_info is fetched once per TCP session when supported.
// wifi_rssi is intentionally excluded on this adapter family. Startup
// requests are capability-gated and paced slightly to avoid a tight
// control-frame burst against fragile adapters.
func (m *Mux) populateInfoCache(tr transport.RawTransport) error {
	infoReq, ok := tr.(transport.InfoRequester)
	if !ok {
		m.clearInfoCache()
		return nil
	}

	cache := make(map[transport.AdapterInfoID][]byte)

	// Try version first — if it fails, adapter doesn't support INFO.
	// Called via the setup transport (2s timeout); connect() replaces
	// m.upstream with a fast-timeout transport for readLoop afterward.
	data, err := infoReq.RequestInfo(transport.AdapterInfoVersion)
	if err != nil {
		m.logger.Printf("adaptermux: INFO not supported by adapter: %v", err)
		m.clearInfoCache()
		return nil
	}
	cache[transport.AdapterInfoVersion] = append([]byte(nil), data...)
	version, err := transport.ParseAdapterVersion(data)
	if err != nil {
		m.logger.Printf("adaptermux: INFO version parse failed: %v", err)
		m.infoCacheMu.Lock()
		m.infoCache = cache
		m.infoCacheMu.Unlock()
		m.logger.Printf("adaptermux: INFO cache populated (%d entries)", len(cache))
		return nil
	}

	bootstrapIDs := []transport.AdapterInfoID{
		transport.AdapterInfoHardwareConf,
		transport.AdapterInfoHardwareID,
		transport.AdapterInfoResetInfo,
		transport.AdapterInfoTemperature,
		transport.AdapterInfoSupplyVolt,
		transport.AdapterInfoBusVoltage,
	}
	if overridden, ok := infoBootstrapIDsOverride(); ok {
		bootstrapIDs = overridden
	}
	interRequestDelay := infoCacheInterRequestDelay
	if overridden, ok := infoBootstrapDelayOverride(); ok {
		interRequestDelay = overridden
	}
	supportedIDs := bootstrapIDs[:0]
	for _, id := range bootstrapIDs {
		if !version.SupportsInfoID(id) {
			continue
		}
		supportedIDs = append(supportedIDs, id)
	}
	for idx, id := range supportedIDs {
		data, err := infoReq.RequestInfo(id)
		if err == nil {
			cache[id] = append([]byte(nil), data...)
		}
		if idx < len(supportedIDs)-1 {
			if err := m.waitInfoBootstrapDelay(interRequestDelay, cache); err != nil {
				return err
			}
		}
	}

	m.infoCacheMu.Lock()
	m.infoCache = cache
	m.infoCacheMu.Unlock()

	m.logger.Printf("adaptermux: INFO cache populated (%d entries)", len(cache))
	return nil
}

func (m *Mux) waitInfoBootstrapDelay(delay time.Duration, cache map[transport.AdapterInfoID][]byte) error {
	if m.ctx == nil {
		time.Sleep(delay)
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-m.ctx.Done():
		m.infoCacheMu.Lock()
		m.infoCache = cache
		m.infoCacheMu.Unlock()
		m.logger.Printf("adaptermux: INFO cache populated (%d entries)", len(cache))
		return m.ctx.Err()
	}
}

func infoBootstrapIDsOverride() ([]transport.AdapterInfoID, bool) {
	raw := strings.TrimSpace(os.Getenv(infoBootstrapIDsEnv))
	if raw == "" {
		return nil, false
	}
	parts := strings.Split(raw, ",")
	ids := make([]transport.AdapterInfoID, 0, len(parts))
	for _, part := range parts {
		token := strings.TrimSpace(part)
		if token == "" {
			continue
		}
		value, err := strconv.ParseUint(token, 0, 8)
		if err != nil {
			continue
		}
		ids = append(ids, transport.AdapterInfoID(value))
	}
	if len(ids) == 0 {
		return nil, false
	}
	return ids, true
}

func infoBootstrapDelayOverride() (time.Duration, bool) {
	raw := strings.TrimSpace(os.Getenv(infoDelayMSEnv))
	if raw == "" {
		return 0, false
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms < 0 {
		return 0, false
	}
	return time.Duration(ms) * time.Millisecond, true
}

// clearInfoCache removes all cached INFO entries so that CachedInfo
// returns an error until the cache is repopulated.
func (m *Mux) clearInfoCache() {
	m.infoCacheMu.Lock()
	m.infoCache = nil
	m.infoCacheMu.Unlock()
}

// arbitrationSendsSource checks the upstream transport to determine
// whether the adapter already places the initiator byte on the wire
// during START arbitration. Returns true for ENH/ENS adapters.
//
// Used by both the wire phase tracker (completeArbitrationGrant) and
// activeTransport.ArbitrationSendsSource (for bus.sendTransaction).
// External proxy sessions need correct phase tracking via
// startRequestWithSource when the upstream sends SRC. For the gateway
// session, onReceived skips phase.advance during ownership, so the
// pre-loaded SRC value is harmless but consistent.
func (m *Mux) arbitrationSendsSource() bool {
	m.connMu.Lock()
	tr := m.upstream
	m.connMu.Unlock()
	if tr == nil {
		return false
	}
	if checker, ok := tr.(interface{ ArbitrationSendsSource() bool }); ok {
		return checker.ArbitrationSendsSource()
	}
	return false
}

// bytesAreUnescaped reports whether the upstream transport delivers
// pre-unescaped (logical) bytes. ENH/ENS transports return true: they
// decode wire-level {0xA9,0x01}→0xAA / {0xA9,0x00}→0xA9 before surfacing
// bytes. Protocol.Bus uses this via EscapeAware to skip escape expansion
// on read and to avoid double-escaping on write.
//
// Without this, protocol.Bus treats 0xAA as SYN (bus idle) on the wire
// even when it is a legitimate data byte on the logical layer.
func (m *Mux) bytesAreUnescaped() bool {
	m.connMu.Lock()
	tr := m.upstream
	m.connMu.Unlock()
	if tr == nil {
		return false
	}
	if ea, ok := tr.(transport.EscapeAware); ok {
		return ea.BytesAreUnescaped()
	}
	return false
}

// CachedInfo returns a copy of the cached INFO response for the given
// ID. Returns an error if the cache is empty (adapter doesn't support
// INFO) or the requested ID was not cached.
func (m *Mux) CachedInfo(id transport.AdapterInfoID) ([]byte, error) {
	m.infoCacheMu.RLock()
	defer m.infoCacheMu.RUnlock()

	if len(m.infoCache) == 0 {
		return nil, errors.New("adaptermux: INFO not available")
	}
	data, ok := m.infoCache[id]
	if !ok {
		return nil, fmt.Errorf("adaptermux: INFO id 0x%02X not cached", byte(id))
	}
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

// reconnect tears down the current connection and re-establishes it.
// Called from the read loop on adapter disconnect.
func (m *Mux) reconnect() error {
	// Invalidate INFO cache immediately so CachedInfo returns errors
	// during the disconnect window rather than serving stale data.
	m.clearInfoCache()

	m.emitPassive(PassiveEvent{
		Kind:       PassiveEventDisconnected,
		ObservedAt: time.Now(),
	})

	// Reset multiplexer state.
	m.stateMu.Lock()
	m.phase.reset(wirePhaseIdle)
	m.arb.forceRelease()
	m.gatewayEcho.reset()
	if m.gatewayTxnActive {
		m.gatewayTxnActive = false
		m.recordGatewayInactive(ReasonReconnect)
	}
	// Bump gen forward (monotonic) — any in-flight goroutine's captured
	// arbGen will no longer match, so it cannot clear blockingArbActive.
	// Clear blockingArbActive here because the transport is being replaced.
	m.blockingArbGen++
	m.blockingArbActive = false
	pendingToCancel := m.pendingStart
	m.pendingStart = nil
	m.pendingStartAbsorb = 0
	if pendingToCancel != nil && pendingToCancel.deadline != nil {
		pendingToCancel.deadline.Stop() // AM8: cancel deadline timer
	}
	m.stateMu.Unlock()

	// Cancel in-flight pending START if any.
	if pendingToCancel != nil {
		// AM53: guarded send to avoid blocking reconnect if nobody reads notify.
		select {
		case pendingToCancel.notify <- startResult{granted: false, initiator: pendingToCancel.initiator, err: errors.New("adaptermux: adapter disconnected")}:
		case <-time.After(1 * time.Second):
			m.logger.Printf("adaptermux: reconnect: timed out sending to pendingStart.notify (AM53)")
		}
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

	// AM27: duration-based TCP blackhole detection.
	// Track when the first timeout in the current streak started and
	// when we last received actual data. A bus that has NEVER sent
	// data is legitimately quiet — don't treat consecutive timeouts
	// as a TCP blackhole in that case.
	//
	// XR_BLACKHOLE_DURATION: uses cfg.BlackholeThreshold (default 30s)
	// instead of counting timeout iterations. This decouples the
	// detection threshold from ReadTimeout — changing ReadTimeout no
	// longer silently changes when blackhole reconnect fires.
	var firstTimeoutTime time.Time // zero until a streak starts
	var lastDataTime time.Time

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
				// AM27: only treat as TCP blackhole if we were actively
				// receiving data before the timeout streak. A bus that
				// has NEVER sent data within this session is legitimately
				// quiet — not a blackhole.
				if !lastDataTime.IsZero() && firstTimeoutTime.IsZero() {
					firstTimeoutTime = time.Now()
				}

				// AM27/XR_BLACKHOLE_DURATION: detect TCP blackhole after
				// cfg.BlackholeThreshold (default 30s) of consecutive
				// timeouts since the bus was last active.
				if !firstTimeoutTime.IsZero() && time.Since(firstTimeoutTime) > m.cfg.BlackholeThreshold {
					m.logger.Printf("adaptermux: consecutive timeouts for %v (threshold %v), triggering reconnect (AM27)", time.Since(firstTimeoutTime).Round(time.Millisecond), m.cfg.BlackholeThreshold)
					firstTimeoutTime = time.Time{}
					lastDataTime = time.Time{} // reset after reconnect so quiet bus doesn't re-trigger
					if reconnErr := m.reconnect(); reconnErr != nil {
						m.logger.Printf("adaptermux: reconnect gave up: %v", reconnErr)
						return
					}
					continue
				}

				// Enforce ownership timeout on quiet bus — onReceived
				// won't run to check MaxOwnershipDuration.
				quietBusTimedOut := false
				m.stateMu.Lock()
				ownerID, _, hasOwner := m.arb.owner()
				if hasOwner && !m.busOwned.IsZero() &&
					time.Since(m.busOwned) > m.cfg.MaxOwnershipDuration {
					m.arb.releaseOwnership(ownerID)
					m.gatewayEcho.reset()
					if ownerID == gatewaySessionID && m.gatewayTxnActive {
						m.gatewayTxnActive = false
						m.recordGatewayInactive(ReasonMaxOwnership)
					}
					quietBusTimedOut = true
				}
				m.stateMu.Unlock()

				// AM11: clear external session echo trackers on ownership timeout.
				if quietBusTimedOut {
					m.resetAllSessionEchoes()
				}

				// On a quiet bus no SYN or transaction-complete events
				// reach onReceived, so tryGrantAndStart is never called.
				// Drain pending START requests here to prevent stalls.
				if m.arb.hasPending() {
					m.tryGrantAndStart()
				}
				continue
			}
			if errors.Is(err, ebuserrors.ErrInvalidPayload) {
				m.logger.Printf("adaptermux: soft parser error (no reconnect): %v", err)
				firstTimeoutTime = time.Time{}
				lastDataTime = time.Now()
				continue
			}
			m.logger.Printf("adaptermux: read error: %v", err)
			firstTimeoutTime = time.Time{}
			lastDataTime = time.Time{} // reset after reconnect so quiet bus doesn't trigger blackhole
			if reconnErr := m.reconnect(); reconnErr != nil {
				m.logger.Printf("adaptermux: reconnect gave up: %v", reconnErr)
				return
			}
			continue
		}

		firstTimeoutTime = time.Time{} // AM27: reset timeout streak on successful read
		lastDataTime = time.Now()      // AM27: track last data for blackhole detection

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
//
// Boundary invariant — 0xAA at the mux layer:
//
//	StreamEventByte{Byte: 0xAA} arriving here IS treated as SYN
//	(bus idle marker). This is correct for ENH transports: the
//	ENH parser never produces ENHResReceived(0xAA) because logical
//	0xAA data is wire-escaped as {0xA9, 0x01} and raw wire 0xAA is
//	the SYN symbol consumed by the adapter before framing.
//
//	See TestOnReceived_0xAA_WireLayerSYNInvariant and
//	TestMux_0xAA_MockTransportAlwaysSYN for the test coverage.
//	A cleaner long-term contract is an explicit StreamEventSyn
//	instead of inferring SYN from byte value; not changed here.
func (m *Mux) onReceived(symbol byte) {
	now := time.Now()

	// --- Phase 1: state update under stateMu ---
	m.stateMu.Lock()

	ownerID, _, hasOwner := m.arb.owner()

	// Skip wire phase tracking for non-SYN bytes during gateway ownership.
	// The gateway's bus.Send handles the transaction directly via echo
	// matching. Skipping advance() for data bytes prevents premature
	// WaitCmdAck → CmdNACK → idle from off-by-one byte counting.
	//
	// SYN is always processed: ownership release depends on the SYN
	// handler (SYNIdle + IdleReleaseGrace or SYNTimeout). During gateway
	// ownership, treat SYN as SYNIdle since the data-phase tracking is
	// skipped.
	var phaseEvent wirePhaseEvent
	if symbol == protocol.SymbolSyn {
		if hasOwner && ownerID == gatewaySessionID {
			// Gateway owns bus, phase tracking skipped for data bytes.
			// Treat SYN as idle so IdleReleaseGrace controls release.
			phaseEvent = wirePhaseEventSYNIdle
			m.phase.reset(wirePhaseIdle)
		} else {
			phaseEvent = m.phase.advance(symbol)
		}
	} else if !hasOwner || ownerID != gatewaySessionID {
		phaseEvent = m.phase.advance(symbol)
	}
	// AM11: track ownership timeout so we can reset external session
	// echo trackers after releasing stateMu (ABBA avoidance).
	ownershipTimedOut := false
	if hasOwner && !m.busOwned.IsZero() &&
		time.Since(m.busOwned) > m.cfg.MaxOwnershipDuration {
		m.arb.releaseOwnership(ownerID)
		m.gatewayEcho.reset()
		if ownerID == gatewaySessionID && m.gatewayTxnActive {
			m.gatewayTxnActive = false
			m.recordGatewayInactive(ReasonMaxOwnership)
		}
		hasOwner = false
		ownershipTimedOut = true
	}

	// Collect passive events under lock, emit after unlock (Issue#3 fix).
	var passiveEvents []PassiveEvent
	var shouldTryGrant bool

	if symbol == protocol.SymbolSyn {
		// Shape diag: count SYN markers seen during gateway ownership
		// regardless of whether we deliver (for classification). Must
		// run BEFORE onSYNLocked so the counter reflects the SYN that
		// triggers a possible inactive transition.
		if hasOwner && ownerID == gatewaySessionID {
			m.recordReadPrefixAndClassify(symbol)
		}
		// Runtime-soak: bus.Send returns BEFORE the trailing SYN (reads
		// are count-based, not SYN-terminated). So onSYNLocked's
		// gatewayTxnActive=false is the correct state for delivery —
		// capture AFTER to skip the trailing SYN that has no consumer.
		var preEchoSuppressed bool
		passiveEvents, shouldTryGrant, preEchoSuppressed = m.onSYNLocked(phaseEvent, ownerID, hasOwner, now)
		activeExpects := m.activePathExpectsBytes()
		if preEchoSuppressed {
			// Pre-echo SYN suppression (echo_mismatch root cause fix).
			// After completeArbitrationGrant, readLoop can read a SYN from
			// the TCP/ENH buffer BEFORE bus.Send's first Write reaches the
			// adapter. Delivering this SYN to activeCh races the real echo
			// byte — the consumer (sendRawWithEcho) then reads 0xAA in
			// place of the expected echo and emits echo_mismatch (13,904
			// events observed in production soak). When onSYNLocked sees
			// gatewayTxnActive=true && bytesRead==0, it signals us to
			// suppress the activeCh delivery; gatewayTxnActive stays true
			// and the next real echo byte completes the handshake normally.
			activeExpects = false
		}
		m.stateMu.Unlock()

		// AM11: clear external session echo trackers on ownership timeout.
		if ownershipTimedOut {
			m.resetAllSessionEchoes()
		}

		// --- Phase 2: deliver outside all locks ---
		// Flush external session echo trackers. Called outside stateMu to
		// avoid ABBA deadlock with doSend (sessionsMu→stateMu).
		// The flushed bytes are NOT emitted to passive — they were already
		// delivered live in onReceived (they're third-party from gateway's
		// perspective). Re-emitting would produce duplicates (Codex P1).
		m.flushSessionEchoTrackers()

		if activeExpects {
			// activeExpects was decided under stateMu above and implies
			// the gateway owns the bus with gatewayTxnActive=true — count
			// this enqueue so bytesDeliveredToActive reflects real
			// adapter-originated bytes delivered during this grant.
			m.deliverToActive(symbol, true)
		}
		for _, pe := range passiveEvents {
			m.emitPassive(pe)
		}
		m.deliverSYNToSessions(now)
		if shouldTryGrant {
			m.tryGrantAndStart()
		}
		return
	}

	// Non-SYN byte: suppress from passive when gateway owns the bus.
	//
	// When the gateway owns the bus, ALL received bytes belong to the
	// gateway's transaction: echoed request bytes AND the target's
	// response bytes (ACK, LEN, DATA, CRC, final ACK). Without full
	// suppression, orphaned response bytes leak to the passive path
	// and the reconstructor parses them as garbage frames (Source=0x00,
	// fake protocol IDs).
	//
	// The echo tracker still runs for internal state tracking, but its
	// result is not used for passive filtering — we suppress everything.
	isGatewayOwned := hasOwner && ownerID == gatewaySessionID
	if isGatewayOwned {
		m.gatewayEcho.matchEcho(symbol) // track echo state internally
	}
	// Soak fix: gate activeCh delivery to periods when the active path
	// is expecting bytes. Non-SYN bytes during third-party traffic
	// should not accumulate on activeCh.
	activeExpects := m.activePathExpectsBytes()
	// Codex P2: regression signal — if a non-SYN byte arrives while
	// gateway still owns the bus but gatewayTxnActive is already false
	// (post-SYN window before ownership is released), we skipped
	// delivery. Count those events so the snapshot can surface any
	// post-inactive delivery pressure.
	if isGatewayOwned && !activeExpects {
		m.activeTxn.afterInactive.Add(1)
	}

	// Shape diag: capture non-SYN read prefix + echo/non-echo class
	// while stateMu is held (prefix is a struct field, not atomic).
	// Restrict to gateway-owned traffic so third-party bytes don't
	// pollute the per-txn prefix. Includes bytes delivered to activeCh
	// as well as bytes suppressed — both are "seen" on the wire during
	// the transaction window.
	if isGatewayOwned {
		m.recordReadPrefixAndClassify(symbol)
	}

	m.stateMu.Unlock()

	// AM11: clear external session echo trackers on ownership timeout.
	if ownershipTimedOut {
		m.resetAllSessionEchoes()
	}

	// --- Phase 2: deliver outside all locks ---
	// Codex PR #502 P2: revalidate active-path gating atomically with
	// the enqueue. `activeExpects` was decided under stateMu above, but
	// between that unlock and the send below, gatewayTxnActive may have
	// flipped to false (e.g. active read-timeout / write-error /
	// context-cancel paths). Without a re-check, stale bytes can leak
	// onto activeCh and afterInactive misses them (it was computed from
	// the earlier snapshot). Re-acquire stateMu for a short critical
	// section: check + non-blocking send. activeCh is capacity-4096 and
	// deliverToActive already uses a non-blocking select, so holding
	// stateMu across the send cannot deadlock.
	if activeExpects {
		m.stateMu.Lock()
		if m.activePathExpectsBytes() {
			select {
			case m.activeCh <- activeEvent{kind: activeEventByte, b: symbol}:
				// Codex PR #502 P1: count real adapter-originated byte
				// enqueued on activeCh during gateway-owned active txn.
				// Decision was re-validated under stateMu on the line
				// above, so gatewayTxnActive is confirmed true here.
				m.activeTxn.bytesDeliveredToActive.Add(1)
			default:
				m.logger.Printf("adaptermux: active channel full, dropping byte 0x%02X", symbol)
			}
		} else if isGatewayOwned {
			// Gating flipped between snapshot and delivery — the byte
			// arrived while gateway owned the bus but the active path
			// no longer expects bytes. Record the post-inactive event
			// for diagnostics instead of enqueuing a stale byte.
			m.activeTxn.afterInactive.Add(1)
		}
		m.stateMu.Unlock()
	}

	if !isGatewayOwned {
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
		// Codex-R9: atomic release + gatewayTxnActive clear.
		// Must re-verify ownership under stateMu — between the phase
		// event and this point another goroutine may have granted a
		// new session. Only release + clear if ownerID still matches.
		reason := ReasonTransactionDone
		if phaseEvent == wirePhaseEventCmdNACK {
			reason = ReasonCmdNACK
		}
		m.stateMu.Lock()
		curOwner, _, hasCurOwner := m.arb.owner()
		if hasCurOwner && curOwner == ownerID {
			m.arb.releaseOwnership(ownerID)
			if ownerID == gatewaySessionID && m.gatewayTxnActive {
				m.gatewayTxnActive = false
				m.recordGatewayInactive(reason)
			}
		}
		m.stateMu.Unlock()
		m.tryGrantAndStart()
	}
}

// onSYNLocked handles a SYN symbol. Caller holds stateMu.
// Returns buffered passive events, whether tryGrant should be called,
// and whether this SYN is pre-echo noise that the caller must suppress
// from activeCh delivery (see echo_mismatch root-cause comment at the
// call site).
func (m *Mux) onSYNLocked(phaseEvent wirePhaseEvent, ownerID uint64, hasOwner bool, now time.Time) ([]PassiveEvent, bool, bool) {
	var passiveEvents []PassiveEvent

	// SYN-path diagnostics (bounded). Capture gwActiveBefore snapshot
	// under stateMu so the ring entry matches the decision the SYN
	// branches below make. Only record when gateway owns the bus at
	// SYN arrival — that's the hypothesis-relevant window (final-SYN
	// echo potentially consumed by onSYNLocked before Send sees the
	// frame terminator).
	wasGatewayOwned := hasOwner && ownerID == gatewaySessionID
	gwActiveBefore := m.gatewayTxnActive

	// Flush gateway echo tracker at SYN boundary. Flushed bytes are
	// confirmed gateway self-traffic — do NOT emit to passive path
	// (passive is third-party only). They are delivered to external
	// sessions via deliverSYNToSessions + deliverToSessions elsewhere.
	m.gatewayEcho.flushOnSYN()

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
	terminatorDelivered := false
	if hasOwner && ownerID == gatewaySessionID && m.gatewayTxnActive &&
		m.activeTxn.bytesDeliveredToActive.Load() > 0 {
		select {
		case m.activeCh <- activeEvent{kind: activeEventByte, b: protocol.SymbolSyn}:
			terminatorDelivered = true
			// Count the terminator SYN — it is a real adapter byte
			// enqueued while the gateway owned the bus, AM-NEW-41 spec.
			m.activeTxn.bytesDeliveredToActive.Add(1)
		default:
			m.activeTxn.terminatorDropOnFullCh.Add(1)
			m.logger.Printf("adaptermux: active channel full, dropping SYN terminator")
		}
		m.gatewayTxnActive = false
		m.recordGatewayInactive(ReasonSYNTerminator)
	}

	// Note: external session echo tracker flush is done AFTER stateMu.Unlock()
	// by the caller (flushSessionEchoTrackersOnSYN) to avoid ABBA deadlock
	// with doSend which acquires sessionsMu→stateMu.

	// Release ownership if SYN timeout.
	if phaseEvent == wirePhaseEventSYNTimeout && hasOwner {
		m.logger.Printf("adaptermux: ownership released for session %d (SYN timeout) (AM6)", ownerID)
		m.arb.releaseOwnership(ownerID)
		if ownerID == gatewaySessionID && m.gatewayTxnActive {
			m.gatewayTxnActive = false
			m.recordGatewayInactive(ReasonSYNTimeout)
		}
	}

	// Release ownership on idle SYN after grace period.
	if phaseEvent == wirePhaseEventSYNIdle && hasOwner {
		if time.Since(m.busOwned) > m.cfg.IdleReleaseGrace {
			m.logger.Printf("adaptermux: ownership released for session %d (idle grace expired) (AM6)", ownerID)
			m.arb.releaseOwnership(ownerID)
			// Codex: clear gatewayTxnActive here too — the "SYN-before-read"
			// guard above only clears when bytesRead > 0, so an abandoned
			// grant (no write, no read) keeps the flag true until idle
			// grace releases ownership. Without this clear, ownership is
			// gone but activePathExpectsBytes() still returns true,
			// routing third-party traffic into activeCh indefinitely.
			if ownerID == gatewaySessionID && m.gatewayTxnActive {
				m.gatewayTxnActive = false
				m.recordGatewayInactive(ReasonSYNIdle)
			}
		}
	}

	// SYN is always visible on passive path.
	passiveEvents = append(passiveEvents, PassiveEvent{
		Kind: PassiveEventSymbol, Symbol: protocol.SymbolSyn, ObservedAt: now,
	})

	// Pre-echo SYN suppression decision (echo_mismatch fix).
	// Computed AFTER the terminator/idle branches so that a gwActive flag
	// that was just cleared by those branches does not incorrectly trigger
	// suppression. The pre-echo case is: gateway still owns, gatewayTxnActive
	// still true, and no real adapter byte has been enqueued on activeCh
	// yet for this txn — i.e. the mux has not yet observed post-grant
	// traffic on the active path. A SYN observed in this window is buffer
	// noise that pre-dates the grant's first Write and must not race the
	// real echo on activeCh. See onReceived for the counter increment and
	// activeExpects gating.
	//
	// Codex PR #502 P1: the gate here is complementary to the terminator
	// gate above — one single signal (bytesDeliveredToActive) governs
	// both branches. See the terminator comment block for the full
	// rationale on why bytesDeliveredToActive beats bytesRead (lags,
	// consumer side) and bytesWritten (leads, initiator side before echo
	// returns). The critical win on busy/idle-chatter links: bytesWritten
	// flips to ≥1 the moment Write returns, so a buffered idle SYN in the
	// window between Write and echo-enqueue would falsely pass the
	// terminator gate and deliver 0xAA to sendRawWithEcho; with
	// bytesDeliveredToActive, that same SYN is correctly classified as
	// pre-echo and suppressed.
	preEchoSuppressed := hasOwner && ownerID == gatewaySessionID &&
		m.gatewayTxnActive && m.activeTxn.bytesDeliveredToActive.Load() == 0
	if preEchoSuppressed {
		m.activeTxn.synSuppressedPreEcho.Add(1)
	}

	// SYN-path diagnostics: record only when gateway owned the bus at
	// SYN arrival OR at the instant one of the branches above just
	// cleared gatewayTxnActive (so we see the transition). The caller's
	// activePathExpectsBytes() check (== gatewayTxnActive AFTER this
	// function returns) determines whether the SYN will be delivered to
	// activeCh — if gwActiveAfter is false, onSYNLocked consumed this
	// SYN as an end-of-txn terminator and the Send consumer on activeCh
	// does NOT see it.
	if wasGatewayOwned || gwActiveBefore {
		gwActiveAfter := m.gatewayTxnActive
		// synDelivered is true iff the SYN actually reaches activeCh.
		// That happens in exactly two cases:
		//   (a) onSYNLocked delivered the SYN inline as the frame terminator
		//       (terminatorDelivered, bytesRead>0 branch, PR #502 E2E fix), OR
		//   (b) the txn is still active after onSYNLocked AND this SYN is
		//       NOT pre-echo noise (gwActiveAfter && !preEchoSuppressed).
		synDelivered := terminatorDelivered || (gwActiveAfter && !preEchoSuppressed)
		m.recordSynDiagLocked(ownerID, gwActiveBefore, gwActiveAfter, synDelivered)
	}

	shouldTryGrant := m.arb.hasPending()
	// When onSYNLocked itself delivered the terminator, signal the caller
	// via the returned shouldTryGrant alone is insufficient — the caller
	// also needs to skip its own deliverToActive(symbol) call because
	// activePathExpectsBytes() is now false. That already happens
	// naturally: activeExpects is re-read AFTER onSYNLocked returns (see
	// onReceived), so with gatewayTxnActive cleared it becomes false and
	// the caller's deliverToActive is skipped. No double-delivery.
	_ = terminatorDelivered
	return passiveEvents, shouldTryGrant, preEchoSuppressed
}

// handleReset handles an adapter RESETTED event.
func (m *Mux) handleReset() {
	now := time.Now()

	// NOTE: We do NOT clear the INFO cache on in-band RESETTED.
	// Stable INFO entries (version, hardware ID) do not change on
	// in-band reset — the adapter's identity is constant. The cache
	// is only cleared on TCP disconnect (in reconnect()), which calls
	// connect() to repopulate it after re-establishing the connection.

	m.stateMu.Lock()
	m.phase.reset(wirePhaseIdle)
	m.gatewayEcho.reset()
	if m.gatewayTxnActive {
		m.gatewayTxnActive = false
		m.recordGatewayInactive(ReasonReset)
	}
	// Bump gen forward (monotonic) — see reconnect() for rationale.
	m.blockingArbGen++
	m.blockingArbActive = false
	pendingToCancel := m.pendingStart
	m.pendingStart = nil
	m.pendingStartAbsorb = 0 // reset clears adapter state; no stale responses will arrive
	if pendingToCancel != nil && pendingToCancel.deadline != nil {
		pendingToCancel.deadline.Stop() // AM8: cancel deadline timer
	}
	m.stateMu.Unlock()

	// Cancel in-flight pending START if any.
	if pendingToCancel != nil {
		// AM53: guarded send to avoid blocking handleReset if nobody reads notify.
		select {
		case pendingToCancel.notify <- startResult{granted: false, initiator: pendingToCancel.initiator, err: fmt.Errorf("adaptermux: %w", ebuserrors.ErrAdapterReset)}:
		case <-time.After(1 * time.Second):
			m.logger.Printf("adaptermux: handleReset: timed out sending to pendingStart.notify (AM53)")
		}
	}

	// Reset external session echo trackers.
	m.sessionsMu.Lock()
	for _, sess := range m.sessions {
		sess.echoTracker.reset()
	}
	m.sessionsMu.Unlock()

	m.arb.forceRelease()
	m.arb.failAllPending(fmt.Errorf("adaptermux: %w", ebuserrors.ErrAdapterReset))

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

	// NOTE: We intentionally do NOT re-INIT after in-band RESETTED.
	// Re-INIT sends ENHReqInit which the adapter answers with another
	// RESETTED, creating an infinite reset loop on ENS adapters.
	// The INIT handshake is performed once in connect() at TCP connection
	// time and again in reconnect() after a TCP disconnect. In-band
	// RESETTED only requires state cleanup (above), not re-negotiation.
	//
	// PROXY DIVERGENCE: The standalone proxy performed guarded re-INIT
	// after in-band RESETTED with a stabilization delay. This mux
	// intentionally skips re-INIT because ENS adapters enter an infinite
	// reset loop (INIT -> RESETTED -> INIT -> ...). The INFO cache is
	// preserved across in-band RESETTED because stable adapter identity
	// (version, hardware ID) does not change on soft reset. Volatile
	// telemetry (temperature, voltage) is a startup snapshot by design.
	// This divergence is documented and tested.
}

// drainActiveCh discards all buffered events from the active channel.
// Called before reset/reconnect to ensure consumers don't see stale
// pre-boundary bytes after a reset event.
// Returns the number of events drained.
func (m *Mux) drainActiveCh() int {
	n := 0
	for {
		select {
		case <-m.activeCh:
			n++
		default:
			return n
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

// resetAllSessionEchoes resets echo trackers for all external sessions.
// Called on ownership timeout to prevent stale echo state from leaking
// into the next ownership cycle (AM11). Must be called WITHOUT stateMu
// held to avoid ABBA deadlock with doSend.
func (m *Mux) resetAllSessionEchoes() {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()
	for _, sess := range m.sessions {
		sess.echoTracker.reset()
	}
}

// activePathExpectsBytes reports whether bus.Send is CURRENTLY consuming
// activeCh for a gateway transaction. Ownership alone is insufficient:
// after a transaction completes or aborts, ownership can linger up to
// IdleReleaseGrace while bus.Send has already returned. Third-party
// bytes in that idle window must NOT accumulate on activeCh.
//
// Policy (runtime soak fix):
//   - gatewayTxnActive: true iff bus.Send was granted and has not yet
//     released (set in completeArbitrationGrant, cleared on ownership
//     release via SYN timeout/idle grace, NACK, or TransactionDone).
//   - Do NOT deliver idle SYN bursts (no active txn → no consumer).
//   - Do NOT use activeCh as a passive backlog — passive traffic goes
//     through the passive path and external sessions, not activeCh.
//
// Caller must hold stateMu.
func (m *Mux) activePathExpectsBytes() bool {
	return m.gatewayTxnActive
}

// deliverToActive sends a byte to the active path channel.
// Logs on overflow instead of silently dropping (CRITICAL-1 fix).
//
// If countAsDelivered is true and the send succeeds, increment
// bytesDeliveredToActive — the precise "at least one real adapter byte
// has been enqueued on activeCh during this gateway-owned txn" signal
// used by onSYNLocked's terminator/pre-echo suppression gates (Codex PR
// #502 P1). Callers pass true when the enqueue corresponds to an
// active-path delivery under gateway ownership (decided under stateMu
// via activePathExpectsBytes); pass false for shutdown/reset events or
// for non-gateway-owned passthrough.
func (m *Mux) deliverToActive(symbol byte, countAsDelivered bool) {
	select {
	case m.activeCh <- activeEvent{kind: activeEventByte, b: symbol}:
		if countAsDelivered {
			m.activeTxn.bytesDeliveredToActive.Add(1)
		}
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
	// Snapshot transport BEFORE acquiring stateMu to avoid stateMu → connMu
	// lock nesting. doSend uses connMu → (release) → stateMu, so while not
	// strictly ABBA, keeping consistent ordering is defensive best practice.
	m.connMu.Lock()
	tr := m.upstream
	m.connMu.Unlock()

	_, hasRequestStart := tr.(arbitrationRequester)
	_, hasBlockingStart := tr.(interface{ StartArbitration(byte) error })
	isBlockingPath := !hasRequestStart && hasBlockingStart

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
	// Codex-R5: block regrant while a blocking StartArbitration goroutine
	// is still in-flight. The deadline may have cleared pendingStart, but
	// the goroutine is still running on the transport. Starting another
	// arbitration would overlap STARTs on the same transport.
	if m.blockingArbActive {
		m.logger.Printf("adaptermux: tryGrantAndStart skipped — blocking StartArbitration gen %d still in-flight", m.blockingArbGen)
		m.stateMu.Unlock()
		return
	}

	sessionID, initiator, notify, granted := m.arb.tryGrant()
	if !granted {
		m.stateMu.Unlock()
		return
	}

	m.pendingStart = &pendingStartState{
		sessionID:   sessionID,
		initiator:   initiator,
		notify:      notify,
		blockingArb: isBlockingPath,
	}
	// AM8: start a deadline timer so pendingStart cannot block indefinitely
	// if the adapter never responds with STARTED/FAILED.
	m.pendingStart.deadline = time.AfterFunc(m.cfg.StartDeadline, func() {
		m.stateMu.Lock()
		if m.pendingStart != nil && m.pendingStart.notify == notify {
			pending := m.pendingStart
			m.pendingStart = nil
			m.pendingStartAbsorb++
			// C2 (PR #502 Copilot): on the blocking path, the
			// StartArbitration goroutine may still be stuck inside the
			// transport call. We MUST NOT simply clear blockingArbActive
			// and re-grant here — that would let a second blocking
			// goroutine overlap the first on the same transport.
			// Instead, trigger a transport reconnect: closing m.conn
			// forces the hung read/write call to return with an I/O
			// error; readLoop observes the error and invokes reconnect()
			// which bumps blockingArbGen, clears blockingArbActive, and
			// fails/re-queues arbitration safely. The hung goroutine's
			// late return finds a stale gen and skips state mutation.
			// This yields no overlap AND no queue starvation.
			needReconnect := pending.blockingArb
			m.stateMu.Unlock()
			m.logger.Printf("adaptermux: pendingStart deadline expired for session %d (AM8)", pending.sessionID)
			// AM8: guard the send — if another path already delivered a
			// result, the channel is full and this send would block forever.
			select {
			case pending.notify <- startResult{granted: false, initiator: pending.initiator, err: errors.New("adaptermux: START deadline expired")}:
			default:
				m.logger.Printf("adaptermux: pendingStart deadline: notify channel full for session %d, result already delivered", pending.sessionID)
			}
			if needReconnect {
				// Close the current conn to force the hung blocking
				// StartArbitration call to return with an I/O error.
				// readLoop's error handler invokes reconnect() which
				// advances blockingArbGen and clears blockingArbActive
				// atomically under stateMu.
				m.connMu.Lock()
				c := m.conn
				m.connMu.Unlock()
				if c != nil {
					if err := c.Close(); err != nil {
						m.logger.Printf("adaptermux: deadline-triggered conn close: %v", err)
					} else {
						m.logger.Printf("adaptermux: deadline triggered transport reconnect to unstick hung StartArbitration (C2)")
					}
				}
			} else if m.arb.hasPending() {
				// Non-blocking path: no hung goroutine to worry about,
				// just advance the queue as before.
				m.tryGrantAndStart()
			}
		} else {
			m.stateMu.Unlock()
		}
	})
	m.stateMu.Unlock()

	// Forward START to adapter via non-blocking RequestStart.
	// tr was already captured above (before pendingStart creation).
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
				if m.pendingStart.deadline != nil {
					m.pendingStart.deadline.Stop() // AM8: cancel deadline timer
				}
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
		// blockingArb was already set in the pendingStart struct literal
		// above, so the deadline callback sees it from the start.
		// The AM8 deadline timer stays active to preserve liveness —
		// if StartArbitration hangs, the deadline clears pendingStart
		// and notifies the session. The deadline path triggers a
		// transport reconnect (via m.conn.Close) to force the hung
		// StartArbitration call to return with an I/O error;
		// reconnect() will then bump blockingArbGen and clear
		// blockingArbActive so the hung goroutine's late return is
		// observed with a stale gen and skips state mutation.
		//
		// PR502-Fix1: Run blocking StartArbitration in a goroutine so
		// readLoop is not stalled. The AM8 deadline timer handles
		// liveness if the call hangs indefinitely.
		// Codex-R5/R6: set blockingArbGen to prevent tryGrantAndStart
		// from starting another arbitration while this goroutine runs.
		// Generation-scoped: goroutine only clears if gen matches,
		// so a reconnect+relaunch won't be cleared by an old goroutine.
		// C1 / PR #502 P2: refuse to spawn if shutdown is in progress.
		// Without this gate, m.wg.Add(1) below can race with m.wg.Wait()
		// in Close() and panic with "sync: WaitGroup misuse". Emit FAILED
		// to the pending session so it is not left orphaned.
		//
		// PR #502 P2 (Codex): the closing-check and wg.Add MUST be
		// serialized under closeMu with Close()'s closing.Store(true).
		// A bare `if m.closing.Load()` check followed by a later
		// `m.wg.Add(1)` is a TOCTOU race: Close can flip closing to true
		// and reach m.wg.Wait() between the two statements. Holding
		// closeMu across the check and the Add (paired with Close
		// holding closeMu across the Store) guarantees the Add cannot
		// run after closing has been observed as true by Close.
		m.closeMu.Lock()
		if m.closing.Load() {
			m.closeMu.Unlock()
			m.stateMu.Lock()
			if m.pendingStart != nil && m.pendingStart.notify == notify {
				if m.pendingStart.deadline != nil {
					m.pendingStart.deadline.Stop()
				}
				m.pendingStart = nil
			}
			m.stateMu.Unlock()
			select {
			case notify <- startResult{granted: false, initiator: initiator, err: errors.New("adaptermux: closed")}:
			default:
			}
			return
		}
		m.stateMu.Lock()
		m.blockingArbGen++
		arbGen := m.blockingArbGen
		m.blockingArbActive = true
		m.stateMu.Unlock()
		// Codex-R9: track the blocking goroutine in mux.wg so Close()
		// waits for it. Without this, Close() can return while a hung
		// StartArbitration is still running, causing post-close state
		// mutations and goroutine leaks across reconnect/restart cycles.
		m.wg.Add(1)
		m.closeMu.Unlock()
		go func() {
			defer m.wg.Done()
			if err := starter.StartArbitration(initiator); err != nil {
				m.logger.Printf("adaptermux: START arbitration failed for session %d: %v", sessionID, err)
				// P1 fix (#3063005909): only send failure if we still own
				// the pending slot.  cancelPendingStart may have cleared
				// m.pendingStart and already sent a failure on notify while
				// StartArbitration was blocking.  A second send on the
				// cap-1 channel would block the caller indefinitely.
				m.stateMu.Lock()
				if m.blockingArbGen == arbGen {
					// Only our generation may clear the active flag.
					// A stale gen (prior reconnect/handleReset) won't match.
					m.blockingArbActive = false
				}
				if m.pendingStart != nil && m.pendingStart.notify == notify {
					if m.pendingStart.deadline != nil {
						m.pendingStart.deadline.Stop() // AM8: cancel deadline timer
					}
					m.pendingStart = nil
					m.stateMu.Unlock()
					notify <- startResult{granted: false, initiator: initiator, err: err}
				} else {
					// StartArbitration failed AND pending was already cancelled.
					// Codex-R8: scope absorb decrement + queue advance to our
					// generation. A stale goroutine (from before reconnect/
					// handleReset) must not consume absorb budget or advance
					// the queue on behalf of a newer request.
					isCurrentGen := m.blockingArbGen == arbGen
					if isCurrentGen && m.pendingStartAbsorb > 0 {
						m.pendingStartAbsorb--
					}
					m.stateMu.Unlock()
					if isCurrentGen && m.arb.hasPending() {
						m.tryGrantAndStart()
					}
				}
				return
			}
			// Blocking path: adapter already confirmed — handle inline.
			// P1 fix (#3063005909): verify ownership before completing.
			// cancelPendingStart may have run while StartArbitration was
			// blocking, clearing pendingStart and sending a failure on
			// notify.  Completing here would double-send on the cap-1
			// channel and re-grant the bus to a cancelled session.
			//
			// Codex-R10: do NOT clear blockingArbActive in the success
			// branch until AFTER completeArbitrationGrant confirms
			// ownership. Clearing earlier opens a race window where
			// tryGrantAndStart (from readLoop SYN/idle or RemoveSession)
			// observes pendingStart==nil + blockingArbActive==false +
			// no owner and launches a second overlapping arbitration.
			m.stateMu.Lock()
			isCurrentGen := m.blockingArbGen == arbGen
			if m.pendingStart == nil || m.pendingStart.notify != notify {
				// Already cancelled (deadline or session disconnect) — don't
				// double-send. Safe to clear blockingArbActive here: no
				// completeArbitrationGrant will follow.
				if isCurrentGen {
					m.blockingArbActive = false
				}
				// Codex-R8: scope absorb + queue advance to our generation.
				if isCurrentGen && m.pendingStartAbsorb > 0 {
					m.pendingStartAbsorb--
				}
				m.stateMu.Unlock()
				if isCurrentGen && m.arb.hasPending() {
					m.tryGrantAndStart()
				}
				return
			}
			if m.pendingStart.deadline != nil {
				m.pendingStart.deadline.Stop() // AM8: cancel deadline timer
			}
			m.pendingStart = nil
			m.stateMu.Unlock()
			m.completeArbitrationGrant(sessionID, initiator, notify)
			// Clear blockingArbActive AFTER ownership is confirmed.
			// Codex-R11: re-check generation under the lock at the clear
			// site. The cached isCurrentGen could be stale if reconnect()
			// or handleReset() bumped blockingArbGen while
			// completeArbitrationGrant was running — a stale stale-gen
			// goroutine must not clear the guard for a newer in-flight
			// START.
			m.stateMu.Lock()
			if m.blockingArbGen == arbGen {
				m.blockingArbActive = false
			}
			m.stateMu.Unlock()
		}()
		return
	} else {
		m.logger.Printf("adaptermux: transport does not support arbitration")
		m.stateMu.Lock()
		if m.pendingStart != nil && m.pendingStart.notify == notify {
			if m.pendingStart.deadline != nil {
				m.pendingStart.deadline.Stop() // AM8: cancel deadline timer
			}
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
	// Check ArbitrationSendsSource BEFORE acquiring stateMu to avoid
	// ABBA deadlock: arbitrationSendsSource acquires connMu, and doSend
	// acquires connMu -> stateMu. Reversing that order here (stateMu ->
	// connMu) would deadlock.
	sendsSource := m.arbitrationSendsSource()

	// AM57+Codex-P1: set busOwned, phase, AND confirmOwnership atomically
	// under stateMu. For external sessions, the liveness check is also
	// done under stateMu (via sessionsMu nested inside) to prevent a
	// race where RemoveSession deletes the session between the liveness
	// check and confirmOwnership, leaving a dead owner in the arbitrator.
	// Lock order: stateMu → sessionsMu → arb.mu. No path holds
	// sessionsMu or arb.mu then acquires stateMu, so ABBA-safe.
	m.stateMu.Lock()
	if sessionID != gatewaySessionID {
		m.sessionsMu.Lock()
		_, alive := m.sessions[sessionID]
		m.sessionsMu.Unlock()
		if !alive {
			m.stateMu.Unlock()
			m.logger.Printf("adaptermux: session %d disconnected during START, discarding grant", sessionID)
			notify <- startResult{granted: false, initiator: initiator, err: errors.New("session disconnected")}
			return
		}
	}
	if sendsSource {
		m.phase.startRequestWithSource(initiator)
	} else {
		m.phase.startRequest()
	}
	m.busOwned = time.Now()
	// Drain activeCh BEFORE marking the transaction active so the
	// drain count belongs to this grant (not the previous transaction).
	// Safety net: normal flow should produce zero drains after the
	// gatewayTxnActive-on-SYN lifecycle fix.
	drained := 0
	if sessionID == gatewaySessionID {
		drained = m.drainActiveCh()
		if drained > 0 {
			m.logger.Printf("adaptermux: drained %d bytes from activeCh on grant", drained)
		}
		m.gatewayEcho.markRequestStart()
		m.gatewayTxnActive = true
		m.recordGatewayGrant(initiator, drained)
	}
	m.arb.confirmOwnership(sessionID, initiator)
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
			// AM56 fix: mismatch means a stale STARTED from a previous
			// session/request. Leaving pendingStart set permanently would
			// deadlock all future START requests. Fail the pending session
			// cleanly so it can retry.
			// The adapter sent this STARTED for a previous RequestStart that
			// is no longer tracked. The real response for our pending request
			// may still arrive — absorb it when it does.
			m.pendingStart = nil
			m.pendingStartAbsorb++
			if pending.deadline != nil {
				pending.deadline.Stop() // AM8: cancel deadline timer
			}
			m.stateMu.Unlock()
			m.logger.Printf("adaptermux: STARTED mismatch: got initiator 0x%02X, pending 0x%02X — failing pending session %d (AM56)", data, pending.initiator, pending.sessionID)
			pending.notify <- startResult{
				granted:   false,
				initiator: pending.initiator,
				err:       fmt.Errorf("adaptermux: STARTED mismatch (got 0x%02X, want 0x%02X)", data, pending.initiator),
			}
			// After resolving, check if more requests are pending.
			if m.arb.hasPending() {
				m.tryGrantAndStart()
			}
			return
		}
		m.pendingStart = nil
		if pending.deadline != nil {
			pending.deadline.Stop() // AM8: cancel deadline timer
		}
		m.stateMu.Unlock()
		m.completeArbitrationGrant(pending.sessionID, pending.initiator, pending.notify)
	} else {
		m.pendingStart = nil
		if pending.deadline != nil {
			pending.deadline.Stop() // AM8: cancel deadline timer
		}
		m.stateMu.Unlock()
		pending.notify <- startResult{
			granted:   false,
			initiator: data,
			err:       fmt.Errorf("adaptermux: arbitration lost to 0x%02X: %w", data, ebuserrors.ErrBusCollision),
		}
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
		if pending.deadline != nil {
			pending.deadline.Stop() // AM8: cancel deadline timer
		}
		// The adapter will still send STARTED or FAILED for the cancelled
		// RequestStart. Increment absorb counter so handleArbitrationResponse
		// discards that stale response instead of failing a newer request.
		m.pendingStartAbsorb++
		// Codex PR #502 P1 (v2 — mirror C2 reconnect pattern): if the
		// cancelled pending was on the blocking StartArbitration path,
		// the goroutine may still be hung in the transport call.
		// Previously we cleared blockingArbActive + called
		// tryGrantAndStart in-line — but that lets a second blocking
		// goroutine overlap the first on the same transport AND the
		// hung goroutine may still have been granted START by the
		// adapter, so mux/arbitrator state can diverge. Instead,
		// trigger a transport reconnect: closing m.conn forces the
		// hung read/write call to return with an I/O error; readLoop
		// observes the error and invokes reconnect() which bumps
		// blockingArbGen, clears blockingArbActive, and fails/re-queues
		// arbitration safely under stateMu. The hung goroutine's late
		// return finds a stale gen and skips state mutation. Do NOT
		// advance the queue in-line — the reconnect path does that.
		wasBlocking := pending.blockingArb
		m.stateMu.Unlock()
		// AM8: guard the send — if another path already delivered a
		// result for this notify channel, skip to avoid blocking.
		select {
		case pending.notify <- startResult{granted: false, initiator: pending.initiator, cancelled: true}:
		default:
			m.logger.Printf("adaptermux: cancelPendingStart: notify channel full for session %d, result already delivered", sessionID)
		}
		if wasBlocking {
			// Close the current conn to force the hung blocking
			// StartArbitration call to return with an I/O error.
			// readLoop's error handler invokes reconnect() which
			// advances blockingArbGen and clears blockingArbActive
			// atomically under stateMu. No in-line queue advance —
			// reconnect drives that safely.
			m.connMu.Lock()
			c := m.conn
			m.connMu.Unlock()
			if c != nil {
				if err := c.Close(); err != nil {
					m.logger.Printf("adaptermux: cancelPendingStart-triggered conn close: %v", err)
				} else {
					m.logger.Printf("adaptermux: cancelPendingStart triggered transport reconnect to unstick hung StartArbitration (session %d)", sessionID)
				}
			}
		} else if m.arb.hasPending() {
			// Non-blocking path: no hung goroutine to worry about,
			// just advance the queue as before.
			m.tryGrantAndStart()
		}
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
		m.stateMu.Unlock()
	} else {
		m.sessionsMu.Lock()
		if sess, ok := m.sessions[sessionID]; ok {
			sess.echoTracker.recordSent(data)
		}
		m.sessionsMu.Unlock()
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
//
// AM40: sessionsMu.Lock is used because matchEcho mutates per-session
// echo trackers. Moving to per-session locks would reduce contention
// but adds complexity. Current lock convoy is acceptable for <=100 sessions.
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
//
// Sessions are collected under sessionsMu, but deliverReset is called
// after releasing the lock. This is required because deliverReset blocks
// until space is available in sendCh (non-droppable delivery), and
// holding sessionsMu during a blocking send would prevent RemoveSession
// from acquiring the lock to close the session's done channel — deadlock.
func (m *Mux) broadcastResetToSessions() {
	features := byte(m.upstreamFeatures.Load())

	m.sessionsMu.Lock()
	sessions := make([]*session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		sessions = append(sessions, sess)
	}
	m.sessionsMu.Unlock()

	// AM4/AM17: deliver resets concurrently with a 1-second deadline.
	// deliverReset blocks until sendCh has space or s.done closes.
	// The deadline prevents broadcastResetToSessions from stalling
	// readLoop indefinitely if a session has a full sendCh.
	// Goroutines that exceed the deadline are NOT leaked — they will
	// self-terminate when the session eventually closes (s.done) via
	// writeLoop backpressure or RemoveSession.
	var wg sync.WaitGroup
	for _, sess := range sessions {
		wg.Add(1)
		go func(s *session) {
			defer wg.Done()
			s.deliverReset(features)
		}(sess)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		m.logger.Printf("adaptermux: broadcastResetToSessions timed out after 1s — %d session(s) still pending (AM4)", len(sessions))
	}
}

// isNetTimeout reports whether err is a net.Error timeout (read deadline
// exceeded). These are transient idle conditions on a quiet bus, not
// connection failures.
func isNetTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
