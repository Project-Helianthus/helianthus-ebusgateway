package adaptermux

import (
	"context"
	"errors"
	"expvar"
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

	// ExternalSessionSYNGrace is the grace period applied to SYN-timeout
	// events (wirePhaseEventSYNTimeout) when the bus owner is an external
	// session, rather than the gateway itself. Default: 2s.
	//
	// The wire-phase machine emits SYN-timeout when SYN appears during
	// the WaitCmdAck/CollectRequest phase. For the gateway's own tight
	// protocol (B524 directed reads with no inter-byte gap) that signal
	// genuinely means "txn died". For external sessions like ebusd
	// running broadcast scans (0xFE address), the protocol naturally
	// has multi-second gaps while each responder replies in turn — those
	// inter-responder gaps look identical to SYN-timeout but are NOT a dead
	// transaction.
	//
	// F-10v2 (EBUSD-VERIFICATION-2026-05-11-batch3.md): immediate-
	// release on SYN-timeout fired 80 times against ebusd's session vs
	// 0 times against the gateway in a 5000-line window. Adding a grace
	// window for external sessions decouples the per-protocol timing
	// expectations from the immediate-release policy that fits the
	// gateway's own pattern.
	ExternalSessionSYNGrace time.Duration

	// SYNInterval is the expected eBUS SYN cadence on the wire (default
	// 4576 µs at 2400 baud). Used by tryGrantAndStart to decide whether
	// the bus is idle enough to short-circuit fairness arbitration and
	// hand the next pending external START to its session immediately.
	// (Proxy-bug C1 / R1.)
	SYNInterval time.Duration

	// PendingStartTTL bounds the dwell time of an external pending
	// START in the arbitrator's pendingExternal queue. Requests whose
	// enqueuedAt has aged past this value are dropped from the queue
	// head and rejected with errStaleStartRequest so external clients
	// (ebusd) can retry cleanly when they actually want the bus.
	// (Proxy-bug C3 / R3.)
	//
	// Sentinels:
	//   - 0 (the zero value): use the production default (currently
	//     250 ms — slightly above the default ReadTimeout so an
	//     enqueue right after a read timeout still sees its first
	//     grant attempt before the TTL drain fires; the request will
	//     have been kicked via requestStartForSession's idle path
	//     well before that, so the TTL only catches genuinely stuck
	//     entries).
	//   - Negative: disables the drain entirely. Tests and the very
	//     rare deployment that needs pre-C3 behavior under long
	//     gateway contention can pass -1 (or any negative duration)
	//     to opt out.
	PendingStartTTL time.Duration

	// IsKnownInitiatorByte is an optional predicate that classifies a
	// FAILED arbitration data byte as a real bus initiator (true) versus
	// a transient AND-collision artifact (false). When two real
	// initiators arbitrate, the wire byte is the bit-wise AND of their
	// addresses, which can land on a value that is NOT itself a
	// physically-present initiator — e.g. 0x7F (gateway) AND'd with 0xF1
	// (radio) produces 0x71 on the wire even though no initiator at
	// 0x71 exists on the bus. Mirror clients (ebusd) that consume
	// the proxy's StreamEventFailed bytes interpret 0x71 as a
	// fictitious bus initiator and pollute their passive view.
	//
	// Returning false on a byte suppresses its delivery to the
	// per-session sendCh mirror; the passive emit and the error
	// logging still fire so observability stays complete.
	//
	// When nil (the default), no filtering is performed — all FAILED
	// data bytes flow to mirror clients as before. Production wires
	// this to a runtime_state-backed lookup. (Proxy-bug C5 / R5.)
	IsKnownInitiatorByte func(b byte) bool

	// LatencyHistogramReportInterval controls how often a single-line
	// summary of the adaptermux_session_frame_latency_us_bucket_total
	// expvar is emitted via the logger. Default: 60s. Set to a negative
	// duration to disable the periodic emission entirely (still
	// available via /debug/vars).
	//
	// This was added so the F-10 falsifying data is visible in
	// `ha addons logs` without requiring an operator to wget
	// /debug/vars from inside the addon container (per the batch-3
	// side-observation in EBUSD-VERIFICATION-2026-05-11-batch3.md).
	LatencyHistogramReportInterval time.Duration

	// StartDeadline is the maximum time to wait for the adapter to
	// respond with STARTED/FAILED after a START request. Default: 5s.
	// If the adapter does not respond within this duration, the pending
	// start is cleared and the session is notified of failure (AM8).
	StartDeadline time.Duration

	// PostExternalReleaseGrace is the F-28 (batch-25, iter5,
	// 2026-05-14) "polite yield" window. When an external session
	// releases bus ownership, the arbitrator defers gateway-only
	// grants for this duration so ebusd's TCP-delayed next
	// ENH_REQ_START has time to arrive and be queued before the
	// gateway's semantic poll loop re-bids and locks the bus.
	//
	// Live evidence (iter5 byte trace): without this gate, the
	// gateway re-bids within microseconds after every external
	// release, outracing ebusd's ~10-100ms TCP roundtrip every
	// time. The result was a 200-500ms median ebusd START → STARTED
	// latency, which compounds across multi-register scans into
	// the residual 1-5s tail observed post-F-27.
	//
	// Sentinels:
	//   - 0 (default): use 50ms.
	//   - Negative: disable the cooldown entirely (legacy).
	//
	// 50ms is approximately:
	//   ebusd's typical TCP roundtrip (~5-15ms)
	//   + ebusd's bus-state-machine transition (~5-20ms)
	//   + safety margin (10-30ms)
	// Each 50ms cooldown sacrifices ~10% of the gateway's
	// raw poll throughput when tight ebusd activity is happening,
	// in exchange for ebusd actually completing its scans.
	PostExternalReleaseGrace time.Duration

	// FairnessRatio controls the gateway-vs-external grant rotation
	// when BOTH a gateway pending START and at least one external
	// pending START coexist. The arbitrator counts contended rotations
	// and every Nth rotation hands the grant to external FIFO instead
	// of the gateway (the default-priority bidder). Larger values
	// favor the gateway; smaller values favor external sessions
	// (ebusd).
	//
	// F-25 (batch-22, iter2, 2026-05-14): default 2 (50/50 split).
	// Pre-F-25 this was a hard-coded constant of 4 (75% gateway /
	// 25% external) which produced live `arbitration won in invalid
	// state` cascades from ebusd because gateway-poll bursts left
	// only ~25% of bus slots for ebusd to bid into, and the resulting
	// stale STARTED dispatches collided with ebusd's bs_recvCmd
	// state. See DefaultFairnessRatio in arbitration.go for the full
	// rationale and the live evidence trail (iter1-result-
	// 20260514T145344Z.md).
	//
	// Sentinels:
	//   - 0 (zero value): use DefaultFairnessRatio (2).
	//   - Negative: clamped to DefaultFairnessRatio (defensive).
	//   - 1: every contended rotation alternates; expected gateway
	//     share ~50% but ebusd MAY pre-empt back-to-back if it has
	//     a queue. Tests use 1 to force-deterministic ebusd grants.
	FairnessRatio int

	// ExternalStartStaleness bounds the wall-clock age (measured from
	// startRequest.enqueuedAt) of an EXTERNAL-session START at the
	// moment the adapter reports `StreamEventStarted`. If the request
	// has been in the gateway's path (pendingExternal queue + in-flight
	// adapter arbitration) for longer than this budget, the grant is
	// silently aborted: the session's notify channel receives
	// `cancelled: true` (session.go's handleStart silent-suppress per
	// F-17 takes the no-deliver branch) and ownership is NOT confirmed.
	// The wire-level arbitration win still happened — bus state
	// recovers naturally via the next SYN idle burst when no SEND
	// frame follows.
	//
	// Iter1 fix (F-24, batch-21, 2026-05-14): ebusd's bus state machine
	// times out internally before the gateway's queue+arbitration
	// pipeline can deliver ENH_RES_STARTED for tight-scan-08 sub-
	// register reads. When the late STARTED arrives, ebusd is in
	// bs_recvCmd / bs_skip (mid-parsing the gateway's prior poll
	// response) and logs `arbitration won in invalid state {skip|
	// receive command|ready}`, silently dropping the request. The
	// staleness guard prevents the gateway from delivering a grant
	// that ebusd has already discarded, eliminating the bus-state
	// desync. Default: 300ms (well below ebusd's typical 1-2s
	// arbitration-wait timeout, but above the steady-state median
	// queue+arbitration latency of ~50-150ms).
	//
	// Sentinels:
	//   - 0 (zero value): use the 300ms default.
	//   - Negative: disable the guard entirely (legacy behavior;
	//     useful for regression testing). Tests use -1.
	ExternalStartStaleness time.Duration

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
	if c.LatencyHistogramReportInterval == 0 {
		c.LatencyHistogramReportInterval = 60 * time.Second
	}
	if c.SYNInterval == 0 {
		// 4576 µs ≈ one SYN period at 2400 baud on a clean eBUS line.
		// (Proxy-bug C1 / R1.)
		c.SYNInterval = 4576 * time.Microsecond
	}
	if c.PendingStartTTL == 0 {
		// Default 250 ms — slightly above the default ReadTimeout
		// (200 ms) so a START enqueued right after a read timeout
		// still sees a grant attempt before the TTL drain fires.
		// requestStartForSession kicks tryGrantAndStart on every
		// enqueue, so on an idle bus the request typically lands a
		// grant in < 1 ms; the 250 ms window is only there for
		// genuinely stuck requests where the kick saw no idle path
		// (e.g. blockingArbActive, pendingStartAbsorb > 0).
		// Negative values disable the drain. (Proxy-bug C3 / R3 +
		// Codex P1 round 1 on PR #623.)
		c.PendingStartTTL = 250 * time.Millisecond
	}
	if c.ExternalSessionSYNGrace == 0 {
		// F-27 (batch-24, iter4, 2026-05-14): lowered from 2s to 250ms.
		//
		// Pre-F-27 rationale was "leave generous headroom for buses with
		// more participants" — calibrated against a 13:46:13 ebusd scan
		// trace where the longest inter-responder gap was ~190ms. The
		// 2s value was a 10x safety margin over that.
		//
		// The 2s margin turned out to be the dominant cause of the
		// tight-scan-08 failures observed in batch-23 (iter3): when
		// ebusd's bus state machine silently dropped a grant due to
		// "arbitration won in invalid state" (a state collision caused
		// by gateway queue delay timing out ebusd's internal arbitration
		// wait), the gateway then held the now-dead external ownership
		// for the FULL 2s grace before reclaiming. Byte-trace forensic
		// evidence: /tmp/iter3-postfix-enh.txt latency analysis showed
		// 246/580 events in the 1-5s bucket, with concrete examples of
		// 8-second lockouts (gateway log 2026-05-14 21:53:25-21:53:39:
		// 14 consecutive ebusd START 0x31 requests during a single
		// stalled external ownership hold).
		//
		// Why 250ms: legitimate ebusd transactions bump
		// `lastWireActivity` per echoed byte (mux.go ~1622, gated on
		// !wire-SYN). At 2400 baud each byte is ~4-5ms on the wire, plus
		// ENH framing overhead through TCP; observed median ebusd
		// inter-byte gap is well under 50ms even for multi-responder
		// scans. 250ms is 5x that worst-case while killing the
		// silent-drop lockout. If a target's response really takes
		// 250+ms (none observed in production traces), the grace
		// expires and the next tryGrantAndStart re-grants the still-
		// pending session immediately.
		c.ExternalSessionSYNGrace = 250 * time.Millisecond
	}
	if c.StartDeadline <= 0 {
		c.StartDeadline = 5 * time.Second
	}
	if c.ExternalStartStaleness == 0 {
		// F-24 (batch-21, 2026-05-14): 300ms default. Well below ebusd's
		// arbitration-wait timeout (~1-2s) so legitimate grants flow
		// through; above the steady-state queue+arbitration latency
		// (~50-150ms median) so normal traffic is unaffected. Operators
		// can tune via the addon options.
		c.ExternalStartStaleness = 300 * time.Millisecond
	}
	if c.PostExternalReleaseGrace == 0 {
		// F-29 (batch-26, iter6, 2026-05-14): widened from F-28 50ms to
		// 100ms. iter5 byte-trace showed median ebusd START→STARTED
		// latency dropped to 70ms with the 50ms cooldown, but 60 events
		// remained in the 1-5s tail caused by ebusd's internal
		// arbitration-wait timing out before STARTED arrives, then
		// re-issuing STARTs that cascade through the same race window.
		// 100ms gives ebusd ~50ms of "guaranteed gateway-silence" after
		// every external release, well within ebusd's typical
		// arbitration-wait tolerance (~150ms), while still allowing the
		// gateway to do ~10 polls/sec when ebusd is idle.
		c.PostExternalReleaseGrace = 100 * time.Millisecond
	}
	if c.FairnessRatio == 0 {
		// F-25 (batch-22, iter2, 2026-05-14): 2 = 50/50 split under
		// gateway+external contention. Lowered from the previous
		// hard-coded 4 (75% gateway / 25% external) which starved
		// ebusd of bus slots during tight-scan-08 loops.
		c.FairnessRatio = DefaultFairnessRatio
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
//
// F-23 (batch-19, 2026-05-13, Codex bot review on PR-2): WasEscaped
// carries the wire-side provenance flag for Symbol. It is valid for
// PassiveEventSymbol only; other kinds leave it at the zero value.
// The flag is sourced from upstream transport.StreamEvent.WasEscaped
// (added in helianthus-ebusgo PR #154 / F-23) so adapter-direct
// consumers — which read from the mux's passiveTransport bridge —
// can distinguish escape-decoded payload 0xAA (WasEscaped=true) from
// raw wire SYN (WasEscaped=false), matching the contract honored by
// raw-TCP passive observers via the local escape decoder.
type PassiveEvent struct {
	Kind       PassiveEventKind
	Symbol     byte
	WasEscaped bool
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

	// req is the arbitrator's startRequest pointer kept by the mux for
	// the duration of the in-flight grant. It carries the cancelled
	// flag the mux checks before delivering a late STARTED: if the
	// session has re-submitted a START while this grant was in flight
	// at the adapter, the cancelled flag is true and the delivery path
	// converts STARTED into FAILED instead of handing the bus to a
	// session that abandoned the grant. (Proxy-bug C4 / R4.)
	req *startRequest
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
	stateMu  sync.Mutex
	phase    wirePhaseTracker
	arb      *arbitrator
	busOwned time.Time // when current owner acquired the bus

	// lastWireActivity is the timestamp of the most recent adapter
	// byte (StreamEventByte) seen on the wire after the current
	// ownership grant, plus the grant moment itself. The release
	// paths use this — rather than busOwned — to measure how long
	// the wire has actually been idle, so a long-running external
	// transaction (e.g. an ebusd broadcast scan that runs > 2s with
	// real inter-responder traffic) is not torn down purely because
	// the grant is old. Reset to time.Now() in
	// completeArbitrationGrant alongside busOwned, then bumped on
	// every observed adapter byte while ownership is held.
	// (Codex P1+P2 round on PR #621.)
	lastWireActivity time.Time

	pendingStart       *pendingStartState // in-flight START awaiting STARTED/FAILED
	pendingStartAbsorb int                // stale adapter responses to absorb (FAILED/STARTED from cancelled requests)
	pendingAbsorbGen   uint64             // generation for stale-response absorb fail-safe timers
	// absorbResetTotal counts how many times the absorb-timeout
	// fail-safe (armPendingStartAbsorbLocked) fired and reset the
	// pending absorb counter to zero. F-22 (batch-19, 2026-05-13):
	// replaces the prior transport-reconnect side effect at the
	// absorb-timeout site. The old behavior closed the upstream ENH
	// connection on every timeout, which severed external sessions
	// (ebusd) and triggered cascade RequestStart failures for no
	// transport-level reason — batch-19 measured 5 reconnects /
	// 68 min producing 92 cascade failures. The new behavior just
	// resets the counter and lets the next poll iteration issue a
	// fresh RequestStart cleanly. atomic.Uint64 because external
	// readers (ActiveTxnSnapshot, tests) may load it without
	// holding stateMu.
	absorbResetTotal atomic.Uint64
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

	// sessionRemoteAddrs is a lock-free side index from session ID to
	// the client's RemoteAddr string, written on AddSession and deleted
	// on RemoveSession. It exists so code paths that already hold
	// stateMu (e.g. onSYNLocked) can label diagnostic logs with the
	// remote endpoint without acquiring sessionsMu — that would invert
	// the established sessionsMu → stateMu lock order and risk ABBA
	// deadlock with doSend. Reads use sync.Map.Load and are safe from
	// any goroutine; writes happen under sessionsMu in AddSession /
	// RemoveSession but do not require any extra synchronization.
	sessionRemoteAddrs sync.Map // key uint64 sessionID → value string remote addr

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
//
// F-23 (batch-19, Codex bot on PR-2): wasEscaped carries the
// upstream WasEscaped provenance for activeEventByte. Surfaced via
// activeTransport.ReadByteWithEscape (transport.EscapeFlaggedReader)
// so protocol.Bus's waitForSyn / sendRawWithEcho can distinguish
// real wire SYN (false) from escape-decoded payload 0xAA (true).
type activeEvent struct {
	kind       activeEventKind
	b          byte  // valid when kind == activeEventByte
	wasEscaped bool  // valid when kind == activeEventByte (F-23)
	err        error // valid when kind == activeEventError
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
	arb := newArbitrator()
	arb.setPolicy(cfg.PendingStartTTL, cfg.FairnessRatio, cfg.PostExternalReleaseGrace)
	return &Mux{
		cfg:          cfg,
		logger:       cfg.Logger,
		arb:          arb,
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

	if m.cfg.LatencyHistogramReportInterval > 0 {
		m.wg.Add(1)
		go m.latencyHistogramReportLoop() // goroutine: periodic dump of F-10 histogram
	}

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

	// F-18: per-session echo trackers were removed; gateway-side reset
	// happens via m.gatewayEcho.reset() in flushOnSYN paths.

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

				// F-18: per-session echo trackers removed; nothing to
				// reset for external sessions on ownership timeout.
				_ = quietBusTimedOut

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
			// Peek the pending bidder's session ID BEFORE
			// handleArbitrationResponse fires. Without this, an
			// external winner can call RemoveSession immediately
			// after receiving STARTED (e.g. ebusd disconnects on
			// transaction completion) which releases ownership;
			// our subsequent m.arb.owner() would return owned=false
			// and we'd miss the chance to synthesize for non-winner
			// sessions. Codex P2 round 4 on PR #620.
			//
			// pendingStartAbsorb > 0 means the response is for a
			// cancelled bid — bus state is already torn down, no
			// synthesis needed regardless of bidder identity.
			m.stateMu.Lock()
			var startedBidderSessionID uint64
			var startedBidderInitiator byte
			var startedBidderValid bool
			if m.pendingStartAbsorb == 0 && m.pendingStart != nil {
				startedBidderSessionID = m.pendingStart.sessionID
				startedBidderInitiator = m.pendingStart.initiator
				startedBidderValid = true
			}
			// The STARTED event represents a real winner byte that
			// the adapter consumed from the wire. Bump
			// lastWireActivity so the C1 fast path / requestStart-
			// ForSession enqueue-kick correctly treat the wire as
			// non-idle during the third-party transaction that is
			// about to flow through onReceived. Without this, a new
			// external START enqueued in the gap between this
			// control event and the next StreamEventByte would see
			// an old timestamp and idle-kick mid-frame. (Codex P2
			// round 6 on PR #623.)
			m.lastWireActivity = time.Now()
			m.stateMu.Unlock()
			m.handleArbitrationResponse(true, event.Data)
			// Synthesize the arbitration-winner byte as a passive
			// observation for external sessions ONLY when the gateway
			// is the winner. The wire DID carry this byte (e.g. 0x7F)
			// at this point, but the ENH adapter consumes it as a
			// STARTED control event instead of echoing it via
			// ResReceived. Without this synthesis, external sessions
			// like ebusd see the gateway's transaction stream as
			// `... SYN, 0x15 (target), 0xB5, 0x24, ...` with no
			// initiator-byte boundary. Their bus parser discards the
			// frame as malformed (0x15 isn't a valid initiator byte
			// — low nibble 5 isn't a valid arbitration nibble) and
			// the gateway's traffic vanishes from their grab buffer
			// / initiator enumeration. Worse, their bus state
			// machine thinks the bus is idle when it isn't, leading
			// to mis-timed bid attempts and the "won in invalid
			// state" + read-timeout cascade seen in
			// EBUSD-VERIFICATION-2026-05-10.md F-9 / F-10.
			//
			// We restrict synthesis to gateway-wins because:
			//   - When an external session wins, it receives
			//     ENHResStarted via its own handleStart goroutine
			//     (session.go: deliverStarted). That goroutine and
			//     this readLoop are independent, so an unconditional
			//     synthesis here could enqueue ENHResReceived
			//     (winner_byte) BEFORE the session's own
			//     ENHResStarted, presenting an out-of-order
			//     arbitration result to a strict ENH parser
			//     (Codex P2 follow-up on PR #620).
			//   - For gateway wins, there is no competing per-
			//     session control event — the gateway consumes
			//     STARTED synchronously, so no goroutine race
			//     exists.
			//
			// If hasOwner is false after handleArbitrationResponse,
			// the STARTED was absorbed as stale (cancelled bid).
			// Don't synthesize — nobody is using the bus from this
			// stale win.
			// Use the snapshotted bidder identity (NOT a fresh
			// owner() read) so a winner-disconnect-then-release
			// race doesn't skip synthesis for the remaining
			// monitors. The wire byte was already consumed by the
			// adapter; synthesis is required regardless of whether
			// the winning session is still attached.
			if startedBidderValid {
				if event.Data == startedBidderInitiator {
					// Matched STARTED — handleArbitrationResponse
					// granted ownership to the bidder, so event.Data
					// is the bidder's own initiator byte.
					if startedBidderSessionID == gatewaySessionID {
						// Gateway won — every external session
						// needs the byte; no per-session control
						// event competes. Passive observers are
						// intentionally NOT notified (gateway-
						// owned bytes are suppressed in
						// onReceived too — consistent).
						m.deliverToSessions(event.Data, gatewaySessionID, true, time.Now())
					} else {
						// External session won — skip the winner
						// (its handleStart goroutine delivers
						// ENHResStarted asynchronously; synthesis
						// here would race that frame). Other
						// external sessions (multi-monitor
						// deployments) still need the byte to
						// keep their bus state synced — without
						// it they'd misread the next target/PB
						// byte as the frame source. (Codex P2
						// round 3.)
						m.deliverWinnerByteToOtherSessions(event.Data, startedBidderSessionID)
						// Passive observer pipeline also needs
						// the non-gateway winner byte — the
						// PassiveTransport / bus reconstructor
						// otherwise sees the next target/PB byte
						// as the frame source for externally-
						// owned frames. onReceived's normal
						// PassiveEventSymbol path is bypassed for
						// StreamEventStarted, so synthesize here.
						// (Codex P2 round 5 on PR #620.)
						m.emitPassive(PassiveEvent{
							Kind:       PassiveEventSymbol,
							Symbol:     event.Data,
							ObservedAt: time.Now(),
						})
					}
				} else {
					// Mismatched STARTED (AM56): the wire byte
					// belongs to some third party, not the bidder.
					// handleArbitrationResponse has already failed
					// the bidder session and armed absorb, so the
					// bidder is no longer the winner. The third-
					// party winner byte still needs to flow to the
					// non-bidder sessions and passive observers so
					// their bus state stays synced with physical
					// reality. (Codex P2 round 6 on PR #620.)
					if startedBidderSessionID == gatewaySessionID {
						// Gateway was bidder but lost to a third
						// party — every external session needs
						// the byte (gateway has no session in
						// m.sessions; no echo suppression needed).
						m.deliverToSessions(event.Data, 0, false, time.Now())
					} else {
						// External session was bidder but lost.
						// Skip the loser: handleArbitrationResponse's
						// mismatch path now notifies the bidder with
						// initiator=event.Data (the third-party
						// winner) — see Codex P2 round 7 fix. The
						// bidder's handleStart goroutine therefore
						// delivers ENHResFailed(winner_byte), which
						// conveys the actual wire owner and keeps the
						// bidder's bus reconstructor in sync. A
						// synthesized RECEIVED(winner) here would
						// race that frame on the same session.
						m.deliverWinnerByteToOtherSessions(event.Data, startedBidderSessionID)
					}
					// Third-party winner byte is a true passive
					// observation regardless of who the bidder was
					// — always emit it for the passive pipeline.
					m.emitPassive(PassiveEvent{
						Kind:       PassiveEventSymbol,
						Symbol:     event.Data,
						ObservedAt: time.Now(),
					})
				}
			}
			continue
		case transport.StreamEventFailed:
			m.logger.Printf("adaptermux: readLoop got StreamEventFailed data=0x%02X", event.Data)
			// Peek the *active* bidder (if any) BEFORE
			// handleArbitrationResponse clears pendingStart. The
			// presence and identity of the bidder dictate how we
			// route the synthesized winner byte; the wire byte
			// itself always needs to flow to observers regardless.
			//
			// `pendingStartAbsorb > 0` indicates this FAILED is the
			// stale response to a cancelled bid. The bid is gone, so
			// no per-session control event races us — but the byte
			// was still consumed from the wire and the third-party
			// transaction continues, so observers still need it.
			// (Codex P2 round 8 on PR #620.)
			//
			// `pendingStart == nil` (no bid at all) is similarly a
			// pure passive observation: deliver to all sessions +
			// emit passive.
			// Phantom-collision filter (proxy-bug C5 / R5): compute
			// the predicate result BEFORE we take stateMu so the
			// (potentially operator-supplied) callback never runs
			// under lock. The IsKnownInitiatorByte predicate is
			// pure / read-only by contract, so this snapshot is
			// safe regardless of any concurrent state change.
			isPhantom := false
			if m.cfg.IsKnownInitiatorByte != nil && !m.cfg.IsKnownInitiatorByte(event.Data) {
				isPhantom = true
				m.logger.Printf("adaptermux: phantom FAILED data=0x%02X (not a known bus initiator) — substituting bidder's own initiator on notify", event.Data)
			}

			// Single stateMu critical section that fuses three
			// previously-separate steps and closes the round-6
			// unlock/relock race (Codex P2 on PR #624). Order matters:
			//
			//   - Bump lastWireActivity FIRST so any concurrent
			//     requestStartForSession that grabs stateMu after
			//     this section observes the fresh timestamp and does
			//     NOT take the idle-kick path. (Codex P2 round 6
			//     on PR #623 + Codex P2 on PR #624.)
			//
			//   - Snapshot the active bidder so the FAILED routing
			//     below can decide where to deliver the winner byte
			//     and whether to suppress the bidder's own notify.
			//     `pendingStartAbsorb > 0` indicates a stale FAILED
			//     for a cancelled bid; `pendingStart == nil` is a
			//     pure passive observation.
			//
			//   - Substitute the phantom-byte → bidder's own
			//     initiator atomically with the snapshot so a
			//     concurrent re-submit cannot move the pointer
			//     between read and use.
			//
			// All three reads happen under one acquire/release of
			// stateMu — no goroutine can interleave between the
			// bump and the snapshot.
			notifyByte := event.Data
			var activeBidderSessionID uint64
			var hasActiveBidder bool
			m.stateMu.Lock()
			m.lastWireActivity = time.Now()
			if m.pendingStartAbsorb == 0 && m.pendingStart != nil {
				activeBidderSessionID = m.pendingStart.sessionID
				hasActiveBidder = true
				if isPhantom && m.pendingStart.sessionID == activeBidderSessionID {
					notifyByte = m.pendingStart.initiator
				}
			}
			m.stateMu.Unlock()

			m.handleArbitrationResponse(false, notifyByte)

			// Synthesize the winner byte:
			//   - No active bidder (absorbed-stale OR no pending):
			//     deliver to all external sessions. The wire byte
			//     was consumed; the rest of the third-party frame
			//     will continue through onReceived. Without this,
			//     external monitors and the passive reconstructor
			//     would parse the next target/PB byte as the frame
			//     source and desynchronize.
			//   - Gateway-lost: deliver to all external sessions
			//     (gateway has no session in m.sessions, no skip).
			//   - External-lost: deliver to all OTHER external
			//     sessions (skip the losing bidder; their session's
			//     handleStart goroutine delivers ENHResFailed via
			//     deliverFailed, which would race a synthesized
			//     RECEIVED(winner)). Multi-monitor deployments need
			//     the byte on the non-bidder sessions to keep their
			//     bus state synced. (Codex P2 round 3 on PR #620.)
			if !isPhantom {
				switch {
				case !hasActiveBidder:
					m.deliverToSessions(event.Data, 0, false, time.Now())
				case activeBidderSessionID == gatewaySessionID:
					m.deliverToSessions(event.Data, 0, false, time.Now())
				default:
					m.deliverWinnerByteToOtherSessions(event.Data, activeBidderSessionID)
				}
			}
			// FAILED always means a non-gateway initiator won the
			// arbitration (gateway-win produces STARTED, not FAILED).
			// Emit the winner byte to passive observers so the bus
			// reconstructor sees the correct frame source for the
			// third-party transaction that's about to flow through
			// onReceived. Emit unconditionally — the wire byte
			// happened irrespective of whether a gateway bid was
			// pending. (Codex P2 round 5 + round 8 on PR #620.)
			m.emitPassive(PassiveEvent{
				Kind:       PassiveEventSymbol,
				Symbol:     event.Data,
				ObservedAt: time.Now(),
			})
			continue
		case transport.StreamEventReset:
			m.handleReset()
			continue
		case transport.StreamEventByte:
			// F-23 (batch-19, Codex bot on PR-2): thread the upstream
			// WasEscaped flag through onReceived so adapter-direct
			// passive consumers see the same provenance the raw-TCP
			// passive observers get via their local escape decoder.
			m.onReceived(event.Byte, event.WasEscaped)
		default:
			m.onReceived(event.Byte, event.WasEscaped)
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
//
// onReceived processes one logical byte from the upstream
// transport. wasEscaped (F-23 / Codex bot on PR-2) is the provenance
// flag from transport.StreamEvent.WasEscaped — true when the byte
// was decoded from an eBUS wire escape pair (so a logical 0xAA value
// is user payload, not a wire SYN; a logical 0xA9 is data, not an
// escape lead). It's threaded into the passive-event emissions and
// into activeCh so consumers downstream of the mux see the same
// per-byte truth as a raw-TCP passive observer would via the local
// escape decoder.
func (m *Mux) onReceived(symbol byte, wasEscaped bool) {
	now := time.Now()

	// --- Phase 1: state update under stateMu ---
	m.stateMu.Lock()

	ownerID, _, hasOwner := m.arb.owner()

	// Track wire activity for two purposes:
	//   1. F-10v2 (PR #621): inter-byte-gap measurement that drives
	//      SYN-timeout / idle-release decisions for external owners.
	//      Every non-SYN byte resets the activity clock, so a long-
	//      running external transaction with real inter-responder
	//      traffic does not get torn down purely because the grant
	//      itself is old.
	//   2. Proxy-bug C1 (PR #623): the bus-idle fast path in
	//      tryGrant + the requestStartForSession enqueue-kick both
	//      consult lastWireActivity to decide whether the wire is
	//      quiescent. THIRD-PARTY passive frames must count as "wire
	//      busy" too — otherwise the idle check fires while a non-
	//      mux initiator is mid-transaction and the kick issues an
	//      adapter START in the middle of passive traffic. (Codex
	//      P2 round 5 on PR #623.)
	//
	// SYN markers do NOT bump the activity clock — they are the very
	// signal we use to decide when the bus has been idle long enough
	// for a release.
	//
	// F-23 (batch-19, Codex bot on PR-2): the SYN exclusion keys on
	// the wire-side classification, not on byte value. An escape-
	// decoded payload 0xAA (wasEscaped=true) is genuine bus traffic
	// from a third-party frame and MUST refresh the activity clock;
	// only a real un-escaped wire SYN counts as bus-idle.
	if !(symbol == protocol.SymbolSyn && !wasEscaped) {
		m.lastWireActivity = now
	}

	// Skip wire phase tracking for non-SYN bytes during gateway ownership.
	// The gateway's bus.Send handles the transaction directly via echo
	// matching. Skipping advance() for data bytes prevents premature
	// WaitCmdAck → CmdNACK → idle from off-by-one byte counting.
	//
	// SYN is always processed: ownership release depends on the SYN
	// handler (SYNIdle + IdleReleaseGrace or SYNTimeout). During gateway
	// ownership, treat SYN as SYNIdle since the data-phase tracking is
	// skipped.
	//
	// F-23 (batch-19, Codex bot on PR-2): require wasEscaped=false
	// for the SYN classification. An escape-decoded payload 0xAA
	// (wasEscaped=true) is user data from a third-party frame, NOT
	// a wire SYN — treating it as SYN would falsely release
	// ownership and corrupt the wire-phase tracker. Same
	// distinction the passive reconstructor already enforces (F-19d).
	isWireSyn := symbol == protocol.SymbolSyn && !wasEscaped
	var phaseEvent wirePhaseEvent
	if isWireSyn {
		if hasOwner && ownerID == gatewaySessionID {
			// Gateway owns bus, phase tracking skipped for data bytes.
			// Treat SYN as idle so IdleReleaseGrace controls release.
			phaseEvent = wirePhaseEventSYNIdle
			m.phase.reset(wirePhaseIdle)
		} else {
			// F-23 (Codex bot on PR-2): provenance-aware advance —
			// isWireSyn=true tells the tracker this byte IS a real
			// wire SYN. Equivalent to the value-only contract here
			// but propagates the classification consistently.
			phaseEvent = m.phase.advanceWithProvenance(symbol, true)
		}
	} else if !hasOwner || ownerID != gatewaySessionID {
		// F-23 (Codex bot on PR-2): non-SYN branch. Pass
		// isWireSyn=false explicitly so the tracker does NOT
		// internally fall back to its value-only `symbol ==
		// SymbolSyn` check — an escape-decoded payload 0xAA
		// (wasEscaped=true here) must be treated as data, not as
		// a SYN frame terminator.
		phaseEvent = m.phase.advanceWithProvenance(symbol, false)
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

	// F-23 (batch-19, Codex bot on PR-2): use the wire-SYN
	// classification computed above so escape-decoded payload 0xAA
	// (wasEscaped=true) takes the non-SYN branch.
	if isWireSyn {
		// Shape diag: count SYN markers seen during gateway ownership
		// while a gateway transaction is actually active. Pre-write or
		// post-inactive idle chatter must not contaminate the current txn.
		// Must run BEFORE onSYNLocked so the counter reflects the SYN that
		// triggers a possible inactive transition.
		if hasOwner && ownerID == gatewaySessionID && m.gatewayTxnActive {
			m.recordReadPrefixAndClassify(symbol)
		}
		// Runtime-soak: bus.Send returns BEFORE the trailing SYN (reads
		// are count-based, not SYN-terminated). So onSYNLocked's
		// gatewayTxnActive=false is the correct state for delivery —
		// capture AFTER to skip the trailing SYN that has no consumer.
		var preEchoSuppressed bool
		passiveEvents, shouldTryGrant, preEchoSuppressed = m.onSYNLocked(phaseEvent, ownerID, hasOwner, now)
		activeExpects := m.activePathExpectsByte(symbol)
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

		// F-18: per-session echo trackers removed; nothing to reset
		// for external sessions on ownership timeout, and no SYN-flush
		// to perform. Gateway-side echo handling continues unchanged
		// via m.gatewayEcho.
		_ = ownershipTimedOut

		// --- Phase 2: deliver outside all locks ---

		if activeExpects {
			// activeExpects was decided under stateMu above and implies
			// the gateway owns the bus with gatewayTxnActive=true — count
			// this enqueue so bytesDeliveredToActive reflects real
			// adapter-originated bytes delivered during this grant.
			//
			// P12 pass-2 (Codex P1): re-acquire stateMu and re-validate
			// gatewayTxnActive before delivering. Mirrors the non-SYN
			// path at line ~1220. Pre-pass-2 sendLoop could drain+arm
			// for byte K+1 between stateMu.Unlock above and this
			// deliverToActive call — landing the SYN in activeCh AFTER
			// the drain, where bus.Send.sendRawWithEcho on byte K+1
			// reads it as 0xAA → echo_mismatch (pre_echo_syn). With
			// re-acquisition, P12's drain-then-arm under stateMu is
			// genuinely atomic with this enqueue.
			m.stateMu.Lock()
			if m.gatewayTxnActive {
				select {
				case m.activeCh <- activeEvent{kind: activeEventByte, b: symbol}:
					m.activeTxn.bytesDeliveredToActive.Add(1)
				default:
					m.activeTxn.terminatorDropOnFullCh.Add(1)
					m.logger.Printf("adaptermux: active channel full, dropping SYN 0x%02X", symbol)
				}
			} else if hasOwner && ownerID == gatewaySessionID {
				m.activeTxn.afterInactive.Add(1)
			}
			m.stateMu.Unlock()
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
	// P11 round-2 (Codex P1 findings on PR #606) — capture the echo
	// queue head BEFORE matchEcho consumes a matching byte. The filter
	// uses the PRE-MATCH queue head as the protocol-accurate "what byte
	// is bus.Send currently waiting for" signal.
	//
	// Why not echoCursor / writePrefix:
	//   - writePrefix is capped at txnPrefixCap=8 (diagnostic). Frames
	//     longer than 8 bytes would have echoCursor==writePrefixLen
	//     forever after byte 8, opening response phase prematurely.
	//   - recordReadPrefixAndClassify advances echoCursor on a NON-match
	//     too (diagnostic side effect). A single mid-write stale byte
	//     would advance the cursor past the missing echo, and a second
	//     stale byte before the real echo would slip through.
	//
	// gatewayEcho.expectedEchoes is the authoritative protocol state:
	// every byte bus.Send writes is recorded; matchEcho only consumes
	// on actual match. Uncapped, mismatch-resistant.
	preMatchHead, hadPendingEcho := m.gatewayEcho.peekNextExpected()
	// P11 round-2 cascade fix: only call matchEcho when it will MATCH
	// (or when the queue is empty — then it's a no-op). matchEcho's
	// mismatch branch CLEARS the queue, which would destroy our filter
	// state for cascading stale bytes (a single mid-write stale byte
	// would clear the queue and the second stale byte would slip
	// through as response-phase). By gating the call on a match check
	// we preserve the queue across stale bytes.
	matchWouldHit := !hadPendingEcho || (hadPendingEcho && symbol == preMatchHead)
	if isGatewayOwned && matchWouldHit {
		m.gatewayEcho.matchEcho(symbol) // track echo state internally
	}
	// P11 — gate activeCh delivery: mid-write requires queue-head match;
	// response phase (no pending writes) accepts any byte.
	//
	// Pre-P11 the non-SYN path used the bulk filter
	// (activePathExpectsBytes — just gatewayTxnActive), so any byte
	// during ownership flowed through and post_grant_ack accounted for
	// ~52% of all echo_mismatch. P11 round-1 used echoCursor /
	// writePrefix as the gate but those are capped diagnostic state;
	// P11 round-2 uses gatewayEcho's pre-match queue head, which is
	// protocol-accurate and uncapped.
	activeExpects := m.gatewayTxnActive
	if activeExpects && hadPendingEcho {
		// Mid-write: only the next expected echo passes.
		activeExpects = symbol == preMatchHead
	}
	// hadPendingEcho == false: response phase open, activeExpects
	// stays as gatewayTxnActive (true) — any byte delivered.
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
	// Restrict to the live gateway transaction window so pre-write or
	// post-inactive ownership periods do not pollute the per-txn prefix.
	if isGatewayOwned && m.gatewayTxnActive {
		m.recordReadPrefixAndClassify(symbol)
	}

	m.stateMu.Unlock()

	// F-18: per-session echo trackers removed; nothing to reset on
	// ownership timeout.
	_ = ownershipTimedOut

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
		if m.gatewayTxnActive {
			select {
			// F-23 (Codex bot on PR-2): thread the upstream
			// WasEscaped flag into activeCh so the gateway's
			// protocol.Bus consumer (via
			// activeTransport.ReadByteWithEscape) can distinguish
			// real wire SYN from escape-decoded payload 0xAA in
			// waitForSyn / sendRawWithEcho.
			case m.activeCh <- activeEvent{kind: activeEventByte, b: symbol, wasEscaped: wasEscaped}:
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
			Kind: PassiveEventSymbol, Symbol: symbol, WasEscaped: wasEscaped, ObservedAt: now,
		})
	}
	for _, pe := range passiveEvents {
		m.emitPassive(pe)
	}

	m.deliverToSessions(symbol, ownerID, hasOwner, now)

	if phaseEvent == wirePhaseEventTransactionDone ||
		phaseEvent == wirePhaseEventCmdNACK {
		// F-21 (batch-20): under F-21 this non-SYN release block is
		// reachable ONLY via the protocol-error garbled-byte fallback
		// in advanceWaitCmdAck (which still resets to Idle and returns
		// wirePhaseEventCmdNACK at the non-SYN byte position — see the
		// "Neither ACK nor NACK" branch comment in wire_phase.go for
		// the rationale). All five normal structural-terminal sites
		// (broadcast ACK, i2i ACK, CMD double-NACK, response double-
		// NACK, response success ACK) now transition to
		// wirePhaseWaitTerminalSyn and defer TransactionDone to the
		// trailing-SYN observation; that release happens in
		// onSYNLocked's F-21 branch. Keeping this block intact closes
		// the rare protocol-error case where the bus is too degraded
		// to emit a clean trailing SYN.
		//
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
	nextExpectedEcho, hasPendingEcho := m.gatewayEcho.peekNextExpected()
	midWriteSyn := hasPendingEcho && nextExpectedEcho != protocol.SymbolSyn

	// Flush gateway echo tracker at SYN boundary. Flushed bytes are
	// confirmed gateway self-traffic — do NOT emit to passive path
	// (passive is third-party only). They are delivered to external
	// sessions via deliverSYNToSessions + deliverToSessions elsewhere.
	//
	// P10.2: skip flush when this SYN is mid-write noise. The expected-
	// echo queue must survive so the next real echo of the pending
	// non-SYN byte still matches.
	preEchoMidFrameSuppress := wasGatewayOwned && m.gatewayTxnActive && midWriteSyn
	if !preEchoMidFrameSuppress {
		m.gatewayEcho.flushOnSYN()
	}

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
	// P10.2: terminator gate gains an additional guard `!midWriteSyn`.
	// Without this guard, mid-write SYNs (stale buffered SYN from before
	// grant interleaved into mid-frame echo flow, wire collision,
	// foreign-initiator glitch) would be delivered to activeCh while
	// bus.Send is waiting for echo of a non-SYN data byte → 0xAA in
	// echo position → echo_mismatch with subclass=pre_echo_syn (671
	// events observed in production soak before this fix; ~16% of all
	// echo_mismatch). The legacy "queue empty → assume terminator"
	// branch (pre-P10.2 default) still fires for the bytesDeliveredToActive>0
	// case where the gateway has consumed all its echoes — that
	// preserves the lifecycle contract and existing tests. Only the
	// specific mid-write race (queue head is a non-SYN byte) is closed.
	terminatorDelivered := false
	if hasOwner && ownerID == gatewaySessionID && m.gatewayTxnActive &&
		m.activeTxn.bytesDeliveredToActive.Load() > 0 &&
		!midWriteSyn {
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
	if phaseEvent == wirePhaseEventTransactionDone && hasOwner {
		m.logger.Printf("adaptermux: ownership released for session %d remote=%s (terminal SYN, F-21)",
			ownerID, m.sessionRemoteAddrOrUnknown(ownerID))
		m.arb.releaseOwnership(ownerID)
		if ownerID == gatewaySessionID && m.gatewayTxnActive {
			m.gatewayTxnActive = false
			m.recordGatewayInactive(ReasonTransactionDone)
		}
		hasOwner = false
	}

	// Release ownership if SYN timeout.
	//
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
	if phaseEvent == wirePhaseEventSYNTimeout && hasOwner {
		releaseNow := ownerID == gatewaySessionID
		if !releaseNow {
			// Measure the actual inter-byte gap, not time since
			// grant. m.lastWireActivity is bumped by every non-SYN
			// adapter byte while ownership is held, so an external
			// transaction with live wire traffic resets the clock
			// each time. A 5-second-long ebusd scan with regular
			// inter-responder bytes will never trip this branch
			// based on grant age alone. (Codex P2 round-1 on PR
			// #621.)
			elapsed := time.Since(m.lastWireActivity)
			if elapsed >= m.cfg.ExternalSessionSYNGrace {
				releaseNow = true
				m.logger.Printf("adaptermux: external session %d remote=%s SYN-timeout idle-gap %v ≥ grace %v — releasing",
					ownerID, m.sessionRemoteAddrOrUnknown(ownerID), elapsed, m.cfg.ExternalSessionSYNGrace)
			}
			// Otherwise: hold ownership. The external session's
			// protocol is still mid-frame from its own perspective;
			// the next legitimate adapter event will either advance
			// the wire phase or trigger MaxOwnershipDuration.
		}
		if releaseNow {
			m.logger.Printf("adaptermux: ownership released for session %d remote=%s (SYN timeout) (AM6)",
				ownerID, m.sessionRemoteAddrOrUnknown(ownerID))
			m.arb.releaseOwnership(ownerID)
			if ownerID == gatewaySessionID && m.gatewayTxnActive {
				m.gatewayTxnActive = false
				m.recordGatewayInactive(ReasonSYNTimeout)
			}
		}
	}

	// Release ownership on idle SYN after grace period.
	//
	// P10.2 — gate idle release ONLY for live mid-frame races, NOT for
	// stuck-write timeouts. The distinguishing signal:
	//   - bytesDeliveredToActive > 0 + preEchoMidFrameSuppress: the
	//     gateway has live echo activity AND a noise SYN arrived
	//     mid-write. Aborting the txn would discard real progress.
	//     Suppress idle release.
	//   - bytesDeliveredToActive == 0 + preEchoMidFrameSuppress: the
	//     gateway wrote bytes but NO echo ever arrived. The bus is
	//     stuck/unresponsive. The IdleReleaseGrace mechanism is exactly
	//     designed to bail out here. Allow idle release. (Used by
	//     TestRegression_StartedButNoResponse to model production
	//     unresponsive-target scenarios.)
	suppressIdleRelease := preEchoMidFrameSuppress &&
		m.activeTxn.bytesDeliveredToActive.Load() > 0
	if phaseEvent == wirePhaseEventSYNIdle && hasOwner && !suppressIdleRelease {
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
		var releaseNow bool
		var elapsed time.Duration
		var graceUsed time.Duration
		var f26ExternalBypass bool
		if ownerID == gatewaySessionID {
			elapsed = time.Since(m.busOwned)
			graceUsed = m.cfg.IdleReleaseGrace
			releaseNow = elapsed > graceUsed
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
			if !releaseNow && m.arb.hasExternalPending() {
				releaseNow = true
				f26ExternalBypass = true
			}
		} else {
			elapsed = time.Since(m.lastWireActivity)
			graceUsed = m.cfg.ExternalSessionSYNGrace
			releaseNow = elapsed >= graceUsed
		}
		if releaseNow {
			if f26ExternalBypass {
				m.logger.Printf("adaptermux: ownership released for session %d remote=%s (F-26 external START pending: elapsed %v skipping grace %v) (AM6)",
					ownerID, m.sessionRemoteAddrOrUnknown(ownerID), elapsed, graceUsed)
			} else {
				m.logger.Printf("adaptermux: ownership released for session %d remote=%s (idle grace expired: %v ≥ %v) (AM6)",
					ownerID, m.sessionRemoteAddrOrUnknown(ownerID), elapsed, graceUsed)
			}
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
	preFirstEchoSyn := hasOwner && ownerID == gatewaySessionID &&
		m.gatewayTxnActive && m.activeTxn.bytesDeliveredToActive.Load() == 0
	preEchoSuppressed := preFirstEchoSyn || preEchoMidFrameSuppress
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

	// F-18: per-session echo trackers were removed.

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

// drainActiveChBytesOnly discards only byte events (activeEventByte)
// from the active channel, preserving lifecycle events (activeEventError
// — reset / disconnect boundaries). Codex P12 review pass-1 P1: the
// inter-write drain must NOT swallow reset boundary signals; bus.Send
// needs to see them to abort cleanly. Re-enqueues any preserved error
// event back onto activeCh in order.
//
// Returns the number of byte events drained.
//
// Caller must hold stateMu (we're racing onReceived which also writes
// to activeCh under stateMu — peer-of-equals discipline).
func (m *Mux) drainActiveChBytesOnly() int {
	n := 0
	var preserved []activeEvent
	for {
		select {
		case ev := <-m.activeCh:
			if ev.kind == activeEventByte {
				n++
				continue
			}
			preserved = append(preserved, ev)
		default:
			// Re-enqueue preserved lifecycle events in order. Non-
			// blocking — if activeCh is full (capacity 4096), log
			// and continue (the lifecycle event was already in the
			// channel pre-drain so this is a no-regression path).
			for _, ev := range preserved {
				select {
				case m.activeCh <- ev:
				default:
					m.logger.Printf("adaptermux: drain re-enqueue full, lifecycle event dropped: kind=%d err=%v", ev.kind, ev.err)
				}
			}
			return n
		}
	}
}

// activePathExpectsBytes — REMOVED in P11. Pre-P11 the non-SYN
// delivery path used this bulk filter (just gatewayTxnActive); P11
// switched to the per-byte filter activePathExpectsByte(symbol) which
// is symmetric across pre/post-first-echo and rejects mid-write stale
// bytes. The function's comments still appear historically in
// onSYNLocked (referring to the gating semantics, which are now
// embodied by activePathExpectsByte's mid-write/response phase
// branches).

// activePathExpectsByte — REMOVED in P11 round-2. The per-byte gate
// is now inlined in onReceived using gatewayEcho's pre-match queue
// head (protocol-accurate; uncapped). The previous round used
// echoCursor/writePrefix which were capped at txnPrefixCap=8 AND
// advanced on non-match — both broken signals (Codex P1 findings on
// PR #606). The SYN-path call site (line 1120) still uses the legacy
// helper; preserved as a stub returning the same gatewayTxnActive
// signal the SYN handler expects (the SYN-specific suppression logic
// lives in onSYNLocked's preEchoMidFrameSuppress / midWriteSyn —
// independent of this byte-level filter).
func (m *Mux) activePathExpectsByte(symbol byte) bool {
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
//
// P12 pass-2 (Codex P1): both call sites now perform their own
// stateMu.Lock + revalidation around the enqueue (mirror of the non-SYN
// path at mux.go:1220-1239). This helper is kept for callers that need
// the standalone non-blocking send semantics; currently unused at
// compile time but retained for the documented "shutdown/reset
// passthrough" use case.
//
//nolint:unused // retained for future shutdown/reset callers per godoc
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
// requestStartForSession submits a START request via the arbitrator
// and additionally:
//
//   - Marks any in-flight grant for the SAME session as cancelled
//     (proxy-bug C4 / R4 in-flight branch — Codex P1 round 1 on
//     PR #623). The arbitrator's replace path inside requestStart
//     can only flag entries that are still in pendingExternal /
//     pendingGateway; once tryGrant has popped a request into
//     m.pendingStart, a same-session re-submit no longer finds it
//     in the arbitrator and the cancelled flag would otherwise stay
//     false. Catching the in-flight case here is what makes
//     handleArbitrationResponse's cancelled-check actually fire in
//     production.
//
//   - Opportunistically kicks tryGrantAndStart if the bus is idle
//     (proxy-bug C1 fast-path enqueue trigger — Codex P1 round 1
//     on PR #623). The mux's primary grant rhythm is driven by SYN
//     arrivals via readLoop; on a quiet bus that means a new START
//     can wait up to ReadTimeout (~200 ms default) before the first
//     grant attempt — long past the C3 PendingStartTTL window of
//     50 ms. Kicking on enqueue lets the idle fast-path grant a
//     submission immediately when no SYN is imminent.
//
// All Mux/session/active-path callers MUST route through this method
// rather than calling m.arb.requestStart directly, so both branches
// of the C4 cancel + the C1 enqueue-kick are guaranteed to run.
func (m *Mux) requestStartForSession(sessionID uint64, initiator byte) <-chan startResult {
	// Steps 1+2 MUST be atomic under stateMu so a concurrent
	// tryGrantAndStart cannot pop the OLD request out of
	// pendingExternal between the in-flight check and the
	// arbitrator replacement — that gap would leave a re-submitted
	// session with neither flag set (Codex P1 round 4 on PR #623).
	// Lock order stateMu → arb.mu matches tryGrantAndStart's order,
	// so no ABBA risk.
	m.stateMu.Lock()
	// Step 1: cancel any in-flight grant for THIS session. The
	// arbitrator's replace path (inside requestStart) only sees
	// pendingExternal/pendingGateway entries; once an entry has
	// been popped into m.pendingStart this is the only place the
	// cancelled flag gets set.
	if m.pendingStart != nil && m.pendingStart.sessionID == sessionID && m.pendingStart.req != nil {
		m.arb.markInFlightCancelled(m.pendingStart.req)
	}

	// Step 2: hand off to the arbitrator. Holding stateMu across
	// this call is what closes the race: tryGrantAndStart's pop
	// is also gated on stateMu, so it cannot interleave between
	// step 1 and step 2.
	ch := m.arb.requestStart(sessionID, initiator)

	// Step 3: opportunistic kick — only when the wire has been quiet
	// for at least one SYN interval. The SYN-driven grant rhythm in
	// readLoop won't fire for up to ReadTimeout (~200 ms) on a
	// genuinely quiet bus; without this kick a START can sit
	// unattended past the C3 PendingStartTTL window. On a busy bus
	// the next SYN handler will pick up the grant naturally, so we
	// don't intrude on the established arbitration cadence. The
	// idle check mirrors the snapshot used inside tryGrantAndStart
	// for the C1 fast path. Read the snapshot while still holding
	// stateMu so we get a coherent view before releasing.
	idle := m.lastWireActivity.IsZero() ||
		time.Since(m.lastWireActivity) >= m.cfg.SYNInterval
	m.stateMu.Unlock()
	if idle {
		m.tryGrantAndStart()
	}

	return ch
}

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
	// tryGrantLegacy(arb) dequeue, and the pendingStart assignment in a
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
	// Do not launch a new START while we still expect at least one stale
	// STARTED/FAILED from a cancelled or deadline-expired RequestStart.
	// A client often reuses the same initiator, so once we have a new
	// pending request there is no reliable way to distinguish the old
	// STARTED from the new one. Treat pendingStartAbsorb as a real regrant
	// barrier, not just a best-effort diagnostic counter.
	if m.pendingStartAbsorb > 0 {
		m.logger.Printf("adaptermux: tryGrantAndStart skipped — waiting to absorb %d stale arbitration response(s)", m.pendingStartAbsorb)
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

	// Bus-idle snapshot for the proxy-bug C1 (R1) fast path: the
	// wire has been quiet for at least one SYN interval. When the
	// fast path engages, tryGrant hands the external FIFO head over
	// directly and skips the fairness rotation — fairness has
	// nothing to balance when there is no real contention.
	// lastWireActivity is bumped on every non-SYN adapter byte while
	// ownership is held; on a fresh boot it may be zero (no traffic
	// yet), which we treat as idle so the first external START
	// doesn't wait a fairness quantum. tryGrant itself rejects when
	// the bus is owned, so this snapshot does not need to consult
	// the arbitrator's ownership state.
	busIdle := m.lastWireActivity.IsZero() ||
		time.Since(m.lastWireActivity) >= m.cfg.SYNInterval

	req, granted := m.arb.tryGrant(busIdle)
	if !granted {
		m.stateMu.Unlock()
		return
	}
	sessionID := req.sessionID
	initiator := req.initiator
	notify := req.notify

	m.pendingStart = &pendingStartState{
		sessionID:   sessionID,
		initiator:   initiator,
		notify:      notify,
		blockingArb: isBlockingPath,
		req:         req,
	}
	// AM8: start a deadline timer so pendingStart cannot block indefinitely
	// if the adapter never responds with STARTED/FAILED.
	m.pendingStart.deadline = time.AfterFunc(m.cfg.StartDeadline, func() {
		m.stateMu.Lock()
		if m.pendingStart != nil && m.pendingStart.notify == notify {
			pending := m.pendingStart
			m.pendingStart = nil
			m.armPendingStartAbsorbLocked("deadline")
			// F-15 (operator hand-off, batch-9/11): gate the reconnect by
			// transport type, mirroring `cancelPendingStart`'s existing
			// `wasBlocking := pending.blockingArb` pattern.
			//
			// PRIOR BUG (`needReconnect := true` unconditionally): every
			// deadline expiry forced an upstream reconnect — including on
			// the non-blocking ENH RequestStart path where the adapter is
			// only slow, not hung. F-17's retry storm amplified this:
			// adapter backlog → slow STARTED → AM8 trips → forced
			// reconnect → drops the next ReqStart → loop. Even without
			// F-17 the asymmetric reconnect is a design defect on the
			// non-blocking path.
			//
			// LIVE EVIDENCE: 0 absorb-timeout reconnects across the entire
			// 30k-line batch-9 log. The absorb safety-net inside
			// `armPendingStartAbsorbLocked` drained naturally on every
			// late-STARTED in production. The unconditional reconnect
			// here was therefore never needed for the non-blocking
			// transport in observed runs.
			//
			// F-22 (batch-19, 2026-05-13): the absorb safety-net's own
			// timeout side effect is now a counter reset ONLY — it does
			// NOT close the transport. Covered by
			// `TestF22_AbsorbTimerResetsCounterWithoutReconnect`. The
			// truly-hung scenario the original code feared is recovered
			// via the bus poll loop's next RequestStart on the still-
			// open connection; the prior reconnect-on-absorb-timeout
			// was load-bearing only on the legacy blocking transport.
			//
			// BLOCKING TRANSPORT (legacy StartArbitration): the goroutine
			// may still be hung in the transport call after the deadline,
			// so the reconnect is still required to unstick it — same
			// shape as `cancelPendingStart`.
			wasBlocking := pending.blockingArb
			// F-17 follow-up (PR #626 review round-3, angry-tester
			// finding M1): the AM8 deadline path resolves the same
			// pending startRequest that the three branches of
			// handleArbitrationResponse resolve, and it can fire
			// PRECISELY when the adapter is slow — the exact
			// production condition that motivated this PR cycle.
			// Without the cancelled-check, a same-session re-submit
			// that flipped `pending.req.cancelled` via
			// markInFlightCancelled will see the old wait goroutine
			// fall through to deliverFailed("…deadline expired…")
			// → ENHResFailed on the wire. Same retry-feedback-loop
			// class as F-17 / F-NEW-1, just gated on AM8 instead of
			// the normal arrival.
			cancelledInFlight := pending.req != nil && pending.req.cancelled.Load()
			m.stateMu.Unlock()
			m.logger.Printf("adaptermux: pendingStart deadline expired for session %d (AM8, wasBlocking=%v, cancelledInFlight=%v)", pending.sessionID, wasBlocking, cancelledInFlight)
			// AM8: guard the send — if another path already delivered a
			// result, the channel is full and this send would block forever.
			var result startResult
			if cancelledInFlight {
				result = startResult{
					granted:   false,
					cancelled: true,
					initiator: pending.initiator,
					err:       errors.New("adaptermux: START deadline expired (superseded by client re-submit)"),
				}
			} else {
				result = startResult{
					granted:   false,
					initiator: pending.initiator,
					err:       errors.New("adaptermux: START deadline expired"),
				}
			}
			select {
			case pending.notify <- result:
			default:
				m.logger.Printf("adaptermux: pendingStart deadline: notify channel full for session %d, result already delivered", pending.sessionID)
			}
			if wasBlocking {
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
				// F-17 follow-up (PR #626 review round-3, angry-tester
				// finding M2): a same-session re-submit could race
				// with the transport-call returning an error. If
				// cancelled-in-flight, suppress with `cancelled: true`
				// so the old wait goroutine silent-returns rather than
				// emitting ENHResFailed on the wire (same class as M1).
				//
				// F-17 follow-up (PR #626 Codex bot P2, 2026-05-11):
				// EXCEPTION — when the transport err is a reset/
				// disconnect boundary event (isResetOrDisconnectError),
				// the cancelled flag MUST NOT be set, even if the
				// in-flight req is cancelled. Session.go's branch order
				// (`granted → cancelled → err(reset) → deliverFailed`)
				// would silent-return on `cancelled: true` and the
				// client would miss the RESETTED delivery it needs to
				// observe the boundary event. This is the same
				// precedence rule documented in
				// `arbitration.failAllPending`'s contract block.
				cancelledInFlight := m.pendingStart.req != nil && m.pendingStart.req.cancelled.Load()
				deliverAsCancelled := cancelledInFlight && !isResetOrDisconnectError(err)
				if m.pendingStart.deadline != nil {
					m.pendingStart.deadline.Stop() // AM8: cancel deadline timer
				}
				m.pendingStart = nil
				m.stateMu.Unlock()
				if deliverAsCancelled {
					notify <- startResult{granted: false, cancelled: true, initiator: initiator, err: err}
				} else {
					notify <- startResult{granted: false, initiator: initiator, err: err}
				}
			} else {
				// RequestStart failed AND pending was already cancelled.
				// Since the adapter never received the START, no stale
				// response will arrive — decrement absorb counter so the
				// next real arbitration response is not incorrectly consumed.
				shouldAdvance := false
				if m.pendingStartAbsorb > 0 {
					m.pendingStartAbsorb--
					shouldAdvance = m.pendingStartAbsorb == 0 && m.arb.hasPending()
				}
				m.stateMu.Unlock()
				if shouldAdvance {
					m.tryGrantAndStart()
				}
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
					// F-17 follow-up (PR #626 review round-3, angry-tester
					// finding M2, blocking-half): same as the non-blocking
					// RequestStart-err path — a same-session re-submit
					// could race with the transport-call returning an
					// error. Suppress with `cancelled: true` when the
					// in-flight req is cancelled.
					//
					// F-17 follow-up (PR #626 Codex bot P2, 2026-05-11):
					// EXCEPTION — when the transport err is a reset/
					// disconnect boundary event, the cancelled flag MUST
					// NOT be set so session.go's err-routing path drives
					// deliverReset(...) instead of silent-returning. Same
					// precedence as the non-blocking RequestStart-err
					// path above.
					cancelledInFlight := m.pendingStart.req != nil && m.pendingStart.req.cancelled.Load()
					deliverAsCancelled := cancelledInFlight && !isResetOrDisconnectError(err)
					if m.pendingStart.deadline != nil {
						m.pendingStart.deadline.Stop() // AM8: cancel deadline timer
					}
					m.pendingStart = nil
					m.stateMu.Unlock()
					if deliverAsCancelled {
						notify <- startResult{granted: false, cancelled: true, initiator: initiator, err: err}
					} else {
						notify <- startResult{granted: false, initiator: initiator, err: err}
					}
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
	now := time.Now()
	m.busOwned = now
	m.lastWireActivity = now
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
		m.gatewayTxnActive = false
		m.recordGatewayGrant(initiator, drained)
	}
	m.arb.confirmOwnership(sessionID, initiator)
	m.stateMu.Unlock()

	// F-18: per-session echo tracker removed; gateway-side
	// markRequestStart is invoked above for the gateway path only.

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
		shouldAdvance := !started && m.pendingStartAbsorb == 0 && m.pendingStart == nil && m.arb.hasPending()
		// F-15 follow-up (PR #626 Codex bot review on b3b7f13 + e6b96ee
		// — P1 finding): the absorb-consume branch used to reconnect
		// unconditionally on STARTED (`needReconnect := started`). On
		// the non-blocking ENH path that's the same asymmetric design
		// defect F-15 fixed in the AM8 deadline callback — just
		// delayed: AM8 arms absorb without closing, but then the late
		// STARTED arrives, hits THIS branch, and conn.Close fires.
		// The F-15 fix only prevented the IMMEDIATE close; the late
		// absorb still tore down TCP, preserving the retry-feedback
		// loop F-15 was meant to eliminate.
		//
		// Mirror F-15's transport-type gate: reconnect only on the
		// blocking StartArbitration path (where a hung goroutine may
		// still need unsticking). On the non-blocking RequestStart
		// path the adapter is merely slow, eBUS arbitration is
		// per-SYN stateless, and the absorb counter has already done
		// its bookkeeping job by decrementing.
		m.connMu.Lock()
		tr := m.upstream
		m.connMu.Unlock()
		_, hasRequestStart := tr.(arbitrationRequester)
		_, hasBlockingStart := tr.(interface{ StartArbitration(byte) error })
		isBlockingPath := !hasRequestStart && hasBlockingStart
		needReconnect := started && isBlockingPath
		m.stateMu.Unlock()
		kind := "FAILED"
		if started {
			kind = "STARTED"
		}
		m.logger.Printf("adaptermux: absorbed stale %s from cancelled request (data=0x%02X, isBlockingPath=%v)", kind, data, isBlockingPath)
		if needReconnect {
			m.connMu.Lock()
			c := m.conn
			m.connMu.Unlock()
			if c != nil {
				if err := c.Close(); err != nil {
					m.logger.Printf("adaptermux: stale STARTED reconnect close: %v", err)
				} else {
					m.logger.Printf("adaptermux: stale STARTED triggered transport reconnect for arbitration resync (blocking path)")
				}
			}
		}
		if shouldAdvance {
			m.tryGrantAndStart()
		}
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
			//
			// F-17 follow-up (PR #626 review round-2, angry-tester finding
			// F-NEW-1): like the matched-STARTED and FAILED branches below,
			// the AM56 mismatch branch must also honor an in-flight
			// `pending.req.cancelled` flag. If the session has issued a
			// same-session re-submit (markInFlightCancelled flipped the
			// struct flag) and the adapter responds with a STALE STARTED
			// whose byte happens not to match the cancelled bid, the
			// session has already moved on. Delivering
			// `ENHResFailed(winner_byte)` to the old wait goroutine
			// reproduces F-17's retry feedback loop on a narrow but real
			// path. Suppress silently with cancelled=true.
			cancelledInFlight := pending.req != nil && pending.req.cancelled.Load()
			m.pendingStart = nil
			m.armPendingStartAbsorbLocked("started-mismatch")
			if pending.deadline != nil {
				pending.deadline.Stop() // AM8: cancel deadline timer
			}
			// The mismatched-STARTED's `data` byte was consumed
			// from the wire and a third-party transaction is about
			// to flow through onReceived; bump lastWireActivity so
			// the C1 fast path / enqueue-kick treat the wire as
			// non-idle until the next byte event. (Codex P2 round 6
			// on PR #623.)
			m.lastWireActivity = time.Now()
			m.stateMu.Unlock()
			if cancelledInFlight {
				m.logger.Printf("adaptermux: suppressing STARTED-mismatch(0x%02X, pending 0x%02X) for session %d — request was cancelled/replaced; absorbed (C4/R4 AM56-half)", data, pending.initiator, pending.sessionID)
				select {
				case pending.notify <- startResult{
					granted:   false,
					cancelled: true,
					initiator: pending.initiator,
					err:       errors.New("adaptermux: STARTED-mismatch superseded by client re-submit"),
				}:
				default:
				}
			} else {
				m.logger.Printf("adaptermux: STARTED mismatch: got initiator 0x%02X, pending 0x%02X — failing pending session %d (AM56)", data, pending.initiator, pending.sessionID)
				// Notify the bidder with the THIRD-PARTY winner byte (`data`),
				// not the bidder's own `pending.initiator`. This matches the
				// regular FAILED-path semantics below: the bidder's handleStart
				// goroutine forwards `result.initiator` to deliverFailed, so the
				// external session sees ENHResFailed(winner_byte) and learns
				// the actual wire owner. Without this, the bidder would only
				// learn its own bid byte from the failure and its bus
				// reconstructor would misread the next target/PB byte as the
				// frame source (Codex P2 round 7 on PR #620).
				pending.notify <- startResult{
					granted:   false,
					initiator: data,
					err:       fmt.Errorf("adaptermux: STARTED mismatch (got 0x%02X, want 0x%02X)", data, pending.initiator),
				}
			}
			// After resolving, check if more requests are pending.
			if m.arb.hasPending() {
				m.tryGrantAndStart()
			}
			return
		}
		// Proxy-bug C4 / R4: if the session has re-submitted a START
		// for this session while THIS grant was in flight at the
		// adapter, the original startRequest's cancelled flag has
		// been set by arb.requestStart's replace path. The session
		// has already drained its old notify channel (granted=false
		// on the replace) and is now waiting on a fresh notify.
		// Handing it ENHResStarted for the abandoned grant would
		// leave the mux thinking the bus is owned by a session that
		// has moved on; instead, convert the STARTED into a FAILED
		// (no-grant) so the bus is released cleanly and the next
		// tryGrantAndStart re-picks the session's current request.
		if pending.req != nil && pending.req.cancelled.Load() {
			// REVERTED in 0.6.6 (operator hand-off after the v0.6.5
			// regression): previously this branch closed the upstream
			// TCP to the adapter to "resync" after a cancelled bid's
			// STARTED arrived. Live capture proved that reconnect
			// produced a continuous 5-second cycle of "device invalid"
			// / "signal lost" on ebusd's side and tore down the
			// upstream every time any session replaced an in-flight
			// START. The adapter does NOT need TCP resync on a
			// cancelled-bid STARTED — the eBUS protocol is per-SYN
			// stateless from the adapter's perspective; the next SYN
			// boundary resets arbitration. The minimum-correct
			// behaviour is to absorb the STARTED, never deliver it
			// to the originating session, and leave the upstream and
			// every other session alone.
			//
			// This is essentially the original spec for C4/R4:
			// "convert STARTED to FAILED for cancelled requests"
			// without any cross-cutting transport action.
			m.pendingStart = nil
			if pending.deadline != nil {
				pending.deadline.Stop() // AM8: cancel deadline timer
			}
			m.stateMu.Unlock()
			m.logger.Printf("adaptermux: suppressing STARTED for session %d — request was cancelled/replaced; absorbed (C4/R4)", pending.sessionID)
			// F-17 follow-up (PR #626 review round-1, angry-tester
			// finding F-1): the old notify channel MUST receive
			// `cancelled: true` so session.go's handleStart goroutine
			// (session.go:524) takes the silent-return branch instead
			// of falling through to deliverFailed(initiator). Without
			// this flag, the old goroutine still emits
			// ENHResFailed(0x31) on the wire — the exact failure mode
			// F-17 was filed to fix, just on the in-flight-cancel
			// branch instead of the pendingExternal-replace branch.
			//
			// Symmetry: `arb.requestStart`'s pendingExternal-replace
			// and pendingGateway-replace paths set both the struct
			// flag AND the channel-value flag. `arb.cancelStart` sets
			// both as well. `markInFlightCancelled` (called from
			// `requestStartForSession`) sets only the struct flag
			// because the channel-value send was deferred to whichever
			// `handleArbitrationResponse` path eventually fires —
			// THIS branch (matched STARTED for cancelled bid), the
			// FAILED-with-cancelled-req branch below, AND the AM56
			// STARTED-mismatch-with-cancelled-req branch above.
			//
			// The contract is: any code in `handleArbitrationResponse`
			// that observes a cancelled startRequest while resolving
			// the bid normally (no transport-level error) MUST set
			// `cancelled: true` on the value so `session.handleStart`'s
			// branch order (`granted → cancelled → err(reset) →
			// deliverFailed`) silent-returns.
			//
			// EXCEPTION: code paths that deliver a transport-level
			// boundary error via `err` matching `isResetOrDisconnectError`
			// (e.g., `failAllPending` on shutdown / `reconnect`) MUST
			// NOT set `cancelled: true`. Session.go's check order
			// favors `cancelled` over `err(reset)`, so setting both
			// would suppress the RESETTED delivery the client needs.
			//
			// Best-effort send: the old notify channel is buffered to
			// 1 and may already hold the replace's granted=false
			// result. A late STARTED for a cancelled bid still
			// resolves the channel cleanly when the read-side hasn't
			// drained it yet.
			select {
			case pending.notify <- startResult{
				granted:   false,
				cancelled: true,
				initiator: pending.initiator,
				err:       errors.New("adaptermux: START superseded by client re-submit"),
			}:
			default:
			}
			// Advance the queue if anything else is pending. The
			// adapter is fine; eBUS arbitration is per-SYN; no
			// transport-level action is required.
			if m.arb.hasPending() {
				m.tryGrantAndStart()
			}
			return
		}
		// F-24 (batch-21, 2026-05-14): external-session START staleness
		// guard. ebusd's bus state machine has an internal arbitration-
		// wait timeout (~1-2s); when the gateway's queue+arbitration
		// pipeline takes longer than ebusd's timeout, ebusd silently
		// abandons the request internally. A late ENH_RES_STARTED then
		// arrives in ebusd's bs_recvCmd / bs_skip / bs_ready-wrong
		// state and ebusd logs `arbitration won in invalid state` and
		// silently drops the bid — never sending the M2S frame bytes.
		// ebusctl reports `ERR: arbitration lost`. Live evidence
		// (2026-05-14 17:03-17:24): 31 invalid-state events over 27
		// minutes; tight-scan-08 success rate 13-20% vs 95% target.
		//
		// Suppress grants whose age exceeds ExternalStartStaleness so
		// the gateway never delivers a STARTED that ebusd will
		// discard. Wire-level arbitration win recovers via the next
		// SYN-idle burst because no SEND frame follows. The session's
		// notify is set to cancelled:true so session.go's handleStart
		// takes the silent-suppress branch (mirroring the F-17 C4/R4
		// path semantics). Gateway session 0 is not subject to this
		// guard because the gateway's own send path doesn't fall
		// through ebusd's state machine.
		if pending.sessionID != gatewaySessionID && pending.req != nil &&
			m.cfg.ExternalStartStaleness > 0 &&
			time.Since(pending.req.enqueuedAt) > m.cfg.ExternalStartStaleness {
			m.pendingStart = nil
			if pending.deadline != nil {
				pending.deadline.Stop()
			}
			age := time.Since(pending.req.enqueuedAt)
			m.stateMu.Unlock()
			m.logger.Printf("adaptermux: suppressing stale STARTED for session %d — request age %v > budget %v (F-24)",
				pending.sessionID, age, m.cfg.ExternalStartStaleness)
			select {
			case pending.notify <- startResult{
				granted:   false,
				cancelled: true,
				initiator: pending.initiator,
				err:       errors.New("adaptermux: external START exceeded ExternalStartStaleness budget (F-24)"),
			}:
			default:
			}
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
		// F-17 follow-up (PR #626 review round-1, angry-tester finding
		// F-1, FAILED-half): if the in-flight request was cancelled by
		// markInFlightCancelled (called from requestStartForSession on
		// a same-session re-submit), the FAILED arriving for the OLD
		// bid is also addressed to a session that has already moved
		// on. Without `cancelled: true` on the notify, session.go's
		// handleStart goroutine takes the deliverFailed branch and
		// emits ENHResFailed(winner) on the wire — same
		// retry-feedback-loop class as F-17's STARTED-half. Suppress
		// silently with the cancelled flag; the new request waits on
		// its own fresh notify channel and the queue advances below.
		cancelledInFlight := pending.req != nil && pending.req.cancelled.Load()
		m.pendingStart = nil
		if pending.deadline != nil {
			pending.deadline.Stop() // AM8: cancel deadline timer
		}
		m.stateMu.Unlock()
		if cancelledInFlight {
			m.logger.Printf("adaptermux: suppressing FAILED(0x%02X) for session %d — request was cancelled/replaced; absorbed (C4/R4 FAILED-half)", data, pending.sessionID)
			// Best-effort send (cap-1 channel may already hold the
			// replace's granted=false result).
			select {
			case pending.notify <- startResult{
				granted:   false,
				cancelled: true,
				initiator: pending.initiator,
				err:       errors.New("adaptermux: START superseded by client re-submit (FAILED arrived)"),
			}:
			default:
			}
		} else {
			pending.notify <- startResult{
				granted:   false,
				initiator: data,
				err:       fmt.Errorf("adaptermux: arbitration lost to 0x%02X: %w", data, ebuserrors.ErrBusCollision),
			}
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
		m.armPendingStartAbsorbLocked("cancel")
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

// armPendingStartAbsorbLocked records that the mux expects one stale
// STARTED/FAILED from a cancelled adapter-level START. The hard absorb barrier
// prevents stale responses from being misapplied to newer requests, but it
// needs a fail-safe because some adapters may never emit the stale response.
//
// F-22 (batch-19, 2026-05-13): on timeout, RESET the absorb counter
// only. The prior behavior also closed the upstream ENH transport on
// every timeout, which severed external sessions (ebusd) and produced
// a cascade of RequestStart failures for no transport-level reason
// (batch-19 measured 13 reconnects / 90 min producing 263 cascade
// failures). The wire-level stale response we expected to absorb was
// lost (likely dropped by upstream soft parser errors). The transport
// itself is fine. Mirrors F-15's reasoning that internal state-machine
// timeouts don't justify a transport reset.
//
// Caller must hold stateMu.
func (m *Mux) armPendingStartAbsorbLocked(reason string) {
	m.pendingStartAbsorb++
	m.pendingAbsorbGen++
	gen := m.pendingAbsorbGen
	deadline := m.cfg.StartDeadline
	if deadline <= 0 {
		deadline = 2 * time.Second
	}
	time.AfterFunc(deadline, func() {
		m.stateMu.Lock()
		if gen != m.pendingAbsorbGen || m.pendingStartAbsorb == 0 {
			m.stateMu.Unlock()
			return
		}
		// F-22 (batch-19): log the reset event and bump the metric,
		// but do NOT close the upstream transport. The next semantic
		// poll iteration will issue a fresh RequestStart cleanly
		// over the still-open ENH connection.
		count := m.pendingStartAbsorb
		m.logger.Printf("adaptermux: absorb timeout reset reason=%s (was waiting for %d stale arbitration response(s)) (F-22)", reason, count)
		m.absorbResetTotal.Add(1)
		m.pendingStartAbsorb = 0
		m.pendingAbsorbGen++
		shouldAdvance := m.pendingStart == nil && m.arb.hasPending()
		m.stateMu.Unlock()

		// F-22: NO transport-reconnect side effect. Closing the
		// upstream ENH connection here would have severed ebusd
		// external sessions and produced cascade RequestStart
		// failures (batch-19 evidence). The bus poll loop's next
		// iteration will issue a fresh START arbitration which the
		// adapter handles on the existing connection.
		if shouldAdvance {
			m.tryGrantAndStart()
		}
	})
}

// sendLoop processes send requests from the active path and external sessions.
func (m *Mux) sendLoop() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case req := <-m.activeSendCh:
			if req.sessionID == gatewaySessionID {
				// Arm after sendLoop accepts the request but before doSend
				// writes to the adapter. That closes the immediate-echo
				// race without arming transactions that never entered the
				// mux write path.
				//
				// P11 round-3 (Codex pass-2 P1): record the echo expectation
				// in the SAME critical section as gatewayTxnActive flip +
				// recordWritePrefix. Pre-round-3 recordSent ran in doSend
				// AFTER stateMu was released, leaving a state-ordering race
				// where gatewayTxnActive=true && gatewayEcho empty: stale
				// bytes leaked through as response-phase. Hoisting the call
				// closes the gap.
				//
				// P12 (echo_mismatch deep-dive 2026-05-10): drain activeCh
				// of any stale bytes BEFORE arming the next write. The
				// inter-byte queue-empty window (between matchEcho
				// consuming byte K's echo and sendLoop recording byte K+1)
				// briefly opens response-phase delivery in
				// activePathExpectsByte. Stale bytes that landed in that
				// window must be discarded before we arm gatewayTxnActive
				// on the next write — otherwise bus.Send.sendRawWithEcho
				// would read them in echo position and fire
				// echo_mismatch (post_grant_ack on stale 0x00,
				// pre_echo_syn on stale 0xAA). Both subclasses share this
				// root cause; agent deep-dive 2026-05-10 confirmed.
				//
				// stateMu is held throughout drain+arm+recordSent so
				// onReceived (which also acquires stateMu) cannot
				// interleave a new stale byte between drain and arm.
				//
				// CONTRACT (Codex P12 pass-1 P1): drainActiveChBytesOnly
				// preserves lifecycle (activeEventError) events for
				// bus.Send abort path. Drain is byte-only.
				//
				// CONTRACT (Codex P12 pass-1 P2): bus.sendRawWithEcho calls
				// activeTransport.Write with single-byte slices and reads
				// each echo before issuing the next write — so by the
				// time sendLoop receives a new request, the prior echo
				// has already been consumed by bus.Send.ReadByte and is
				// no longer in activeCh. Multi-byte gateway writes would
				// risk draining a legitimate prior echo; the API
				// supports them but no production caller uses them.
				// Tests that use multi-byte Write sequence echoes after
				// Write returns, so the drain is still safe in practice.
				m.stateMu.Lock()
				drained := m.drainActiveChBytesOnly()
				if drained > 0 {
					m.activeTxn.interWriteDrainTotal.Add(uint64(drained))
				}
				if !m.gatewayTxnActive {
					m.gatewayTxnActive = true
				}
				m.recordWritePrefix(req.data)
				m.gatewayEcho.recordSent(req.data)
				m.stateMu.Unlock()
			}
			err := m.doSend(req.sessionID, req.data)
			req.result <- err
		}
	}
}

// doSend writes a byte to the adapter for the given session.
//
// P11 round-5 (Codex P2 finding on PR #606): every error path —
// errNotBusOwner / errNotConnected / tr.Write failure — must roll
// back gateway echo state (the byte never reached the wire) AND
// clear gatewayTxnActive in the SAME stateMu critical section. The
// pre-write returns previously skipped both, leaving phantom echo
// expectations and a stuck gatewayTxnActive=true state until the
// next SYN/grant reset. The deferred rollback below handles all
// failure exits uniformly.
func (m *Mux) doSend(sessionID uint64, data byte) error {
	isGateway := sessionID == gatewaySessionID
	sendOK := false
	defer func() {
		if !isGateway || sendOK {
			return
		}
		m.stateMu.Lock()
		if m.gatewayTxnActive {
			m.gatewayTxnActive = false
			m.recordGatewayInactive(ReasonActiveWriteError)
		}
		m.gatewayEcho.rollbackSent()
		m.stateMu.Unlock()
	}()

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
	//
	// P11 round-3: gateway session's recordSent is hoisted into sendLoop
	// (same critical section as gatewayTxnActive flip) to close the
	// state-ordering race where gatewayTxnActive=true but gatewayEcho
	// queue is still empty.
	//
	// F-18 (_work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-12-batch13.md):
	// the external-session recordSent + rollback block that used to live
	// here is removed. Per the ENH protocol spec, external sessions must
	// receive their own echoes (handled by deliverToSessions above), and
	// they no longer need a per-session expected-echoes queue. Keeping
	// recordSent without the matching matchEcho consumer would let the
	// per-session queue grow to the 256-byte cap and trigger spurious
	// totalOverflowResets alarms every 256 external SENDs.
	_, err := tr.Write([]byte{data})
	if err != nil {
		return fmt.Errorf("%w: %v", errAdapterWrite, err)
	}

	sendOK = true
	return nil
}

// latencyHistogramReportLoop periodically emits a single-line summary
// of the F-10 per-session-frame pipeline latency histogram to the
// logger. The bucket counters themselves remain authoritative on
// /debug/vars; this loop exists so the same data is visible in
// `ha addons logs` for ad-hoc operator inspection — the batch-3
// EBUSD-VERIFICATION audit flagged that grepping the addon log for
// histogram data returned nothing despite the data being live in
// memory.
//
// Cadence is governed by Config.LatencyHistogramReportInterval
// (default 60s). The loop exits when m.ctx is cancelled.
//
// The cumulative semantics of the histogram are preserved in the log
// surface: each `le_*` figure is the count of frames at or under the
// labelled upper bound; `gt_100000` is the non-cumulative overflow.
// Operators can therefore compute "frames over the 25 ms ebusd
// budget" as `(le_100000 + gt_100000) - le_25000` without leaving
// the addon log.
func (m *Mux) latencyHistogramReportLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.LatencyHistogramReportInterval)
	defer ticker.Stop()
	bucketNames := []string{"le_1000", "le_5000", "le_25000", "le_100000", "gt_100000"}
	readBucket := func(name string) int64 {
		if v := adaptermuxSessionFrameLatencyBucketTotal.Get(name); v != nil {
			if ev, ok := v.(*expvar.Int); ok {
				return ev.Value()
			}
		}
		return 0
	}
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			vals := make([]string, 0, len(bucketNames))
			for _, name := range bucketNames {
				vals = append(vals, fmt.Sprintf("%s=%d", name, readBucket(name)))
			}
			m.logger.Printf("adaptermux: session frame latency histogram (cumulative le_*, overflow gt_*): %s",
				strings.Join(vals, " "))
		}
	}
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

// deliverToSessions delivers a non-SYN byte to every external session,
// including the owner. This is the F-18 fix
// (_work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-12-batch13.md):
// external ENH sessions MUST receive their own post-arbitration echoes
// per john30/ebusd's enhanced_proto.md ("ENH_RES_RECEIVED ... shall not
// be sent when the byte received was part of an arbitration request
// initiated by ebusd"). Arbitration-byte echoes are handled separately
// by deliverWinnerByteToOtherSessions below, which correctly skips the
// winning session — that path delivers the 0x31 byte to OTHER sessions,
// while the winner gets ENHResStarted via session.handleStart.
//
// Gateway echo accounting is performed via m.gatewayEcho on the
// activeSendCh/sendLoop path; the gateway's sessionID (gatewaySessionID
// = 0) is provably never inserted into m.sessions (nextSessionID starts
// at 1), so this loop only ever operates on external sessions.
//
// Pre-F-18 this method per-session-suppressed each owner byte via
// sess.echoTracker.matchEcho — that suppression matched the standalone
// proxy's behavior for non-ENH clients but broke ENH external clients
// (ebusd's DirectProtocolHandler at protocol_direct.cpp:412-414 requires
// the echo to advance bs_sendCmd through multi-byte frames). It also
// hosted a latent reorder hazard via the echoMatchFlushed branch
// (Codex bonus finding in batch-13), which is eliminated by the
// deletion.
//
// AM40 lock note: sessionsMu is held purely to iterate m.sessions
// safely. No tracker mutation happens here anymore.
func (m *Mux) deliverToSessions(symbol byte, currentOwner uint64, hasOwner bool, now time.Time) {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()

	for _, sess := range m.sessions {
		sess.deliverReceived(symbol)
	}
}

// deliverWinnerByteToOtherSessions delivers an arbitration-winner byte
// as a passive observation to every external session EXCEPT the one
// identified by exceptSessionID. Used by the readLoop's STARTED/FAILED
// synthesis paths when an external session won (STARTED) or lost
// (FAILED): the winning/losing session's handleStart goroutine
// delivers ENHResStarted/ENHResFailed asynchronously, so adding a
// synthesized ENHResReceived(winner_byte) to that session would race
// the control frame. Other external sessions (in multi-monitor
// deployments) still need the byte to keep their bus state synced
// with physical wire reality.
//
// EBUSD-VERIFICATION-2026-05-10.md F-9/F-10 follow-up; Codex P2
// round 3 on PR #620.
func (m *Mux) deliverWinnerByteToOtherSessions(symbol byte, exceptSessionID uint64) {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()
	for _, sess := range m.sessions {
		if sess.id == exceptSessionID {
			continue
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
