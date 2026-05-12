package ebusgateway

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

const (
	defaultPassiveCriticalSubscriberBuffer    = 128
	defaultPassiveNonCriticalSubscriberBuffer = 32
	minPassiveTransactionWatchdog             = 250 * time.Millisecond
	maxPassiveTransactionWatchdog             = 5 * time.Second
	maxPassiveRequestBytes                    = 512

	// maxPassiveDataLen is the eBUS spec-mandated upper bound on the LEN
	// byte (Spec_Prot_7 §3.1: a single service frame carries up to 16
	// payload bytes). Used by isMidRequestFrame / isMidResponseFrame
	// (P7.1) to refuse to absorb wire-SYN bytes as data when the
	// declared LEN is structurally impossible — i.e. the parser is
	// almost certainly looking at a misclassified byte (e.g. an
	// arbitrary bus byte admitted as an initiator-class source after a
	// prior abandon). Bounding the predicate here prevents a single bad
	// position from cascading into watchdog-bounded byte absorption.
	//
	// F-19c (batch-16) also uses this cap as the at-observation reject
	// threshold for NN_m / NN_s: any LEN byte > maxPassiveDataLen is
	// spec-illegal and triggers an immediate abandon with reason
	// invalid_nn_m / invalid_nn_s, before the F-19a `5+LEN+1`
	// completion target is computed (which would otherwise overshoot
	// the next bus SYN and let the buffer eat next-frame bytes).
	maxPassiveDataLen = 16

	// maxPassiveLogicalRequestBytes is the F-19c (batch-16) tight
	// defensive cap on the post-unescape request-buffer length. The
	// worst-case legitimate initiator/target exchange is:
	//
	//   5 (header: QQ ZZ PB SB NN_m) + 16 (NN_m data) + 1 (CRC_m)
	//   + 1 (T_ACK) + 1 (NN_s) + 16 (NN_s data) + 1 (CRC_s)
	//   + 1 (I_ACK) + 1 (SYN) = 43 logical bytes.
	//
	// 50 bytes gives 7 bytes of margin without permitting runaway
	// accumulation. Replaces the loose maxPassiveRequestBytes = 512 cap
	// for the early-abandon path; the buffer_overflow reason fires
	// here before the looser 512-cap can be reached.
	maxPassiveLogicalRequestBytes = 50
)

type PassiveClassifiedEventKind uint8

const (
	PassiveClassifiedEventBroadcastFrame PassiveClassifiedEventKind = iota + 1
	PassiveClassifiedEventMasterFrame
	PassiveClassifiedEventTransaction
	PassiveClassifiedEventAbandonedTransaction
	PassiveClassifiedEventDiscontinuity
)

type PassiveDiscontinuityReason string

const (
	PassiveDiscontinuityConnected               PassiveDiscontinuityReason = "connected"
	PassiveDiscontinuityDisconnected            PassiveDiscontinuityReason = "disconnected"
	PassiveDiscontinuityTransportReset          PassiveDiscontinuityReason = "transport_reset"
	PassiveDiscontinuityDecodeFault             PassiveDiscontinuityReason = "decode_fault"
	PassiveDiscontinuityReadTimeout             PassiveDiscontinuityReason = "read_timeout"
	PassiveDiscontinuityShutdown                PassiveDiscontinuityReason = "shutdown"
	PassiveDiscontinuityCriticalSubscriberFault PassiveDiscontinuityReason = "critical_subscriber_overflow"
)

type PassiveAbandonReason string

const (
	PassiveAbandonReasonCorruptedRequest    PassiveAbandonReason = "corrupted_request"
	PassiveAbandonReasonCorruptedTarget     PassiveAbandonReason = "corrupted_target"
	PassiveAbandonReasonNACK                PassiveAbandonReason = "nack"
	PassiveAbandonReasonNoResponse          PassiveAbandonReason = "no_response"
	PassiveAbandonReasonNoProgress          PassiveAbandonReason = "no_progress"
	PassiveAbandonReasonUnexpectedSYN       PassiveAbandonReason = "unexpected_syn"
	PassiveAbandonReasonUnexpectedSymbol    PassiveAbandonReason = "unexpected_symbol"
	PassiveAbandonReasonTransportReset      PassiveAbandonReason = "transport_reset"
	PassiveAbandonReasonDecodeFault         PassiveAbandonReason = "decode_fault"
	PassiveAbandonReasonDisconnected        PassiveAbandonReason = "disconnected"
	PassiveAbandonReasonCRCMismatch         PassiveAbandonReason = "crc_mismatch"
	PassiveAbandonReasonAmbiguousRetransmit PassiveAbandonReason = "ambiguous_retransmission"
	PassiveAbandonReasonShutdown            PassiveAbandonReason = "shutdown"
	PassiveAbandonReasonScanTimeout         PassiveAbandonReason = "scan_timeout"
	PassiveAbandonReasonScanCollision       PassiveAbandonReason = "scan_collision"
	PassiveAbandonReasonArbitrationFragment PassiveAbandonReason = "arbitration_fragment"
	PassiveAbandonReasonSelfEcho            PassiveAbandonReason = "self_echo"

	// F-19c (batch-16): defensive bound-check abandon reasons. These
	// fire at byte-observation time in handleRequestSymbolLocked /
	// handleResponseSymbolLocked when the candidate frame violates the
	// eBUS spec at a structural offset (QQ initiator-address rule, ZZ
	// non-SYN/non-ESC, NN_m / NN_s ≤ maxPassiveDataLen), before the
	// LEN-completion or SYN-trigger paths could mis-classify the
	// buffer.
	//
	// Spec references:
	//   - OSI-7 Application Layer Spec V1.6.1 §2.3 (NN cap: 14
	//     mfr-specific, 10 standardised; codebase uses 16 per
	//     industry folklore via maxPassiveDataLen).
	//   - john30/ebusd symbol.h:39-66 + symbol.cpp:209-229
	//     (initiator-address nibble rule).
	//   - Wikipedia OSI-2 / eBUS data-link layer reference for
	//     escape encoding scope (QQ/ZZ never escape-encoded).
	PassiveAbandonReasonInvalidQQ       PassiveAbandonReason = "invalid_qq"
	PassiveAbandonReasonInvalidZZ       PassiveAbandonReason = "invalid_zz"
	PassiveAbandonReasonInvalidNNMaster PassiveAbandonReason = "invalid_nn_m"
	PassiveAbandonReasonInvalidNNSlave  PassiveAbandonReason = "invalid_nn_s"
	PassiveAbandonReasonBufferOverflow  PassiveAbandonReason = "buffer_overflow"
)

type PassiveTimingMarkers struct {
	RequestStart  time.Time
	RequestEnd    time.Time
	ResponseStart time.Time
	ResponseEnd   time.Time
	Terminal      time.Time
}

type PassiveACKPosition = protocol.ACKPosition

const (
	PassiveACKPositionRequestACK = protocol.ACKPositionRequestACK
)

type PassiveACKCorrelator = protocol.ACKCorrelator

const (
	PassiveACKCorrelatorM2A = protocol.ACKCorrelatorM2A
)

type PassiveACKCorrelation = protocol.ACKCorrelation

type PassiveClassifiedEvent struct {
	Kind                PassiveClassifiedEventKind
	FrameType           protocol.FrameType
	Request             protocol.Frame
	Response            protocol.Frame
	HasRequest          bool
	HasResponse         bool
	Timing              PassiveTimingMarkers
	ObservedAt          time.Time
	AbandonReason       PassiveAbandonReason
	DiscontinuityReason PassiveDiscontinuityReason
	ACKCorrelation      PassiveACKCorrelation
	Err                 error
	Subscriber          string
}

type PassiveSubscriberPriority uint8

const (
	PassiveSubscriberCritical PassiveSubscriberPriority = iota + 1
	PassiveSubscriberNonCritical
)

type PassiveClassifiedSubscription struct {
	id            uint64
	name          string
	priority      PassiveSubscriberPriority
	ch            chan PassiveClassifiedEvent
	reconstructor *PassiveTransactionReconstructor
	closeOnce     sync.Once
}

func (subscription *PassiveClassifiedSubscription) Events() <-chan PassiveClassifiedEvent {
	if subscription == nil {
		return nil
	}
	return subscription.ch
}

func (subscription *PassiveClassifiedSubscription) Close() {
	if subscription == nil || subscription.reconstructor == nil {
		return
	}
	subscription.reconstructor.unsubscribe(subscription, nil)
}

type passiveTransactionPhase uint8

const (
	passivePhaseIdle passiveTransactionPhase = iota
	passivePhaseRequest
	passivePhaseWaitACK
	passivePhaseWaitResponse
	passivePhaseWaitFinalACK
	passivePhaseWaitTerminal
	passivePhaseAbandoned
)

type passiveTransactionState struct {
	phase               passiveTransactionPhase
	requestRaw          []byte
	request             protocol.Frame
	responseRaw         []byte
	response            protocol.Frame
	responseExpectedLen int
	responseCRCValid    bool
	ackCorrelation      PassiveACKCorrelation
	frameType           protocol.FrameType
	timing              PassiveTimingMarkers
	lastProgressAt      time.Time
	// P6 Layer 1 — inter-frame SYN gate.
	//
	// synced records whether the parser has observed at least one
	// SymbolSyn since the previous frame boundary (commit, abandon,
	// transport reset, or startup). The reconstructor only accepts a
	// non-SYN byte as a new frame's source when synced=true; otherwise
	// the byte is dropped and counted in
	// passiveReconstructorMetrics.prefixResyncSkippedTotal. This makes
	// the eBUS bus-idle invariant explicit: a fresh request can only
	// begin after the bus visibly went idle. Default-false on every
	// resetStateLocked() call ensures the gate re-engages after every
	// classified or abandoned transaction.
	synced bool
	// P6 Layer 2 cascade suppression.
	//
	// awaitingResync is set to true after a Layer 2 rejection (a
	// non-initiator-class byte appearing in source position, e.g.
	// "[SYN] [TGT] [PB=0xB5] [SB] [data]" — the operator-confirmed
	// dropped-SRC signature). While awaitingResync=true and synced=false
	// the parser drops the trailing PB/SB/data/CRC bytes silently
	// without inflating prefixResyncSkippedTotal. Cleared on the next
	// SymbolSyn (which also re-flips synced to true).
	awaitingResync bool
}

type PassiveTransactionReconstructor struct {
	cfg      Config
	ctx      context.Context
	cancel   context.CancelFunc
	tap      *PassiveBusTap
	watchdog time.Duration

	stateMu               sync.Mutex
	state                 passiveTransactionState
	pendingRecoveryReason string
	localAddrSnapshotter  LocalBusAddressSnapshotter

	subscribersMu sync.Mutex
	subscribers   map[uint64]*PassiveClassifiedSubscription
	nextSubID     atomic.Uint64

	metricsMu sync.Mutex
	metrics   passiveReconstructorMetrics

	closeOnce sync.Once
}

type PassiveReconstructorSnapshot struct {
	TapStatus           PassiveTapStatus
	FanoutOverflowTotal map[string]uint64
	RecoveryTotal       map[string]uint64
	// AbandonsByReason counts how many transactions the reconstructor
	// classified into each PassiveAbandonReason. Operators query this
	// to determine if a specific (src, dst) pair is failing
	// classification at unusual rates — e.g. live evidence of B503
	// frames hitting unexpected_symbol despite Grafana ground truth
	// showing positive ACKs on the wire (A.9 diagnostic surface).
	AbandonsByReason map[string]uint64
	// PrefixResyncSkippedTotal counts non-SYN bytes the parser dropped
	// because no SymbolSyn was observed since the previous frame
	// boundary (P6 Layer 1 — inter-frame SYN gate). Direct measure of
	// continuation-byte / startup resync events; expected to spike at
	// startup then plateau near zero on a clean bus.
	PrefixResyncSkippedTotal uint64
	// InvalidSrcClassSkippedTotal counts non-initiator-class bytes the
	// parser rejected in source position (P6 Layer 2 — SRC AddressClass
	// validation). Direct measure of upstream byte-loss frequency
	// (operator-confirmed Mode B: "[SYN] [TGT] [PB=0xB5] [SB] [data]"
	// signature where the actual SRC byte was eaten by the ENH
	// transport's StreamEventStarted capture or by an analogous proxy
	// drop). Sustained non-zero rate after deploy quantifies the
	// cost-of-deferral for the upstream P6.1/P6.2 follow-ups.
	InvalidSrcClassSkippedTotal uint64
}

type passiveReconstructorMetrics struct {
	fanoutOverflowTotal         map[string]uint64
	recoveryTotal               map[string]uint64
	abandonsByReason            map[string]uint64
	prefixResyncSkippedTotal    uint64
	invalidSrcClassSkippedTotal uint64
}

func StartPassiveTransactionReconstructor(ctx context.Context, cfg Config) (*PassiveTransactionReconstructor, error) {
	return StartPassiveTransactionReconstructorWithTransport(ctx, cfg, nil)
}

func StartPassiveTransactionReconstructorWithTransport(ctx context.Context, cfg Config, wrap func(transport.RawTransport) transport.RawTransport) (*PassiveTransactionReconstructor, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	reconstructor := newPassiveTransactionReconstructorCore(cfg)
	reconstructor.ctx, reconstructor.cancel = context.WithCancel(ctx)

	tap, err := StartPassiveBusTapWithTransport(reconstructor.ctx, reconstructor.cfg, reconstructor, wrap)
	if err != nil {
		reconstructor.cancel()
		return nil, err
	}
	reconstructor.tap = tap
	return reconstructor, nil
}

func newPassiveTransactionReconstructorCore(cfg Config) *PassiveTransactionReconstructor {
	cfg = applyDefaults(cfg)
	return &PassiveTransactionReconstructor{
		cfg:         cfg,
		watchdog:    clampPassiveTransactionWatchdog(cfg.PassiveTransactionWatchdog),
		subscribers: make(map[uint64]*PassiveClassifiedSubscription),
		metrics: passiveReconstructorMetrics{
			fanoutOverflowTotal: make(map[string]uint64),
			recoveryTotal:       make(map[string]uint64),
		},
	}
}

func clampPassiveTransactionWatchdog(value time.Duration) time.Duration {
	if value <= 0 {
		return time.Second
	}
	if value < minPassiveTransactionWatchdog {
		return minPassiveTransactionWatchdog
	}
	if value > maxPassiveTransactionWatchdog {
		return maxPassiveTransactionWatchdog
	}
	return value
}

func (reconstructor *PassiveTransactionReconstructor) Subscribe(name string, priority PassiveSubscriberPriority, buffer int) (*PassiveClassifiedSubscription, error) {
	if reconstructor == nil {
		return nil, fmt.Errorf("passive reconstructor missing: %w", ebuserrors.ErrInvalidPayload)
	}
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("passive subscriber missing name: %w", ebuserrors.ErrInvalidPayload)
	}
	if priority == 0 {
		priority = PassiveSubscriberNonCritical
	}
	if buffer <= 0 {
		buffer = defaultPassiveSubscriberBuffer(priority)
	}

	subscription := &PassiveClassifiedSubscription{
		id:            reconstructor.nextSubID.Add(1),
		name:          strings.TrimSpace(name),
		priority:      priority,
		ch:            make(chan PassiveClassifiedEvent, buffer),
		reconstructor: reconstructor,
	}

	reconstructor.subscribersMu.Lock()
	defer reconstructor.subscribersMu.Unlock()
	if reconstructor.subscribers == nil {
		return nil, fmt.Errorf("passive reconstructor closed: %w", ebuserrors.ErrTransportClosed)
	}
	reconstructor.subscribers[subscription.id] = subscription
	return subscription, nil
}

func defaultPassiveSubscriberBuffer(priority PassiveSubscriberPriority) int {
	if priority == PassiveSubscriberCritical {
		return defaultPassiveCriticalSubscriberBuffer
	}
	return defaultPassiveNonCriticalSubscriberBuffer
}

func (reconstructor *PassiveTransactionReconstructor) Close() error {
	if reconstructor == nil {
		return nil
	}

	var closeErr error
	reconstructor.closeOnce.Do(func() {
		if reconstructor.cancel != nil {
			reconstructor.cancel()
		}
		if reconstructor.tap != nil {
			closeErr = reconstructor.tap.Close()
		}
		reconstructor.publishAll(reconstructor.shutdownEvents())
		reconstructor.closeAllSubscribers()
	})
	return closeErr
}

func (reconstructor *PassiveTransactionReconstructor) Snapshot() PassiveReconstructorSnapshot {
	if reconstructor == nil {
		return PassiveReconstructorSnapshot{}
	}

	var tapStatus PassiveTapStatus
	if reconstructor.tap != nil {
		tapStatus = reconstructor.tap.Snapshot()
	}

	reconstructor.metricsMu.Lock()
	defer reconstructor.metricsMu.Unlock()

	return PassiveReconstructorSnapshot{
		TapStatus:                   tapStatus,
		FanoutOverflowTotal:         cloneUint64Map(reconstructor.metrics.fanoutOverflowTotal),
		RecoveryTotal:               cloneUint64Map(reconstructor.metrics.recoveryTotal),
		AbandonsByReason:            cloneUint64Map(reconstructor.metrics.abandonsByReason),
		PrefixResyncSkippedTotal:    reconstructor.metrics.prefixResyncSkippedTotal,
		InvalidSrcClassSkippedTotal: reconstructor.metrics.invalidSrcClassSkippedTotal,
	}
}

func (reconstructor *PassiveTransactionReconstructor) OnPassiveTapEvent(event PassiveTapEvent) {
	if reconstructor == nil {
		return
	}

	var pending [4]PassiveClassifiedEvent
	reconstructor.stateMu.Lock()
	events := reconstructor.handleTapEventLocked(pending[:0], event)
	reconstructor.stateMu.Unlock()
	reconstructor.publishAll(events)
}

func (reconstructor *PassiveTransactionReconstructor) handleTapEventLocked(events []PassiveClassifiedEvent, event PassiveTapEvent) []PassiveClassifiedEvent {
	switch event.Kind {
	case PassiveTapEventSymbol:
		events = reconstructor.expireIfStaleLocked(events, event.ObservedAt)
		return reconstructor.handleSymbolLocked(events, event.Symbol, event.ObservedAt)
	case PassiveTapEventConnected:
		reconstructor.resetStateLocked()
		return append(events, PassiveClassifiedEvent{
			Kind:                PassiveClassifiedEventDiscontinuity,
			ObservedAt:          event.ObservedAt,
			DiscontinuityReason: PassiveDiscontinuityConnected,
			Err:                 event.Err,
		})
	case PassiveTapEventDisconnected:
		return reconstructor.handleTransportDiscontinuityLocked(events, event.ObservedAt, PassiveDiscontinuityDisconnected, PassiveAbandonReasonDisconnected, event.Err)
	case PassiveTapEventReset:
		return reconstructor.handleTransportDiscontinuityLocked(events, event.ObservedAt, PassiveDiscontinuityTransportReset, PassiveAbandonReasonTransportReset, event.Err)
	case PassiveTapEventDecodeFault:
		return reconstructor.handleTransportDiscontinuityLocked(events, event.ObservedAt, PassiveDiscontinuityDecodeFault, PassiveAbandonReasonDecodeFault, event.Err)
	case PassiveTapEventReadTimeout:
		return reconstructor.expireIfStaleLocked(events, event.ObservedAt)
	default:
		return events
	}
}

func (reconstructor *PassiveTransactionReconstructor) handleTransportDiscontinuityLocked(events []PassiveClassifiedEvent, observedAt time.Time, reason PassiveDiscontinuityReason, abandonReason PassiveAbandonReason, err error) []PassiveClassifiedEvent {
	if reconstructor.state.phase != passivePhaseIdle && reconstructor.state.phase != passivePhaseAbandoned {
		events = append(events, reconstructor.abandonLocked(abandonReason, observedAt, err))
	}
	switch reason {
	case PassiveDiscontinuityTransportReset, PassiveDiscontinuityDecodeFault:
		reconstructor.pendingRecoveryReason = string(reason)
	}
	reconstructor.resetStateLocked()
	return append(events, PassiveClassifiedEvent{
		Kind:                PassiveClassifiedEventDiscontinuity,
		ObservedAt:          observedAt,
		DiscontinuityReason: reason,
		Err:                 err,
	})
}

func (reconstructor *PassiveTransactionReconstructor) expireIfStaleLocked(events []PassiveClassifiedEvent, observedAt time.Time) []PassiveClassifiedEvent {
	if reconstructor.state.phase == passivePhaseIdle || reconstructor.state.phase == passivePhaseAbandoned || reconstructor.state.lastProgressAt.IsZero() {
		return events
	}
	if observedAt.Sub(reconstructor.state.lastProgressAt) <= reconstructor.watchdog {
		return events
	}
	events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonNoProgress, observedAt, ebuserrors.ErrTimeout))
	reconstructor.state.phase = passivePhaseAbandoned
	return events
}

func (reconstructor *PassiveTransactionReconstructor) handleSymbolLocked(events []PassiveClassifiedEvent, symbol byte, observedAt time.Time) []PassiveClassifiedEvent {
	switch reconstructor.state.phase {
	case passivePhaseIdle:
		// P6 Layer 1 — inter-frame SYN gate.
		//
		// SYN re-engages the synced flag and clears any in-flight
		// Layer-2 cascade-suppression marker so the next non-SYN byte
		// can attempt to start a frame.
		if symbol == protocol.SymbolSyn {
			reconstructor.state.synced = true
			reconstructor.state.awaitingResync = false
			return events
		}
		// Non-SYN byte but the bus has not visibly gone idle since the
		// previous frame ended (or since startup). Drop and count.
		// awaitingResync==true means the byte was already classified as
		// "garbage trailing a Layer 2 rejection"; suppress the Layer 1
		// cascade counter to avoid double-counting the same upstream
		// drop event.
		if !reconstructor.state.synced {
			if !reconstructor.state.awaitingResync {
				reconstructor.metricsMu.Lock()
				reconstructor.metrics.prefixResyncSkippedTotal++
				reconstructor.metricsMu.Unlock()
			}
			return events
		}
		// P6 Layer 2 — SRC AddressClass validation.
		//
		// Every legitimate eBUS frame source is initiator-class
		// (initiator addresses per AD05 / Phase C / sourceAddressTableV1).
		// A non-initiator byte in source position is structural
		// evidence that the upstream pipeline lost the actual SRC byte
		// (e.g. ENH StreamEventStarted captured-as-control-event and
		// dropped for third-party arbitration). Count, then drop the
		// rest of the frame body silently until the next SYN re-syncs.
		if protocol.AddressClassOf(symbol) != protocol.AddressClassMaster {
			reconstructor.metricsMu.Lock()
			reconstructor.metrics.invalidSrcClassSkippedTotal++
			reconstructor.metricsMu.Unlock()
			reconstructor.state.synced = false
			reconstructor.state.awaitingResync = true
			return events
		}
		// F-19c (batch-16) QQ defense-in-depth check (Codex CLI
		// review FINDING_1, 2026-05-12): Layer 2 above admits the
		// byte by matching protocol.AddressClassMaster, which is
		// currently equivalent to the nibble rule because
		// sourceAddressTableV1 in helianthus-ebusgo contains exactly
		// the 25 nibble-rule initiators (verified against
		// symbol.cpp:209-229). But if Layer 2's lookup table is ever
		// widened, the spec's nibble rule must still be enforced —
		// otherwise a non-nibble-rule QQ would be appended into
		// requestRaw by startRequestLocked below and the F-19c
		// per-byte switch in handleRequestSymbolLocked would never
		// see it (the first byte arriving at handleRequestSymbolLocked
		// is ZZ at rawLen=2, not QQ at rawLen=1). Place the QQ check
		// HERE, between the Layer-2 gate and startRequestLocked, so
		// it is reachable independent of Layer 2's evolution.
		if !isInitiatorAddr(symbol) {
			events = append(events, reconstructor.abandonLocked(
				PassiveAbandonReasonInvalidQQ, observedAt, ebuserrors.ErrInvalidPayload))
			reconstructor.state.phase = passivePhaseAbandoned
			reconstructor.resetStateLocked()
			return events
		}
		reconstructor.startRequestLocked(symbol, observedAt)
		return events
	case passivePhaseRequest:
		return reconstructor.handleRequestSymbolLocked(events, symbol, observedAt)
	case passivePhaseWaitACK:
		return reconstructor.handleACKSymbolLocked(events, symbol, observedAt)
	case passivePhaseWaitResponse:
		return reconstructor.handleResponseSymbolLocked(events, symbol, observedAt)
	case passivePhaseWaitFinalACK:
		return reconstructor.handleFinalACKSymbolLocked(events, symbol, observedAt)
	case passivePhaseWaitTerminal:
		return reconstructor.handleTerminalSymbolLocked(events, symbol, observedAt)
	case passivePhaseAbandoned:
		if symbol == protocol.SymbolSyn {
			// SYN-consuming reset: re-engage the Layer 1 gate so the
			// very next non-SYN byte may legitimately start a frame.
			reconstructor.resetStateLockedAfterSyn()
		}
		return events
	default:
		reconstructor.resetStateLocked()
		return events
	}
}

func (reconstructor *PassiveTransactionReconstructor) startRequestLocked(symbol byte, observedAt time.Time) {
	reconstructor.state.phase = passivePhaseRequest
	reconstructor.state.requestRaw = append(reconstructor.state.requestRaw[:0], symbol)
	reconstructor.state.responseRaw = reconstructor.state.responseRaw[:0]
	reconstructor.state.request = protocol.Frame{}
	reconstructor.state.response = protocol.Frame{}
	reconstructor.state.responseExpectedLen = 0
	reconstructor.state.responseCRCValid = false
	reconstructor.state.ackCorrelation = PassiveACKCorrelation{}
	reconstructor.state.frameType = protocol.FrameTypeUnknown
	reconstructor.state.timing = PassiveTimingMarkers{RequestStart: observedAt}
	reconstructor.state.lastProgressAt = observedAt
	// We are now mid-frame; the trailing SYN that ends this frame
	// will re-flip both flags via resetStateLockedAfterSyn at the
	// commit/abandon site. (P6 Layer 1 + Layer 2 invariant.)
	reconstructor.state.synced = false
	reconstructor.state.awaitingResync = false
}

// isMidRequestFrame reports whether requestRaw is structurally
// mid-frame: the LEN byte (position 4) is observed and the buffer is
// shorter than the LEN-declared full-frame length 6+LEN. P7.1 uses
// this predicate to disambiguate a logical 0xAA byte (escape-decoded
// from wire 0xA9 0x01) from a real wire-SYN frame terminator: in
// mid-frame state the byte must structurally be data, regardless of
// whether the upstream tap reported it via the escape decoder or as a
// raw symbol.
//
// SCOPE / DEFERRED: this predicate only disambiguates positions
// strictly after the LEN byte (positions 5 .. 6+LEN-1, i.e. data and
// CRC). Logical 0xAA bytes at positions 0..4 (SRC, DST, PB, SB, LEN)
// remain ambiguous under this approach because the reconstructor
// hasn't yet learnt the structural length. In practice on the eBUS
// wire those positions are either statically constrained (SRC must be
// initiator-class, never 0xAA — Layer 2 rejects; DST must be target
// or broadcast, never 0xAA — frame would abandon either way) or
// extremely unlikely (PB/SB=0xAA is not a catalogued opcode for
// Vaillant traffic; LEN=0xAA = 170 bytes would exceed any service
// payload bound). Full disambiguation would require plumbing a
// per-byte WasEscaped flag through transport.StreamEvent →
// PassiveEvent → PassiveTapEvent (Approach A in the P7.1 consult);
// deferred until live captures justify the cross-repo change.
//
// SAFETY BOUND: the predicate also returns false when the declared
// LEN exceeds maxPassiveDataLen (eBUS spec §3.1 limit, 16 bytes). An
// out-of-spec LEN means the parser is almost certainly mid-state on a
// misclassified byte sequence (e.g. an arbitrary byte was admitted as
// an initiator-class source after a previous abandon). Refusing to
// absorb wire-SYNs in that state lets the SYN reach the existing
// abandon/resync path instead of cascading into a watchdog-bounded
// byte-absorption window.
func (reconstructor *PassiveTransactionReconstructor) isMidRequestFrame() bool {
	if len(reconstructor.state.requestRaw) < 5 {
		return false
	}
	declaredLen := int(reconstructor.state.requestRaw[4])
	if declaredLen > maxPassiveDataLen {
		return false
	}
	return len(reconstructor.state.requestRaw) < 6+declaredLen
}

// isMidResponseFrame reports whether responseRaw is structurally
// mid-frame: the response LEN byte (position 0) is observed and the
// buffer is shorter than responseExpectedLen (= LEN+2 to include the
// trailing CRC). Mirrors isMidRequestFrame's role for the response
// region — see that doc-comment for scope/deferred notes. Also bounds
// by maxPassiveDataLen for the same safety reason.
func (reconstructor *PassiveTransactionReconstructor) isMidResponseFrame() bool {
	if len(reconstructor.state.responseRaw) == 0 {
		return false
	}
	if reconstructor.state.responseExpectedLen > maxPassiveDataLen+2 {
		return false
	}
	return len(reconstructor.state.responseRaw) < reconstructor.state.responseExpectedLen
}

func (reconstructor *PassiveTransactionReconstructor) handleRequestSymbolLocked(events []PassiveClassifiedEvent, symbol byte, observedAt time.Time) []PassiveClassifiedEvent {
	// P7.1 — escape-decoded 0xAA disambiguation.
	//
	// The passive bus tap's escape decoder produces logical 0xAA when
	// the wire carried the 2-byte sequence 0xA9 0x01. A naive `symbol
	// == SymbolSyn` check at this entry treats that logical 0xAA as a
	// frame-end SYN, abandoning every M2I/M2T/Broadcast frame whose
	// data or CRC region contains a 0xAA byte. With the LEN byte
	// already in `requestRaw` (positions 0..4 buffered), the structural
	// length is known; an SYN-valued byte arriving while
	// len(requestRaw) < 6+LEN must be data (escape-decoded), not a wire
	// SYN. Treat it as data and fall through to the data-accumulation
	// path. See isMidRequestFrame for scope and deferred edge cases.
	if symbol != protocol.SymbolSyn || reconstructor.isMidRequestFrame() {
		reconstructor.state.requestRaw = append(reconstructor.state.requestRaw, symbol)
		reconstructor.state.lastProgressAt = observedAt

		// F-19c (batch-16) spec-bound checks at structural offsets.
		// These fire AT byte-observation time, after the new symbol
		// has been appended to requestRaw, so the candidate frame
		// preserves the offending byte for forensics. All abandon
		// paths use the plain resetStateLocked variant (no wire SYN
		// has been consumed; the next bus SYN re-engages Layer 1
		// via the Idle handler — same Codex-P2-round-2 safe-fail
		// rationale used by F-19a's abandon site).
		rawLen := len(reconstructor.state.requestRaw)
		switch rawLen {
		// NOTE: the QQ (rawLen=1) defense-in-depth check is NOT
		// here — startRequestLocked appends QQ before
		// handleRequestSymbolLocked is reached, so the first byte
		// to arrive HERE is ZZ at rawLen=2. The QQ check lives at
		// the Idle-handler call site (passivePhaseIdle branch above)
		// between the Layer-2 AddressClass gate and
		// startRequestLocked. See Codex CLI review FINDING_1 on
		// PR #629.
		case 2:
			// ZZ (target address): per `symbol.h:41` QQ/ZZ are NEVER
			// escape-encoded on the wire, so a literal 0xAA or 0xA9
			// at this position is invalid (an escape sequence would
			// require a leading 0xA9 byte that wasn't present in
			// this position). Anything that's not the broadcast
			// address (0xFE), not an initiator, and not a valid
			// secondary-class address is also invalid; the
			// redundant compound check is kept as an explicit
			// guard since a future addition of reserved address
			// classes could otherwise slip through.
			zz := reconstructor.state.requestRaw[1]
			if zz == 0xAA || zz == 0xA9 {
				events = append(events, reconstructor.abandonLocked(
					PassiveAbandonReasonInvalidZZ, observedAt, ebuserrors.ErrInvalidPayload))
				reconstructor.state.phase = passivePhaseAbandoned
				reconstructor.resetStateLocked()
				return events
			}
			if zz != 0xFE && !isInitiatorAddr(zz) && !isValidTargetAddr(zz) {
				events = append(events, reconstructor.abandonLocked(
					PassiveAbandonReasonInvalidZZ, observedAt, ebuserrors.ErrInvalidPayload))
				reconstructor.state.phase = passivePhaseAbandoned
				reconstructor.resetStateLocked()
				return events
			}
		case 5:
			// NN_m (initiator-side LEN byte) observation. The spec
			// caps NN at 16. Live evidence (batch-16): bogus values
			// 0x84, 0xAF, 0xFF appear in production. Without this
			// check, F-19a's `5+LEN+1` completion target overshoots
			// the next bus SYN and the buffer eats next-frame bytes
			// before the SYN-trigger path classifies the abandon.
			nnM := reconstructor.state.requestRaw[4]
			if int(nnM) > maxPassiveDataLen {
				events = append(events, reconstructor.abandonLocked(
					PassiveAbandonReasonInvalidNNMaster, observedAt, ebuserrors.ErrInvalidPayload))
				reconstructor.state.phase = passivePhaseAbandoned
				reconstructor.resetStateLocked()
				return events
			}
		}

		// F-19c watchdog: tight defensive cap on the post-unescape
		// request buffer. Worst-case legitimate MS exchange is 43
		// logical bytes (see maxPassiveLogicalRequestBytes
		// docstring). Hitting 51 here means either runaway
		// accumulation (a real wire SYN was missed and bytes from
		// multiple frames are piling up) or a spec violation that
		// the per-offset checks above didn't catch. Replaces the
		// looser maxPassiveRequestBytes=512 cap that previously
		// fired here.
		if rawLen > maxPassiveLogicalRequestBytes {
			events = append(events, reconstructor.abandonLocked(
				PassiveAbandonReasonBufferOverflow, observedAt, ebuserrors.ErrInvalidPayload))
			reconstructor.state.phase = passivePhaseAbandoned
			reconstructor.resetStateLocked()
			return events
		}

		// P7 — LEN-based request completion (Mode C fix).
		//
		// eBUS protocol fact (Spec_Prot_7 §3, mirrored in
		// helianthus-docs-ebus protocols/ebus-services/ebus-overview.md
		// §"Frame Sequence"): for M2I (initiator/initiator) and M2T
		// (initiator/target) transactions, there is NO SYN between the
		// command's CRC byte and the target's ACK byte. The wire is
		//
		//     I -> T: SRC DST PB SB LEN [data×LEN] CRC
		//     T -> I: ACK
		//     [M2T only] T -> I: RESP_LEN [data×RESP_LEN] RESP_CRC
		//     [M2T only] I -> T: ACK
		//     I -> T: SYN  (only at the very end of the transaction)
		//
		// Without this early-transition, the parser keeps accumulating
		// post-CRC bytes (ACK + response + final ACK) into requestRaw
		// until the trailing SYN. parseFrame then rejects the buffer
		// because len != 6 + LEN, abandoning every M2I/M2T transaction
		// on real wire as `corrupted_request` — the dominant failure
		// mode left after P6 (live-validated 2026-05-09: 0/min Layer 1,
		// ~4/min Layer 2, ~68/min uncategorised — this fix targets that
		// remainder).
		//
		// As soon as `requestRaw` reaches the structurally-complete
		// length 6 + LEN_byte AND parseFrame succeeds, transition to
		// the appropriate phase based on frameType:
		//
		//   * Broadcast: emit the BroadcastFrame event immediately and
		//     reset to Idle (synced=false; the next observed SYN will
		//     re-engage the Layer 1 gate). Real broadcast wire IS
		//     [request_bytes][SYN] so the SYN that follows is consumed
		//     cleanly by the Idle handler.
		//
		//   * M2I / M2T: transition to passivePhaseWaitACK so the next
		//     wire byte (the target's ACK or NACK) is processed by
		//     handleACKSymbolLocked.
		//
		// parseFrame failure at LEN reach (e.g. CRC mismatch) does NOT
		// abandon here — keeps accumulating. The trailing SYN path
		// below preserves the existing abandon classification (and
		// matches the historical behavior for genuinely corrupt
		// frames).
		//
		// P7.1 — the entry condition above (`symbol != SymbolSyn ||
		// isMidRequestFrame()`) routes logical 0xAA bytes (escape-
		// decoded from wire 0xA9 0x01) appearing at positions 5 ..
		// 6+LEN-1 into the data-accumulation path. They reach this
		// LEN-completion check naturally and frames whose data /
		// CRC region contains 0xAA now classify cleanly.
		if len(reconstructor.state.requestRaw) >= 5 {
			expectedLen := 6 + int(reconstructor.state.requestRaw[4])
			if len(reconstructor.state.requestRaw) == expectedLen {
				if events, ok := reconstructor.commitRequestFrameLocked(events, observedAt); ok {
					return events
				}
				// commitRequestFrameLocked returned ok=false for one of
				// two distinct reasons:
				//
				//   (A) parseFrame failed (CRC mismatch / structural
				//       invalid) — this is the F-19a case.
				//
				//   (B) parseFrame succeeded but the frame is Broadcast
				//       or Unknown; commitRequestFrameLocked defers to
				//       the SYN-triggered path for canonical timing
				//       (commitRequestFrameLocked docstring at
				//       lines 723-730).
				//
				// Re-parse to disambiguate. For (A), abandon early per
				// F-19a (below). For (B), preserve the pre-F-19a
				// behavior: keep accumulating, let the trailing SYN
				// commit the broadcast cleanly.
				if _, parseOk := parseFrame(reconstructor.state.requestRaw); !parseOk {
					// F-19a (batch-15): parseFrame failed at
					// LEN-completion. This is the "logical 0xAA
					// absorbed at the CRC position" scenario: a
					// wire-SYN byte that should have terminated a
					// shorter frame was routed into the buffer by
					// isMidRequestFrame() (it returns true while
					// len < 6+LEN), and the resulting len=6+LEN buffer
					// fails CRC validation.
					//
					// Pre-F-19a: this path "kept accumulating" —
					// consuming bytes from the NEXT frame into a buffer
					// that would never validate. The eventual abandon
					// (via SYN at line ~684 or over-length at line
					// ~612) cascaded the corruption: bytes that
					// legitimately belonged to the next frame were
					// lost. Live evidence (batch-14): ~146 src=0x10
					// abandons per 30k log lines.
					//
					// Fix: abandon immediately, replicating the same
					// classification helpers the SYN-triggered path
					// uses (line ~689-696).
					//
					// Layer 1 invariant (Codex bot P2 review rounds 1 + 2,
					// 2026-05-12): use plain resetStateLocked here.
					//
					// The current `symbol` MAY be SYN-valued (the
					// operator's `... AA AA` case) but the predicate at
					// line 609 only routes a 0xAA into this accumulation
					// branch when isMidRequestFrame() returns true —
					// which means the byte is being treated as
					// escape-decoded payload data (logical 0xAA decoded
					// from wire `0xA9 0x01`), NOT a real wire SYN. The
					// passive tap does not carry a `WasEscaped` flag, so
					// we cannot distinguish a real wire SYN from an
					// escape-decoded 0xAA at this layer.
					//
					// Using resetStateLockedAfterSyn would incorrectly
					// keep the Layer-1 gate open in the
					// escape-decoded-0xAA case, allowing the very next
					// byte (which is typically an ACK / NACK / body byte
					// of the in-progress wire transaction, NOT a fresh
					// SRC) to be accepted as a new frame's leader. That
					// would create a SECOND false reconstruction
					// cascade.
					//
					// The "wasted SYN" cost of plain resetStateLocked
					// (one frame's SRC byte dropped if the symbol
					// genuinely was a wire SYN) is bounded by the next
					// inter-frame wire SYN, which the eBUS spec
					// guarantees between transactions. Future work
					// (F-19c forensic instrumentation) could plumb
					// WasEscaped through the tap to make this branch
					// precise; for now plain reset is the
					// provably-safe choice.
					reason := PassiveAbandonReasonCorruptedRequest
					if reconstructor.isSelfOriginatedRaw() {
						reason = PassiveAbandonReasonSelfEcho
					} else if isScanProbeRaw(reconstructor.state.requestRaw) {
						reason = PassiveAbandonReasonScanCollision
					}
					events = append(events, reconstructor.abandonLocked(reason, observedAt, ebuserrors.ErrInvalidPayload))
					reconstructor.state.phase = passivePhaseAbandoned
					reconstructor.resetStateLocked()
					return events
				}
				// Case (B): broadcast/unknown defer path. Keep
				// accumulating; the trailing wire SYN commits the
				// frame via dispatchParsedRequestLocked.
			}
		}
		return events
	}

	reconstructor.state.timing.RequestEnd = observedAt
	reconstructor.state.lastProgressAt = observedAt

	frame, ok := parseFrame(reconstructor.state.requestRaw)
	if !ok {
		reason := PassiveAbandonReasonCorruptedRequest
		// F-19b (batch-15): widen arbitration_fragment from `<= 3` to
		// `< 5`. A buffer of length 4 (SRC DST PB SB) has reached SB
		// but never observed LEN — structurally this is a truncated
		// arbitration attempt (lost to a higher-priority initiator,
		// or wire byte loss), not a corrupted frame. The previous
		// `<= 3` threshold mis-attributed these to corrupted_request,
		// inflating the F-19 metric for src values that frequently
		// lose arbitration on a 3-initiator bus (live evidence
		// batch-14: ~115 src=0xF1 abandons per 30k lines, almost all
		// in the 4-byte truncated shape).
		if len(reconstructor.state.requestRaw) < 5 {
			reason = PassiveAbandonReasonArbitrationFragment
		} else if reconstructor.isSelfOriginatedRaw() {
			reason = PassiveAbandonReasonSelfEcho
		} else if isScanProbeRaw(reconstructor.state.requestRaw) {
			reason = PassiveAbandonReasonScanCollision
		}
		events = append(events, reconstructor.abandonLocked(reason, observedAt, ebuserrors.ErrInvalidPayload))
		// P6 Layer 1 — symbol that triggered this reset is the
		// trailing SymbolSyn proven at the top of handleRequestSymbolLocked.
		reconstructor.resetStateLockedAfterSyn()
		return events
	}

	// Successful parse: dispatch via the shared helper. The trailing
	// SYN consumed this frame so the post-commit reset MUST use the
	// AfterSyn variant to keep Layer 1's synced flag engaged for the
	// next frame. Codex P7 review pass 1 NIT FINDING_1.
	return reconstructor.dispatchParsedRequestLocked(events, frame, observedAt, true /*afterSyn*/)
}

// commitRequestFrameLocked is the LEN-based early-transition entry
// point used by handleRequestSymbolLocked while still accumulating
// non-SYN bytes (Mode C / P7).
//
// Returns ok=true when the frame was handled (M2I/M2T transitioned
// to passivePhaseWaitACK without waiting for the SYN that doesn't
// arrive on real wire). Returns ok=false in two cases:
//
//  1. parseFrame itself failed — caller keeps accumulating; the
//     trailing SYN path classifies the abandon (corrupted_request /
//     arbitration_fragment / self_echo / scan_collision).
//
//  2. Frame parsed cleanly but its frameType is Broadcast or Unknown.
//     Codex P7 review pass 3 FINDING_1: real broadcast wire IS
//     [SRC..CRC][SYN] — the trailing SYN is the canonical commit
//     boundary, and broadcast timing observables (RequestEnd,
//     Terminal, ObservedAt) must reflect that SYN's timestamp, not
//     the CRC byte's. Truncated [SYN] SRC..CRC without the trailing
//     SYN must NOT classify as a complete broadcast. So broadcast
//     and Unknown fall through to the SYN-triggered path.
//
// Caller MUST hold stateMu and MUST have populated requestRaw with
// the candidate request bytes. When ok=true, the post-commit reset
// uses resetStateLocked (NOT AfterSyn) because no SYN has been
// consumed at LEN+CRC reach — the trailing wire SYN re-engages
// Layer 1 via the passivePhaseIdle handler.
func (reconstructor *PassiveTransactionReconstructor) commitRequestFrameLocked(events []PassiveClassifiedEvent, observedAt time.Time) ([]PassiveClassifiedEvent, bool) {
	frame, ok := parseFrame(reconstructor.state.requestRaw)
	if !ok {
		return events, false
	}
	switch frame.Type() {
	case protocol.FrameTypeInitiatorInitiator, protocol.FrameTypeInitiatorTarget:
		return reconstructor.dispatchParsedRequestLocked(events, frame, observedAt, false /*afterSyn*/), true
	default:
		// Broadcast / Unknown: defer to SYN-triggered path so the
		// terminal SYN provides canonical timing for broadcast and
		// classification for Unknown.
		return events, false
	}
}

// dispatchParsedRequestLocked is the post-parse-success dispatch used
// by BOTH the LEN-based early-transition path
// (commitRequestFrameLocked, afterSyn=false) AND the legacy SYN-
// triggered path (handleRequestSymbolLocked's SYN branch,
// afterSyn=true). Centralising the frameType switch + Faces / event
// emission / reset here prevents the two entry points from drifting
// (Codex P7 review pass 1 NIT FINDING_1).
//
// afterSyn selects the post-commit reset variant. The SYN-triggered
// path consumes a SymbolSyn so the reset re-engages Layer 1's synced
// flag (resetStateLockedAfterSyn). The LEN-based path has not yet
// consumed a SYN; the trailing wire SYN re-engages Layer 1 via the
// passivePhaseIdle handler (resetStateLocked).
//
// The FrameTypeUnknown case folds into the default branch (corrupted
// target abandon), so callers do not need to pre-filter Unknown.
func (reconstructor *PassiveTransactionReconstructor) dispatchParsedRequestLocked(events []PassiveClassifiedEvent, frame protocol.Frame, observedAt time.Time, afterSyn bool) []PassiveClassifiedEvent {
	reset := reconstructor.resetStateLocked
	if afterSyn {
		reset = reconstructor.resetStateLockedAfterSyn
	}

	// FrameTypeUnknown: abandon before mutating any frame state so
	// the abandon event observes an EMPTY state.request and an
	// untouched state.timing.RequestEnd / lastProgressAt — matches
	// the pre-refactor SYN path AND pre-refactor LEN path observable
	// behavior (Codex P7 review pass 2 FINDING_1).
	frameType := frame.Type()
	if frameType == protocol.FrameTypeUnknown {
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonCorruptedTarget, observedAt, ebuserrors.ErrInvalidPayload))
		reset()
		return events
	}

	reconstructor.state.timing.RequestEnd = observedAt
	reconstructor.state.lastProgressAt = observedAt
	reconstructor.state.request = frame
	reconstructor.state.frameType = frameType

	switch frameType {
	case protocol.FrameTypeBroadcast:
		reconstructor.recordRecoveryLocked()
		events = append(events, PassiveClassifiedEvent{
			Kind:       PassiveClassifiedEventBroadcastFrame,
			FrameType:  frameType,
			Request:    frame,
			HasRequest: true,
			Timing: PassiveTimingMarkers{
				RequestStart: reconstructor.state.timing.RequestStart,
				RequestEnd:   observedAt,
				Terminal:     observedAt,
			},
			ObservedAt: observedAt,
		})
		reset()
	case protocol.FrameTypeInitiatorInitiator, protocol.FrameTypeInitiatorTarget:
		reconstructor.state.phase = passivePhaseWaitACK
	default:
		// protocol.FrameType enum has only Unknown / M2I / M2T /
		// Broadcast today; this branch is defensive against future
		// enum extensions. State has already been populated above
		// (matches the pre-refactor SYN-path default branch which
		// also wrote state.request before abandon).
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonCorruptedTarget, observedAt, ebuserrors.ErrInvalidPayload))
		reset()
	}
	return events
}

// isScanProbeRaw checks raw request bytes for a 0704 device identity query
// without requiring a full frame parse.  Used to reclassify collision-garbled
// scan probes as scan_collision instead of corrupted_request.
func isScanProbeRaw(raw []byte) bool {
	return len(raw) >= 4 && raw[2] == 0x07 && raw[3] == 0x04
}

// isScanTimeoutLocked returns true when the pending request is a device
// identity query (0704) that will never receive a response from a
// non-existent target.  These are expected during background address scans
// and should not inflate the unexpected_syn error counter.
func (reconstructor *PassiveTransactionReconstructor) isScanTimeoutLocked() bool {
	return reconstructor.state.request.Primary == 0x07 &&
		reconstructor.state.request.Secondary == 0x04
}

// SetLocalAddressSnapshotter provides the reconstructor with a way to query the
// gateway's local bus address.  The snapshotter is queried dynamically so the
// local address can be discovered at runtime (e.g. during the startup scan).
// Must be called before AttachReconstructor or at least before passive symbols
// start arriving; it is protected by stateMu.
func (reconstructor *PassiveTransactionReconstructor) SetLocalAddressSnapshotter(snapshotter LocalBusAddressSnapshotter) {
	if reconstructor == nil {
		return
	}
	reconstructor.stateMu.Lock()
	defer reconstructor.stateMu.Unlock()
	reconstructor.localAddrSnapshotter = snapshotter
}

// isSelfOriginatedRaw returns true when the raw request bytes begin with
// the gateway's own bus address.  Self-originated frames that fail parsing
// are collision artifacts from our own active traffic — the active path
// already holds the correct result.
func (reconstructor *PassiveTransactionReconstructor) isSelfOriginatedRaw() bool {
	if reconstructor.localAddrSnapshotter == nil || len(reconstructor.state.requestRaw) < 1 {
		return false
	}
	snapshot := reconstructor.localAddrSnapshotter.LocalAddressSnapshot()
	return snapshot.Known && reconstructor.state.requestRaw[0] == snapshot.Address
}

// isSelfOriginatedParsed returns true when the successfully parsed request
// frame source matches the gateway's own bus address.  Used in post-parse
// phases (ACK wait) where the frame parsed but subsequent protocol steps fail.
func (reconstructor *PassiveTransactionReconstructor) isSelfOriginatedParsed() bool {
	if reconstructor.localAddrSnapshotter == nil {
		return false
	}
	snapshot := reconstructor.localAddrSnapshotter.LocalAddressSnapshot()
	return snapshot.Known && reconstructor.state.request.Source == snapshot.Address
}

func (reconstructor *PassiveTransactionReconstructor) handleACKSymbolLocked(events []PassiveClassifiedEvent, symbol byte, observedAt time.Time) []PassiveClassifiedEvent {
	switch symbol {
	case protocol.SymbolAck:
		reconstructor.state.lastProgressAt = observedAt
		reconstructor.state.ackCorrelation = m2aRequestACKCorrelation(symbol)
		if reconstructor.state.frameType == protocol.FrameTypeInitiatorInitiator {
			reconstructor.state.phase = passivePhaseWaitTerminal
			return events
		}
		reconstructor.state.phase = passivePhaseWaitResponse
		reconstructor.state.responseRaw = reconstructor.state.responseRaw[:0]
		reconstructor.state.responseExpectedLen = 0
		reconstructor.state.responseCRCValid = false
		return events
	case protocol.SymbolNack:
		reconstructor.state.ackCorrelation = m2aRequestACKCorrelation(symbol)
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonNACK, observedAt, ebuserrors.ErrNACK))
		// P6 — after a request-phase NACK the eBUS spec (AM2 in
		// internal/adaptermux/wire_phase.go:185-198) permits the
		// initiator to retransmit the request immediately without a
		// SYN gap or re-arbitration. The retry's source byte follows
		// the NACK directly, so we must keep the Layer 1 inter-frame
		// SYN gate engaged (synced=true) here. Layer 2 still validates
		// that the retry's first byte is initiator-class.
		reconstructor.resetStateLockedAfterSyn()
		return events
	case protocol.SymbolSyn:
		if reconstructor.isScanTimeoutLocked() {
			events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonScanTimeout, observedAt, ebuserrors.ErrTimeout))
		} else if reconstructor.isSelfOriginatedParsed() {
			events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonSelfEcho, observedAt, ebuserrors.ErrTimeout))
		} else {
			events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonUnexpectedSYN, observedAt, ebuserrors.ErrTimeout))
			reconstructor.pendingRecoveryReason = string(PassiveAbandonReasonUnexpectedSYN)
		}
		// P6 Layer 1 — explicit SymbolSyn case in this arm.
		reconstructor.resetStateLockedAfterSyn()
		return events
	default:
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonUnexpectedSymbol, observedAt, ebuserrors.ErrInvalidPayload))
		reconstructor.state.phase = passivePhaseAbandoned
		return events
	}
}

func (reconstructor *PassiveTransactionReconstructor) handleResponseSymbolLocked(events []PassiveClassifiedEvent, symbol byte, observedAt time.Time) []PassiveClassifiedEvent {
	if len(reconstructor.state.responseRaw) == 0 && symbol == protocol.SymbolSyn {
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonNoResponse, observedAt, ebuserrors.ErrTimeout))
		// P6 Layer 1 — explicit SymbolSyn at this branch.
		reconstructor.resetStateLockedAfterSyn()
		return events
	}
	if len(reconstructor.state.responseRaw) > 0 && symbol == protocol.SymbolSyn && !reconstructor.isMidResponseFrame() {
		// P7.1 — only treat a SYN-valued byte as a wire SYN when we are
		// past the structurally-required response length (LEN+2,
		// including CRC). Mid-response 0xAA bytes (escape-decoded from
		// wire 0xA9 0x01) fall through to the data-accumulation path
		// below and reach the LEN-completion check.
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonUnexpectedSYN, observedAt, ebuserrors.ErrTimeout))
		reconstructor.pendingRecoveryReason = string(PassiveAbandonReasonUnexpectedSYN)
		// P6 Layer 1 — explicit SymbolSyn at this branch.
		reconstructor.resetStateLockedAfterSyn()
		return events
	}

	reconstructor.state.lastProgressAt = observedAt
	if len(reconstructor.state.responseRaw) == 0 {
		// F-19c (batch-16): the first byte of responseRaw is NN_s
		// (responder-side LEN). By the time control reaches this
		// handler, handleACKSymbolLocked at the previous phase
		// boundary has already confirmed S_ACK=0x00 (the
		// S_NAK=0xFF path at
		// passive_transaction_reconstructor.go:982-993 abandons
		// with reason=nack BEFORE reaching here, so the
		// S_NAK-retry-restart case doesn't reach this check). NN_s
		// must respect the same OSI-7 §2.3 cap as NN_m. Without
		// this guard, responseExpectedLen = int(symbol) + 2 could
		// reach 257 on a corrupt 0xFF symbol, and the response
		// buffer would accumulate up to 257 bytes of next-frame
		// noise before the response-side mid-frame guard fires.
		if int(symbol) > maxPassiveDataLen {
			events = append(events, reconstructor.abandonLocked(
				PassiveAbandonReasonInvalidNNSlave, observedAt, ebuserrors.ErrInvalidPayload))
			reconstructor.state.phase = passivePhaseAbandoned
			// Plain reset: the offending byte is the NN_s data
			// byte, not a wire SYN. Layer 1 re-syncs on the next
			// observed bus SYN via the Idle handler.
			reconstructor.resetStateLocked()
			return events
		}
		reconstructor.state.timing.ResponseStart = observedAt
		reconstructor.state.responseExpectedLen = int(symbol) + 2
	}
	reconstructor.state.responseRaw = append(reconstructor.state.responseRaw, symbol)

	if len(reconstructor.state.responseRaw) < reconstructor.state.responseExpectedLen {
		return events
	}
	if len(reconstructor.state.responseRaw) > reconstructor.state.responseExpectedLen {
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonUnexpectedSymbol, observedAt, ebuserrors.ErrInvalidPayload))
		reconstructor.state.phase = passivePhaseAbandoned
		return events
	}

	reconstructor.state.response, reconstructor.state.responseCRCValid = parsePassiveResponse(reconstructor.state.request, reconstructor.state.responseRaw)
	reconstructor.state.timing.ResponseEnd = observedAt
	reconstructor.state.phase = passivePhaseWaitFinalACK
	return events
}

func (reconstructor *PassiveTransactionReconstructor) handleFinalACKSymbolLocked(events []PassiveClassifiedEvent, symbol byte, observedAt time.Time) []PassiveClassifiedEvent {
	switch symbol {
	case protocol.SymbolAck:
		if !reconstructor.state.responseCRCValid {
			events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonCRCMismatch, observedAt, ebuserrors.ErrCRCMismatch))
			reconstructor.state.phase = passivePhaseAbandoned
			return events
		}
		reconstructor.state.lastProgressAt = observedAt
		reconstructor.state.phase = passivePhaseWaitTerminal
		return events
	case protocol.SymbolNack:
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonAmbiguousRetransmit, observedAt, ebuserrors.ErrNACK))
		reconstructor.state.phase = passivePhaseAbandoned
		return events
	case protocol.SymbolSyn:
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonUnexpectedSYN, observedAt, ebuserrors.ErrTimeout))
		reconstructor.pendingRecoveryReason = string(PassiveAbandonReasonUnexpectedSYN)
		// P6 Layer 1 — explicit SymbolSyn case in handleFinalACKSymbolLocked.
		reconstructor.resetStateLockedAfterSyn()
		return events
	default:
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonUnexpectedSymbol, observedAt, ebuserrors.ErrInvalidPayload))
		reconstructor.state.phase = passivePhaseAbandoned
		return events
	}
}

func (reconstructor *PassiveTransactionReconstructor) handleTerminalSymbolLocked(events []PassiveClassifiedEvent, symbol byte, observedAt time.Time) []PassiveClassifiedEvent {
	if symbol != protocol.SymbolSyn {
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonUnexpectedSymbol, observedAt, ebuserrors.ErrInvalidPayload))
		reconstructor.state.phase = passivePhaseAbandoned
		return events
	}

	reconstructor.state.lastProgressAt = observedAt
	timing := reconstructor.state.timing
	timing.Terminal = observedAt
	switch reconstructor.state.frameType {
	case protocol.FrameTypeInitiatorInitiator:
		reconstructor.recordRecoveryLocked()
		events = append(events, PassiveClassifiedEvent{
			Kind:           PassiveClassifiedEventMasterFrame,
			FrameType:      reconstructor.state.frameType,
			Request:        reconstructor.state.request,
			HasRequest:     true,
			Timing:         timing,
			ObservedAt:     observedAt,
			ACKCorrelation: reconstructor.state.ackCorrelation,
		})
	case protocol.FrameTypeInitiatorTarget:
		reconstructor.recordRecoveryLocked()
		events = append(events, PassiveClassifiedEvent{
			Kind:           PassiveClassifiedEventTransaction,
			FrameType:      reconstructor.state.frameType,
			Request:        reconstructor.state.request,
			Response:       reconstructor.state.response,
			HasRequest:     true,
			HasResponse:    true,
			Timing:         timing,
			ObservedAt:     observedAt,
			ACKCorrelation: reconstructor.state.ackCorrelation,
		})
	default:
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonUnexpectedSymbol, observedAt, ebuserrors.ErrInvalidPayload))
	}
	// P6 Layer 1 — handleTerminalSymbolLocked only reaches this site
	// after the leading-symbol SymbolSyn check at the top.
	reconstructor.resetStateLockedAfterSyn()
	return events
}

func (reconstructor *PassiveTransactionReconstructor) abandonLocked(reason PassiveAbandonReason, observedAt time.Time, err error) PassiveClassifiedEvent {
	hasRequest := reconstructor.state.frameType != protocol.FrameTypeUnknown
	hasResponse := reconstructor.state.responseExpectedLen > 0
	// P1.5 (post-Phase-C live validation 2026-05-08): strip stale
	// ACKCorrelation for failure reasons that invalidate the prior
	// ACK as evidence of a completed transaction. Phase-3 no_response
	// means responder went silent after ACKing the request; carrying
	// the ACK forward as if the transaction completed is a
	// misclassification. Defense-in-depth alongside P1 inserter
	// kind-filter — if any future consumer relies on ACKCorrelation
	// from abandoned events, they get an empty correlation rather
	// than a stale positive ACK from a doomed transaction.
	ackCorrelation := reconstructor.state.ackCorrelation
	switch reason {
	case PassiveAbandonReasonNoResponse,
		PassiveAbandonReasonNoProgress,
		PassiveAbandonReasonAmbiguousRetransmit,
		PassiveAbandonReasonCRCMismatch:
		// NoProgress is the watchdog/read-timeout sibling of
		// NoResponse: request was ACK'd, then the bus/tap went silent
		// without a SYN. Same stale-ACK hazard — strip the
		// correlation so downstream consumers don't treat it as a
		// completed transaction. (Codex P2 follow-up on PR #579,
		// 2026-05-08.)
		ackCorrelation = PassiveACKCorrelation{}
	}
	event := PassiveClassifiedEvent{
		Kind:           PassiveClassifiedEventAbandonedTransaction,
		FrameType:      reconstructor.state.frameType,
		Request:        reconstructor.state.request,
		Response:       reconstructor.state.response,
		HasRequest:     hasRequest,
		HasResponse:    hasResponse,
		Timing:         reconstructor.state.timing,
		ObservedAt:     observedAt,
		AbandonReason:  reason,
		ACKCorrelation: ackCorrelation,
		Err:            err,
	}
	event.Timing.Terminal = observedAt
	// A.8 — forensic logging for protocol-classification failures the
	// inserter cannot consume.
	if shouldLogReconstructorForensics(reason) {
		reconstructor.logForensicsLocked(reason, observedAt)
	}
	// A.9 — per-reason counter so operators can compare Helianthus
	// abandon rate to Grafana ground-truth frame counts without
	// log-grep aggregation. Lock-protected so Snapshot() can read
	// safely.
	reconstructor.metricsMu.Lock()
	if reconstructor.metrics.abandonsByReason == nil {
		reconstructor.metrics.abandonsByReason = make(map[string]uint64, 8)
	}
	reconstructor.metrics.abandonsByReason[string(reason)]++
	reconstructor.metricsMu.Unlock()
	return event
}

func shouldLogReconstructorForensics(reason PassiveAbandonReason) bool {
	switch reason {
	case PassiveAbandonReasonUnexpectedSymbol,
		PassiveAbandonReasonCorruptedRequest,
		PassiveAbandonReasonCorruptedTarget,
		PassiveAbandonReasonNoResponse:
		return true
	}
	return false
}

func (reconstructor *PassiveTransactionReconstructor) logForensicsLocked(reason PassiveAbandonReason, observedAt time.Time) {
	src, dst, prim, sec := byte(0), byte(0), byte(0), byte(0)
	if reconstructor.state.frameType != protocol.FrameTypeUnknown {
		src = reconstructor.state.request.Source
		dst = reconstructor.state.request.Target
		prim = reconstructor.state.request.Primary
		sec = reconstructor.state.request.Secondary
	} else if len(reconstructor.state.requestRaw) >= 4 {
		src = reconstructor.state.requestRaw[0]
		dst = reconstructor.state.requestRaw[1]
		prim = reconstructor.state.requestRaw[2]
		sec = reconstructor.state.requestRaw[3]
	}
	reqHex := hexBytes(reconstructor.state.requestRaw)
	respHex := hexBytes(reconstructor.state.responseRaw)
	log.Printf("passive_reconstructor abandon reason=%s phase=%d src=0x%02X dst=0x%02X prim=0x%02X sec=0x%02X req_raw=%s resp_raw=%s observed_at=%s",
		reason,
		int(reconstructor.state.phase),
		src, dst, prim, sec,
		reqHex, respHex,
		observedAt.Format(time.RFC3339Nano))
}

func hexBytes(b []byte) string {
	if len(b) == 0 {
		return "<empty>"
	}
	const hex = "0123456789ABCDEF"
	const maxBytes = 16
	count := len(b)
	truncated := false
	if count > maxBytes {
		count = maxBytes
		truncated = true
	}
	out := make([]byte, 0, count*3+3)
	for i := 0; i < count; i++ {
		if i > 0 {
			out = append(out, ' ')
		}
		out = append(out, hex[b[i]>>4], hex[b[i]&0x0F])
	}
	if truncated {
		out = append(out, '.', '.', '.')
	}
	return string(out)
}

func (reconstructor *PassiveTransactionReconstructor) resetStateLocked() {
	reconstructor.state.phase = passivePhaseIdle
	reconstructor.state.requestRaw = reconstructor.state.requestRaw[:0]
	reconstructor.state.responseRaw = reconstructor.state.responseRaw[:0]
	reconstructor.state.request = protocol.Frame{}
	reconstructor.state.response = protocol.Frame{}
	reconstructor.state.responseExpectedLen = 0
	reconstructor.state.responseCRCValid = false
	reconstructor.state.ackCorrelation = PassiveACKCorrelation{}
	reconstructor.state.frameType = protocol.FrameTypeUnknown
	reconstructor.state.timing = PassiveTimingMarkers{}
	reconstructor.state.lastProgressAt = time.Time{}
	// P6 Layer 1 + Layer 2 — re-engage the inter-frame SYN gate. Every
	// reset (transport reset, abandon, NACK, default-case fallthrough)
	// returns the parser to the pre-startup state where it must
	// observe at least one SymbolSyn before accepting another frame.
	// Call sites that have just consumed a SYN should use
	// resetStateLockedAfterSyn instead.
	reconstructor.state.synced = false
	reconstructor.state.awaitingResync = false
}

// resetStateLockedAfterSyn is the SYN-consuming variant of
// resetStateLocked. Used at every commit/abandon site where the byte
// that triggered the reset was a SymbolSyn — that SYN already
// satisfies the Layer 1 inter-frame invariant, so we can leave the
// gate engaged for the next non-SYN byte. See the P6 plan / Codex
// Pass 1 verification for the canonical list of SYN-consuming sites.
func (reconstructor *PassiveTransactionReconstructor) resetStateLockedAfterSyn() {
	reconstructor.resetStateLocked()
	reconstructor.state.synced = true
}

func m2aRequestACKCorrelation(symbol byte) PassiveACKCorrelation {
	return PassiveACKCorrelation{
		Byte:            symbol,
		Position:        PassiveACKPositionRequestACK,
		CompleteRequest: true,
		Correlator:      PassiveACKCorrelatorM2A,
	}
}

func parsePassiveResponse(request protocol.Frame, raw []byte) (protocol.Frame, bool) {
	if len(raw) < 2 {
		return protocol.Frame{}, false
	}
	length := int(raw[0])
	if len(raw) != length+2 {
		return protocol.Frame{}, false
	}
	payload := append([]byte(nil), raw[1:1+length]...)
	frame := protocol.Frame{
		Source:    request.Target,
		Target:    request.Source,
		Primary:   request.Primary,
		Secondary: request.Secondary,
		Data:      payload,
	}
	return frame, protocol.CRC(raw[:len(raw)-1]) == raw[len(raw)-1]
}

func (reconstructor *PassiveTransactionReconstructor) publishAll(events []PassiveClassifiedEvent) {
	for _, event := range events {
		reconstructor.publish(event)
	}
}

func (reconstructor *PassiveTransactionReconstructor) publish(event PassiveClassifiedEvent) {
	reconstructor.subscribersMu.Lock()
	subscribers := make([]*PassiveClassifiedSubscription, 0, len(reconstructor.subscribers))
	for _, subscription := range reconstructor.subscribers {
		subscribers = append(subscribers, subscription)
	}
	reconstructor.subscribersMu.Unlock()

	for _, subscription := range subscribers {
		if trySendPassiveEvent(subscription.ch, event) {
			continue
		}
		if subscription.priority != PassiveSubscriberCritical {
			continue
		}
		reconstructor.recordFanoutOverflow(subscription.name)
		reconstructor.unsubscribe(subscription, &PassiveClassifiedEvent{
			Kind:                PassiveClassifiedEventDiscontinuity,
			ObservedAt:          event.ObservedAt,
			DiscontinuityReason: PassiveDiscontinuityCriticalSubscriberFault,
			Subscriber:          subscription.name,
		})
	}
}

func trySendPassiveEvent(ch chan PassiveClassifiedEvent, event PassiveClassifiedEvent) (sent bool) {
	defer func() {
		if recover() != nil {
			sent = false
		}
	}()

	select {
	case ch <- event:
		return true
	default:
		return false
	}
}

func (reconstructor *PassiveTransactionReconstructor) unsubscribe(subscription *PassiveClassifiedSubscription, fault *PassiveClassifiedEvent) {
	if reconstructor == nil || subscription == nil {
		return
	}

	subscription.closeOnce.Do(func() {
		reconstructor.subscribersMu.Lock()
		if reconstructor.subscribers != nil {
			delete(reconstructor.subscribers, subscription.id)
		}
		reconstructor.subscribersMu.Unlock()

		if fault != nil {
			drainPassiveSubscription(subscription.ch)
			_ = trySendPassiveEvent(subscription.ch, *fault)
		}
		close(subscription.ch)
	})
}

func drainPassiveSubscription(ch chan PassiveClassifiedEvent) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func (reconstructor *PassiveTransactionReconstructor) shutdownEvents() []PassiveClassifiedEvent {
	reconstructor.stateMu.Lock()
	defer reconstructor.stateMu.Unlock()

	events := make([]PassiveClassifiedEvent, 0, 2)
	now := time.Now()
	if reconstructor.state.phase != passivePhaseIdle && reconstructor.state.phase != passivePhaseAbandoned {
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonShutdown, now, context.Canceled))
	}
	reconstructor.resetStateLocked()
	return append(events, PassiveClassifiedEvent{
		Kind:                PassiveClassifiedEventDiscontinuity,
		ObservedAt:          now,
		DiscontinuityReason: PassiveDiscontinuityShutdown,
		Err:                 context.Canceled,
	})
}

func (reconstructor *PassiveTransactionReconstructor) closeAllSubscribers() {
	reconstructor.subscribersMu.Lock()
	subscribers := make([]*PassiveClassifiedSubscription, 0, len(reconstructor.subscribers))
	for _, subscription := range reconstructor.subscribers {
		subscribers = append(subscribers, subscription)
	}
	reconstructor.subscribers = nil
	reconstructor.subscribersMu.Unlock()

	for _, subscription := range subscribers {
		reconstructor.unsubscribe(subscription, nil)
	}
}

func (reconstructor *PassiveTransactionReconstructor) recordFanoutOverflow(name string) {
	reconstructor.metricsMu.Lock()
	defer reconstructor.metricsMu.Unlock()
	reconstructor.metrics.fanoutOverflowTotal[normalizePassiveConsumerName(name)]++
}

func (reconstructor *PassiveTransactionReconstructor) recordRecoveryLocked() {
	if reconstructor.pendingRecoveryReason == "" {
		return
	}
	reconstructor.metricsMu.Lock()
	reconstructor.metrics.recoveryTotal[reconstructor.pendingRecoveryReason]++
	reconstructor.metricsMu.Unlock()
	reconstructor.pendingRecoveryReason = ""
}

func normalizePassiveConsumerName(name string) string {
	switch strings.TrimSpace(strings.ToLower(name)) {
	case "broadcast-listener":
		return "broadcast_listener"
	case "active-passive-dedup":
		return "dedup"
	case "observability-store":
		return "observability_store"
	default:
		return "debug_summary"
	}
}

func cloneUint64Map(input map[string]uint64) map[string]uint64 {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]uint64, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
