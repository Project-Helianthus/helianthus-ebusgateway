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
}

type passiveReconstructorMetrics struct {
	fanoutOverflowTotal map[string]uint64
	recoveryTotal       map[string]uint64
	abandonsByReason    map[string]uint64
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
		TapStatus:           tapStatus,
		FanoutOverflowTotal: cloneUint64Map(reconstructor.metrics.fanoutOverflowTotal),
		RecoveryTotal:       cloneUint64Map(reconstructor.metrics.recoveryTotal),
		AbandonsByReason:    cloneUint64Map(reconstructor.metrics.abandonsByReason),
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
		if symbol == protocol.SymbolSyn {
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
			reconstructor.resetStateLocked()
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
}

func (reconstructor *PassiveTransactionReconstructor) handleRequestSymbolLocked(events []PassiveClassifiedEvent, symbol byte, observedAt time.Time) []PassiveClassifiedEvent {
	if symbol != protocol.SymbolSyn {
		reconstructor.state.requestRaw = append(reconstructor.state.requestRaw, symbol)
		reconstructor.state.lastProgressAt = observedAt
		if len(reconstructor.state.requestRaw) > maxPassiveRequestBytes {
			reason := PassiveAbandonReasonCorruptedRequest
			if reconstructor.isSelfOriginatedRaw() {
				reason = PassiveAbandonReasonSelfEcho
			}
			events = append(events, reconstructor.abandonLocked(reason, observedAt, ebuserrors.ErrInvalidPayload))
			reconstructor.state.phase = passivePhaseAbandoned
		}
		return events
	}

	reconstructor.state.timing.RequestEnd = observedAt
	reconstructor.state.lastProgressAt = observedAt

	frame, ok := parseFrame(reconstructor.state.requestRaw)
	if !ok {
		reason := PassiveAbandonReasonCorruptedRequest
		if len(reconstructor.state.requestRaw) <= 3 {
			reason = PassiveAbandonReasonArbitrationFragment
		} else if reconstructor.isSelfOriginatedRaw() {
			reason = PassiveAbandonReasonSelfEcho
		} else if isScanProbeRaw(reconstructor.state.requestRaw) {
			reason = PassiveAbandonReasonScanCollision
		}
		events = append(events, reconstructor.abandonLocked(reason, observedAt, ebuserrors.ErrInvalidPayload))
		reconstructor.resetStateLocked()
		return events
	}

	frameType := frame.Type()
	if frameType == protocol.FrameTypeUnknown {
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonCorruptedTarget, observedAt, ebuserrors.ErrInvalidPayload))
		reconstructor.resetStateLocked()
		return events
	}

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
		reconstructor.resetStateLocked()
	case protocol.FrameTypeInitiatorInitiator, protocol.FrameTypeInitiatorTarget:
		reconstructor.state.phase = passivePhaseWaitACK
	default:
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonCorruptedTarget, observedAt, ebuserrors.ErrInvalidPayload))
		reconstructor.resetStateLocked()
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
		reconstructor.resetStateLocked()
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
		reconstructor.resetStateLocked()
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
		reconstructor.resetStateLocked()
		return events
	}
	if len(reconstructor.state.responseRaw) > 0 && symbol == protocol.SymbolSyn {
		events = append(events, reconstructor.abandonLocked(PassiveAbandonReasonUnexpectedSYN, observedAt, ebuserrors.ErrTimeout))
		reconstructor.pendingRecoveryReason = string(PassiveAbandonReasonUnexpectedSYN)
		reconstructor.resetStateLocked()
		return events
	}

	reconstructor.state.lastProgressAt = observedAt
	if len(reconstructor.state.responseRaw) == 0 {
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
		reconstructor.resetStateLocked()
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
	reconstructor.resetStateLocked()
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
		PassiveAbandonReasonAmbiguousRetransmit,
		PassiveAbandonReasonCRCMismatch:
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
