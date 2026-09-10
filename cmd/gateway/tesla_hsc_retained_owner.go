package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

var errTeslaHSCRetainedUnavailable = errors.New("tesla HSC retained owner unavailable")

// TeslaGen3CompletedFrame is one immutable normal response already correlated
// by the transport to the request in TeslaGen3CompletedExchange.
type TeslaGen3CompletedFrame struct {
	Payload []byte
	ADU     []byte
}

// TeslaGen3CompletedExchange is a completed non-send input. The owner accepts
// no exchanger, serial path, request builder, retry callback, or live handle.
type TeslaGen3CompletedExchange struct {
	EndpointID       string
	SourceID         string
	SourceEpoch      string
	DriverGeneration uint64
	Node             byte
	OperationVersion string
	Operation        modbusreg.TeslaFC100Operation
	CorrelationID    uint64
	RequestPayload   []byte
	RequestADU       []byte
	ResponseFrames   []TeslaGen3CompletedFrame
	ReceiptWall      time.Time
	ReceiptMonotonic time.Duration
	TerminalOutcome  string
}

type TeslaGen3PersistentOutcome struct {
	Exchange             TeslaGen3CompletedExchange
	MaxOutputCurrentAmps uint32
}

type TeslaGen3ProvisionalOutcome struct {
	Set, Readback       TeslaGen3CompletedExchange
	LimitCurrentMaxAmps uint32
	LimitTimeoutSeconds uint32
	InhibitCharging     bool
}

// TeslaGen3RetainedExchangeEvidence preserves the exact transport bytes and
// lifecycle axes. Every slice returned to a caller is a detached copy.
type TeslaGen3RetainedExchangeEvidence struct {
	EndpointID, SourceID, SourceEpoch string
	DriverGeneration, CorrelationID   uint64
	Node                              byte
	OperationVersion                  string
	Operation                         modbusreg.TeslaFC100Operation
	RequestPayload, RequestADU        []byte
	ResponsePayloads, ResponseADUs    [][]byte
	ReceiptWall                       time.Time
	ReceiptMonotonic                  time.Duration
	TerminalOutcome                   string
}

func (e TeslaGen3RetainedExchangeEvidence) clone() TeslaGen3RetainedExchangeEvidence {
	e.RequestPayload = append([]byte(nil), e.RequestPayload...)
	e.RequestADU = append([]byte(nil), e.RequestADU...)
	e.ResponsePayloads = cloneTeslaBytes(e.ResponsePayloads)
	e.ResponseADUs = cloneTeslaBytes(e.ResponseADUs)
	return e
}

type teslaHSCRetainedOwner struct {
	mu                sync.Mutex
	cfg               ebusgateway.TeslaGen3HSCRetainedConfig
	active            bool
	nextGeneration    uint64
	successorReserved bool
	successorConsumed bool
	persistent        *modbusreg.TeslaGen3PersistentCurrentLimit
	provisional       *modbusreg.TeslaGen3ProvisionalCurrentLimit
	persistentInput   TeslaGen3RetainedExchangeEvidence
	provisionalIn     [2]TeslaGen3RetainedExchangeEvidence
	havePersistent    bool
	haveProvisional   bool
	publication       *mcp.TeslaGen3EVSESemanticPublication
	sequence          uint64
	lastCorrelation   uint64
	lastReceiptWall   time.Time
	lastReceiptMono   time.Duration
}

func startTeslaHSCRetainedOwner(config ebusgateway.TeslaGen3HSCRetainedConfig) (*teslaHSCRetainedOwner, error) {
	if !config.Enabled {
		if config != (ebusgateway.TeslaGen3HSCRetainedConfig{}) {
			return nil, errors.New("disabled Tesla HSC retained configuration contains active fields")
		}
		return nil, nil
	}
	if !validTeslaRetainedConfig(config) {
		return nil, errors.New("enabled Tesla HSC retained configuration is invalid")
	}
	publication, err := mcp.NewTeslaGen3EVSESemanticPublication(mcp.TeslaGen3EVSESemanticConfig{
		AssetID: config.AssetID, SourceID: config.SourceID, EVSEID: config.EVSEID, ConnectorID: config.ConnectorID,
		SourceEpoch: config.SourceEpoch, ClockEpoch: config.ClockEpoch, DriverGeneration: config.DriverGeneration,
	})
	if err != nil {
		return nil, err
	}
	return &teslaHSCRetainedOwner{cfg: config, active: true, publication: publication}, nil
}

func validTeslaRetainedConfig(config ebusgateway.TeslaGen3HSCRetainedConfig) bool {
	return validTeslaPublicLabel(config.EndpointID) && validGrowattAssetID(config.AssetID) && validGrowattSourceID(config.SourceID) &&
		config.AssetID != config.SourceID && semreg.SourceEpochID(config.SourceEpoch).Validate() == nil &&
		semreg.ClockEpochID(config.ClockEpoch).Validate() == nil && validTeslaPublicLabel(config.EVSEID) &&
		validTeslaPublicLabel(config.ConnectorID) && config.Profile == modbusreg.TeslaGen3CurrentLimitOperationVersion24443 &&
		config.DriverGeneration != 0 && config.Node != 0 && config.Node <= 247
}

func validTeslaPublicLabel(value string) bool {
	if len(value) == 0 || len(value) > 256 {
		return false
	}
	for _, r := range value {
		if r <= 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func (owner *teslaHSCRetainedOwner) IngestPersistent(ctx context.Context, outcome TeslaGen3PersistentOutcome) error {
	if owner == nil || ctx == nil {
		return errTeslaHSCRetainedUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.active {
		return errTeslaHSCRetainedUnavailable
	}
	evidence, terminal, err := owner.validateExchange(outcome.Exchange, modbusreg.TeslaFC100OperationWCConfigureSettings)
	if err != nil {
		return err
	}
	if err := owner.admitAfterRetained(evidence); err != nil {
		return err
	}
	record, err := modbusreg.NewTeslaGen3PersistentCurrentLimit(modbusreg.TeslaGen3PersistentCurrentLimitSpec{
		OperationVersion: outcome.Exchange.OperationVersion, MaxOutputCurrentAmps: outcome.MaxOutputCurrentAmps,
		RequestPayload: outcome.Exchange.RequestPayload, TerminalPayload: terminal,
	})
	if err != nil {
		return fmt.Errorf("tesla HSC persistent outcome: %w", err)
	}
	return owner.publishAndCommit(&record, nil, evidence, [2]TeslaGen3RetainedExchangeEvidence{}, true, false)
}

func (owner *teslaHSCRetainedOwner) IngestProvisional(ctx context.Context, outcome TeslaGen3ProvisionalOutcome) error {
	if owner == nil || ctx == nil {
		return errTeslaHSCRetainedUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.active {
		return errTeslaHSCRetainedUnavailable
	}
	if outcome.Set.CorrelationID == 0 || outcome.Set.CorrelationID >= outcome.Readback.CorrelationID {
		return errors.New("tesla HSC provisional correlation is invalid")
	}
	setEvidence, ack, err := owner.validateExchange(outcome.Set, modbusreg.TeslaFC100OperationWCSetProvisional)
	if err != nil {
		return err
	}
	readEvidence, terminal, err := owner.validateExchange(outcome.Readback, modbusreg.TeslaFC100OperationWCGetProvisional)
	if err != nil {
		return err
	}
	if readEvidence.ReceiptMonotonic < setEvidence.ReceiptMonotonic || readEvidence.ReceiptWall.Before(setEvidence.ReceiptWall) {
		return errors.New("tesla HSC provisional lifecycle regressed")
	}
	if err := owner.admitAfterRetained(setEvidence); err != nil {
		return err
	}
	record, err := modbusreg.NewTeslaGen3ProvisionalCurrentLimit(modbusreg.TeslaGen3ProvisionalCurrentLimitSpec{
		OperationVersion: outcome.Set.OperationVersion, LimitCurrentMaxAmps: outcome.LimitCurrentMaxAmps,
		LimitTimeoutSeconds: outcome.LimitTimeoutSeconds, InhibitCharging: outcome.InhibitCharging,
		SetRequestPayload: outcome.Set.RequestPayload, AckPayload: ack,
		ReadbackRequestPayload: outcome.Readback.RequestPayload, ReadbackTerminalPayload: terminal,
	})
	if err != nil {
		return fmt.Errorf("tesla HSC provisional outcome: %w", err)
	}
	return owner.publishAndCommit(nil, &record, TeslaGen3RetainedExchangeEvidence{}, [2]TeslaGen3RetainedExchangeEvidence{setEvidence, readEvidence}, false, true)
}

func (owner *teslaHSCRetainedOwner) admitAfterRetained(evidence TeslaGen3RetainedExchangeEvidence) error {
	if evidence.CorrelationID <= owner.lastCorrelation ||
		owner.lastCorrelation != 0 && (evidence.ReceiptMonotonic < owner.lastReceiptMono || evidence.ReceiptWall.Before(owner.lastReceiptWall)) {
		return errors.New("tesla HSC completed exchange is late or replayed")
	}
	return nil
}

func (owner *teslaHSCRetainedOwner) validateExchange(exchange TeslaGen3CompletedExchange, operation modbusreg.TeslaFC100Operation) (TeslaGen3RetainedExchangeEvidence, []byte, error) {
	if exchange.EndpointID != owner.cfg.EndpointID || exchange.SourceID != owner.cfg.SourceID || exchange.SourceEpoch != owner.cfg.SourceEpoch ||
		exchange.DriverGeneration != owner.cfg.DriverGeneration || exchange.Node != owner.cfg.Node ||
		exchange.OperationVersion != owner.cfg.Profile || exchange.Operation != operation || exchange.CorrelationID == 0 ||
		exchange.TerminalOutcome != "success" || exchange.ReceiptWall.IsZero() || exchange.ReceiptMonotonic < 0 ||
		len(exchange.RequestPayload) == 0 || len(exchange.RequestADU) == 0 || len(exchange.ResponseFrames) == 0 || len(exchange.ResponseFrames) > 8 {
		return TeslaGen3RetainedExchangeEvidence{}, nil, errors.New("tesla HSC completed exchange identity or lifecycle is invalid")
	}
	function, err := modbus.NewPrivateFunctionCode(100)
	if err != nil {
		return TeslaGen3RetainedExchangeEvidence{}, nil, err
	}
	request, err := modbus.NewPrivateFunctionRequest(function, exchange.RequestPayload)
	if err != nil {
		return TeslaGen3RetainedExchangeEvidence{}, nil, err
	}
	requestADU, err := modbus.EncodeRTUPrivateFunctionADU(exchange.Node, request)
	if err != nil || !bytes.Equal(requestADU, exchange.RequestADU) {
		return TeslaGen3RetainedExchangeEvidence{}, nil, errors.New("tesla HSC request ADU correlation is invalid")
	}
	payloads := make([][]byte, 0, len(exchange.ResponseFrames))
	adus := make([][]byte, 0, len(exchange.ResponseFrames))
	for _, frame := range exchange.ResponseFrames {
		response, err := modbus.DecodeRTUPrivateFunctionResponseADU(exchange.Node, request, frame.ADU)
		if err != nil || !bytes.Equal(response.Payload(), frame.Payload) {
			return TeslaGen3RetainedExchangeEvidence{}, nil, errors.New("tesla HSC response ADU correlation is invalid")
		}
		payloads = append(payloads, append([]byte(nil), frame.Payload...))
		adus = append(adus, append([]byte(nil), frame.ADU...))
	}
	results, err := modbusreg.DecodeTeslaFC100OperationSequence(exchange.OperationVersion, operation, exchange.RequestPayload, payloads)
	if err != nil || len(results) == 0 || results[len(results)-1].Kind != modbusreg.TeslaFC100OperationTerminal {
		return TeslaGen3RetainedExchangeEvidence{}, nil, errors.New("tesla HSC operation outcome correlation is invalid")
	}
	evidence := TeslaGen3RetainedExchangeEvidence{
		EndpointID: exchange.EndpointID, SourceID: exchange.SourceID, SourceEpoch: exchange.SourceEpoch,
		DriverGeneration: exchange.DriverGeneration, CorrelationID: exchange.CorrelationID, Node: exchange.Node,
		OperationVersion: exchange.OperationVersion, Operation: exchange.Operation,
		RequestPayload: append([]byte(nil), exchange.RequestPayload...), RequestADU: append([]byte(nil), exchange.RequestADU...),
		ResponsePayloads: payloads, ResponseADUs: adus, ReceiptWall: exchange.ReceiptWall.UTC(),
		ReceiptMonotonic: exchange.ReceiptMonotonic, TerminalOutcome: exchange.TerminalOutcome,
	}
	return evidence, append([]byte(nil), payloads[len(payloads)-1]...), nil
}

func (owner *teslaHSCRetainedOwner) publishAndCommit(persistent *modbusreg.TeslaGen3PersistentCurrentLimit, provisional *modbusreg.TeslaGen3ProvisionalCurrentLimit, persistentInput TeslaGen3RetainedExchangeEvidence, provisionalInput [2]TeslaGen3RetainedExchangeEvidence, setPersistent, setProvisional bool) error {
	nextPersistent, nextProvisional := owner.persistent, owner.provisional
	nextPersistentEvidence, nextProvisionalEvidence := owner.persistentInput, owner.provisionalIn
	havePersistent, haveProvisional := owner.havePersistent, owner.haveProvisional
	if setPersistent {
		nextPersistent, nextPersistentEvidence, havePersistent = persistent, persistentInput, true
	}
	if setProvisional {
		nextProvisional, nextProvisionalEvidence, haveProvisional = provisional, provisionalInput, true
	}
	if havePersistent {
		latest := nextPersistentEvidence
		if haveProvisional {
			provisionalLatest := nextProvisionalEvidence[1]
			if provisionalLatest.ReceiptMonotonic > latest.ReceiptMonotonic ||
				provisionalLatest.ReceiptMonotonic == latest.ReceiptMonotonic && provisionalLatest.ReceiptWall.After(latest.ReceiptWall) {
				latest = provisionalLatest
			}
		}
		nextSequence := owner.sequence + 1
		if nextSequence == 0 {
			return errors.New("tesla HSC publication sequence exhausted")
		}
		source := mcp.TeslaGen3EVSECurrentLimitV1Source{Persistent: nextPersistent}
		// An unchanged provisional sibling must not inherit a later persistent
		// outcome's receipt and thereby gain a new lifetime. The one exception is
		// the initial semantic batch: a provisional accepted before the first
		// persistent outcome has not been published yet, and its own retained
		// receipt axes preserve its original lifetime.
		publishProvisional := haveProvisional && (!setPersistent || owner.sequence == 0)
		if publishProvisional {
			source.Provisional = nextProvisional
		}
		ns := latest.ReceiptMonotonic.Nanoseconds()
		semanticEvidence := mcp.TeslaGen3EVSESemanticEvidence{
			ObservationID: fmt.Sprintf("observation:%s:%d:%d", owner.cfg.EndpointID, owner.cfg.DriverGeneration, nextSequence),
			ObservedAt:    latest.ReceiptWall, EvaluatedAt: latest.ReceiptWall, MonotonicNS: ns, EvaluatedMonotonicNS: ns, Sequence: nextSequence,
			PersistentObservedAt: nextPersistentEvidence.ReceiptWall, PersistentMonotonicNS: nextPersistentEvidence.ReceiptMonotonic.Nanoseconds(),
		}
		if publishProvisional {
			semanticEvidence.ProvisionalObservedAt = nextProvisionalEvidence[1].ReceiptWall
			semanticEvidence.ProvisionalMonotonicNS = nextProvisionalEvidence[1].ReceiptMonotonic.Nanoseconds()
		}
		if err := owner.publication.Publish(source, semanticEvidence); err != nil {
			return err
		}
		owner.sequence = nextSequence
	}
	owner.persistent, owner.provisional = nextPersistent, nextProvisional
	owner.persistentInput, owner.provisionalIn = nextPersistentEvidence, nextProvisionalEvidence
	owner.havePersistent, owner.haveProvisional = havePersistent, haveProvisional
	latestEvidence := persistentInput
	if setProvisional {
		latestEvidence = provisionalInput[1]
	}
	owner.lastCorrelation = latestEvidence.CorrelationID
	owner.lastReceiptWall = latestEvidence.ReceiptWall
	owner.lastReceiptMono = latestEvidence.ReceiptMonotonic
	return nil
}

func (owner *teslaHSCRetainedOwner) TeslaGen3EVSECurrentLimitV1(context.Context) (mcp.TeslaGen3EVSECurrentLimitV1Source, error) {
	if owner == nil {
		return mcp.TeslaGen3EVSECurrentLimitV1Source{}, errTeslaHSCRetainedUnavailable
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.active || !owner.havePersistent && !owner.haveProvisional {
		return mcp.TeslaGen3EVSECurrentLimitV1Source{}, errTeslaHSCRetainedUnavailable
	}
	return mcp.TeslaGen3EVSECurrentLimitV1Source{Persistent: owner.persistent, Provisional: owner.provisional}, nil
}

func (owner *teslaHSCRetainedOwner) TeslaGen3EVSESemanticCurrent(ctx context.Context) (any, error) {
	if owner == nil || ctx == nil {
		return nil, errTeslaHSCRetainedUnavailable
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.active || owner.publication == nil || !owner.havePersistent {
		return nil, errTeslaHSCRetainedUnavailable
	}
	return owner.publication.TeslaGen3EVSESemanticCurrent(ctx)
}

// SemanticEVSECurrentAt exposes only the detached accepted SemReg tuple used
// by passive metrics bindings. The owner remains the lifecycle fence, so a
// stopped or superseded generation cannot be revived by a later scrape.
func (owner *teslaHSCRetainedOwner) SemanticEVSECurrentAt(at time.Time) (mcp.SemanticEVSECurrent, bool) {
	if owner == nil {
		return mcp.SemanticEVSECurrent{}, false
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.active || owner.publication == nil || !owner.havePersistent {
		return mcp.SemanticEVSECurrent{}, false
	}
	return owner.publication.SemanticEVSECurrentAt(at)
}

func (owner *teslaHSCRetainedOwner) RetainedEvidence() []TeslaGen3RetainedExchangeEvidence {
	if owner == nil {
		return nil
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	out := make([]TeslaGen3RetainedExchangeEvidence, 0, 3)
	if owner.havePersistent {
		out = append(out, owner.persistentInput.clone())
	}
	if owner.haveProvisional {
		out = append(out, owner.provisionalIn[0].clone(), owner.provisionalIn[1].clone())
	}
	return out
}

func (owner *teslaHSCRetainedOwner) Fence(current, next uint64) error {
	if owner == nil {
		return errTeslaHSCRetainedUnavailable
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.active || current != owner.cfg.DriverGeneration || current == ^uint64(0) || next != current+1 {
		return errors.New("tesla HSC generation fence is invalid")
	}
	owner.active, owner.nextGeneration = false, next
	return nil
}

func (owner *teslaHSCRetainedOwner) Successor(config ebusgateway.TeslaGen3HSCRetainedConfig) (*teslaHSCRetainedOwner, error) {
	if owner == nil {
		return nil, errTeslaHSCRetainedUnavailable
	}
	owner.mu.Lock()
	previous := owner.cfg
	if owner.active || owner.successorReserved || owner.successorConsumed || config.DriverGeneration != owner.nextGeneration || config.SourceEpoch == previous.SourceEpoch ||
		config.EndpointID != previous.EndpointID || config.AssetID != previous.AssetID || config.SourceID != previous.SourceID ||
		config.ClockEpoch != previous.ClockEpoch || config.EVSEID != previous.EVSEID || config.ConnectorID != previous.ConnectorID ||
		config.Profile != previous.Profile || config.Node != previous.Node {
		owner.mu.Unlock()
		return nil, errors.New("tesla HSC successor identity or generation is invalid")
	}
	owner.successorReserved = true
	owner.mu.Unlock()

	successor, err := startTeslaHSCRetainedOwner(config)
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.successorReserved = false
	if err != nil {
		return nil, err
	}
	if successor == nil {
		return nil, errors.New("tesla HSC successor construction returned no owner")
	}
	owner.successorConsumed = true
	return successor, nil
}

func (owner *teslaHSCRetainedOwner) Close() error {
	if owner == nil {
		return nil
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.active = false
	return nil
}

func cloneTeslaBytes(values [][]byte) [][]byte {
	if values == nil {
		return nil
	}
	out := make([][]byte, len(values))
	for i := range values {
		out[i] = append([]byte(nil), values[i]...)
	}
	return out
}
