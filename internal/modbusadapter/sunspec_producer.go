package modbusadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	"github.com/Project-Helianthus/helianthus-modbusreg/fronius"
	"github.com/Project-Helianthus/helianthus-modbusreg/sunspec"
)

const (
	sunSpecBaseAddress    uint16 = 40000
	sunSpecMaxTotalWords  uint32 = 1024
	sunSpecMaxOccurrences uint32 = 32
)

type SunSpecQualificationOutcome string

const (
	SunSpecQualificationGO   SunSpecQualificationOutcome = "GO"
	SunSpecQualificationNoGo SunSpecQualificationOutcome = "NO_GO"
	SunSpecQualificationStop SunSpecQualificationOutcome = "STOP"
)

type SunSpecProducerConfig struct {
	UnitID             byte
	AuthorizationScope string
	ReadTimeout        time.Duration
}

type SunSpecPollIdentity struct {
	PollGeneration   uint64
	DeadlineIdentity uint64
}

// SunSpecFlavorEvaluation is one registry flavor decision: the flavor ID and
// why that flavor matched or was rejected. It carries no device evidence.
type SunSpecFlavorEvaluation struct {
	FlavorID string
	Reason   fronius.SunSpecFroniusFlavorReason
}

type SunSpecQualificationResult struct {
	Outcome          SunSpecQualificationOutcome
	CapabilityID     string
	CapabilityReason sunspec.CapabilityReason
	FlavorID         string
	FlavorReason     fronius.SunSpecFroniusFlavorReason
	// FlavorEvaluations lists every registry flavor decision in registry order
	// once the capability is admitted, so a refusal stays diagnosable when the
	// flavors reject for different reasons. FlavorReason stays the single
	// reason shared by all of them, or empty when they differ.
	FlavorEvaluations []SunSpecFlavorEvaluation
	ObservationCount  int
	SampleID          string
	Chain             sunspec.ChainSnapshot
}

// SunSpecProducer owns bounded acquisition and delegates all model, capability,
// and vendor-flavor decisions to helianthus-modbusreg.
type SunSpecProducer struct {
	adapter  *Adapter
	config   SunSpecProducerConfig
	registry sunspec.DecoderRegistry
	plan     sunspec.ChainPlan
	mu       sync.Mutex
}

type sunSpecSourceView struct {
	view      modbus.LogicalReadView
	wireBytes []byte
}

func NewSunSpecProducer(adapter *Adapter, config SunSpecProducerConfig) (*SunSpecProducer, error) {
	if adapter == nil || adapter.RuntimeAcquisitionSource() == nil || config.UnitID == 0 || config.UnitID > 247 ||
		config.AuthorizationScope == "" || config.ReadTimeout <= 0 {
		return nil, errors.New("SunSpec producer configuration is incomplete")
	}
	registry, err := sunspec.NewStandardSunSpecDecoderRegistry(sunspec.ModelsRevisionV1)
	if err != nil {
		return nil, fmt.Errorf("create SunSpec decoder registry: %w", err)
	}
	plan, err := sunspec.NewChainPlan(sunspec.ChainPlanSpec{
		SchemaRevision: sunspec.ModelsRevisionV1,
		BaseCandidates: []uint16{sunSpecBaseAddress},
		Limits: sunspec.ChainLimits{
			MaxTotalWords:  sunSpecMaxTotalWords,
			MaxOccurrences: sunSpecMaxOccurrences,
		},
		DecoderKeys: registry.DecoderKeys(),
	})
	if err != nil {
		return nil, fmt.Errorf("create SunSpec chain plan: %w", err)
	}
	return &SunSpecProducer{adapter: adapter, config: config, registry: registry, plan: plan}, nil
}

func (producer *SunSpecProducer) Qualify(ctx context.Context, identity SunSpecPollIdentity) (SunSpecQualificationResult, error) {
	return producer.qualify(ctx, identity, false, true)
}

// Refresh acquires and publishes a newer canonical state with bounded,
// retrievable native evidence for every public SemReg evidence reference.
func (producer *SunSpecProducer) Refresh(ctx context.Context, identity SunSpecPollIdentity) (SunSpecQualificationResult, error) {
	return producer.qualify(ctx, identity, false, false)
}

func (producer *SunSpecProducer) qualifyMixedGenerationForTest(ctx context.Context, identity SunSpecPollIdentity) (SunSpecQualificationResult, error) {
	return producer.qualify(ctx, identity, true, true)
}

func (producer *SunSpecProducer) qualify(ctx context.Context, identity SunSpecPollIdentity, mixGeneration, retainEvidence bool) (SunSpecQualificationResult, error) {
	if producer == nil || ctx == nil || identity.PollGeneration == 0 || identity.DeadlineIdentity == 0 {
		return SunSpecQualificationResult{}, errors.New("SunSpec qualification request is incomplete")
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()

	snapshot, invalid, err := producer.acquire(ctx, identity, mixGeneration)
	if err != nil {
		return SunSpecQualificationResult{}, err
	}
	if invalid {
		return SunSpecQualificationResult{
			Outcome:          SunSpecQualificationStop,
			CapabilityID:     sunspec.ThreePhaseMonitoringCapabilityID,
			CapabilityReason: sunspec.CapabilityReasonInvalidChain,
		}, nil
	}
	return producer.classify(identity, snapshot, retainEvidence), nil
}

func (producer *SunSpecProducer) acquire(ctx context.Context, identity SunSpecPollIdentity, mixGeneration bool) (sunspec.ChainSnapshot, bool, error) {
	chain := sunspec.NewChain(producer.plan)
	logicalID := uint64(0)
	readIndex := 0
	for {
		requests := chain.NextRequests()
		if len(requests) == 0 {
			return sunspec.ChainSnapshot{}, true, nil
		}
		for _, request := range requests {
			logicalID++
			source, err := producer.read(ctx, identity, request, logicalID)
			if err != nil {
				return sunspec.ChainSnapshot{}, false, err
			}
			snapshot, err := source.snapshot()
			if err != nil {
				return sunspec.ChainSnapshot{}, true, nil
			}
			if mixGeneration && readIndex == 1 {
				record := snapshot.Record()
				record.TransportGeneration++
				snapshot, err = modbusreg.NewLogicalViewSnapshot(record)
				if err != nil {
					return sunspec.ChainSnapshot{}, true, nil
				}
			}
			readIndex++
			completed, err := chain.AdmitReplay(request, snapshot)
			if err != nil {
				return sunspec.ChainSnapshot{}, true, nil
			}
			if len(completed.RawWords()) != 0 {
				return completed, false, nil
			}
		}
	}
}

func (producer *SunSpecProducer) read(ctx context.Context, identity SunSpecPollIdentity, request sunspec.ReadRequest, logicalID uint64) (sunSpecSourceView, error) {
	modbusRequest, err := modbus.NewReadRegistersRequest(modbus.FunctionReadHoldingRegisters, request.Address(), request.WordCount())
	if err != nil {
		return sunSpecSourceView{}, err
	}
	batch, err := producer.adapter.ExecuteRead(ctx, ReadPlan{
		UnitID:             producer.config.UnitID,
		AuthorizationScope: producer.config.AuthorizationScope,
		PollGeneration:     identity.PollGeneration,
		DeadlineIdentity:   identity.DeadlineIdentity,
		Timeout:            producer.config.ReadTimeout,
		Reads:              []modbus.TCPLogicalRead{{LogicalViewID: logicalID, Request: modbusRequest}},
	})
	if err != nil {
		return sunSpecSourceView{}, err
	}
	if len(batch.Views) != 1 || len(batch.Views[0].Words()) != int(request.WordCount()) {
		return sunSpecSourceView{}, errors.New("SunSpec read did not return its exact logical view")
	}
	for _, response := range batch.Responses {
		if response.WireResponseID() == batch.Views[0].WireResponseID() {
			return sunSpecSourceView{view: batch.Views[0], wireBytes: response.Bytes()}, nil
		}
	}
	return sunSpecSourceView{}, errors.New("SunSpec read did not retain wire evidence")
}

func (source sunSpecSourceView) snapshot() (modbusreg.LogicalViewSnapshot, error) {
	view, provenance := source.view, source.view.Provenance()
	return modbusreg.NewLogicalViewSnapshot(modbusreg.LogicalViewRecord{
		LogicalViewID: view.LogicalViewID(), WireResponseID: view.WireResponseID(),
		PhysicalRequestID: provenance.PhysicalRequestID, Endpoint: provenance.Wire.Endpoint,
		ConnectionID: provenance.Wire.ConnectionID, Transport: provenance.Wire.Transport,
		TransportGeneration: provenance.Wire.TransportGeneration, UnitID: provenance.Wire.UnitID,
		RequestedFunction: provenance.Wire.RequestedFunction, ReceivedFunction: provenance.Wire.ReceivedFunction,
		Table: provenance.Wire.Table, PhysicalOffset: provenance.Wire.Offset,
		PhysicalWordCount: provenance.Wire.Quantity, AuthorizationScope: provenance.AuthorizationScope,
		PollGeneration: provenance.PollGeneration, DeadlineIdentity: provenance.DeadlineIdentity,
		LogicalOffset: provenance.LogicalOffset, LogicalWordCount: provenance.LogicalWordCount,
		SliceOffset: provenance.SliceOffset, SliceWordCount: provenance.SliceWordCount,
		Words: view.Words(), WireResponseBytes: source.wireBytes,
	})
}

func (producer *SunSpecProducer) classify(identity SunSpecPollIdentity, snapshot sunspec.ChainSnapshot, retainEvidence bool) SunSpecQualificationResult {
	capability := producer.registry.EvaluateThreePhaseMonitoring(snapshot)
	result := SunSpecQualificationResult{
		CapabilityID: capability.ProfileID(), CapabilityReason: capability.Reason(),
	}
	if !capability.Admitted() {
		if capability.Reason() == sunspec.CapabilityReasonInvalidChain {
			result.Outcome = SunSpecQualificationStop
		} else {
			result.Outcome = SunSpecQualificationNoGo
		}
		return result
	}

	selection := fronius.SelectObservedFlavor(producer.registry, snapshot)
	result.FlavorEvaluations = sunSpecFlavorEvaluations(selection.Evaluations())
	if selection.Matched() {
		flavor, ok := selection.Decision()
		if !ok || selection.Reason() != fronius.SunSpecFroniusFlavorSelectionReasonMatched ||
			!flavor.Matched() || flavor.Reason() != fronius.SunSpecFroniusFlavorReasonMatched {
			result.Outcome = SunSpecQualificationStop
			return result
		}
		observation, err := fronius.NewSunSpecQualificationObservation(producer.registry, snapshot)
		if err != nil {
			result.Outcome = SunSpecQualificationStop
			return result
		}
		if retainEvidence {
			if err := producer.adapter.RecordSunSpecQualificationObservation(observation); err != nil {
				result.Outcome = SunSpecQualificationStop
				return result
			}
		} else if err := producer.adapter.RecordSunSpecCurrentObservation(observation); err != nil {
			result.Outcome = SunSpecQualificationStop
			return result
		}
		result.FlavorID, result.FlavorReason = flavor.FlavorID(), flavor.Reason()
		result.Outcome = SunSpecQualificationGO
		result.SampleID = observation.SampleID()
		if retainEvidence {
			result.ObservationCount = 1
		}
		result.Chain = snapshot
		return result
	}
	switch selection.Reason() {
	case fronius.SunSpecFroniusFlavorSelectionReasonNoMatch:
		result.Outcome = SunSpecQualificationNoGo
		result.FlavorReason = commonRejectedFroniusFlavorReason(selection.Evaluations())
	default:
		result.Outcome = SunSpecQualificationStop
	}
	return result
}

func sunSpecFlavorEvaluations(evaluations []fronius.SunSpecFroniusFlavorDecision) []SunSpecFlavorEvaluation {
	if len(evaluations) == 0 {
		return nil
	}
	out := make([]SunSpecFlavorEvaluation, len(evaluations))
	for index, evaluation := range evaluations {
		out[index] = SunSpecFlavorEvaluation{FlavorID: evaluation.FlavorID(), Reason: evaluation.Reason()}
	}
	return out
}

func commonRejectedFroniusFlavorReason(evaluations []fronius.SunSpecFroniusFlavorDecision) fronius.SunSpecFroniusFlavorReason {
	if len(evaluations) == 0 || evaluations[0].Matched() {
		return ""
	}
	reason := evaluations[0].Reason()
	for _, evaluation := range evaluations[1:] {
		if evaluation.Matched() || evaluation.Reason() != reason {
			return ""
		}
	}
	return reason
}
