package modbusadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
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

type SunSpecQualificationResult struct {
	Outcome          SunSpecQualificationOutcome
	CapabilityID     string
	CapabilityReason modbusreg.SunSpecCapabilityReason
	FlavorID         string
	FlavorReason     modbusreg.SunSpecFroniusFlavorReason
	ObservationCount int
	SampleID         string
	Chain            modbusreg.SunSpecChainSnapshot
}

// SunSpecProducer owns bounded acquisition and delegates all model, capability,
// and vendor-flavor decisions to helianthus-modbusreg.
type SunSpecProducer struct {
	adapter  *Adapter
	config   SunSpecProducerConfig
	registry modbusreg.SunSpecDecoderRegistry
	plan     modbusreg.SunSpecChainPlan
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
	registry, err := modbusreg.NewStandardSunSpecDecoderRegistry(modbusreg.SunSpecModelsRevisionV1)
	if err != nil {
		return nil, fmt.Errorf("create SunSpec decoder registry: %w", err)
	}
	plan, err := modbusreg.NewSunSpecChainPlan(modbusreg.SunSpecChainPlanSpec{
		SchemaRevision: modbusreg.SunSpecModelsRevisionV1,
		BaseCandidates: []uint16{sunSpecBaseAddress},
		Limits: modbusreg.SunSpecChainLimits{
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
	return producer.qualify(ctx, identity, false)
}

func (producer *SunSpecProducer) qualifyMixedGenerationForTest(ctx context.Context, identity SunSpecPollIdentity) (SunSpecQualificationResult, error) {
	return producer.qualify(ctx, identity, true)
}

func (producer *SunSpecProducer) qualify(ctx context.Context, identity SunSpecPollIdentity, mixGeneration bool) (SunSpecQualificationResult, error) {
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
			CapabilityID:     modbusreg.SunSpecThreePhaseMonitoringCapabilityID,
			CapabilityReason: modbusreg.SunSpecCapabilityReasonInvalidChain,
			FlavorID:         modbusreg.SunSpecFroniusObservedFlavorID,
		}, nil
	}
	return producer.classify(identity, snapshot), nil
}

func (producer *SunSpecProducer) acquire(ctx context.Context, identity SunSpecPollIdentity, mixGeneration bool) (modbusreg.SunSpecChainSnapshot, bool, error) {
	chain := modbusreg.NewSunSpecChain(producer.plan)
	logicalID := uint64(0)
	readIndex := 0
	for {
		requests := chain.NextRequests()
		if len(requests) == 0 {
			return modbusreg.SunSpecChainSnapshot{}, true, nil
		}
		for _, request := range requests {
			logicalID++
			source, err := producer.read(ctx, identity, request, logicalID)
			if err != nil {
				return modbusreg.SunSpecChainSnapshot{}, false, err
			}
			snapshot, err := source.snapshot()
			if err != nil {
				return modbusreg.SunSpecChainSnapshot{}, true, nil
			}
			if mixGeneration && readIndex == 1 {
				record := snapshot.Record()
				record.TransportGeneration++
				snapshot, err = modbusreg.NewLogicalViewSnapshot(record)
				if err != nil {
					return modbusreg.SunSpecChainSnapshot{}, true, nil
				}
			}
			readIndex++
			completed, err := chain.AdmitReplay(request, snapshot)
			if err != nil {
				return modbusreg.SunSpecChainSnapshot{}, true, nil
			}
			if len(completed.RawWords()) != 0 {
				return completed, false, nil
			}
		}
	}
}

func (producer *SunSpecProducer) read(ctx context.Context, identity SunSpecPollIdentity, request modbusreg.SunSpecReadRequest, logicalID uint64) (sunSpecSourceView, error) {
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

func (producer *SunSpecProducer) classify(identity SunSpecPollIdentity, snapshot modbusreg.SunSpecChainSnapshot) SunSpecQualificationResult {
	capability := producer.registry.EvaluateThreePhaseMonitoring(snapshot)
	result := SunSpecQualificationResult{
		CapabilityID: capability.ProfileID(), CapabilityReason: capability.Reason(),
		FlavorID: modbusreg.SunSpecFroniusObservedFlavorID, Chain: snapshot,
	}
	if !capability.Admitted() {
		if capability.Reason() == modbusreg.SunSpecCapabilityReasonInvalidChain {
			result.Outcome = SunSpecQualificationStop
		} else {
			result.Outcome = SunSpecQualificationNoGo
		}
		return result
	}

	flavor := producer.registry.EvaluateFroniusObservedFlavor(snapshot)
	result.FlavorID, result.FlavorReason = flavor.FlavorID(), flavor.Reason()
	if flavor.Matched() {
		result.Outcome = SunSpecQualificationGO
		result.ObservationCount = 1
		result.SampleID = fmt.Sprintf("sunspec-%d-%d", identity.PollGeneration, identity.DeadlineIdentity)
		return result
	}
	switch flavor.Reason() {
	case modbusreg.SunSpecFroniusFlavorReasonCommonIdentityMismatch,
		modbusreg.SunSpecFroniusFlavorReasonFirmwareMismatch,
		modbusreg.SunSpecFroniusFlavorReasonChainMismatch:
		result.Outcome = SunSpecQualificationNoGo
	default:
		result.Outcome = SunSpecQualificationStop
	}
	return result
}
