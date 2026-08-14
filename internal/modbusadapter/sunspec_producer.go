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

const sunSpecProfileID = "sunspec.phase1"

type SunSpecQualificationOutcome string

const (
	SunSpecQualificationSupported          SunSpecQualificationOutcome = "supported"
	SunSpecQualificationUnsupportedProfile SunSpecQualificationOutcome = "unsupported_profile"
	SunSpecQualificationIncoherentCapture  SunSpecQualificationOutcome = "incoherent_capture"
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
	Outcome            SunSpecQualificationOutcome
	UnsupportedProfile string
	ObservationCount   int
	SampleID           string
	Chain              modbusreg.SunSpecPhaseOneChain
}

// SunSpecProducer composes the transport-owned adapter with the registry-owned
// SunSpec phase-one contracts. It has no write or freshness policy surface.
type SunSpecProducer struct {
	adapter *Adapter
	config  SunSpecProducerConfig
	decoder modbusreg.SunSpecPhaseOneDecoder
	factory *modbusreg.ObservationFactory
	mu      sync.Mutex
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
	profile, err := modbusreg.NewSunSpecPhaseOneProfile(modbusreg.SunSpecPhaseOneVersions{
		Profile: modbusreg.CurrentSchemaVersion(), Codec: modbusreg.CurrentCodecContractVersion(),
	})
	if err != nil {
		return nil, fmt.Errorf("create SunSpec phase-one profile: %w", err)
	}
	decoder, err := modbusreg.NewSunSpecPhaseOneDecoder(profile)
	if err != nil {
		return nil, fmt.Errorf("create SunSpec phase-one decoder: %w", err)
	}
	initial, err := modbusreg.EmptySampleLedgerState("gateway-modbus", profile)
	if err != nil {
		return nil, fmt.Errorf("create SunSpec sample ledger state: %w", err)
	}
	ledger, err := modbusreg.NewSampleLedger(initial, 0)
	if err != nil {
		return nil, fmt.Errorf("create SunSpec sample ledger: %w", err)
	}
	factory, err := modbusreg.NewObservationFactory(profile, ledger, &inMemoryPublicationCommitter{state: initial})
	if err != nil {
		return nil, fmt.Errorf("create SunSpec observation factory: %w", err)
	}
	return &SunSpecProducer{adapter: adapter, config: config, decoder: decoder, factory: factory}, nil
}

func (producer *SunSpecProducer) Qualify(ctx context.Context, identity SunSpecPollIdentity) (SunSpecQualificationResult, error) {
	if producer == nil || ctx == nil || identity.PollGeneration == 0 || identity.DeadlineIdentity == 0 {
		return SunSpecQualificationResult{}, errors.New("SunSpec qualification request is incomplete")
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()
	views, err := producer.acquire(ctx, identity)
	if err != nil {
		return SunSpecQualificationResult{}, err
	}
	return producer.qualifyCapture(ctx, identity, views)
}

func (producer *SunSpecProducer) acquire(ctx context.Context, identity SunSpecPollIdentity) ([]sunSpecSourceView, error) {
	read := func(offset, count uint16, logicalID uint64) (sunSpecSourceView, error) {
		request, err := modbus.NewReadRegistersRequest(modbus.FunctionReadHoldingRegisters, offset, count)
		if err != nil {
			return sunSpecSourceView{}, err
		}
		batch, err := producer.adapter.ExecuteRead(ctx, ReadPlan{UnitID: producer.config.UnitID, AuthorizationScope: producer.config.AuthorizationScope, PollGeneration: identity.PollGeneration, DeadlineIdentity: identity.DeadlineIdentity, Timeout: producer.config.ReadTimeout, Reads: []modbus.TCPLogicalRead{{LogicalViewID: logicalID, Request: request}}})
		if err != nil {
			return sunSpecSourceView{}, err
		}
		if len(batch.Views) != 1 || len(batch.Views[0].Words()) != int(count) {
			return sunSpecSourceView{}, errors.New("SunSpec read did not return its exact logical view")
		}
		for _, response := range batch.Responses {
			if response.WireResponseID() == batch.Views[0].WireResponseID() {
				return sunSpecSourceView{view: batch.Views[0], wireBytes: response.Bytes()}, nil
			}
		}
		return sunSpecSourceView{}, errors.New("SunSpec read did not retain wire evidence")
	}
	first, err := read(40000, 1, 1)
	if err != nil {
		return nil, err
	}
	views := []sunSpecSourceView{first}
	offset, nextID := uint16(40001), uint64(2)
	// The remaining signature word and first header establish the dynamic walk.
	head, err := read(offset, 3, nextID)
	if err != nil {
		return nil, err
	}
	views = append(views, head)
	offset += 3
	nextID++
	words := append(append([]uint16(nil), first.view.Words()...), head.view.Words()...)
	if len(words) < 4 || words[0] != 0x5375 || words[1] != 0x6e53 {
		return views, nil
	}
	for {
		modelID, length := words[len(words)-2], words[len(words)-1]
		if modelID == 0xffff {
			break
		}
		// The dynamic budget includes the following header: otherwise a model
		// payload can reach the bound and then over-read its terminator.
		if length == 0 || len(words)+int(length)+2 > modbusreg.MaxSunSpecPhaseOneChainWords {
			break
		}
		for remaining := length; remaining > 0; {
			count := remaining
			if count > 125 {
				count = 125
			}
			view, err := read(offset, count, nextID)
			if err != nil {
				return nil, err
			}
			views, words = append(views, view), append(words, view.view.Words()...)
			offset += count
			nextID++
			remaining -= count
		}
		header, err := read(offset, 2, nextID)
		if err != nil {
			return nil, err
		}
		views, words = append(views, header), append(words, header.view.Words()...)
		offset += 2
		nextID++
	}
	return views, nil
}

// qualifyCapture is deliberately internal: tests can feed exact source views
// without adding a production-only test hook to the public producer surface.
func (producer *SunSpecProducer) qualifyCapture(ctx context.Context, identity SunSpecPollIdentity, views []sunSpecSourceView) (SunSpecQualificationResult, error) {
	capture, raw, err := sunSpecCapture(views)
	if err != nil {
		return SunSpecQualificationResult{Outcome: SunSpecQualificationIncoherentCapture}, nil
	}
	if !completeSunSpecChain(raw) {
		return SunSpecQualificationResult{Outcome: SunSpecQualificationIncoherentCapture}, nil
	}
	if deferredSunSpecModelInRaw(raw) {
		return SunSpecQualificationResult{Outcome: SunSpecQualificationUnsupportedProfile, UnsupportedProfile: sunSpecProfileID}, nil
	}
	chain, err := producer.decoder.Parse(raw)
	if err != nil {
		return SunSpecQualificationResult{Outcome: SunSpecQualificationIncoherentCapture}, nil
	}
	now := time.Now().UTC()
	attempt, err := producer.factory.BeginRuntimeAttempt(modbusreg.RuntimeAttemptRequest{Source: producer.adapter.RuntimeAcquisitionSource(), AttemptKey: fmt.Sprintf("sunspec-%d", identity.PollGeneration), Identity: modbusreg.AttemptIdentity{PollGenerationID: identity.PollGeneration}, Observation: modbusreg.RuntimeObservationFacts{SourceValidity: modbusreg.SourceValid, SourceTime: modbusreg.SourceTimeUnavailable(), LocalReceiptTime: now, LocalReceiptTimePresent: true}, Dependencies: []modbusreg.RuntimeDependencyFacts{{SourceTime: modbusreg.SourceTimeUnavailable()}}, Diagnostics: []string{"sunspec_detection:standard_chain"}})
	if err != nil {
		return SunSpecQualificationResult{}, fmt.Errorf("begin SunSpec observation: %w", err)
	}
	published := false
	defer func() {
		if !published {
			_, _ = attempt.Cancel()
		}
	}()
	normalization, err := producer.adapter.RuntimeAcquisitionSource().ParseNormalizationRecord([]byte(`{"schema_version":1,"source_kind":"runtime","source_evidence_id":"urn:helianthus:evidence:sunspec-phase-one-v1","documentary_notation":"40001","documentary_address":40001,"documentary_address_base":"one_based_register","function_code":3,"logical_table":"holding_registers","normalized_zero_based_pdu_offset":40000,"word_count":1}`))
	if err != nil {
		return SunSpecQualificationResult{}, err
	}
	if err := attempt.Issue(0, views[0].view, normalization); err != nil {
		return SunSpecQualificationResult{}, err
	}
	if err := attempt.Admit(); err != nil {
		return SunSpecQualificationResult{}, err
	}
	if outcome, err := attempt.Claim(0); err != nil || outcome != modbusreg.ClaimSucceeded {
		return SunSpecQualificationResult{}, fmt.Errorf("claim SunSpec observation: %w", err)
	}
	if err := attempt.Seal(); err != nil {
		return SunSpecQualificationResult{}, err
	}
	observation, err := attempt.Publish(ctx)
	if err != nil {
		return SunSpecQualificationResult{}, fmt.Errorf("publish SunSpec observation: %w", err)
	}
	// The registry-owned runtime observation preserves the original base-view
	// wire evidence. Reuse that exact snapshot for activation's dependency bind.
	capture.SourceViews[0] = observation.Spec().Dependencies[0].View
	activated, err := producer.decoder.Activate(modbusreg.SunSpecPhaseOneActivation{Chain: chain, RawWords: raw, Capture: capture, Observation: observation.Spec()})
	if err != nil {
		return SunSpecQualificationResult{}, fmt.Errorf("activate SunSpec capture: %w", err)
	}
	if err := producer.adapter.RecordProfileObservation(ProfileObservationRecord{Observation: observation, DetectionEvidence: []string{"sunspec_detection:standard_chain"}, ActivationEvidence: []string{"sunspec_activation:coherent_chain"}}); err != nil {
		return SunSpecQualificationResult{}, err
	}
	published = true
	_ = activated
	return SunSpecQualificationResult{Outcome: SunSpecQualificationSupported, ObservationCount: 1, SampleID: observation.Spec().SampleID, Chain: chain}, nil
}

func sunSpecCapture(views []sunSpecSourceView) (modbusreg.SunSpecPhaseOneCapture, []uint16, error) {
	if len(views) == 0 {
		return modbusreg.SunSpecPhaseOneCapture{}, nil, errors.New("SunSpec capture is empty")
	}
	snapshots := make([]modbusreg.LogicalViewSnapshot, 0, len(views))
	for _, sourceView := range views {
		view, provenance := sourceView.view, sourceView.view.Provenance()
		snapshot, err := modbusreg.NewLogicalViewSnapshot(modbusreg.LogicalViewRecord{
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
			Words: view.Words(), WireResponseBytes: sourceView.wireBytes,
		})
		if err != nil {
			return modbusreg.SunSpecPhaseOneCapture{}, nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return sunSpecCaptureSnapshots(snapshots)
}

func sunSpecCaptureSnapshots(snapshots []modbusreg.LogicalViewSnapshot) (modbusreg.SunSpecPhaseOneCapture, []uint16, error) {
	capture := modbusreg.SunSpecPhaseOneCapture{SourceViews: append([]modbusreg.LogicalViewSnapshot(nil), snapshots...)}
	raw := make([]uint16, 0, modbusreg.MaxSunSpecPhaseOneChainWords)
	for _, snapshot := range snapshots {
		raw = append(raw, snapshot.Record().Words...)
	}
	// Activate repeats this validation at the profile boundary; running it here
	// rejects detached generations before a factory attempt can be retained.
	if _, err := producerCaptureValidator(capture, raw); err != nil {
		return modbusreg.SunSpecPhaseOneCapture{}, nil, err
	}
	return capture, raw, nil
}

func producerCaptureValidator(capture modbusreg.SunSpecPhaseOneCapture, raw []uint16) ([]uint16, error) {
	if len(capture.SourceViews) == 0 || len(raw) > modbusreg.MaxSunSpecPhaseOneChainWords {
		return nil, errors.New("SunSpec capture exceeds bounds")
	}
	first := capture.SourceViews[0].Record()
	offset := uint32(40000)
	for _, snapshot := range capture.SourceViews {
		record := snapshot.Record()
		if uint32(record.LogicalOffset) != offset || record.Endpoint != first.Endpoint ||
			record.UnitID != first.UnitID || record.PollGeneration != first.PollGeneration ||
			record.Transport != first.Transport || record.TransportGeneration != first.TransportGeneration ||
			record.ConnectionID != first.ConnectionID || record.AuthorizationScope != first.AuthorizationScope ||
			record.RequestedFunction != modbusreg.FunctionReadHoldingRegisters ||
			record.ReceivedFunction != modbusreg.FunctionReadHoldingRegisters {
			return nil, errors.New("SunSpec source views are detached or incoherent")
		}
		offset += uint32(record.LogicalWordCount)
	}
	return raw, nil
}

func deferredSunSpecModelInRaw(words []uint16) bool {
	for offset := 2; offset+1 < len(words); {
		id, length := words[offset], words[offset+1]
		if (id >= 111 && id <= 113) || (id >= 120 && id <= 124) || id == 160 || (id >= 200 && id <= 219) || (id >= 700 && id <= 799) {
			return true
		}
		if id == 0xffff || length == 0 || offset+2+int(length) > len(words) {
			return false
		}
		offset += 2 + int(length)
	}
	return false
}

func completeSunSpecChain(words []uint16) bool {
	if len(words) < 4 || words[0] != 0x5375 || words[1] != 0x6e53 {
		return false
	}
	for offset := 2; offset+1 < len(words); {
		id, length := words[offset], words[offset+1]
		if id == 0xffff {
			return length == 0 && offset+2 == len(words)
		}
		if length == 0 || offset+2+int(length) > len(words) {
			return false
		}
		offset += 2 + int(length)
	}
	return false
}

type inMemoryPublicationCommitter struct {
	mu      sync.Mutex
	state   modbusreg.SampleLedgerState
	restart modbusreg.LedgerRestartState
}

func (c *inMemoryPublicationCommitter) CommitPublication(ctx context.Context, request modbusreg.PublicationCommitRequest) (modbusreg.PublicationCommitDecision, error) {
	if err := ctx.Err(); err != nil {
		return modbusreg.PublicationCommitCancelled, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != request.ExpectedState {
		return "", errors.New("volatile publication state conflict")
	}
	c.state, c.restart = request.PublishedState, request.PublishedRestartState
	return modbusreg.PublicationCommitCommitted, nil
}
func (c *inMemoryPublicationCommitter) CommitTerminalState(ctx context.Context, request modbusreg.TerminalStateCommitRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.restart = request.TerminalRestartState
	return nil
}
