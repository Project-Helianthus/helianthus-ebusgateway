package mcp

// This file is deliberately an injected-record semantic boundary.  It does
// not know how to open a Tesla endpoint, build a FC100 request, or execute an
// operation.  The native current-limit tool remains the only native surface.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const SemanticV1EVSECurrentGetTool = "semantic.v1.evse.current.get"

var ErrTeslaGen3EVSESemanticUnavailable = errors.New("tesla Gen3 EVSE semantic publication unavailable")

// TeslaGen3EVSESemanticConfig deliberately makes every identity and lifecycle
// axis explicit.  Native payloads cannot manufacture an asset identity.
type TeslaGen3EVSESemanticConfig struct {
	AssetID, SourceID, EVSEID, ConnectorID, SourceEpoch, ClockEpoch string
	DriverGeneration                                                uint64
}

// TeslaGen3EVSESemanticEvidence is supplied by the injection owner.  It is
// metadata only: payloads stay in the accepted registry records.
type TeslaGen3EVSESemanticEvidence struct {
	ObservationID string
	ObservedAt    time.Time
	EvaluatedAt   time.Time
	MonotonicNS   int64
	Sequence      uint64
}

// TeslaGen3EVSESemanticProvider is read-only.  It provides one detached,
// evaluated SemReg view and has no operation or transport method.
type TeslaGen3EVSESemanticProvider interface {
	TeslaGen3EVSESemanticCurrent(context.Context) (any, error)
}

type TeslaGen3EVSESemanticPublication struct {
	mu                 sync.RWMutex
	cfg                TeslaGen3EVSESemanticConfig
	kernel             *semreg.PublicationKernel
	current            semreg.Snapshot
	manifest           projection.ProjectionManifest
	requested          []projection.RequestedItem
	dispositions       []projection.ProjectionDisposition
	evaluatedAt        time.Time
	evaluatedMonotonic semreg.MonotonicPoint
	lastReadAt         time.Time
	lastReadMonotonic  semreg.MonotonicPoint
	allocatedExpiresAt *time.Time
	sequence           uint64
	now                func() time.Time
}

func NewTeslaGen3EVSESemanticPublication(cfg TeslaGen3EVSESemanticConfig) (*TeslaGen3EVSESemanticPublication, error) {
	for _, value := range []string{cfg.AssetID, cfg.SourceID, cfg.EVSEID, cfg.ConnectorID, cfg.SourceEpoch, cfg.ClockEpoch} {
		if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
			return nil, errors.New("tesla Gen3 EVSE semantic identity is incomplete")
		}
	}
	if cfg.AssetID == cfg.SourceID || semreg.AssetID(cfg.AssetID).Validate() != nil || semreg.SourceID(cfg.SourceID).Validate() != nil || cfg.DriverGeneration == 0 {
		return nil, errors.New("tesla Gen3 EVSE semantic identity is invalid")
	}
	kernel, err := semreg.NewPublicationKernel(semreg.AssetID(cfg.AssetID), evse.New())
	if err != nil {
		return nil, err
	}
	return &TeslaGen3EVSESemanticPublication{cfg: cfg, kernel: kernel, now: time.Now}, nil
}

// Publish atomically maps one accepted WC3 24.44.3 record bundle.  A rejected
// bundle never advances the kernel or replaces the retained public snapshot.
func (p *TeslaGen3EVSESemanticPublication) Publish(source TeslaGen3EVSECurrentLimitV1Source, evidence TeslaGen3EVSESemanticEvidence) error {
	if p == nil || p.kernel == nil {
		return ErrTeslaGen3EVSESemanticUnavailable
	}
	if err := p.validate(source, evidence); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if evidence.Sequence <= p.sequence {
		return errors.New("tesla Gen3 EVSE semantic replay or collision")
	}
	receiptMono := semreg.MonotonicPoint{ClockEpochID: semreg.ClockEpochID(p.cfg.ClockEpoch), Nanoseconds: semreg.Uint64(strconv.FormatInt(evidence.MonotonicNS, 10))}
	evaluationMono, err := teslaGen3EVSEEvaluationMonotonic(receiptMono, evidence)
	if err != nil {
		return err
	}
	staged, err := p.kernel.Fork()
	if err != nil {
		return err
	}
	_, _, exists := staged.Current()
	batch, manifest, requested, dispositions, err := p.batch(source, evidence, p.current, exists, receiptMono, evaluationMono)
	if err != nil {
		return err
	}
	snapshot, _, err := staged.Apply(batch, evaluationMono)
	if err != nil {
		return err
	}
	expiresAt := teslaGen3EVSEAllocatedExpiry(source.Provisional, evidence)
	_, err = p.publicAt(snapshot, manifest, requested, dispositions, evidence.EvaluatedAt, evaluationMono, expiresAt)
	if err != nil {
		return err
	}
	p.kernel, p.current, p.manifest, p.requested, p.dispositions = staged, snapshot, manifest, append([]projection.RequestedItem(nil), requested...), append([]projection.ProjectionDisposition(nil), dispositions...)
	p.evaluatedAt, p.evaluatedMonotonic, p.lastReadAt, p.lastReadMonotonic, p.allocatedExpiresAt, p.sequence = evidence.EvaluatedAt, evaluationMono, evidence.EvaluatedAt, evaluationMono, expiresAt, evidence.Sequence
	return nil
}

func (p *TeslaGen3EVSESemanticPublication) TeslaGen3EVSESemanticCurrent(context.Context) (any, error) {
	if p == nil {
		return nil, ErrTeslaGen3EVSESemanticUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sequence == 0 {
		return nil, ErrTeslaGen3EVSESemanticUnavailable
	}
	now := p.now()
	if now.Before(p.evaluatedAt) {
		// Wall-clock corrections cannot make a retained publication appear to
		// predate its own receipt.  Keep the sealed evaluation point until the
		// injected clock advances again; monotonic progress remains authoritative.
		now = p.evaluatedAt
	}
	if now.Before(p.lastReadAt) {
		// An external retained-record owner can provide wall-only timestamps.
		// Once a read has exposed an age, a wall-clock rollback must not make
		// the same provisional allocation younger without new native evidence.
		now = p.lastReadAt
	}
	mono, err := p.readMonotonic(now)
	if err != nil {
		return nil, err
	}
	mono, err = teslaGen3EVSEAtLeastMonotonic(mono, p.lastReadMonotonic)
	if err != nil {
		return nil, err
	}
	// Re-evaluation applies the SemReg lifecycle to the immutable snapshot.
	// It performs no provider call and does not fabricate a replacement batch.
	view, err := p.publicAt(p.current, p.manifest, p.requested, p.dispositions, now, mono, p.allocatedExpiresAt)
	if err != nil {
		return nil, err
	}
	p.lastReadAt, p.lastReadMonotonic = now, mono
	return view, nil
}

func (p *TeslaGen3EVSESemanticPublication) validate(s TeslaGen3EVSECurrentLimitV1Source, e TeslaGen3EVSESemanticEvidence) error {
	if e.ObservationID == "" || e.ObservedAt.IsZero() || e.EvaluatedAt.IsZero() || e.EvaluatedAt.Before(e.ObservedAt) || e.MonotonicNS < 0 || e.Sequence == 0 {
		return errors.New("tesla Gen3 EVSE semantic lifecycle is invalid")
	}
	if s.Persistent == nil || s.Persistent.OperationVersion() != modbusreg.TeslaGen3CurrentLimitOperationVersion24443 || len(s.Persistent.RequestPayload()) == 0 || len(s.Persistent.TerminalPayload()) == 0 {
		return errors.New("tesla Gen3 EVSE persistent evidence is invalid")
	}
	return nil
}

func (p *TeslaGen3EVSESemanticPublication) batch(s TeslaGen3EVSECurrentLimitV1Source, e TeslaGen3EVSESemanticEvidence, current semreg.Snapshot, exists bool, receiptMono, evaluationMono semreg.MonotonicPoint) (semreg.PublicationBatch, projection.ProjectionManifest, []projection.RequestedItem, []projection.ProjectionDisposition, error) {
	asset, source := semreg.AssetID(p.cfg.AssetID), semreg.SourceID(p.cfg.SourceID)
	epoch, generation := semreg.SourceEpochID(p.cfg.SourceEpoch), semreg.Uint64(strconv.FormatUint(p.cfg.DriverGeneration, 10))
	seq := semreg.Uint64(strconv.FormatUint(e.Sequence, 10))
	binding := semreg.NativeBindingID("binding:tesla-wc3:" + evseHash(p.cfg.AssetID, p.cfg.SourceID)[:32])
	receivedAt, evaluatedAt := evseWall(e.ObservedAt), evseWall(e.EvaluatedAt)
	evidence := evseEvidence("native.tesla.wc3.current_limit", struct {
		Source   TeslaGen3EVSECurrentLimitV1Source
		Evidence TeslaGen3EVSESemanticEvidence
	}{s, e})
	registry := evseDigestEvidence("registry.tesla.wc3_24_44_3", modbusreg.TeslaGen3CurrentLimitOperationVersion24443)
	// MappingRevision is the kernel's monotonic label; the immutable accepted
	// docs commit is retained in the registry evidence digest below.
	manifest := projection.ProjectionManifest{TargetID: "target:gateway-semantic-evse", TargetVersion: "1.0.0", KernelVersion: semreg.ContractKernelV1, PackVersions: []semreg.PackRef{{ID: "helianthus.pack.evse", Version: "1.0.0"}}, MappingRevision: "1"}
	configured := p.candidate("evse.limit.configured_current", "evse.dimension.evse", p.cfg.EVSEID, s.Persistent.MaxOutputCurrentAmps(), 60*time.Second, 300*time.Second, current, binding, source, epoch, generation, evidence, receivedAt, receiptMono, evaluatedAt, evaluationMono)
	requested := []projection.RequestedItem{{Kind: projection.ItemFact, ItemID: "evse.limit.configured_current"}, {Kind: projection.ItemFact, ItemID: "evse.limit.allocated_current"}}
	dispositions := []projection.ProjectionDisposition{{Kind: projection.ItemFact, ItemID: "evse.limit.configured_current", Outcome: projection.ProjectionExact, SourceKeys: []semreg.FactKey{configured.Key}, Loss: []projection.LossDetail{}}}
	facts := []semreg.FactCandidate{configured}
	if provisional, reason := p.provisional(s.Provisional, e); provisional != nil {
		timeout := time.Duration(s.Provisional.LimitTimeoutSeconds()) * time.Second
		allocated := p.candidate("evse.limit.allocated_current", "evse.dimension.connector", p.cfg.ConnectorID, *provisional, timeout, timeout+time.Nanosecond, current, binding, source, epoch, generation, evidence, receivedAt, receiptMono, evaluatedAt, evaluationMono)
		facts = append(facts, allocated)
		dispositions = append(dispositions, projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: "evse.limit.allocated_current", Outcome: projection.ProjectionExact, SourceKeys: []semreg.FactKey{allocated.Key}, Loss: []projection.LossDetail{}})
	} else {
		r := semreg.DefinitionID("withheld_provisional_" + reason)
		dispositions = append(dispositions, projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: "evse.limit.allocated_current", Outcome: projection.ProjectionWithheld, Reason: &r, SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{{Kind: projection.LossPolicy, SourceItems: []semreg.DefinitionID{"native.tesla.wc3.provisional_current_limit"}, Description: "allocated current withheld: " + reason}}})
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].CandidateID < facts[j].CandidateID })
	sort.Slice(requested, func(i, j int) bool { return requested[i].ItemID < requested[j].ItemID })
	sort.Slice(dispositions, func(i, j int) bool { return dispositions[i].ItemID < dispositions[j].ItemID })
	expected := semreg.Uint64("0")
	if exists {
		expected = current.Revisions.Semantic
	}
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:tesla-wc3:" + evseHash(e.ObservationID)[:32]), AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: seq, ExpectedSemanticRevision: expected, ObservedAt: receivedAt,
		SourceUpserts:       []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "modbus_fc100", ProfileID: "tesla.wc3.24_44_3.current_limit", ProfileVersion: "24.44.3", RegistryEvidence: registry, StartedAt: receivedAt, State: semreg.SourceCurrent, Revision: seq}},
		BindingUpserts:      []semreg.NativeBinding{{BindingID: binding, AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: registry, State: semreg.BindingCurrent, Revision: seq}},
		IdentityLinkUpserts: []semreg.IdentityLink{{AssetID: asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: seq}}, FactUpserts: facts,
		ServiceUpserts:    []semreg.ServiceInstance{p.service("evse.service.evse", p.cfg.EVSEID, binding, asset, epoch, generation, seq), p.service("evse.service.connector", p.cfg.ConnectorID, binding, asset, epoch, generation, seq)},
		CapabilityUpserts: []semreg.CapabilityInstance{p.capability("evse.capability.read.evse", "evse.service.evse", binding, asset, epoch, generation, seq), p.capability("evse.capability.read.connector", "evse.service.connector", binding, asset, epoch, generation, seq)},
		SourceRetirements: []semreg.SourceEpochID{}, FactWithdrawals: []semreg.CandidateID{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	sort.Slice(batch.ServiceUpserts, func(i, j int) bool { return batch.ServiceUpserts[i].InstanceID < batch.ServiceUpserts[j].InstanceID })
	sort.Slice(batch.CapabilityUpserts, func(i, j int) bool {
		return batch.CapabilityUpserts[i].InstanceID < batch.CapabilityUpserts[j].InstanceID
	})
	if exists {
		batch.SourceUpserts, batch.BindingUpserts, batch.IdentityLinkUpserts, batch.ServiceUpserts, batch.CapabilityUpserts = []semreg.SourceDescriptor{}, []semreg.NativeBinding{}, []semreg.IdentityLink{}, []semreg.ServiceInstance{}, []semreg.CapabilityInstance{}
	}
	digest, err := batch.ComputedDigest()
	if err != nil {
		return semreg.PublicationBatch{}, manifest, nil, nil, err
	}
	batch.BatchDigest = digest
	if err := batch.Validate(); err != nil {
		return semreg.PublicationBatch{}, manifest, nil, nil, fmt.Errorf("tesla Gen3 EVSE semantic batch: %w", err)
	}
	return batch, manifest, requested, dispositions, nil
}

func (p *TeslaGen3EVSESemanticPublication) provisional(v *modbusreg.TeslaGen3ProvisionalCurrentLimit, e TeslaGen3EVSESemanticEvidence) (*uint32, string) {
	if v == nil {
		return nil, "missing"
	}
	if v.OperationVersion() != modbusreg.TeslaGen3CurrentLimitOperationVersion24443 || len(v.SetRequestPayload()) == 0 || len(v.AckPayload()) == 0 || len(v.ReadbackRequestPayload()) == 0 || len(v.ReadbackTerminalPayload()) == 0 {
		return nil, "malformed_or_correlation_mismatched"
	}
	if v.InhibitCharging() {
		return nil, "inhibited"
	}
	timeout := v.LimitTimeoutSeconds()
	if timeout == 0 {
		return nil, "zero_timeout"
	}
	if timeout > 86399 {
		return nil, "timeout_out_of_range"
	}
	if !e.EvaluatedAt.Before(e.ObservedAt.Add(time.Duration(timeout) * time.Second)) {
		return nil, "expired"
	}
	value := v.LimitCurrentMaxAmps()
	return &value, ""
}

func (p *TeslaGen3EVSESemanticPublication) candidate(id, dimension, value string, amps uint32, freshFor, retainFor time.Duration, current semreg.Snapshot, binding semreg.NativeBindingID, source semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64, evidence semreg.EvidenceRef, receivedAt semreg.TimePoint, receiptMono semreg.MonotonicPoint, evaluatedAt semreg.TimePoint, evaluationMono semreg.MonotonicPoint) semreg.FactCandidate {
	key := semreg.FactKey{PackID: "helianthus.pack.evse", PackVersion: "1.0.0", FactID: semreg.DefinitionID(id), Dimensions: []semreg.Dimension{{ID: semreg.DefinitionID(dimension), Value: semreg.Value{Kind: semreg.ValueText, Text: &value}}}}
	v := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: strconv.FormatUint(uint64(amps), 10), Exponent10: 0}, Unit: "unit.ampere"}}
	h := evseHash(p.cfg.AssetID, string(binding), id)
	candidateID := semreg.CandidateID("candidate:tesla-wc3:" + h[:32])
	return semreg.FactCandidate{CandidateID: candidateID, Key: key, Value: &v, Quality: semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}}, Times: semreg.Times{ReceivedAt: receivedAt, ReceiptMonotonic: receiptMono, EvaluatedAt: evaluatedAt, EvaluateMonotonic: evaluationMono}, FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:tesla-wc3-native-receipt", Version: "1.0.0", FreshForNS: semreg.Uint64(strconv.FormatInt(freshFor.Nanoseconds(), 10)), RetainForNS: semreg.Uint64(strconv.FormatInt(retainFor.Nanoseconds(), 10)), MaxWallUncertaintyNS: "0"}, BindingID: &binding, SourceEpochID: &epoch, DriverGeneration: &generation, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:tesla-wc3:" + h[:32]), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}}, Evidence: []semreg.EvidenceRef{evidence}, Revision: nextTeslaGen3EVSECandidateRevision(current, candidateID)}
}

func (p *TeslaGen3EVSESemanticPublication) service(id, dimension string, binding semreg.NativeBindingID, asset semreg.AssetID, epoch semreg.SourceEpochID, generation, revision semreg.Uint64) semreg.ServiceInstance {
	return semreg.ServiceInstance{InstanceID: semreg.ServiceInstanceID("service:tesla-wc3:" + evseHash(p.cfg.AssetID, id)[:32]), AssetID: asset, Definition: semreg.DefinitionRef{Pack: semreg.PackRef{ID: "helianthus.pack.evse", Version: "1.0.0"}, ID: semreg.DefinitionID(id), Version: "1.0.0"}, BindingID: binding, SourceEpochID: epoch, DriverGeneration: generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: revision}
}
func (p *TeslaGen3EVSESemanticPublication) capability(id, service string, binding semreg.NativeBindingID, asset semreg.AssetID, epoch semreg.SourceEpochID, generation, revision semreg.Uint64) semreg.CapabilityInstance {
	activation := evseDigestEvidence("native.tesla.wc3.current_limit.activation", p.cfg.SourceID)
	return semreg.CapabilityInstance{InstanceID: semreg.CapabilityInstanceID("capability:tesla-wc3:" + evseHash(p.cfg.AssetID, id)[:32]), AssetID: asset, ServiceInstance: semreg.ServiceInstanceID("service:tesla-wc3:" + evseHash(p.cfg.AssetID, service)[:32]), Definition: semreg.DefinitionRef{Pack: semreg.PackRef{ID: "helianthus.pack.evse", Version: "1.0.0"}, ID: semreg.DefinitionID(id), Version: "1.0.0"}, BindingID: binding, SourceEpochID: epoch, DriverGeneration: generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Constraints: []semreg.TypedField{}, ActivationEvidence: []semreg.EvidenceRef{activation}, Revision: revision}
}

func (p *TeslaGen3EVSESemanticPublication) publicAt(snapshot semreg.Snapshot, manifest projection.ProjectionManifest, requested []projection.RequestedItem, dispositions []projection.ProjectionDisposition, evaluated time.Time, mono semreg.MonotonicPoint, allocatedExpiresAt *time.Time) (json.RawMessage, error) {
	evaluation, err := semreg.EvaluateSnapshot(snapshot, semreg.EvaluationContext{EvaluatedAt: evseWall(evaluated), EvaluateMonotonic: mono})
	if err != nil {
		return nil, err
	}
	publicDispositions := append([]projection.ProjectionDisposition(nil), dispositions...)
	if allocatedExpiresAt != nil && !evaluated.Before(*allocatedExpiresAt) {
		for index := range publicDispositions {
			if publicDispositions[index].ItemID != "evse.limit.allocated_current" {
				continue
			}
			reason := semreg.DefinitionID("withheld_provisional_expired")
			publicDispositions[index] = projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: "evse.limit.allocated_current", Outcome: projection.ProjectionWithheld, Reason: &reason, SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{{Kind: projection.LossPolicy, SourceItems: []semreg.DefinitionID{"native.tesla.wc3.provisional_current_limit"}, Description: "allocated current withheld: expired"}}}
		}
	}
	report, err := projection.Project(snapshot, manifest, requested, publicDispositions, nil)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(map[string]any{"snapshot": snapshot, "evaluation": evaluation, "selections": []semreg.Selection{}, "projection": report})
	return json.RawMessage(b), err
}

func (p *TeslaGen3EVSESemanticPublication) readMonotonic(now time.Time) (semreg.MonotonicPoint, error) {
	if now.Before(p.evaluatedAt) {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE read clock precedes publication")
	}
	base, err := strconv.ParseUint(string(p.evaluatedMonotonic.Nanoseconds), 10, 64)
	if err != nil {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE publication monotonic clock is invalid")
	}
	delta := uint64(now.Sub(p.evaluatedAt).Nanoseconds())
	if base > ^uint64(0)-delta {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE read monotonic clock overflows")
	}
	return semreg.MonotonicPoint{ClockEpochID: p.evaluatedMonotonic.ClockEpochID, Nanoseconds: semreg.Uint64(strconv.FormatUint(base+delta, 10))}, nil
}

func teslaGen3EVSEEvaluationMonotonic(receipt semreg.MonotonicPoint, evidence TeslaGen3EVSESemanticEvidence) (semreg.MonotonicPoint, error) {
	base, err := strconv.ParseUint(string(receipt.Nanoseconds), 10, 64)
	if err != nil {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE receipt monotonic clock is invalid")
	}
	delay := evidence.EvaluatedAt.Sub(evidence.ObservedAt)
	if delay < 0 {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE evaluation precedes observation")
	}
	delta := uint64(delay.Nanoseconds())
	if base > ^uint64(0)-delta {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE evaluation monotonic clock overflows")
	}
	return semreg.MonotonicPoint{ClockEpochID: receipt.ClockEpochID, Nanoseconds: semreg.Uint64(strconv.FormatUint(base+delta, 10))}, nil
}

func teslaGen3EVSEAtLeastMonotonic(candidate, floor semreg.MonotonicPoint) (semreg.MonotonicPoint, error) {
	if floor.ClockEpochID == "" {
		return candidate, nil
	}
	if candidate.ClockEpochID != floor.ClockEpochID {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE read monotonic clock epoch changed")
	}
	candidateNS, err := strconv.ParseUint(string(candidate.Nanoseconds), 10, 64)
	if err != nil {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE read monotonic clock is invalid")
	}
	floorNS, err := strconv.ParseUint(string(floor.Nanoseconds), 10, 64)
	if err != nil {
		return semreg.MonotonicPoint{}, errors.New("tesla Gen3 EVSE retained monotonic clock is invalid")
	}
	if candidateNS < floorNS {
		return floor, nil
	}
	return candidate, nil
}

func teslaGen3EVSEAllocatedExpiry(v *modbusreg.TeslaGen3ProvisionalCurrentLimit, evidence TeslaGen3EVSESemanticEvidence) *time.Time {
	if v == nil || v.OperationVersion() != modbusreg.TeslaGen3CurrentLimitOperationVersion24443 || len(v.SetRequestPayload()) == 0 || len(v.AckPayload()) == 0 || len(v.ReadbackRequestPayload()) == 0 || len(v.ReadbackTerminalPayload()) == 0 || v.InhibitCharging() || v.LimitTimeoutSeconds() == 0 || v.LimitTimeoutSeconds() > 86399 || !evidence.EvaluatedAt.Before(evidence.ObservedAt.Add(time.Duration(v.LimitTimeoutSeconds())*time.Second)) {
		return nil
	}
	expires := evidence.ObservedAt.Add(time.Duration(v.LimitTimeoutSeconds()) * time.Second)
	return &expires
}

func nextTeslaGen3EVSECandidateRevision(snapshot semreg.Snapshot, id semreg.CandidateID) semreg.Uint64 {
	for _, envelope := range snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			if candidate.CandidateID != id {
				continue
			}
			revision, err := strconv.ParseUint(string(candidate.Revision), 10, 64)
			if err != nil || revision == ^uint64(0) {
				return ""
			}
			return semreg.Uint64(strconv.FormatUint(revision+1, 10))
		}
	}
	return "1"
}
func evseWall(t time.Time) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(t.UTC().UnixNano(), 10)), ClockID: "wall.utc", UncertaintyNS: "0"}
}
func evseHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}
func evseEvidence(kind semreg.DefinitionID, v any) semreg.EvidenceRef {
	b, _ := json.Marshal(v)
	return evseDigestEvidence(kind, string(b))
}
func evseDigestEvidence(kind semreg.DefinitionID, v string) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: "helianthus.pack.evse", Kind: kind, Digest: semreg.Digest("sha256:" + evseHash(v)), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

var teslaGen3EVSESemanticProviders = struct {
	sync.RWMutex
	byServer map[*Server]TeslaGen3EVSESemanticProvider
}{byServer: make(map[*Server]TeslaGen3EVSESemanticProvider)}

func registerTeslaGen3EVSESemanticTool(server *Server, provider ModbusV1Provider) {
	p, ok := provider.(TeslaGen3EVSESemanticProvider)
	if !ok || p == nil {
		return
	}
	teslaGen3EVSESemanticProviders.Lock()
	teslaGen3EVSESemanticProviders.byServer[server] = p
	teslaGen3EVSESemanticProviders.Unlock()
	server.tools = append(server.tools, Tool{Name: SemanticV1EVSECurrentGetTool, Description: "Get one evaluated Tesla Gen3 EVSE SemReg current projection.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}})
}
func (server *Server) handleTeslaGen3EVSESemanticCall(ctx context.Context, name string, args map[string]any) (map[string]any, bool) {
	if name != SemanticV1EVSECurrentGetTool {
		return nil, false
	}
	if len(args) != 0 {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, errors.New("invalid Tesla Gen3 EVSE semantic arguments"), false, "EVALUATED_SEMREG_PUBLICATION", "")), true), true
	}
	teslaGen3EVSESemanticProviders.RLock()
	p := teslaGen3EVSESemanticProviders.byServer[server]
	teslaGen3EVSESemanticProviders.RUnlock()
	if p == nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, ErrTeslaGen3EVSESemanticUnavailable, false, "EVALUATED_SEMREG_PUBLICATION", "")), true), true
	}
	d, e := p.TeslaGen3EVSESemanticCurrent(ctx)
	if e != nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, e, true, "EVALUATED_SEMREG_PUBLICATION", "")), true), true
	}
	timestamp, e := growattStorageEvaluatedTimestamp(d)
	if e != nil {
		return callToolResultText(mustJSON(newModbusV1Envelope(nil, e, true, "EVALUATED_SEMREG_PUBLICATION", "")), true), true
	}
	return callToolResultText(mustJSON(newModbusV1Envelope(d, nil, true, "EVALUATED_SEMREG_PUBLICATION", timestamp)), false), true
}

var _ = fmt.Sprintf
