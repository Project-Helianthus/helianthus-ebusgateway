package modbusadapter

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	pvpack "github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

// CanonicalPVShadowMode is intentionally an adapter-local typed setting. It
// has no environment, CLI, persisted, or public-output representation.
type CanonicalPVShadowMode string

const (
	CanonicalPVShadowModeOff    CanonicalPVShadowMode = ""
	CanonicalPVShadowModeSemReg CanonicalPVShadowMode = "semreg"
)

type CanonicalPVShadowConfig struct{ Mode CanonicalPVShadowMode }

func (config CanonicalPVShadowConfig) Validate() error {
	switch config.Mode {
	case CanonicalPVShadowModeOff, CanonicalPVShadowModeSemReg:
		return nil
	default:
		return errors.New("canonical PV shadow mode is invalid")
	}
}

// CanonicalPVSemRegShadowSnapshot is an internal, detached test and later
// integration witness. It is not a SemReg output-selection mechanism.
type CanonicalPVSemRegShadowSnapshot struct {
	Snapshot   semreg.Snapshot
	Canonical  []byte
	Evaluation semreg.EvaluationView
	Projection projection.ProjectionReport
}

type canonicalPVSemRegShadowRecord struct {
	assetRef       string
	semregSnapshot semreg.Snapshot
	canonical      []byte
	evaluation     semreg.EvaluationView
	projection     projection.ProjectionReport
}

func (record canonicalPVSemRegShadowRecord) detachedSnapshot() (CanonicalPVSemRegShadowSnapshot, error) {
	snapshot, err := cloneSemRegSnapshot(record.semregSnapshot)
	if err != nil {
		return CanonicalPVSemRegShadowSnapshot{}, err
	}
	evaluation, err := cloneSemRegEvaluation(record.evaluation)
	if err != nil {
		return CanonicalPVSemRegShadowSnapshot{}, err
	}
	report, err := cloneSemRegProjection(record.projection)
	if err != nil {
		return CanonicalPVSemRegShadowSnapshot{}, err
	}
	return CanonicalPVSemRegShadowSnapshot{
		Snapshot:   snapshot,
		Canonical:  append([]byte(nil), record.canonical...),
		Evaluation: evaluation,
		Projection: report,
	}, nil
}

type canonicalPVSemRegShadow struct {
	byAsset map[string]*canonicalPVSemRegShadowAsset
	records map[string]canonicalPVSemRegShadowRecord
	compare func(pv.Snapshot, semreg.Snapshot, semreg.EvaluationView, projection.ProjectionReport) error
}

type canonicalPVSemRegShadowAsset struct {
	kernel       *semreg.PublicationKernel
	publications []canonicalPVSemRegPublication
	current      canonicalPVSemRegShadowRecord
}

type canonicalPVSemRegPublication struct {
	batch     semreg.PublicationBatch
	monotonic pv.MonotonicNanos
}

func newCanonicalPVSemRegShadow() *canonicalPVSemRegShadow {
	return &canonicalPVSemRegShadow{byAsset: make(map[string]*canonicalPVSemRegShadowAsset), records: make(map[string]canonicalPVSemRegShadowRecord), compare: compareCanonicalPVSemRegShadow}
}

func (shadow *canonicalPVSemRegShadow) close() {
	shadow.byAsset = nil
	shadow.records = nil
}

func (shadow *canonicalPVSemRegShadow) apply(
	observation modbusreg.SunSpecQualificationObservation,
	encoded []byte,
	legacy pv.Snapshot,
	producedAt time.Time,
	monotonic pv.MonotonicNanos,
) (canonicalPVSemRegShadowRecord, error) {
	if shadow == nil || len(encoded) == 0 || legacy.AssetRef == "" || producedAt.IsZero() || monotonic < 0 {
		return canonicalPVSemRegShadowRecord{}, errors.New("canonical PV SemReg shadow input is incomplete")
	}
	asset := shadow.byAsset[legacy.AssetRef]
	if asset == nil && len(shadow.byAsset) >= maxRetainedProfileObservations {
		return canonicalPVSemRegShadowRecord{}, errors.New("canonical PV SemReg shadow retention limit reached")
	}
	history, err := canonicalPVSemRegHistory(asset)
	if err != nil {
		return canonicalPVSemRegShadowRecord{}, err
	}
	// PublicationKernel has no restore API. Replaying the bounded accepted
	// batches into a fresh kernel stages every mutation, so a failed build,
	// Apply, evaluation, projection, or comparator never advances live state.
	staged, err := canonicalPVSemRegStage(legacy.AssetRef, history)
	if err != nil {
		return canonicalPVSemRegShadowRecord{}, err
	}
	batch, err := canonicalPVSemRegBatch(observation, encoded, legacy, producedAt, monotonic, staged)
	if err != nil {
		return canonicalPVSemRegShadowRecord{}, err
	}
	snapshot, canonical, err := staged.Apply(batch, canonicalPVSemRegMonotonic(monotonic))
	if err != nil {
		return canonicalPVSemRegShadowRecord{}, fmt.Errorf("publish SemReg PV shadow: %w", err)
	}
	reencoded, err := semreg.CanonicalJSON(snapshot)
	if err != nil || !bytes.Equal(reencoded, canonical) {
		return canonicalPVSemRegShadowRecord{}, errors.New("SemReg canonical snapshot bytes drifted")
	}
	evaluation, err := semreg.EvaluateSnapshot(snapshot, semreg.EvaluationContext{
		EvaluatedAt:       canonicalPVSemRegWall(producedAt),
		EvaluateMonotonic: canonicalPVSemRegMonotonic(monotonic),
	})
	if err != nil {
		return canonicalPVSemRegShadowRecord{}, fmt.Errorf("evaluate SemReg PV shadow: %w", err)
	}
	report, err := canonicalPVSemRegProjection(snapshot, legacy)
	if err != nil {
		return canonicalPVSemRegShadowRecord{}, err
	}
	if err := shadow.compare(legacy, snapshot, evaluation, report); err != nil {
		return canonicalPVSemRegShadowRecord{}, fmt.Errorf("compare canonical PV SemReg shadow: %w", err)
	}
	clonedBatch, err := cloneSemReg(batch)
	if err != nil {
		return canonicalPVSemRegShadowRecord{}, fmt.Errorf("copy SemReg shadow publication: %w", err)
	}
	if asset == nil {
		asset = &canonicalPVSemRegShadowAsset{}
	}
	record := canonicalPVSemRegShadowRecord{assetRef: legacy.AssetRef, semregSnapshot: snapshot, canonical: canonical, evaluation: evaluation, projection: report}
	asset.kernel = staged
	asset.publications = append(history, canonicalPVSemRegPublication{batch: clonedBatch, monotonic: monotonic})
	asset.current = record
	shadow.byAsset[legacy.AssetRef] = asset
	return record, nil
}

func canonicalPVSemRegHistory(asset *canonicalPVSemRegShadowAsset) ([]canonicalPVSemRegPublication, error) {
	if asset == nil || len(asset.publications) < maxRetainedProfileObservations {
		if asset == nil {
			return nil, nil
		}
		return append([]canonicalPVSemRegPublication(nil), asset.publications...), nil
	}
	// Compact to a seed containing the immutable source/binding identity and the
	// latest complete fact set. The next accepted update continues publication
	// without retaining unbounded replay history.
	first, last := asset.publications[0], asset.publications[len(asset.publications)-1]
	seed, err := cloneSemReg(last.batch)
	if err != nil {
		return nil, err
	}
	seed.BatchID = semreg.BatchID("batch:canonical-pv-shadow:reseed:" + string(last.batch.SourceID))
	seed.Sequence, seed.ExpectedSemanticRevision = "1", "0"
	seed.SourceUpserts = append([]semreg.SourceDescriptor(nil), first.batch.SourceUpserts...)
	seed.BindingUpserts = append([]semreg.NativeBinding(nil), first.batch.BindingUpserts...)
	seed.IdentityLinkUpserts = append([]semreg.IdentityLink(nil), first.batch.IdentityLinkUpserts...)
	for index := range seed.FactUpserts {
		seed.FactUpserts[index].Revision = "1"
	}
	digest, err := seed.ComputedDigest()
	if err != nil {
		return nil, err
	}
	seed.BatchDigest = digest
	return []canonicalPVSemRegPublication{{batch: seed, monotonic: last.monotonic}}, nil
}

func canonicalPVSemRegStage(assetRef string, accepted []canonicalPVSemRegPublication) (*semreg.PublicationKernel, error) {
	kernel, err := semreg.NewPublicationKernel(semreg.AssetID(assetRef), pvpack.New())
	if err != nil {
		return nil, fmt.Errorf("construct staged SemReg PV kernel: %w", err)
	}
	if accepted == nil {
		return kernel, nil
	}
	for _, publication := range accepted {
		batch, err := cloneSemReg(publication.batch)
		if err != nil {
			return nil, fmt.Errorf("copy accepted SemReg shadow publication: %w", err)
		}
		if _, _, err := kernel.Apply(batch, canonicalPVSemRegMonotonic(publication.monotonic)); err != nil {
			return nil, fmt.Errorf("replay accepted SemReg shadow publication: %w", err)
		}
	}
	return kernel, nil
}

type canonicalPVSemRegMapping struct {
	target, dimension, dimensionValue, unit string
	policy                                  string
	symbol                                  bool
}

var canonicalPVSemRegMappings = map[string]canonicalPVSemRegMapping{
	"inverter.ac.power.active":     {"pv.ac.aggregate_active_power", "pv.dimension.inverter", "inverter:fixture", "unit.watt", "pv.telemetry.fast.v1", false},
	"inverter.ac.frequency":        {"pv.ac.frequency", "pv.dimension.inverter", "inverter:fixture", "unit.hertz", "pv.telemetry.fast.v1", false},
	"inverter.ac.energy_lifetime":  {"pv.energy.generated", "pv.dimension.system", "system:fixture", "unit.kilowatt_hour", "pv.accumulator.v1", false},
	"inverter.temperature.cabinet": {"pv.temperature.inverter", "pv.dimension.inverter", "inverter:fixture", "unit.celsius", "pv.telemetry.fast.v1", false},
	"inverter.operating_state":     {"pv.status.operating", "pv.dimension.inverter", "inverter:fixture", "", "pv.status.v1", true},
	"inverter.ac.current.phase_a":  {"pv.ac.current", "pv.dimension.phase", "phase:L1", "unit.ampere", "pv.telemetry.fast.v1", false},
	"inverter.ac.current.phase_b":  {"pv.ac.current", "pv.dimension.phase", "phase:L2", "unit.ampere", "pv.telemetry.fast.v1", false},
	"inverter.ac.current.phase_c":  {"pv.ac.current", "pv.dimension.phase", "phase:L3", "unit.ampere", "pv.telemetry.fast.v1", false},
	"inverter.ac.voltage.phase_a":  {"pv.ac.voltage", "pv.dimension.phase", "phase:L1", "unit.volt", "pv.telemetry.fast.v1", false},
	"inverter.ac.voltage.phase_b":  {"pv.ac.voltage", "pv.dimension.phase", "phase:L2", "unit.volt", "pv.telemetry.fast.v1", false},
	"inverter.ac.voltage.phase_c":  {"pv.ac.voltage", "pv.dimension.phase", "phase:L3", "unit.volt", "pv.telemetry.fast.v1", false},
}

func canonicalPVSemRegMappingsForAsset(assetRef string) (map[string]canonicalPVSemRegMapping, error) {
	id := strings.TrimPrefix(assetRef, "pv-asset-")
	if id == assetRef || len(id) < 16 {
		return nil, errors.New("canonical PV asset identity is invalid")
	}
	mappings := make(map[string]canonicalPVSemRegMapping, len(canonicalPVSemRegMappings))
	for nativeID, mapping := range canonicalPVSemRegMappings {
		switch mapping.dimension {
		case "pv.dimension.inverter":
			mapping.dimensionValue = "inverter:" + id
		case "pv.dimension.system":
			mapping.dimensionValue = "system:" + id
		}
		mappings[nativeID] = mapping
	}
	return mappings, nil
}

func canonicalPVSemRegBatch(observation modbusreg.SunSpecQualificationObservation, encoded []byte, legacy pv.Snapshot, producedAt time.Time, monotonic pv.MonotonicNanos, kernel *semreg.PublicationKernel) (semreg.PublicationBatch, error) {
	if kernel == nil {
		return semreg.PublicationBatch{}, errors.New("canonical PV SemReg shadow kernel is unavailable")
	}
	// This is an adapter generation identity, not an observation identity: a
	// refresh publishes the next sequence for the same native source.
	id := strings.TrimPrefix(legacy.AssetRef, "pv-asset-")
	if len(id) < 16 {
		return semreg.PublicationBatch{}, errors.New("canonical PV asset identity is invalid")
	}
	source := semreg.SourceID("source:canonical-pv-shadow:" + id)
	epoch := semreg.SourceEpochID("source-epoch:canonical-pv-shadow:" + id)
	binding := semreg.NativeBindingID("binding:canonical-pv-shadow:" + id)
	generation := semreg.Uint64("1")
	current, _, exists := kernel.Current()
	sequence, expected := semreg.Uint64("1"), semreg.Uint64("0")
	if exists {
		sequence = incrementSemReg(current.Cursors[0].LastSequence)
		expected = current.Revisions.Semantic
	}
	batch := semreg.PublicationBatch{
		Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:canonical-pv-shadow:" + id + ":" + string(sequence)), AssetID: semreg.AssetID(legacy.AssetRef),
		SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: sequence, ExpectedSemanticRevision: expected, ObservedAt: canonicalPVSemRegWall(producedAt),
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: []semreg.SourceEpochID{}, BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{},
	}
	registryEvidence := canonicalPVSemRegEvidence(string(legacy.Source.SourceRegistryRef))
	if !exists {
		batch.SourceUpserts = []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "sunspec_modbus", ProfileID: "sunspec.inverter.three_phase.monitoring", ProfileVersion: "1.0.0", RegistryEvidence: registryEvidence, StartedAt: canonicalPVSemRegWall(producedAt), State: semreg.SourceCurrent, Revision: sequence}}
	}
	if !exists {
		batch.BindingUpserts = []semreg.NativeBinding{{BindingID: binding, AssetID: semreg.AssetID(legacy.AssetRef), SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: canonicalPVSemRegEvidence(string(legacy.Source.SourceShadowRef)), State: semreg.BindingCurrent, Revision: sequence}}
		batch.IdentityLinkUpserts = []semreg.IdentityLink{{AssetID: semreg.AssetID(legacy.AssetRef), BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{canonicalPVSemRegEvidence(string(legacy.Source.EvidenceRef))}, Revision: sequence}}
	}
	mappings, err := canonicalPVSemRegMappingsForAsset(legacy.AssetRef)
	if err != nil {
		return semreg.PublicationBatch{}, err
	}
	facts := observation.Capability().Facts()
	for _, fact := range facts {
		mapping, ok := mappings[fact.FieldID()]
		if !ok {
			continue
		}
		candidate, err := canonicalPVSemRegCandidate(fact, mapping, source, epoch, binding, generation, legacy, producedAt, monotonic)
		if err != nil {
			return semreg.PublicationBatch{}, err
		}
		candidate.Revision = sequence
		batch.FactUpserts = append(batch.FactUpserts, candidate)
	}
	if len(batch.FactUpserts) != 11 {
		return semreg.PublicationBatch{}, fmt.Errorf("mapped SemReg fact count=%d want=11", len(batch.FactUpserts))
	}
	sort.Slice(batch.FactUpserts, func(i, j int) bool { return batch.FactUpserts[i].CandidateID < batch.FactUpserts[j].CandidateID })
	digestValue, err := batch.ComputedDigest()
	if err != nil {
		return semreg.PublicationBatch{}, err
	}
	batch.BatchDigest = digestValue
	return batch, nil
}

func canonicalPVSemRegCandidate(fact modbusreg.SunSpecCapabilityFact, mapping canonicalPVSemRegMapping, source semreg.SourceID, epoch semreg.SourceEpochID, binding semreg.NativeBindingID, generation semreg.Uint64, legacy pv.Snapshot, producedAt time.Time, monotonic pv.MonotonicNanos) (semreg.FactCandidate, error) {
	value := semreg.Value{}
	if mapping.symbol {
		_, symbol, ok := fact.Value().Enum()
		legacySymbol, representable := mapOperatingState(symbol)
		if !ok || !representable || legacySymbol != pv.OperatingStateOperating {
			return semreg.FactCandidate{}, errors.New("operating state is not representable as generating")
		}
		value = semreg.Value{Kind: semreg.ValueSymbol, Symbol: &semreg.Symbol{Namespace: semreg.DefinitionID(mapping.target), Token: "generating", Known: true}}
	} else {
		number, ok := fact.Value().Number()
		if !ok {
			return semreg.FactCandidate{}, fmt.Errorf("native fact %q is not numeric", fact.FieldID())
		}
		coefficient, exponent, err := canonicalPVSemRegDecimal(number)
		if err != nil {
			return semreg.FactCandidate{}, err
		}
		if fact.FieldID() == "inverter.ac.energy_lifetime" {
			exponent -= 3 // exact Wh -> kWh base-10 conversion
			if coefficient == "0" {
				exponent = 0
			}
		}
		value = semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: coefficient, Exponent10: exponent}, Unit: semreg.DefinitionID(mapping.unit)}}
	}
	dimension := mapping.dimensionValue
	key := semreg.FactKey{PackID: "helianthus.pack.pv", PackVersion: "1.0.0", FactID: semreg.DefinitionID(mapping.target), Dimensions: []semreg.Dimension{{ID: semreg.DefinitionID(mapping.dimension), Value: semreg.Value{Kind: semreg.ValueText, Text: &dimension}}}}
	if err := pvpack.New().ValidateFact(key, &value); err != nil {
		return semreg.FactCandidate{}, fmt.Errorf("validate SemReg PV %s: %w", fact.FieldID(), err)
	}
	policy, err := canonicalPVSemRegPolicy(mapping.policy)
	if err != nil {
		return semreg.FactCandidate{}, err
	}
	id := strings.TrimPrefix(domainDigest("canonical-pv-semreg-candidate-v1", []byte(fact.FieldID())), "sha256:")[:32]
	observationEvidence := canonicalPVSemRegEvidence(string(legacy.Source.SourceObservationRef))
	evidence := []semreg.EvidenceRef{canonicalPVSemRegEvidence(string(legacy.Source.EvidenceRef)), canonicalPVSemRegEvidence(string(legacy.Source.SourceShadowRef))}
	sort.Slice(evidence, func(i, j int) bool { return evidence[i].Digest < evidence[j].Digest })
	originEvidence := []semreg.EvidenceRef{observationEvidence}
	if fact.FieldID() == "inverter.ac.energy_lifetime" {
		legacyFact, err := canonicalPVSemRegLegacyFact(legacy, fact.FieldID())
		if err != nil {
			return semreg.FactCandidate{}, err
		}
		if legacyFact.Continuity == nil {
			return semreg.FactCandidate{}, errors.New("energy continuity evidence is absent")
		}
		if legacyFact.Continuity.State == pv.ContinuityReset || legacyFact.Continuity.State == pv.ContinuityRollover {
			counterEvidence := canonicalPVSemRegEvidence(string(legacyFact.Continuity.EvidenceRef))
			if err := counterEvidence.Validate(); err != nil {
				return semreg.FactCandidate{}, fmt.Errorf("energy continuity evidence is invalid: %w", err)
			}
			evidence = append(evidence, counterEvidence)
			originEvidence = append(originEvidence, counterEvidence)
			sort.Slice(evidence, func(i, j int) bool { return evidence[i].Digest < evidence[j].Digest })
			sort.Slice(originEvidence, func(i, j int) bool { return originEvidence[i].Digest < originEvidence[j].Digest })
		}
	}
	return semreg.FactCandidate{
		CandidateID: semreg.CandidateID("candidate:canonical-pv-shadow:" + id), Key: key, Value: &value,
		Quality: semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationCandidate, Promotion: semreg.PromotionUnpromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times:   semreg.Times{ReceivedAt: canonicalPVSemRegWall(producedAt), ReceiptMonotonic: canonicalPVSemRegMonotonic(monotonic), EvaluatedAt: canonicalPVSemRegWall(producedAt), EvaluateMonotonic: canonicalPVSemRegMonotonic(monotonic)}, FreshnessPolicy: policy,
		BindingID: &binding, SourceEpochID: &epoch, DriverGeneration: &generation,
		Origin:   semreg.OriginRef{OriginID: semreg.OriginID("origin:canonical-pv-shadow:" + id), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: originEvidence},
		Evidence: evidence, Revision: "1",
	}, nil
}

func canonicalPVSemRegDecimal(number string) (string, int32, error) {
	decimal, err := exactPVDecimal(number)
	if err != nil {
		return "", 0, err
	}
	coefficient, exponent := decimal.Coefficient, int32(decimal.Scale)
	if coefficient == "0" {
		return coefficient, 0, nil
	}
	negative := strings.HasPrefix(coefficient, "-")
	digits := strings.TrimPrefix(coefficient, "-")
	for strings.HasSuffix(digits, "0") {
		digits = strings.TrimSuffix(digits, "0")
		exponent++
	}
	if negative {
		digits = "-" + digits
	}
	return digits, exponent, nil
}

func canonicalPVSemRegPolicy(id string) (semreg.FreshnessPolicy, error) {
	policies := map[string]semreg.FreshnessPolicy{
		"pv.telemetry.fast.v1": {PolicyID: "pv.telemetry.fast.v1", Version: "1.0.0", FreshForNS: "30000000000", RetainForNS: "300000000000", MaxWallUncertaintyNS: "0"},
		"pv.accumulator.v1":    {PolicyID: "pv.accumulator.v1", Version: "1.0.0", FreshForNS: "900000000000", RetainForNS: "86400000000000", MaxWallUncertaintyNS: "0"},
		"pv.status.v1":         {PolicyID: "pv.status.v1", Version: "1.0.0", FreshForNS: "60000000000", RetainForNS: "600000000000", MaxWallUncertaintyNS: "0"},
	}
	policy, ok := policies[id]
	if !ok || policy.Validate() != nil {
		return semreg.FreshnessPolicy{}, fmt.Errorf("SemReg policy %q is unavailable", id)
	}
	return policy, nil
}

func canonicalPVSemRegEvidence(digest string) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: "helianthus.modbusadapter", Kind: "canonical_pv.shadow", Digest: semreg.Digest(digest), Contract: "helianthus.canonical-pv/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
}

func canonicalPVSemRegWall(at time.Time) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(at.UnixNano(), 10)), ClockID: "clock.gateway.adapter", UncertaintyNS: "0"}
}

func canonicalPVSemRegMonotonic(at pv.MonotonicNanos) semreg.MonotonicPoint {
	return semreg.MonotonicPoint{ClockEpochID: "clock-epoch:gateway-adapter", Nanoseconds: semreg.Uint64(strconv.FormatInt(int64(at), 10))}
}

func incrementSemReg(value semreg.Uint64) semreg.Uint64 {
	n, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil || n == ^uint64(0) {
		return ""
	}
	return semreg.Uint64(strconv.FormatUint(n+1, 10))
}

func canonicalPVSemRegProjection(snapshot semreg.Snapshot, legacy pv.Snapshot) (projection.ProjectionReport, error) {
	requested := make([]projection.RequestedItem, 0, 14)
	dispositions := make([]projection.ProjectionDisposition, 0, 14)
	mappings, err := canonicalPVSemRegMappingsForAsset(legacy.AssetRef)
	if err != nil {
		return projection.ProjectionReport{}, err
	}
	for nativeID, mapping := range mappings {
		legacyRequest, err := canonicalPVSemRegLegacyRequest(legacy, nativeID)
		if err != nil {
			return projection.ProjectionReport{}, err
		}
		item := canonicalPVSemRegRequestedItem(legacyRequest.RequestedOutputRef)
		requested = append(requested, projection.RequestedItem{Kind: projection.ItemFact, ItemID: item})
		dimension := mapping.dimensionValue
		key := semreg.FactKey{PackID: "helianthus.pack.pv", PackVersion: "1.0.0", FactID: semreg.DefinitionID(mapping.target), Dimensions: []semreg.Dimension{{ID: semreg.DefinitionID(mapping.dimension), Value: semreg.Value{Kind: semreg.ValueText, Text: &dimension}}}}
		disposition := projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: item, Outcome: projection.ProjectionExact, SourceKeys: []semreg.FactKey{key}, Loss: []projection.LossDetail{}}
		if nativeID == "inverter.ac.energy_lifetime" {
			disposition.Outcome, disposition.Loss = projection.ProjectionTransformed, []projection.LossDetail{{Kind: projection.LossUnit, SourceItems: []semreg.DefinitionID{semreg.DefinitionID(mapping.target)}, Description: "unit: Wh_to_kWh_factor_1000", Reversible: true}}
		}
		if nativeID == "inverter.temperature.cabinet" {
			disposition.Outcome, disposition.Loss = projection.ProjectionTransformed, []projection.LossDetail{{Kind: projection.LossProvenance, SourceItems: []semreg.DefinitionID{semreg.DefinitionID(mapping.target)}, Description: "provenance: sensor_id_cabinet_to_inverter"}}
		}
		if nativeID == "inverter.operating_state" {
			disposition.Outcome, disposition.Loss = projection.ProjectionTransformed, []projection.LossDetail{{Kind: projection.LossSymbol, SourceItems: []semreg.DefinitionID{semreg.DefinitionID(mapping.target)}, Description: "symbol: OPERATING_to_generating"}}
		}
		dispositions = append(dispositions, disposition)
	}
	for _, nativeID := range []string{"inverter.ac.current.total", "inverter.events.1", "inverter.events.2"} {
		legacyRequest, err := canonicalPVSemRegLegacyRequest(legacy, nativeID)
		if err != nil {
			return projection.ProjectionReport{}, err
		}
		item := canonicalPVSemRegRequestedItem(legacyRequest.RequestedOutputRef)
		reason := semreg.DefinitionID("compatibility.legacy_path_required")
		requested = append(requested, projection.RequestedItem{Kind: projection.ItemFact, ItemID: item})
		dispositions = append(dispositions, projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: item, Outcome: projection.ProjectionWithheld, SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{{Kind: projection.LossProvenance, SourceItems: []semreg.DefinitionID{semreg.DefinitionID(nativeID)}, Description: "provenance: retained legacy output"}}, Reason: &reason})
	}
	sort.Slice(requested, func(i, j int) bool { return requested[i].ItemID < requested[j].ItemID })
	sort.Slice(dispositions, func(i, j int) bool { return dispositions[i].ItemID < dispositions[j].ItemID })
	return projection.Project(snapshot, projection.ProjectionManifest{TargetID: "target:canonical-pv-comparator", TargetVersion: "1.0.0", KernelVersion: semreg.ContractKernelV1, PackVersions: []semreg.PackRef{{ID: "helianthus.pack.pv", Version: "1.0.0"}}, MappingRevision: "1"}, requested, dispositions, nil)
}

func canonicalPVSemRegLegacyRequest(legacy pv.Snapshot, nativeID string) (pv.RequestedOutput, error) {
	want := pv.Digest(domainDigest("canonical-pv-request-v1", []byte(nativeID)))
	var found *pv.RequestedOutput
	for _, request := range legacy.RequestedOutputs {
		if request.SourceRef != legacy.Source.SourceObservationRef {
			return pv.RequestedOutput{}, fmt.Errorf("legacy request %s lost source observation", request.RequestedOutputRef)
		}
		if request.RequestedOutputRef != want {
			continue
		}
		if found != nil {
			return pv.RequestedOutput{}, fmt.Errorf("duplicate legacy request %s", nativeID)
		}
		copy := request
		found = &copy
	}
	if found == nil {
		return pv.RequestedOutput{}, fmt.Errorf("legacy request %s is absent", nativeID)
	}
	return *found, nil
}

func canonicalPVSemRegRequestedItem(ref pv.Digest) semreg.DefinitionID {
	return semreg.DefinitionID("projection.pv.shadow.request." + strings.TrimPrefix(string(ref), "sha256:"))
}

func compareCanonicalPVSemRegShadow(legacy pv.Snapshot, snapshot semreg.Snapshot, evaluation semreg.EvaluationView, report projection.ProjectionReport) error {
	if err := snapshot.Validate(); err != nil {
		return fmt.Errorf("SemReg snapshot is invalid: %w", err)
	}
	if err := evaluation.Validate(); err != nil {
		return fmt.Errorf("SemReg evaluation is invalid: %w", err)
	}
	if err := report.Validate(); err != nil {
		return fmt.Errorf("SemReg projection report is invalid: %w", err)
	}
	if legacy.ContractID != pv.ContractV1 || len(legacy.Facts) != 11 || len(legacy.RequestedOutputs) != 14 || len(legacy.ProjectionReport) != 14 {
		return errors.New("legacy canonical PV accounting drifted")
	}
	if legacy.Source.Protocol != "sunspec_modbus" || legacy.Source.ProfileID != modbusreg.SunSpecThreePhaseMonitoringCapabilityID || legacy.Source.ProfileVersion != "1.0.0" || legacy.Source.Validity != pv.SourceTerminalVerified {
		return errors.New("legacy terminal source identity drifted")
	}
	if len(snapshot.Facts) != 11 || len(evaluation.Facts) != 11 || len(report.Requested) != 14 || len(report.Dispositions) != 14 {
		return fmt.Errorf("SemReg accounting facts=%d evaluated=%d requests=%d dispositions=%d", len(snapshot.Facts), len(evaluation.Facts), len(report.Requested), len(report.Dispositions))
	}
	evaluated := make(map[semreg.CandidateID]semreg.EvaluatedFact, len(evaluation.Facts))
	for _, fact := range evaluation.Facts {
		if _, duplicate := evaluated[fact.CandidateID]; duplicate {
			return errors.New("duplicate SemReg evaluated candidate")
		}
		evaluated[fact.CandidateID] = fact
	}
	if evaluation.SnapshotID != snapshot.SnapshotID || evaluation.Revisions != snapshot.Revisions || evaluation.Context.EvaluatedAt != snapshot.EvaluatedAt || evaluation.Context.EvaluateMonotonic != snapshot.EvaluateMonotonic {
		return errors.New("SemReg evaluation binding/context drifted")
	}
	for _, envelope := range snapshot.Facts {
		if len(envelope.Candidates) != 1 {
			return errors.New("SemReg candidate cardinality drifted")
		}
		candidate := envelope.Candidates[0]
		result, ok := evaluated[candidate.CandidateID]
		if !ok || result.CandidateRevision != candidate.Revision || result.Freshness != semreg.FreshnessFresh || result.EffectiveAvailability != semreg.AvailabilityAvailable {
			return fmt.Errorf("SemReg evaluation lifecycle drifted for %s", candidate.CandidateID)
		}
	}
	if snapshot.AssetID != semreg.AssetID(legacy.AssetRef) || len(snapshot.Sources) != 1 || len(snapshot.Bindings) != 1 || len(snapshot.IdentityLinks) != 1 || len(snapshot.Cursors) != 1 || report.SnapshotID != snapshot.SnapshotID || report.Revisions != snapshot.Revisions {
		return fmt.Errorf("SemReg source/binding identity drifted asset=%s/%s sources=%d bindings=%d links=%d report_snapshot=%s snapshot=%s report_revisions=%+v snapshot_revisions=%+v", snapshot.AssetID, legacy.AssetRef, len(snapshot.Sources), len(snapshot.Bindings), len(snapshot.IdentityLinks), report.SnapshotID, snapshot.SnapshotID, report.Revisions, snapshot.Revisions)
	}
	if report.Manifest.TargetID != "target:canonical-pv-comparator" || report.Manifest.TargetVersion != "1.0.0" || report.Manifest.KernelVersion != semreg.ContractKernelV1 || report.Manifest.MappingRevision != "1" || len(report.Manifest.PackVersions) != 1 || report.Manifest.PackVersions[0] != (semreg.PackRef{ID: "helianthus.pack.pv", Version: "1.0.0"}) {
		return errors.New("SemReg projection manifest drifted")
	}
	source, binding := snapshot.Sources[0], snapshot.Bindings[0]
	link := snapshot.IdentityLinks[0]
	registryEvidence := canonicalPVSemRegEvidence(string(legacy.Source.SourceRegistryRef))
	initialBindingMismatch := snapshot.Cursors[0].LastSequence == "1" && binding.NativeResource != canonicalPVSemRegEvidence(string(legacy.Source.SourceShadowRef))
	initialLinkMismatch := snapshot.Cursors[0].LastSequence == "1" && (len(link.Basis) != 1 || link.Basis[0] != canonicalPVSemRegEvidence(string(legacy.Source.EvidenceRef)))
	if source.ProtocolID != "sunspec_modbus" || source.ProfileID != "sunspec.inverter.three_phase.monitoring" || source.ProfileVersion != "1.0.0" || source.State != semreg.SourceCurrent || source.RegistryEvidence != registryEvidence || source.Revision != "1" || binding.AssetID != semreg.AssetID(legacy.AssetRef) || binding.SourceID != source.SourceID || binding.SourceEpochID != source.SourceEpochID || binding.DriverGeneration != "1" || binding.NativeResource.Validate() != nil || initialBindingMismatch || binding.State != semreg.BindingCurrent || binding.Revision != "1" || len(link.Basis) != 1 || link.Basis[0].Validate() != nil || initialLinkMismatch || link.AssetID != semreg.AssetID(legacy.AssetRef) || link.BindingID != binding.BindingID || link.State != semreg.LinkQualified || link.Revision != "1" {
		return errors.New("SemReg source/binding/provenance references drifted")
	}
	legacyProjection := make(map[pv.Digest]pv.Projection, 14)
	for _, item := range legacy.ProjectionReport {
		if item.SourceRef != legacy.Source.SourceObservationRef || legacyProjection[item.RequestedOutputRef].RequestedOutputRef != "" {
			return errors.New("legacy projection associations drifted")
		}
		legacyProjection[item.RequestedOutputRef] = item
	}
	reports := make(map[semreg.DefinitionID]projection.ProjectionDisposition, 14)
	for _, item := range report.Dispositions {
		if _, duplicate := reports[item.ItemID]; duplicate {
			return fmt.Errorf("duplicate SemReg projection item %s", item.ItemID)
		}
		reports[item.ItemID] = item
	}
	requested := make(map[semreg.DefinitionID]bool, 14)
	for _, item := range report.Requested {
		if item.Kind != projection.ItemFact || requested[item.ItemID] {
			return errors.New("SemReg requested projection items drifted")
		}
		requested[item.ItemID] = true
	}
	mappings, err := canonicalPVSemRegMappingsForAsset(legacy.AssetRef)
	if err != nil {
		return err
	}
	for nativeID, mapping := range mappings {
		request, err := canonicalPVSemRegLegacyRequest(legacy, nativeID)
		if err != nil {
			return err
		}
		itemID := canonicalPVSemRegRequestedItem(request.RequestedOutputRef)
		if !requested[itemID] {
			return fmt.Errorf("missing requested projection %s", nativeID)
		}
		disposition, ok := reports[itemID]
		if !ok {
			return fmt.Errorf("missing projection disposition %s", nativeID)
		}
		legacyFact, err := canonicalPVSemRegLegacyFact(legacy, nativeID)
		if err != nil {
			return err
		}
		legacyRow, ok := legacyProjection[request.RequestedOutputRef]
		if !ok || legacyRow.Outcome != pv.ProjectionMapped || legacyRow.FactID != legacyFact.ID || legacyRow.Dimensions == nil || *legacyRow.Dimensions != legacyFact.Dimensions {
			return fmt.Errorf("legacy mapped projection association drifted for %s", nativeID)
		}
		if err := canonicalPVSemRegCompareFact(nativeID, mapping, legacyFact, legacy.Source.EvidenceRef, legacy.Source.SourceShadowRef, snapshot, source, binding); err != nil {
			return err
		}
		if err := canonicalPVSemRegCompareDisposition(nativeID, mapping, disposition); err != nil {
			return err
		}
	}
	for _, nativeID := range []string{"inverter.ac.current.total", "inverter.events.1", "inverter.events.2"} {
		request, err := canonicalPVSemRegLegacyRequest(legacy, nativeID)
		if err != nil {
			return err
		}
		itemID := canonicalPVSemRegRequestedItem(request.RequestedOutputRef)
		disposition, ok := reports[itemID]
		legacyRow, legacyOK := legacyProjection[request.RequestedOutputRef]
		if !legacyOK || legacyRow.Outcome != pv.ProjectionWithheld || legacyRow.FactID != "" || legacyRow.Dimensions != nil || !ok || !requested[itemID] || disposition.Kind != projection.ItemFact || disposition.Outcome != projection.ProjectionWithheld || len(disposition.SourceKeys) != 0 || disposition.Reason == nil || *disposition.Reason != "compatibility.legacy_path_required" || len(disposition.Loss) != 1 || disposition.Loss[0].Kind != projection.LossProvenance || disposition.Loss[0].Description != "provenance: retained legacy output" || disposition.Loss[0].Reversible || len(disposition.Loss[0].SourceItems) != 1 || disposition.Loss[0].SourceItems[0] != semreg.DefinitionID(nativeID) {
			return fmt.Errorf("withheld projection %s drifted", nativeID)
		}
	}
	if len(requested) != 14 || len(reports) != 14 || len(legacyProjection) != 14 {
		return errors.New("projection identity accounting drifted")
	}
	return nil
}

func canonicalPVSemRegLegacyFact(legacy pv.Snapshot, nativeID string) (pv.Fact, error) {
	var key pv.FactKey
	switch nativeID {
	case "inverter.ac.power.active":
		key = pv.NewFactKey(pv.FactACActivePower, pv.Dimensions{Scope: pv.ScopeTotal})
	case "inverter.ac.frequency":
		key = pv.NewFactKey(pv.FactACFrequency, pv.Dimensions{Scope: pv.ScopeTotal})
	case "inverter.ac.energy_lifetime":
		key = pv.NewFactKey(pv.FactEnergyActiveExportTotal, pv.Dimensions{Scope: pv.ScopeTotal})
	case "inverter.temperature.cabinet":
		key = pv.NewFactKey(pv.FactTemperature, pv.Dimensions{SensorID: "cabinet"})
	case "inverter.operating_state":
		key = pv.NewFactKey(pv.FactOperatingState, pv.Dimensions{Scope: pv.ScopeTotal})
	case "inverter.ac.current.phase_a":
		key = pv.NewFactKey(pv.FactACCurrent, pv.Dimensions{Phase: pv.PhaseL1})
	case "inverter.ac.current.phase_b":
		key = pv.NewFactKey(pv.FactACCurrent, pv.Dimensions{Phase: pv.PhaseL2})
	case "inverter.ac.current.phase_c":
		key = pv.NewFactKey(pv.FactACCurrent, pv.Dimensions{Phase: pv.PhaseL3})
	case "inverter.ac.voltage.phase_a":
		key = pv.NewFactKey(pv.FactACVoltageLineToNeutral, pv.Dimensions{Phase: pv.PhaseL1})
	case "inverter.ac.voltage.phase_b":
		key = pv.NewFactKey(pv.FactACVoltageLineToNeutral, pv.Dimensions{Phase: pv.PhaseL2})
	case "inverter.ac.voltage.phase_c":
		key = pv.NewFactKey(pv.FactACVoltageLineToNeutral, pv.Dimensions{Phase: pv.PhaseL3})
	default:
		return pv.Fact{}, fmt.Errorf("unknown mapped native identity %s", nativeID)
	}
	fact, ok := legacy.Facts[key]
	if !ok || fact.Quality != pv.QualityGood || fact.Availability != pv.AvailabilityAvailable || fact.Freshness != pv.FreshnessFresh || fact.OriginRef != legacy.Source.SourceObservationRef {
		return pv.Fact{}, fmt.Errorf("legacy fact %s lifecycle/provenance drifted", nativeID)
	}
	return fact, nil
}

func canonicalPVSemRegCompareFact(nativeID string, mapping canonicalPVSemRegMapping, legacy pv.Fact, sourceEvidence, sourceShadow pv.Digest, snapshot semreg.Snapshot, source semreg.SourceDescriptor, binding semreg.NativeBinding) error {
	var candidate *semreg.FactCandidate
	for _, envelope := range snapshot.Facts {
		if canonicalPVSemRegKeyMatches(envelope.Key, mapping) && len(envelope.Candidates) == 1 {
			copy := envelope.Candidates[0]
			if !canonicalPVSemRegKeyMatches(copy.Key, mapping) {
				return fmt.Errorf("SemReg candidate key drifted for %s", nativeID)
			}
			candidate = &copy
			break
		}
	}
	expectedPolicy, err := canonicalPVSemRegPolicy(mapping.policy)
	if err != nil {
		return err
	}
	if candidate == nil || candidate.Revision == "" || candidate.Quality.Assertion != semreg.AssertionObserved || candidate.Quality.Qualification != semreg.QualificationCandidate || candidate.Quality.Promotion != semreg.PromotionUnpromoted || candidate.Quality.Validity != semreg.ValidityGood || candidate.Quality.Availability != semreg.AvailabilityAvailable || candidate.Quality.Freshness != semreg.FreshnessFresh || len(candidate.Quality.Reasons) != 0 || candidate.FreshnessPolicy != expectedPolicy || string(candidate.FreshnessPolicy.PolicyID) != string(legacy.Temporal.Policy) || candidate.Times.ReceivedAt.ClockID != "clock.gateway.adapter" || candidate.Times.ReceivedAt.UncertaintyNS != "0" || candidate.Times.ReceiptMonotonic.ClockEpochID != "clock-epoch:gateway-adapter" || string(candidate.Times.ReceiptMonotonic.Nanoseconds) != strconv.FormatInt(int64(legacy.Temporal.Receipt), 10) || candidate.Times.ReceivedAt != candidate.Times.EvaluatedAt || candidate.Times.ReceiptMonotonic != candidate.Times.EvaluateMonotonic || candidate.Origin.Kind != semreg.OriginNativeObservation || candidate.Origin.SourceID == nil || *candidate.Origin.SourceID != source.SourceID || candidate.Origin.SourceEpochID == nil || *candidate.Origin.SourceEpochID != source.SourceEpochID || candidate.Origin.BindingID == nil || *candidate.Origin.BindingID != binding.BindingID || candidate.BindingID == nil || *candidate.BindingID != binding.BindingID || candidate.SourceEpochID == nil || *candidate.SourceEpochID != source.SourceEpochID || candidate.DriverGeneration == nil || *candidate.DriverGeneration != binding.DriverGeneration || candidate.Causal != nil || candidate.Derivation != nil {
		return fmt.Errorf("SemReg fact lifecycle/provenance drifted for %s", nativeID)
	}
	if err := canonicalPVSemRegCompareEvidence(nativeID, legacy, sourceEvidence, sourceShadow, candidate); err != nil {
		return err
	}
	freshFor, freshErr := strconv.ParseInt(string(candidate.FreshnessPolicy.FreshForNS), 10, 64)
	retainFor, retainErr := strconv.ParseInt(string(candidate.FreshnessPolicy.RetainForNS), 10, 64)
	if freshErr != nil || retainErr != nil || legacy.Temporal.FreshUntil != legacy.Temporal.Receipt+pv.MonotonicNanos(freshFor) || legacy.Temporal.RetainUntil != legacy.Temporal.Receipt+pv.MonotonicNanos(retainFor) {
		return fmt.Errorf("SemReg lifecycle window drifted for %s", nativeID)
	}
	if mapping.symbol {
		if legacy.Unit != pv.UnitOne || legacy.Value.Symbol != pv.OperatingStateOperating || candidate.Value == nil || candidate.Value.Kind != semreg.ValueSymbol || candidate.Value.Symbol == nil || candidate.Value.Symbol.Namespace != semreg.DefinitionID(mapping.target) || candidate.Value.Symbol.Token != "generating" || !candidate.Value.Symbol.Known {
			return fmt.Errorf("SemReg operating state drifted")
		}
		return nil
	}
	if legacy.Unit != canonicalPVSemRegLegacyUnit(nativeID) || legacy.Value.Decimal == nil || candidate.Value == nil || candidate.Value.Kind != semreg.ValueQuantity || candidate.Value.Quantity == nil || candidate.Value.Quantity.Unit != semreg.DefinitionID(mapping.unit) {
		return fmt.Errorf("SemReg quantity/unit drifted for %s", nativeID)
	}
	coefficient, exponent := canonicalPVSemRegNormalize(legacy.Value.Decimal.Coefficient, int32(legacy.Value.Decimal.Scale))
	if nativeID == "inverter.ac.energy_lifetime" && coefficient != "0" {
		exponent -= 3
	}
	if candidate.Value.Quantity.Number.Coefficient != coefficient || candidate.Value.Quantity.Number.Exponent10 != exponent {
		return fmt.Errorf("SemReg rational value drifted for %s", nativeID)
	}
	return nil
}

func canonicalPVSemRegKeyMatches(key semreg.FactKey, mapping canonicalPVSemRegMapping) bool {
	return key.PackID == "helianthus.pack.pv" && key.PackVersion == "1.0.0" && key.FactID == semreg.DefinitionID(mapping.target) && len(key.Dimensions) == 1 && key.Dimensions[0].ID == semreg.DefinitionID(mapping.dimension) && key.Dimensions[0].Value.Kind == semreg.ValueText && key.Dimensions[0].Value.Text != nil && *key.Dimensions[0].Value.Text == mapping.dimensionValue
}

func canonicalPVSemRegCompareEvidence(nativeID string, legacy pv.Fact, sourceEvidence, sourceShadow pv.Digest, candidate *semreg.FactCandidate) error {
	wantOrigin := map[semreg.Digest]bool{semreg.Digest(legacy.OriginRef): true}
	wantEvidence := map[semreg.Digest]bool{semreg.Digest(sourceEvidence): true, semreg.Digest(sourceShadow): true}
	if nativeID == "inverter.ac.energy_lifetime" {
		if legacy.Continuity == nil {
			return errors.New("energy continuity evidence is absent")
		}
		switch legacy.Continuity.State {
		case pv.ContinuityBaseline, pv.ContinuityContiguous, pv.ContinuityDiscontinuity:
			if legacy.Continuity.EvidenceRef != "" {
				return errors.New("energy continuity invented evidence")
			}
		case pv.ContinuityReset, pv.ContinuityRollover:
			if err := legacy.Continuity.EvidenceRef.Validate(); err != nil {
				return errors.New("energy continuity evidence is invalid")
			}
			wantOrigin[semreg.Digest(legacy.Continuity.EvidenceRef)] = true
			wantEvidence[semreg.Digest(legacy.Continuity.EvidenceRef)] = true
		default:
			return errors.New("energy continuity state is invalid")
		}
	}
	if len(candidate.Origin.Evidence) != len(wantOrigin) || len(candidate.Evidence) != len(wantEvidence) {
		return fmt.Errorf("SemReg evidence cardinality drifted for %s", nativeID)
	}
	for _, item := range candidate.Origin.Evidence {
		if !wantOrigin[item.Digest] {
			return fmt.Errorf("SemReg origin evidence drifted for %s", nativeID)
		}
	}
	for _, item := range candidate.Evidence {
		if !wantEvidence[item.Digest] {
			return fmt.Errorf("SemReg evidence drifted for %s", nativeID)
		}
	}
	return nil
}

func canonicalPVSemRegLegacyUnit(nativeID string) pv.Unit {
	switch nativeID {
	case "inverter.ac.power.active":
		return pv.UnitWatt
	case "inverter.ac.frequency":
		return pv.UnitHertz
	case "inverter.ac.energy_lifetime":
		return pv.UnitWattHour
	case "inverter.temperature.cabinet":
		return pv.UnitCelsius
	case "inverter.operating_state":
		return pv.UnitOne
	case "inverter.ac.current.phase_a", "inverter.ac.current.phase_b", "inverter.ac.current.phase_c":
		return pv.UnitAmpere
	case "inverter.ac.voltage.phase_a", "inverter.ac.voltage.phase_b", "inverter.ac.voltage.phase_c":
		return pv.UnitVolt
	default:
		return ""
	}
}

func canonicalPVSemRegNormalize(coefficient string, exponent int32) (string, int32) {
	if coefficient == "0" {
		return "0", 0
	}
	negative := strings.HasPrefix(coefficient, "-")
	digits := strings.TrimPrefix(coefficient, "-")
	for strings.HasSuffix(digits, "0") {
		digits, exponent = strings.TrimSuffix(digits, "0"), exponent+1
	}
	if negative {
		digits = "-" + digits
	}
	return digits, exponent
}

func canonicalPVSemRegCompareDisposition(nativeID string, mapping canonicalPVSemRegMapping, disposition projection.ProjectionDisposition) error {
	want := projection.ProjectionExact
	if nativeID == "inverter.ac.energy_lifetime" || nativeID == "inverter.temperature.cabinet" || nativeID == "inverter.operating_state" {
		want = projection.ProjectionTransformed
	}
	if disposition.Kind != projection.ItemFact || disposition.Outcome != want || len(disposition.SourceKeys) != 1 || !canonicalPVSemRegKeyMatches(disposition.SourceKeys[0], mapping) {
		return fmt.Errorf("projection disposition drifted for %s", nativeID)
	}
	if want == projection.ProjectionExact && len(disposition.Loss) != 0 {
		return fmt.Errorf("exact projection has loss for %s", nativeID)
	}
	if want == projection.ProjectionTransformed && len(disposition.Loss) != 1 {
		return fmt.Errorf("transform projection loss drifted for %s", nativeID)
	}
	if want == projection.ProjectionTransformed {
		loss := disposition.Loss[0]
		kind, description, reversible := projection.LossUnit, "unit: Wh_to_kWh_factor_1000", true
		if nativeID == "inverter.temperature.cabinet" {
			kind, description, reversible = projection.LossProvenance, "provenance: sensor_id_cabinet_to_inverter", false
		}
		if nativeID == "inverter.operating_state" {
			kind, description, reversible = projection.LossSymbol, "symbol: OPERATING_to_generating", false
		}
		if loss.Kind != kind || loss.Description != description || loss.Reversible != reversible || len(loss.SourceItems) != 1 || loss.SourceItems[0] != semreg.DefinitionID(mapping.target) {
			return fmt.Errorf("transform projection loss contents drifted for %s", nativeID)
		}
	}
	return nil
}

func cloneSemRegSnapshot(source semreg.Snapshot) (semreg.Snapshot, error) {
	return cloneSemReg[semreg.Snapshot](source)
}
func cloneSemRegEvaluation(source semreg.EvaluationView) (semreg.EvaluationView, error) {
	return cloneSemReg[semreg.EvaluationView](source)
}
func cloneSemRegProjection(source projection.ProjectionReport) (projection.ProjectionReport, error) {
	return cloneSemReg[projection.ProjectionReport](source)
}
func cloneSemReg[T any](source T) (T, error) {
	raw, err := json.Marshal(source)
	if err != nil {
		var zero T
		return zero, err
	}
	var clone T
	if err := json.Unmarshal(raw, &clone); err != nil {
		var zero T
		return zero, err
	}
	return clone, nil
}
