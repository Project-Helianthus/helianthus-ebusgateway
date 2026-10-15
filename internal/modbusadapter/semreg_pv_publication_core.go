package modbusadapter

import (
	"bytes"
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

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	pvpack "github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const (
	pvPackID                             semreg.DefinitionID    = "helianthus.pack.pv"
	pvPackVersion                        semreg.SemanticVersion = "1.0.0"
	pvMappingRevision                    semreg.Uint64          = "1"
	pvSelectionPolicyID                  semreg.PolicyID        = "policy:gateway-pv-single-qualified"
	pvSelectionPolicyVersion             semreg.SemanticVersion = "1.0.0"
	pvReasonCounterContinuityUnavailable semreg.DefinitionID    = "counter_continuity_unavailable"
	pvReasonFieldInvalid                 semreg.DefinitionID    = "mapping.field_invalid"
	pvReasonNativeMissing                semreg.DefinitionID    = "mapping.native_fact_missing"
	pvReasonSemanticUnavailable          semreg.DefinitionID    = "mapping.semantic_equivalence_unavailable"
	pvReasonSymbolUnavailable            semreg.DefinitionID    = "mapping.symbol_unavailable"
	pvRegistryDigest                     semreg.Digest          = "sha256:e21d5d4914fba2249c68cc147243c22f89cc9e1f2be71e4565a3950f31e94750"
)

// pvPublicationLifecycle contains only lifecycle and clock coordinates. It
// deliberately has no caller-controlled qualification, promotion, or
// availability labels.
type pvPublicationLifecycle struct {
	sourceEpochID     semreg.SourceEpochID
	driverGeneration  semreg.Uint64
	sourceStartedAt   semreg.TimePoint
	receivedAt        semreg.TimePoint
	receiptMonotonic  semreg.MonotonicPoint
	evaluatedAt       semreg.TimePoint
	evaluateMonotonic semreg.MonotonicPoint
}

func (l pvPublicationLifecycle) validate() error {
	times := semreg.Times{
		ReceivedAt: l.receivedAt, ReceiptMonotonic: l.receiptMonotonic,
		EvaluatedAt: l.evaluatedAt, EvaluateMonotonic: l.evaluateMonotonic,
	}
	if err := l.sourceEpochID.Validate(); err != nil {
		return err
	}
	generation, err := strconv.ParseUint(string(l.driverGeneration), 10, 64)
	if err != nil || generation == 0 {
		return &semreg.Error{ID: semreg.InvalidIdentifier, Detail: "PV driver generation"}
	}
	if err := l.sourceStartedAt.Validate(); err != nil {
		return err
	}
	if err := times.Validate(); err != nil {
		return err
	}
	if l.sourceStartedAt.ClockID != l.receivedAt.ClockID || l.receivedAt.ClockID != l.evaluatedAt.ClockID || l.receiptMonotonic.ClockEpochID != l.evaluateMonotonic.ClockEpochID {
		return &semreg.Error{ID: semreg.IncomparableClockEpoch, Detail: "PV lifecycle axes"}
	}
	started, err1 := strconv.ParseInt(string(l.sourceStartedAt.UnixNanoseconds), 10, 64)
	received, err2 := strconv.ParseInt(string(l.receivedAt.UnixNanoseconds), 10, 64)
	evaluated, err3 := strconv.ParseInt(string(l.evaluatedAt.UnixNanoseconds), 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || started > received || received > evaluated {
		return &semreg.Error{ID: semreg.InvalidTime, Detail: "PV lifecycle wall order"}
	}
	return nil
}

type pvPublicationDraft struct {
	assetID             semreg.AssetID
	lifecycle           pvPublicationLifecycle
	source              semreg.SourceDescriptor
	binding             semreg.NativeBinding
	identityLink        semreg.IdentityLink
	observationEvidence semreg.EvidenceRef
	facts               []semreg.FactCandidate
	services            []semreg.ServiceInstance
	capabilities        []semreg.CapabilityInstance
	manifest            projection.ProjectionManifest
	requested           []projection.RequestedItem
	dispositions        []projection.ProjectionDisposition
}

func (d pvPublicationDraft) detached() (pvPublicationDraft, error) {
	clone := d
	var err error
	if clone.source, err = cloneJSON(d.source); err != nil {
		return pvPublicationDraft{}, err
	}
	if clone.binding, err = cloneJSON(d.binding); err != nil {
		return pvPublicationDraft{}, err
	}
	if clone.identityLink, err = cloneJSON(d.identityLink); err != nil {
		return pvPublicationDraft{}, err
	}
	if clone.facts, err = cloneJSON(d.facts); err != nil {
		return pvPublicationDraft{}, err
	}
	if clone.services, err = cloneJSON(d.services); err != nil {
		return pvPublicationDraft{}, err
	}
	if clone.capabilities, err = cloneJSON(d.capabilities); err != nil {
		return pvPublicationDraft{}, err
	}
	if clone.manifest, err = cloneJSON(d.manifest); err != nil {
		return pvPublicationDraft{}, err
	}
	if clone.requested, err = cloneJSON(d.requested); err != nil {
		return pvPublicationDraft{}, err
	}
	if clone.dispositions, err = cloneJSON(d.dispositions); err != nil {
		return pvPublicationDraft{}, err
	}
	return clone, nil
}

func (d pvPublicationDraft) validate() error {
	if err := d.assetID.Validate(); err != nil {
		return err
	}
	if err := d.lifecycle.validate(); err != nil {
		return err
	}
	if err := d.source.Validate(); err != nil {
		return err
	}
	if err := d.binding.Validate(); err != nil {
		return err
	}
	if err := d.identityLink.Validate(); err != nil {
		return err
	}
	if err := d.observationEvidence.Validate(); err != nil {
		return err
	}
	if d.source.SourceID != d.binding.SourceID || d.source.SourceEpochID != d.binding.SourceEpochID || d.binding.AssetID != d.assetID || d.identityLink.AssetID != d.assetID || d.identityLink.BindingID != d.binding.BindingID || d.source.State != semreg.SourceCurrent || d.binding.State != semreg.BindingCurrent || d.identityLink.State != semreg.LinkQualified {
		return errors.New("PV source, binding, and identity path is inconsistent")
	}
	if d.binding.DriverGeneration != d.lifecycle.driverGeneration || d.source.SourceEpochID != d.lifecycle.sourceEpochID {
		return errors.New("PV source path does not match lifecycle")
	}
	registry, err := semreg.NewRegistry(pvpack.New())
	if err != nil {
		return err
	}
	for _, fact := range d.facts {
		if fact.Quality.Qualification != semreg.QualificationQualified || fact.Quality.Promotion != semreg.PromotionPromoted || fact.Quality.Validity != semreg.ValidityGood || fact.Quality.Availability != semreg.AvailabilityAvailable || fact.Quality.Freshness != semreg.FreshnessFresh {
			return errors.New("PV fact labels are not the accepted qualified publication labels")
		}
		if err := registry.ValidateFactCandidate(fact); err != nil {
			return err
		}
	}
	for _, service := range d.services {
		if service.Qualification != semreg.QualificationQualified || service.Availability != semreg.AvailabilityAvailable {
			return errors.New("PV service labels are not the accepted qualified publication labels")
		}
		if err := registry.ValidateService(service); err != nil {
			return err
		}
	}
	for _, capability := range d.capabilities {
		if capability.Qualification != semreg.QualificationQualified || capability.Availability != semreg.AvailabilityAvailable {
			return errors.New("PV capability labels are not the accepted qualified publication labels")
		}
		if err := registry.ValidateCapability(capability); err != nil {
			return err
		}
	}
	if len(d.requested) != 14 || len(d.dispositions) != 14 {
		return errors.New("PV draft accounting is incomplete")
	}
	report := projection.ProjectionReport{
		Contract: projection.ContractProjectionV1, Manifest: d.manifest,
		SnapshotID: "snapshot:pv-draft-validation",
		Revisions:  semreg.RevisionVector{Semantic: "1", Identity: "1", Facts: "1", Services: "1", Capabilities: "1"},
		Requested:  d.requested, Dispositions: d.dispositions,
	}
	if err := report.Validate(); err != nil {
		return err
	}
	return nil
}

type pvMappingSpec struct {
	nativeID      string
	factID        semreg.DefinitionID
	dimensionID   semreg.DefinitionID
	dimensionRole string
	unit          semreg.DefinitionID
	policy        semreg.PolicyID
	outcome       projection.ProjectionOutcome
	lossKind      projection.LossKind
	lossText      string
}

var pvPublicationMappings = []pvMappingSpec{
	{"inverter.ac.current.phase_a", "pv.ac.current", "pv.dimension.phase", "phase:L1", "unit.ampere", "pv.telemetry.fast.v1", projection.ProjectionExact, "", ""},
	{"inverter.ac.current.phase_b", "pv.ac.current", "pv.dimension.phase", "phase:L2", "unit.ampere", "pv.telemetry.fast.v1", projection.ProjectionExact, "", ""},
	{"inverter.ac.current.phase_c", "pv.ac.current", "pv.dimension.phase", "phase:L3", "unit.ampere", "pv.telemetry.fast.v1", projection.ProjectionExact, "", ""},
	{"inverter.ac.voltage.phase_a", "pv.ac.voltage", "pv.dimension.phase", "phase:L1", "unit.volt", "pv.telemetry.fast.v1", projection.ProjectionExact, "", ""},
	{"inverter.ac.voltage.phase_b", "pv.ac.voltage", "pv.dimension.phase", "phase:L2", "unit.volt", "pv.telemetry.fast.v1", projection.ProjectionExact, "", ""},
	{"inverter.ac.voltage.phase_c", "pv.ac.voltage", "pv.dimension.phase", "phase:L3", "unit.volt", "pv.telemetry.fast.v1", projection.ProjectionExact, "", ""},
	{"inverter.ac.power.active", "pv.ac.aggregate_active_power", "pv.dimension.inverter", "inverter", "unit.watt", "pv.telemetry.fast.v1", projection.ProjectionExact, "", ""},
	{"inverter.ac.frequency", "pv.ac.frequency", "pv.dimension.inverter", "inverter", "unit.hertz", "pv.telemetry.fast.v1", projection.ProjectionExact, "", ""},
	{"inverter.ac.energy_lifetime", "pv.energy.generated", "pv.dimension.system", "system", "unit.kilowatt_hour", "pv.accumulator.v1", projection.ProjectionTransformed, projection.LossPolicy, "counter_continuity_unavailable; native baseline/reset/rollover/delta evidence remains native"},
	{"inverter.temperature.cabinet", "pv.temperature.inverter", "pv.dimension.inverter", "inverter", "unit.celsius", "pv.telemetry.fast.v1", projection.ProjectionTransformed, projection.LossProvenance, "provenance: cabinet sensor specificity reduced to inverter"},
	{"inverter.operating_state", "pv.status.operating", "pv.dimension.inverter", "inverter", "", "pv.status.v1", projection.ProjectionTransformed, projection.LossSymbol, "symbol: native MPPT mapped to generating"},
	{"inverter.ac.current.total", "", "", "", "", "", projection.ProjectionWithheld, projection.LossProvenance, "provenance: aggregate current has no accepted semantic equivalence"},
	{"inverter.events.1", "", "", "", "", "", projection.ProjectionWithheld, projection.LossProvenance, "provenance: event word has no accepted semantic mapping"},
	{"inverter.events.2", "", "", "", "", "", projection.ProjectionWithheld, projection.LossProvenance, "provenance: event word has no accepted semantic mapping"},
}

func buildPVPublicationDraft(observation modbusreg.SunSpecQualificationObservation, lifecycle pvPublicationLifecycle) (pvPublicationDraft, error) {
	if err := lifecycle.validate(); err != nil {
		return pvPublicationDraft{}, err
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		return pvPublicationDraft{}, fmt.Errorf("validate qualified SunSpec observation: %w", err)
	}
	capability := observation.Capability()
	if !capability.Admitted() || capability.Reason() != modbusreg.SunSpecCapabilityReasonAdmitted || capability.ProfileID() != modbusreg.SunSpecThreePhaseMonitoringCapabilityID {
		return pvPublicationDraft{}, errors.New("PV source capability is not admitted")
	}
	identity, nativeResource, err := resolvePVPublicationIdentity(observation)
	if err != nil {
		return pvPublicationDraft{}, err
	}
	assetHash := pvCoreRawHash(identity)
	assetID := semreg.AssetID("pv-asset-" + assetHash[:32])
	sourceHash := pvCoreHash("pv-source", nativeResource)
	sourceID := semreg.SourceID("source:semantic-pv:" + sourceHash[:32])
	bindingHash := pvCoreHash("pv-binding", []byte(string(assetID)+"\x00"+string(sourceID)+"\x00"+string(lifecycle.sourceEpochID)+"\x00"+string(lifecycle.driverGeneration)))
	bindingID := semreg.NativeBindingID("binding:semantic-pv:" + bindingHash[:32])
	registryEvidence := pvEvidence("sunspec.registry", pvRegistryDigest)
	observationEvidence := pvEvidence("sunspec.qualification_observation", semreg.Digest("sha256:"+pvCoreHash("pv-observation", encoded)))
	nativeEvidence := pvEvidence("sunspec.native_resource", semreg.Digest("sha256:"+pvCoreHash("pv-native-resource", nativeResource)))
	identityEvidence := pvEvidence("sunspec.common_identity", semreg.Digest("sha256:"+pvCoreHash("pv-common-identity", identity)))
	draft := pvPublicationDraft{
		assetID: assetID, lifecycle: lifecycle, observationEvidence: observationEvidence,
		source:       semreg.SourceDescriptor{SourceID: sourceID, SourceEpochID: lifecycle.sourceEpochID, ProtocolID: "sunspec_modbus", ProfileID: "sunspec.inverter.three_phase.monitoring", ProfileVersion: "1.0.0", RegistryEvidence: registryEvidence, StartedAt: lifecycle.sourceStartedAt, State: semreg.SourceCurrent, Revision: "1"},
		binding:      semreg.NativeBinding{BindingID: bindingID, AssetID: assetID, SourceID: sourceID, SourceEpochID: lifecycle.sourceEpochID, DriverGeneration: lifecycle.driverGeneration, NativeResource: nativeEvidence, State: semreg.BindingCurrent, Revision: "1"},
		identityLink: semreg.IdentityLink{AssetID: assetID, BindingID: bindingID, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{identityEvidence}, Revision: "1"},
		facts:        []semreg.FactCandidate{}, services: []semreg.ServiceInstance{}, capabilities: []semreg.CapabilityInstance{},
		manifest:  projection.ProjectionManifest{TargetID: "target:gateway-semantic-pv", TargetVersion: "1.0.0", KernelVersion: semreg.ContractKernelV1, PackVersions: []semreg.PackRef{{ID: pvPackID, Version: pvPackVersion}}, MappingRevision: pvMappingRevision},
		requested: []projection.RequestedItem{}, dispositions: []projection.ProjectionDisposition{},
	}
	facts := make(map[string]modbusreg.SunSpecCapabilityFact, len(capability.Facts()))
	for _, fact := range capability.Facts() {
		if _, duplicate := facts[fact.FieldID()]; duplicate {
			return pvPublicationDraft{}, fmt.Errorf("duplicate PV native fact %s", fact.FieldID())
		}
		facts[fact.FieldID()] = fact
	}
	registry, err := semreg.NewRegistry(pvpack.New())
	if err != nil {
		return pvPublicationDraft{}, err
	}
	for _, mapping := range pvPublicationMappings {
		itemID := pvProjectionItemID(mapping.nativeID)
		draft.requested = append(draft.requested, projection.RequestedItem{Kind: projection.ItemFact, ItemID: itemID})
		disposition := projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: itemID, Outcome: mapping.outcome, SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{}}
		if mapping.factID == "" {
			reason := pvReasonSemanticUnavailable
			disposition.Reason = &reason
			disposition.Loss = []projection.LossDetail{{Kind: mapping.lossKind, SourceItems: []semreg.DefinitionID{semreg.DefinitionID(mapping.nativeID)}, Description: mapping.lossText}}
			draft.dispositions = append(draft.dispositions, disposition)
			continue
		}
		native, found := facts[mapping.nativeID]
		if !found {
			reason := pvReasonNativeMissing
			disposition.Outcome, disposition.Reason = projection.ProjectionWithheld, &reason
			disposition.Loss = []projection.LossDetail{{Kind: projection.LossProvenance, SourceItems: []semreg.DefinitionID{semreg.DefinitionID(mapping.nativeID)}, Description: "provenance: native field absent"}}
			draft.dispositions = append(draft.dispositions, disposition)
			continue
		}
		candidate, err := pvPublicationCandidate(native, mapping, draft, observationEvidence)
		if err != nil || candidate.Validate() != nil || registry.ValidateFactCandidate(candidate) != nil {
			reason := pvReasonFieldInvalid
			lossKind := projection.LossRange
			if mapping.nativeID == "inverter.operating_state" {
				reason = pvReasonSymbolUnavailable
				lossKind = projection.LossSymbol
			}
			disposition.Outcome, disposition.Reason = projection.ProjectionWithheld, &reason
			disposition.Loss = []projection.LossDetail{{Kind: lossKind, SourceItems: []semreg.DefinitionID{semreg.DefinitionID(mapping.nativeID)}, Description: "policy: native field failed accepted PV validation"}}
			draft.dispositions = append(draft.dispositions, disposition)
			continue
		}
		draft.facts = append(draft.facts, candidate)
		disposition.SourceKeys = []semreg.FactKey{candidate.Key}
		if mapping.outcome == projection.ProjectionTransformed {
			disposition.Loss = []projection.LossDetail{{Kind: mapping.lossKind, SourceItems: []semreg.DefinitionID{semreg.DefinitionID(mapping.nativeID)}, Description: mapping.lossText}}
			if mapping.nativeID == "inverter.ac.energy_lifetime" {
				reason := pvReasonCounterContinuityUnavailable
				disposition.Reason = &reason
			}
		}
		draft.dispositions = append(draft.dispositions, disposition)
	}
	draft.services, draft.capabilities = pvPublicationServices(draft)
	sort.Slice(draft.facts, func(i, j int) bool { return draft.facts[i].CandidateID < draft.facts[j].CandidateID })
	sort.Slice(draft.services, func(i, j int) bool { return draft.services[i].InstanceID < draft.services[j].InstanceID })
	sort.Slice(draft.capabilities, func(i, j int) bool { return draft.capabilities[i].InstanceID < draft.capabilities[j].InstanceID })
	sort.Slice(draft.requested, func(i, j int) bool { return draft.requested[i].ItemID < draft.requested[j].ItemID })
	sort.Slice(draft.dispositions, func(i, j int) bool { return draft.dispositions[i].ItemID < draft.dispositions[j].ItemID })
	if err := draft.validate(); err != nil {
		return pvPublicationDraft{}, err
	}
	return draft, nil
}

func resolvePVPublicationIdentity(observation modbusreg.SunSpecQualificationObservation) ([]byte, []byte, error) {
	decoder, err := modbusreg.NewStandardSunSpecDecoderRegistry(modbusreg.SunSpecModelsRevisionV1)
	if err != nil {
		return nil, nil, err
	}
	identity := make([]string, 0, 3)
	for _, occurrence := range observation.Occurrences() {
		if occurrence.ModelID() != 1 {
			continue
		}
		common, err := decoder.DecodeOccurrence(occurrence)
		if err != nil {
			return nil, nil, err
		}
		for _, fieldID := range []string{"device.manufacturer", "device.model", "device.serial"} {
			fact, ok := common.Fact(fieldID)
			if !ok {
				return nil, nil, errors.New("SunSpec Common identity is incomplete")
			}
			value, ok := fact.Value.Text()
			if !ok || strings.TrimSpace(value) == "" {
				return nil, nil, errors.New("SunSpec Common identity is invalid")
			}
			identity = append(identity, value)
		}
		break
	}
	if len(identity) != 3 {
		return nil, nil, errors.New("SunSpec Common identity is absent")
	}
	views := observation.SourceViews()
	if len(views) == 0 {
		return nil, nil, errors.New("SunSpec native resource evidence is absent")
	}
	first := views[0].Record()
	type nativeResourceIdentity struct {
		Endpoint           string                    `json:"endpoint"`
		Transport          modbusreg.TransportFamily `json:"transport"`
		UnitID             byte                      `json:"unit_id"`
		Table              modbusreg.LogicalTable    `json:"table"`
		AuthorizationScope string                    `json:"authorization_scope"`
	}
	native := nativeResourceIdentity{Endpoint: first.Endpoint, Transport: first.Transport, UnitID: first.UnitID, Table: first.Table, AuthorizationScope: first.AuthorizationScope}
	for _, view := range views[1:] {
		record := view.Record()
		if record.Endpoint != native.Endpoint || record.Transport != native.Transport || record.UnitID != native.UnitID || record.Table != native.Table || record.AuthorizationScope != native.AuthorizationScope {
			return nil, nil, errors.New("SunSpec native resource identity is mixed")
		}
	}
	identityBytes := []byte(strings.Join(identity, "\x00"))
	nativeBytes, _ := json.Marshal(native)
	return identityBytes, nativeBytes, nil
}

func pvPublicationCandidate(native modbusreg.SunSpecCapabilityFact, mapping pvMappingSpec, draft pvPublicationDraft, observationEvidence semreg.EvidenceRef) (semreg.FactCandidate, error) {
	dimension := pvDimensionValue(mapping, draft.assetID)
	key := semreg.FactKey{PackID: pvPackID, PackVersion: pvPackVersion, FactID: mapping.factID, Dimensions: []semreg.Dimension{{ID: mapping.dimensionID, Value: semreg.Value{Kind: semreg.ValueText, Text: &dimension}}}}
	var value semreg.Value
	if mapping.nativeID == "inverter.operating_state" {
		_, symbol, ok := native.Value().Enum()
		if !ok || strings.TrimPrefix(symbol, "gg") != "MPPT" {
			return semreg.FactCandidate{}, errors.New("native operating state is not accepted as generating")
		}
		value = semreg.Value{Kind: semreg.ValueSymbol, Symbol: &semreg.Symbol{Namespace: mapping.factID, Token: "generating", Known: true}}
	} else {
		number, ok := native.Value().Number()
		if !ok {
			return semreg.FactCandidate{}, errors.New("native PV value is not numeric")
		}
		decimal, err := pvCoreDecimal(number)
		if err != nil {
			return semreg.FactCandidate{}, err
		}
		if mapping.nativeID == "inverter.ac.energy_lifetime" {
			// SunSpec exposes this counter in Wh; the accepted PV pack publishes kWh.
			// Canonical decimal zero always uses exponent 0, regardless of unit.
			if decimal.Coefficient != "0" {
				decimal.Exponent10 -= 3
			}
			if err := decimal.Validate(); err != nil {
				return semreg.FactCandidate{}, err
			}
		}
		value = semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: decimal, Unit: mapping.unit}}
	}
	policy, err := pvPublicationPolicy(mapping.policy)
	if err != nil {
		return semreg.FactCandidate{}, err
	}
	binding, epoch, generation := draft.binding.BindingID, draft.lifecycle.sourceEpochID, draft.lifecycle.driverGeneration
	source := draft.source.SourceID
	id := pvCoreHash("pv-candidate", []byte(string(draft.assetID)+"\x00"+string(draft.binding.BindingID)+"\x00"+mapping.nativeID))
	return semreg.FactCandidate{
		CandidateID: semreg.CandidateID("candidate:semantic-pv:" + id[:32]), Key: key, Value: &value,
		Quality:         semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times:           semreg.Times{ReceivedAt: draft.lifecycle.receivedAt, ReceiptMonotonic: draft.lifecycle.receiptMonotonic, EvaluatedAt: draft.lifecycle.evaluatedAt, EvaluateMonotonic: draft.lifecycle.evaluateMonotonic},
		FreshnessPolicy: policy, BindingID: &binding, SourceEpochID: &epoch, DriverGeneration: &generation,
		Origin:   semreg.OriginRef{OriginID: semreg.OriginID("origin:semantic-pv:" + id[:32]), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{observationEvidence}},
		Evidence: []semreg.EvidenceRef{observationEvidence}, Revision: "1",
	}, nil
}

func pvPublicationServices(draft pvPublicationDraft) ([]semreg.ServiceInstance, []semreg.CapabilityInstance) {
	pack := semreg.PackRef{ID: pvPackID, Version: pvPackVersion}
	definitions := []struct{ service, capability semreg.DefinitionID }{
		{"pv.service.system", "pv.capability.read.system"},
		{"pv.service.inverter", "pv.capability.read.inverter"},
		{"pv.service.phase", "pv.capability.read.phase"},
	}
	services := make([]semreg.ServiceInstance, 0, len(definitions))
	capabilities := make([]semreg.CapabilityInstance, 0, len(definitions))
	for _, pair := range definitions {
		serviceHash := pvCoreHash("pv-service", []byte(string(draft.assetID)+"\x00"+string(draft.binding.BindingID)+"\x00"+string(pair.service)))
		serviceID := semreg.ServiceInstanceID("service:semantic-pv:" + serviceHash[:32])
		service := semreg.ServiceInstance{InstanceID: serviceID, AssetID: draft.assetID, Definition: semreg.DefinitionRef{Pack: pack, ID: pair.service, Version: pvPackVersion}, BindingID: draft.binding.BindingID, SourceEpochID: draft.lifecycle.sourceEpochID, DriverGeneration: draft.lifecycle.driverGeneration, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "1"}
		capHash := pvCoreHash("pv-capability", []byte(string(draft.assetID)+"\x00"+string(draft.binding.BindingID)+"\x00"+string(pair.capability)))
		capability := semreg.CapabilityInstance{InstanceID: semreg.CapabilityInstanceID("capability:semantic-pv:" + capHash[:32]), AssetID: draft.assetID, ServiceInstance: serviceID, Definition: semreg.DefinitionRef{Pack: pack, ID: pair.capability, Version: pvPackVersion}, BindingID: draft.binding.BindingID, SourceEpochID: draft.lifecycle.sourceEpochID, DriverGeneration: draft.lifecycle.driverGeneration, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Constraints: []semreg.TypedField{}, ActivationEvidence: []semreg.EvidenceRef{draft.observationEvidence}, Revision: "1"}
		services = append(services, service)
		capabilities = append(capabilities, capability)
	}
	return services, capabilities
}

// pvSemanticDriver is the PV driver label on the semantic publication hub.
const pvSemanticDriver = "pv_sunspec"

// NewSemanticPVClass declares the lazily declared PV asset class on hub.
// Every discovered SunSpec asset is declared through it, per asset.
func NewSemanticPVClass(hub *semhub.Hub) (*semhub.Class, error) {
	if hub == nil {
		return nil, errors.New("SemReg PV publication hub is unavailable")
	}
	return hub.DeclareClass(semhub.ClassSpec{Name: pvSemanticDriver, Driver: pvSemanticDriver, Validators: func() []semreg.PackValidator { return []semreg.PackValidator{pvpack.New()} }})
}

type pvPublicationCore struct {
	mu            sync.RWMutex
	assets        map[semreg.AssetID]*pvPublicationAsset
	activeAssetID semreg.AssetID
	selection     *semreg.SelectionKernel
	hub           *semhub.Hub
	class         *semhub.Class
	// lifecycleMu guards the provisional source and the per-source handles.
	// It is taken alone or inside mu, never the other way round.
	lifecycleMu sync.Mutex
	provisional *semhub.Provisional
	handles     map[semreg.SourceID]*semhub.Handle
	epochs      map[semreg.SourceEpochID]bool
	// staged is a new epoch's provisional source that becomes current only
	// when its first publication commits; retiring holds replaced provisional
	// sources whose hub-level retirement still has to be journalled.
	staged   *semhub.Provisional
	retiring []*semhub.Provisional
	// covers holds transition covers whose initial submission failed after
	// their publication was accepted; each ingest retries them first.
	covers []semhub.ControlID
}

// pvLifecycle is the lifecycle plan of one publication: the handle to
// publish with, an optional generation or epoch transition the commit
// performs atomically, and a staged provisional source installed on commit.
type pvLifecycle struct {
	handle          *semhub.Handle
	generation      uint64
	epoch           semreg.SourceEpochID
	epochGeneration uint64
	staged          *semhub.Provisional
}

type pvPublicationAsset struct {
	declaration *semhub.Declaration
	current     *pvPublicationView
}

type pvPublicationView struct {
	snapshot   semreg.Snapshot
	canonical  []byte
	wallFloor  semreg.TimePoint
	evaluation semreg.EvaluationView
	selections []semreg.Selection
	projection projection.ProjectionReport
}

type pvPublicationReceipt struct {
	assetID    semreg.AssetID
	snapshotID semreg.SnapshotID
	revisions  semreg.RevisionVector
}

type pvPublicationOrder struct {
	sequence                 semreg.Uint64
	expectedSemanticRevision semreg.Uint64
}

// pvPublicationValidator observes a fully built, detached candidate while the
// publication core lock is held. It must not mutate the core. This permits the
// evidence owner to reject a capacity breach before the candidate becomes the
// current public snapshot.
type pvPublicationValidator func(assetID semreg.AssetID, snapshot semreg.Snapshot) error

func newPVPublicationCore(class *semhub.Class) (*pvPublicationCore, error) {
	if class == nil {
		return nil, errors.New("SemReg PV publication hub is unavailable")
	}
	selection, err := semreg.NewSelectionKernel(pvSingleQualifiedSelection{})
	if err != nil {
		return nil, err
	}
	return &pvPublicationCore{assets: make(map[semreg.AssetID]*pvPublicationAsset), selection: selection, hub: class.Hub(), class: class, handles: make(map[semreg.SourceID]*semhub.Handle), epochs: make(map[semreg.SourceEpochID]bool)}, nil
}

// admitEpoch admits the runtime's provisional PV source epoch. Its qualified
// legacy sources, one per native resource tuple, share this epoch.
func (c *pvPublicationCore) admitEpoch(epoch semreg.SourceEpochID, generation uint64) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	return c.admitEpochLocked(epoch, generation)
}

func (c *pvPublicationCore) admitEpochLocked(epoch semreg.SourceEpochID, generation uint64) error {
	if c.epochs[epoch] {
		return &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "retired PV source epoch"}
	}
	if c.provisional != nil {
		if err := c.provisional.Retire(); err != nil {
			return err
		}
	}
	provisional, err := c.hub.AdmitProvisional(pvSemanticDriver, semhub.LegacyPVSunSpec, epoch, generation)
	if err != nil {
		return err
	}
	c.epochs[epoch] = true
	c.provisional, c.handles = provisional, make(map[semreg.SourceID]*semhub.Handle)
	return nil
}

// retire retires the provisional source with its runtime. No SemReg batch
// is submitted; the retained public state is unchanged.
func (c *pvPublicationCore) retire() error {
	if c == nil {
		return nil
	}
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if err := c.retirePendingLocked(); err != nil {
		return err
	}
	if c.staged != nil {
		if err := c.staged.Retire(); err != nil {
			return err
		}
	}
	return c.provisional.Retire()
}

// retirePendingLocked retires replaced provisional sources journal-first and
// keeps any whose retirement failed for the next attempt.
func (c *pvPublicationCore) retirePendingLocked() error {
	var failure error
	kept := c.retiring[:0]
	for _, provisional := range c.retiring {
		if err := provisional.Retire(); err != nil {
			kept = append(kept, provisional)
			if failure == nil {
				failure = err
			}
		}
	}
	c.retiring = kept
	return failure
}

// handleFor plans the lifecycle of one publication. A later generation of the
// same epoch, or a new epoch, is not activated here: the publication's own
// batch carries the fence or the retirement and the hub switches generation
// or epoch atomically with that commit, so a rejected draft leaves the
// current epoch, its handles and the public views untouched.
func (c *pvPublicationCore) handleFor(draft pvPublicationDraft) (pvLifecycle, error) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	generation, err := strconv.ParseUint(string(draft.lifecycle.driverGeneration), 10, 64)
	if err != nil || generation == 0 {
		return pvLifecycle{}, &semreg.Error{ID: semreg.InvalidIdentifier, Detail: "PV driver generation"}
	}
	epoch, source := draft.lifecycle.sourceEpochID, draft.source.SourceID
	// A replaced provisional source whose retirement journal append failed
	// is retried before any requalification: until then its sources stay live
	// in the old epoch and cannot be admitted in the new one.
	if err := c.retirePendingLocked(); err != nil {
		return pvLifecycle{}, err
	}
	if c.provisional == nil {
		if err := c.admitEpochLocked(epoch, generation); err != nil {
			return pvLifecycle{}, err
		}
	}
	if c.provisional.Epoch() != epoch {
		if c.epochs[epoch] {
			return pvLifecycle{}, &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "retired PV source epoch"}
		}
		if c.staged == nil || c.staged.Epoch() != epoch {
			if c.staged != nil {
				if err := c.staged.Retire(); err != nil {
					return pvLifecycle{}, err
				}
			}
			staged, err := c.hub.AdmitProvisional(pvSemanticDriver, semhub.LegacyPVSunSpec, epoch, generation)
			if err != nil {
				return pvLifecycle{}, err
			}
			c.staged = staged
		}
		if current := c.handles[source]; current != nil {
			return pvLifecycle{handle: current, epoch: epoch, epochGeneration: generation, staged: c.staged}, nil
		}
		handle, err := c.staged.Qualify(source)
		if err != nil {
			return pvLifecycle{}, err
		}
		return pvLifecycle{handle: handle, staged: c.staged}, nil
	}
	handle := c.handles[source]
	if handle == nil {
		if handle, err = c.provisional.Qualify(source); err != nil {
			return pvLifecycle{}, err
		}
		c.handles[source] = handle
	}
	switch {
	case handle.Generation() > generation:
		return pvLifecycle{}, &semreg.Error{ID: semreg.StaleDriverGeneration, Detail: "regressed PV driver generation"}
	case handle.Generation() < generation:
		return pvLifecycle{handle: handle, generation: generation}, nil
	}
	return pvLifecycle{handle: handle}, nil
}

// commitLifecycle applies a committed publication's plan: it adopts the new
// handle and, for a new epoch, installs the staged provisional source and
// retires the replaced one journal-first (a failed retirement is retried at
// the next epoch change and at Close).
func (c *pvPublicationCore) commitLifecycle(source semreg.SourceID, plan pvLifecycle, handle *semhub.Handle) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if plan.staged == nil {
		c.handles[source] = handle
		return
	}
	if plan.epoch != "" {
		if err := plan.staged.Adopt(handle); err != nil {
			return
		}
	}
	replaced := c.provisional
	c.provisional, c.staged = plan.staged, nil
	c.epochs[plan.staged.Epoch()] = true
	c.handles = map[semreg.SourceID]*semhub.Handle{source: handle}
	if replaced != nil {
		c.retiring = append(c.retiring, replaced)
	}
	_ = c.retirePendingLocked()
}

func (c *pvPublicationCore) recordCover(id semhub.ControlID) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.covers = append(c.covers, id)
}

// retryCovers retries every pending transition cover outside c.mu. A cover
// that resolves, or that a later control operation of its source superseded,
// is dropped; any other failure keeps it for the next ingest.
func (c *pvPublicationCore) retryCovers() {
	c.lifecycleMu.Lock()
	pending := append([]semhub.ControlID(nil), c.covers...)
	c.lifecycleMu.Unlock()
	if len(pending) == 0 {
		return
	}
	done := make(map[semhub.ControlID]bool, len(pending))
	for _, id := range pending {
		if _, err := c.hub.RetryControl(id); err == nil || errors.Is(err, semhub.ErrControlRequest) {
			done[id] = true
		}
	}
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	kept := c.covers[:0]
	for _, id := range c.covers {
		if !done[id] {
			kept = append(kept, id)
		}
	}
	c.covers = kept
}

// onControl drops the public view of an asset whose withdrawal the hub
// accepted. The kernel keeps the withdrawn snapshot.
func (c *pvPublicationCore) onControl(event semhub.ControlEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if asset := c.assets[event.Asset]; asset != nil {
		asset.current = nil
	}
	return nil
}

func (c *pvPublicationCore) ingest(draft pvPublicationDraft) (pvPublicationReceipt, error) {
	return c.ingestWithOrderAndValidation(draft, nil, nil)
}

func (c *pvPublicationCore) ingestWithOrder(draft pvPublicationDraft, order *pvPublicationOrder) (pvPublicationReceipt, error) {
	return c.ingestWithOrderAndValidation(draft, order, nil)
}

func (c *pvPublicationCore) ingestWithValidation(draft pvPublicationDraft, validate pvPublicationValidator) (pvPublicationReceipt, error) {
	return c.ingestWithOrderAndValidation(draft, nil, validate)
}

func (c *pvPublicationCore) ingestWithOrderAndValidation(draft pvPublicationDraft, order *pvPublicationOrder, validate pvPublicationValidator) (pvPublicationReceipt, error) {
	if c == nil {
		return pvPublicationReceipt{}, errors.New("PV publication core is unavailable")
	}
	detached, err := draft.detached()
	if err != nil {
		return pvPublicationReceipt{}, err
	}
	if err := detached.validate(); err != nil {
		return pvPublicationReceipt{}, err
	}
	// RetryControl runs reprojection callbacks, which take c.mu, so pending
	// covers are retried before c.mu is taken.
	c.retryCovers()
	c.mu.Lock()
	defer c.mu.Unlock()
	plan, err := c.handleFor(detached)
	if err != nil {
		return pvPublicationReceipt{}, err
	}
	asset := c.assets[detached.assetID]
	if asset == nil {
		// A discovered asset is declared per asset; a collision refuses only
		// this asset and is emitted as DeclareRefused.
		declaration, err := c.class.Declare(detached.assetID, detached.source.SourceID)
		if err != nil {
			return pvPublicationReceipt{}, err
		}
		declaration.SetReprojection(c.onControl)
		asset = &pvPublicationAsset{declaration: declaration}
	}
	var tx *semhub.Tx
	switch {
	case plan.epoch != "":
		tx, err = c.hub.BeginEpoch(plan.handle, detached.assetID, plan.epoch, plan.epochGeneration)
	case plan.generation != 0:
		tx, err = c.hub.BeginGeneration(plan.handle, detached.assetID, plan.generation)
	default:
		tx, err = c.hub.Begin(plan.handle, detached.assetID)
	}
	if err != nil {
		return pvPublicationReceipt{}, err
	}
	defer tx.Abort()
	current, exists := tx.Current()
	batch, err := pvPublicationBatch(detached, current, exists, order, tx.NextSequence(), tx.SourceRetirements(), tx.GenerationFences())
	if err != nil {
		return pvPublicationReceipt{}, err
	}
	snapshot, canonical, err := tx.Apply(batch, detached.lifecycle.receiptMonotonic)
	if err != nil {
		return pvPublicationReceipt{}, err
	}
	decoded, err := semreg.Decode[semreg.Snapshot](canonical)
	if err != nil {
		return pvPublicationReceipt{}, err
	}
	reencoded, err := semreg.CanonicalJSON(decoded)
	if err != nil || !bytes.Equal(reencoded, canonical) {
		return pvPublicationReceipt{}, errors.New("PV canonical snapshot round trip drifted")
	}
	evaluation, err := semreg.EvaluateSnapshot(snapshot, semreg.EvaluationContext{EvaluatedAt: detached.lifecycle.evaluatedAt, EvaluateMonotonic: detached.lifecycle.evaluateMonotonic})
	if err != nil {
		return pvPublicationReceipt{}, err
	}
	selections, err := pvPresentationSelections(c.selection, snapshot, evaluation)
	if err != nil {
		return pvPublicationReceipt{}, err
	}
	report, err := projection.Project(snapshot, detached.manifest, detached.requested, detached.dispositions, nil)
	if err != nil {
		return pvPublicationReceipt{}, err
	}
	if validate != nil {
		if err := validate(detached.assetID, snapshot); err != nil {
			return pvPublicationReceipt{}, err
		}
	}
	floor, err := nextPVPublicationWallFloor(asset.current, detached.lifecycle.receivedAt, detached.lifecycle.evaluatedAt)
	if err != nil {
		return pvPublicationReceipt{}, err
	}
	// A detached public view carries the wall coordinate at which its immutable
	// snapshot was committed. Read-time wall clocks may roll back, but cannot be
	// allowed to precede this snapshot's receipt/evaluation coordinate.
	view := &pvPublicationView{snapshot: snapshot, canonical: canonical, wallFloor: floor, evaluation: evaluation, selections: selections, projection: report}
	_, err = tx.Commit(func(semhub.Committed) {
		asset.current = view
		c.assets[detached.assetID] = asset
		// Keep historical identity-keyed public evidence addressable, while the
		// existing single-domain metric surface follows the most recently
		// accepted bounded PV publication across a SunSpec Common identity
		// rotation.
		c.activeAssetID = detached.assetID
	})
	// A CoverError reports an accepted publication whose transition cover
	// over the source's other assets is unfinished: the publication and its
	// lifecycle are committed, the cover is retried by the next ingest, and
	// the failure is reported.
	var cover *semhub.CoverError
	if err != nil && !errors.As(err, &cover) {
		return pvPublicationReceipt{}, err
	}
	c.commitLifecycle(detached.source.SourceID, plan, tx.Handle())
	receipt := pvPublicationReceipt{assetID: detached.assetID, snapshotID: snapshot.SnapshotID, revisions: snapshot.Revisions}
	if cover != nil {
		c.recordCover(cover.ID)
		return receipt, err
	}
	return receipt, nil
}

// nextPVPublicationWallFloor preserves one comparable, nondecreasing trusted
// floor for the current asset view. The lifecycle contract has already ordered
// receipt before evaluation; retaining the prior floor also covers a process
// wall rollback between successive publications.
func nextPVPublicationWallFloor(current *pvPublicationView, received, evaluated semreg.TimePoint) (semreg.TimePoint, error) {
	if err := received.Validate(); err != nil {
		return semreg.TimePoint{}, errors.New("PV receipt wall floor is invalid")
	}
	if err := evaluated.Validate(); err != nil {
		return semreg.TimePoint{}, errors.New("PV evaluation wall floor is invalid")
	}
	if received.ClockID != evaluated.ClockID {
		return semreg.TimePoint{}, errors.New("PV lifecycle wall floors are incompatible")
	}
	floor := evaluated
	if current == nil {
		return floor, nil
	}
	if err := current.wallFloor.Validate(); err != nil {
		return semreg.TimePoint{}, errors.New("PV current wall floor is invalid")
	}
	if current.wallFloor.ClockID != floor.ClockID {
		return semreg.TimePoint{}, errors.New("PV current wall floor is incompatible")
	}
	previous, previousErr := strconv.ParseInt(string(current.wallFloor.UnixNanoseconds), 10, 64)
	next, nextErr := strconv.ParseInt(string(floor.UnixNanoseconds), 10, 64)
	if previousErr != nil || nextErr != nil {
		return semreg.TimePoint{}, errors.New("PV wall floor is invalid")
	}
	if previous > next {
		floor = current.wallFloor
	}
	return floor, nil
}

func pvHasSelectableCandidate(envelope semreg.FactEnvelope, evaluation semreg.EvaluationView) (bool, error) {
	if len(envelope.Conflicts) != 0 {
		return false, errors.New("PV fact is conflicted")
	}
	byID := make(map[semreg.CandidateID]semreg.EvaluatedFact, len(evaluation.Facts))
	for _, fact := range evaluation.Facts {
		byID[fact.CandidateID] = fact
	}
	for _, candidate := range envelope.Candidates {
		fact, found := byID[candidate.CandidateID]
		if !found {
			return false, errors.New("PV candidate evaluation is missing")
		}
		if candidate.Quality.Qualification == semreg.QualificationQualified && candidate.Quality.Promotion == semreg.PromotionPromoted && fact.EffectiveAvailability == semreg.AvailabilityAvailable && fact.Freshness == semreg.FreshnessFresh {
			return true, nil
		}
	}
	return false, nil
}

func pvPresentationSelections(selection *semreg.SelectionKernel, snapshot semreg.Snapshot, evaluation semreg.EvaluationView) ([]semreg.Selection, error) {
	selections := make([]semreg.Selection, 0, len(snapshot.Facts))
	for _, envelope := range snapshot.Facts {
		selectable, err := pvHasSelectableCandidate(envelope, evaluation)
		if err != nil {
			return nil, err
		}
		if !selectable {
			// The immutable evaluation remains public, but a stale/unavailable
			// field has no current presentation selection.
			continue
		}
		selected, err := selection.SelectPresentation(snapshot, evaluation, envelope.Key, pvSelectionPolicyID, pvSelectionPolicyVersion)
		if err != nil {
			return nil, err
		}
		selections = append(selections, selected)
	}
	sort.Slice(selections, func(i, j int) bool {
		left, _ := semreg.CanonicalJSON(selections[i].Key)
		right, _ := semreg.CanonicalJSON(selections[j].Key)
		return bytes.Compare(left, right) < 0
	})
	return selections, nil
}

func (c *pvPublicationCore) publicView(assetID semreg.AssetID) (pvPublicationView, error) {
	if c == nil {
		return pvPublicationView{}, errors.New("PV publication core is unavailable")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	asset := c.assets[assetID]
	if asset == nil || asset.current == nil || !c.hub.Admitted(assetID) {
		return pvPublicationView{}, errors.New("PV publication state is unavailable")
	}
	return asset.current.detached()
}

func (c *pvPublicationCore) singleAssetID() (semreg.AssetID, bool) {
	if c == nil {
		return "", false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	asset := c.assets[c.activeAssetID]
	return c.activeAssetID, c.activeAssetID != "" && asset != nil && asset.current != nil && c.hub.Admitted(c.activeAssetID)
}

// evaluatePublicView evaluates an already detached snapshot. Callers that
// need a current clock must detach first, then obtain that clock, so a later
// concurrent publication cannot make the context precede this snapshot's
// receipt time.
func (c *pvPublicationCore) evaluatePublicView(view pvPublicationView, context semreg.EvaluationContext) (pvPublicationView, error) {
	if c == nil {
		return pvPublicationView{}, errors.New("PV publication core is unavailable")
	}
	evaluation, err := semreg.EvaluateSnapshot(view.snapshot, context)
	if err != nil {
		return pvPublicationView{}, err
	}
	selections, err := pvPresentationSelections(c.selection, view.snapshot, evaluation)
	if err != nil {
		return pvPublicationView{}, err
	}
	view.evaluation, view.selections = evaluation, selections
	return view, nil
}

// currentForValidation is the internal, ungated validation read. Its
// snapshot comes from the hub's Inspect seam, never from a public gate.
func (c *pvPublicationCore) currentForValidation(assetID semreg.AssetID) (pvPublicationView, error) {
	if c == nil {
		return pvPublicationView{}, errors.New("PV publication core is unavailable")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	asset := c.assets[assetID]
	if asset == nil || asset.current == nil {
		return pvPublicationView{}, errors.New("PV publication state is unavailable")
	}
	view, err := asset.current.detached()
	if err != nil {
		return pvPublicationView{}, err
	}
	inspected, err := c.hub.Inspect(assetID)
	if err != nil || !inspected.Present {
		return pvPublicationView{}, errors.New("PV publication state is unavailable")
	}
	view.snapshot, view.canonical = inspected.Snapshot, inspected.Canonical
	return view, nil
}

// currentSunSpecObservationDigests reads every current asset under one core
// read lock so adapter evidence pruning cannot orphan a still-public identity.
func (c *pvPublicationCore) currentSunSpecObservationDigests() map[string]bool {
	if c == nil {
		return make(map[string]bool)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentSunSpecObservationDigestsLocked("", nil)
}

// currentSunSpecObservationDigestsLocked returns the references that would be
// public if replacement were assigned to assetID. c.mu must already be held.
func (c *pvPublicationCore) currentSunSpecObservationDigestsLocked(assetID semreg.AssetID, replacement *semreg.Snapshot) map[string]bool {
	refs := make(map[string]bool)
	collect := func(evidence []semreg.EvidenceRef) {
		for _, ref := range evidence {
			if ref.Kind == "sunspec.qualification_observation" {
				refs[string(ref.Digest)] = true
			}
		}
	}
	collectSnapshot := func(snapshot semreg.Snapshot) {
		for _, envelope := range snapshot.Facts {
			for _, candidate := range envelope.Candidates {
				collect(candidate.Evidence)
				collect(candidate.Origin.Evidence)
			}
		}
		for _, retained := range snapshot.Retained {
			collect(retained.Candidate.Evidence)
			collect(retained.Candidate.Origin.Evidence)
		}
		for _, capability := range snapshot.Capabilities {
			collect(capability.ActivationEvidence)
		}
		for _, fence := range snapshot.Fences {
			collect(fence.Evidence)
		}
	}
	for id, asset := range c.assets {
		if id == assetID && replacement != nil {
			collectSnapshot(*replacement)
			continue
		}
		if asset == nil || asset.current == nil {
			continue
		}
		collectSnapshot(asset.current.snapshot)
	}
	if replacement != nil {
		if _, exists := c.assets[assetID]; !exists {
			collectSnapshot(*replacement)
		}
	}
	return refs
}

func (v *pvPublicationView) detached() (pvPublicationView, error) {
	if v == nil {
		return pvPublicationView{}, errors.New("PV publication view is unavailable")
	}
	clone := *v
	var err error
	if clone.snapshot, err = cloneJSON(v.snapshot); err != nil {
		return pvPublicationView{}, err
	}
	clone.canonical = append([]byte(nil), v.canonical...)
	if clone.evaluation, err = cloneJSON(v.evaluation); err != nil {
		return pvPublicationView{}, err
	}
	if clone.selections, err = cloneJSON(v.selections); err != nil {
		return pvPublicationView{}, err
	}
	if clone.projection, err = cloneJSON(v.projection); err != nil {
		return pvPublicationView{}, err
	}
	return clone, nil
}

// pvPublicationBatch builds one publication of the hub-admitted (source,
// epoch, generation). The sequence comes from the asset cursor; epoch
// retirements and the older unfenced generations come from the hub
// transaction, and both travel inside this one batch.
func pvPublicationBatch(draft pvPublicationDraft, current semreg.Snapshot, exists bool, override *pvPublicationOrder, next semreg.Uint64, retirements []semreg.SourceEpochID, fenced []semreg.Uint64) (semreg.PublicationBatch, error) {
	sequence, expected := next, semreg.Uint64("0")
	hasSource, hasBinding := false, false
	if exists {
		expected = current.Revisions.Semantic
		known := false
		for _, source := range current.Sources {
			if source.SourceID != draft.source.SourceID {
				continue
			}
			known = true
			if source.SourceEpochID == draft.lifecycle.sourceEpochID {
				hasSource = true
				if source.State == semreg.SourceCurrent && source.StartedAt != draft.source.StartedAt {
					return semreg.PublicationBatch{}, &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "same-epoch PV source start drift"}
				}
			}
		}
		if !known {
			return semreg.PublicationBatch{}, &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "PV source identity changed"}
		}
		for _, binding := range current.Bindings {
			if binding.BindingID == draft.binding.BindingID {
				hasBinding = true
			}
		}
	}
	if override != nil {
		sequence, expected = override.sequence, override.expectedSemanticRevision
	}
	batchIDHash := pvCoreHash("pv-batch", []byte(string(draft.assetID)+"\x00"+string(draft.lifecycle.sourceEpochID)+"\x00"+string(draft.lifecycle.driverGeneration)+"\x00"+string(sequence)))
	batch := semreg.PublicationBatch{
		Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:semantic-pv:" + batchIDHash[:32]), AssetID: draft.assetID,
		SourceID: draft.source.SourceID, SourceEpochID: draft.lifecycle.sourceEpochID, DriverGeneration: draft.lifecycle.driverGeneration,
		Sequence: sequence, ExpectedSemanticRevision: expected, ObservedAt: draft.lifecycle.receivedAt,
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: append([]semreg.SourceEpochID{}, retirements...), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{},
		FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{},
		CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{},
	}
	if !hasSource {
		source := draft.source
		source.Revision = "1"
		batch.SourceUpserts = append(batch.SourceUpserts, source)
	}
	for _, generation := range fenced {
		batch.GenerationFences = append(batch.GenerationFences, semreg.GenerationFence{SourceID: draft.source.SourceID, SourceEpochID: draft.lifecycle.sourceEpochID, DriverGeneration: generation, Reason: "lifecycle.driver_generation_replaced", Evidence: []semreg.EvidenceRef{draft.observationEvidence}, Revision: "1"})
	}
	if !hasBinding {
		binding := draft.binding
		binding.Revision = "1"
		link := draft.identityLink
		link.Revision = "1"
		batch.BindingUpserts = append(batch.BindingUpserts, binding)
		batch.IdentityLinkUpserts = append(batch.IdentityLinkUpserts, link)
		for _, service := range draft.services {
			service.Revision = nextPVServiceRevision(current, service.InstanceID)
			batch.ServiceUpserts = append(batch.ServiceUpserts, service)
		}
	}
	// Capability activation is itself backed by the exact refresh observation.
	// Refresh it on every publication so a fully superseding snapshot does not
	// retain an otherwise obsolete current-observation digest.
	for _, capability := range draft.capabilities {
		capability.Revision = nextPVCapabilityRevision(current, capability.InstanceID)
		batch.CapabilityUpserts = append(batch.CapabilityUpserts, capability)
	}
	for _, fact := range draft.facts {
		fact.Revision = nextPVCandidateRevision(current, fact.CandidateID)
		batch.FactUpserts = append(batch.FactUpserts, fact)
	}
	sort.Slice(batch.SourceRetirements, func(i, j int) bool { return batch.SourceRetirements[i] < batch.SourceRetirements[j] })
	sort.Slice(batch.FactUpserts, func(i, j int) bool { return batch.FactUpserts[i].CandidateID < batch.FactUpserts[j].CandidateID })
	sort.Slice(batch.ServiceUpserts, func(i, j int) bool { return batch.ServiceUpserts[i].InstanceID < batch.ServiceUpserts[j].InstanceID })
	sort.Slice(batch.CapabilityUpserts, func(i, j int) bool {
		return batch.CapabilityUpserts[i].InstanceID < batch.CapabilityUpserts[j].InstanceID
	})
	sort.Slice(batch.GenerationFences, func(i, j int) bool {
		return batch.GenerationFences[i].DriverGeneration < batch.GenerationFences[j].DriverGeneration
	})
	digest, err := batch.ComputedDigest()
	if err != nil {
		return semreg.PublicationBatch{}, err
	}
	batch.BatchDigest = digest
	if err := batch.Validate(); err != nil {
		return semreg.PublicationBatch{}, err
	}
	return batch, nil
}

type pvSingleQualifiedSelection struct{}

func (pvSingleQualifiedSelection) PolicyID() semreg.PolicyID       { return pvSelectionPolicyID }
func (pvSingleQualifiedSelection) Version() semreg.SemanticVersion { return pvSelectionPolicyVersion }
func (pvSingleQualifiedSelection) Select(envelope semreg.FactEnvelope, evaluated []semreg.EvaluatedFact) (semreg.CandidateID, error) {
	if len(envelope.Conflicts) != 0 {
		return "", errors.New("PV fact is conflicted")
	}
	byID := make(map[semreg.CandidateID]semreg.EvaluatedFact, len(evaluated))
	for _, fact := range evaluated {
		byID[fact.CandidateID] = fact
	}
	var selected semreg.CandidateID
	for _, candidate := range envelope.Candidates {
		view, ok := byID[candidate.CandidateID]
		if !ok || candidate.Quality.Qualification != semreg.QualificationQualified || candidate.Quality.Promotion != semreg.PromotionPromoted || view.EffectiveAvailability != semreg.AvailabilityAvailable || view.Freshness != semreg.FreshnessFresh {
			continue
		}
		if selected != "" {
			return "", errors.New("PV fact has multiple selectable candidates")
		}
		selected = candidate.CandidateID
	}
	if selected == "" {
		return "", errors.New("PV fact has no selectable candidate")
	}
	return selected, nil
}

func pvProjectionItemID(nativeID string) semreg.DefinitionID {
	return semreg.DefinitionID("projection.gateway.pv." + nativeID)
}

func pvDimensionValue(mapping pvMappingSpec, asset semreg.AssetID) string {
	if strings.HasPrefix(mapping.dimensionRole, "phase:") {
		return mapping.dimensionRole
	}
	id := strings.TrimPrefix(string(asset), "pv-asset-")
	return mapping.dimensionRole + ":" + id
}

func pvPublicationPolicy(id semreg.PolicyID) (semreg.FreshnessPolicy, error) {
	policies := map[semreg.PolicyID]semreg.FreshnessPolicy{
		"pv.telemetry.fast.v1": {PolicyID: "pv.telemetry.fast.v1", Version: "1.0.0", FreshForNS: "30000000000", RetainForNS: "300000000000", MaxWallUncertaintyNS: "0"},
		"pv.accumulator.v1":    {PolicyID: "pv.accumulator.v1", Version: "1.0.0", FreshForNS: "900000000000", RetainForNS: "86400000000000", MaxWallUncertaintyNS: "0"},
		"pv.status.v1":         {PolicyID: "pv.status.v1", Version: "1.0.0", FreshForNS: "60000000000", RetainForNS: "600000000000", MaxWallUncertaintyNS: "0"},
	}
	policy, ok := policies[id]
	if !ok {
		return semreg.FreshnessPolicy{}, errors.New("PV freshness policy is unavailable")
	}
	if err := policy.Validate(); err != nil {
		return semreg.FreshnessPolicy{}, err
	}
	return policy, nil
}

func pvPublicationWall(at time.Time) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(at.UnixNano(), 10)), ClockID: "clock.utc", UncertaintyNS: "0"}
}

func pvPublicationMonotonic(elapsed time.Duration) semreg.MonotonicPoint {
	return semreg.MonotonicPoint{ClockEpochID: "clock-epoch:gateway-process", Nanoseconds: semreg.Uint64(strconv.FormatInt(elapsed.Nanoseconds(), 10))}
}

func pvEvidence(kind semreg.DefinitionID, digest semreg.Digest) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: "helianthus.modbusadapter", Kind: kind, Digest: digest, Contract: "helianthus.modbusreg/sunspec-qualification/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
}

func pvCoreHash(domain string, payload []byte) string {
	input := make([]byte, 0, len(domain)+1+len(payload))
	input = append(input, domain...)
	input = append(input, 0)
	input = append(input, payload...)
	sum := sha256.Sum256(input)
	return hex.EncodeToString(sum[:])
}

func pvCoreRawHash(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func pvCoreDecimal(number string) (semreg.Decimal, error) {
	negative := strings.HasPrefix(number, "-")
	unsigned := strings.TrimPrefix(number, "-")
	parts := strings.Split(unsigned, ".")
	if len(parts) > 2 || len(parts) == 0 || parts[0] == "" {
		return semreg.Decimal{}, &semreg.Error{ID: semreg.InvalidDecimal, Detail: "PV number"}
	}
	coefficient, exponent := parts[0], int32(0)
	if len(parts) == 2 {
		if parts[1] == "" {
			return semreg.Decimal{}, &semreg.Error{ID: semreg.InvalidDecimal, Detail: "PV number"}
		}
		coefficient += parts[1]
		exponent = -int32(len(parts[1]))
	}
	coefficient = strings.TrimLeft(coefficient, "0")
	if coefficient == "" {
		return semreg.Decimal{Coefficient: "0", Exponent10: 0}, nil
	}
	for strings.HasSuffix(coefficient, "0") {
		coefficient = strings.TrimSuffix(coefficient, "0")
		exponent++
	}
	if negative {
		coefficient = "-" + coefficient
	}
	decimal := semreg.Decimal{Coefficient: coefficient, Exponent10: exponent}
	if err := decimal.Validate(); err != nil {
		return semreg.Decimal{}, err
	}
	return decimal, nil
}

func incrementPVUint(value semreg.Uint64) semreg.Uint64 {
	n, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil || n == ^uint64(0) {
		return ""
	}
	return semreg.Uint64(strconv.FormatUint(n+1, 10))
}

func nextPVCandidateRevision(snapshot semreg.Snapshot, id semreg.CandidateID) semreg.Uint64 {
	for _, envelope := range snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			if candidate.CandidateID == id {
				return incrementPVUint(candidate.Revision)
			}
		}
	}
	return "1"
}
func nextPVServiceRevision(snapshot semreg.Snapshot, id semreg.ServiceInstanceID) semreg.Uint64 {
	for _, service := range snapshot.Services {
		if service.InstanceID == id {
			return incrementPVUint(service.Revision)
		}
	}
	return "1"
}
func nextPVCapabilityRevision(snapshot semreg.Snapshot, id semreg.CapabilityInstanceID) semreg.Uint64 {
	for _, capability := range snapshot.Capabilities {
		if capability.InstanceID == id {
			return incrementPVUint(capability.Revision)
		}
	}
	return "1"
}

func cloneJSON[T any](value T) (T, error) {
	var clone T
	raw, err := json.Marshal(value)
	if err != nil {
		return clone, err
	}
	if err := json.Unmarshal(raw, &clone); err != nil {
		return clone, err
	}
	return clone, nil
}
