package main

// This file is the storage-specific SemReg boundary for the exact Growatt
// RS-485 V2.02 observer. It intentionally accepts only the completed typed
// observation plus its immutable transport evidence; it owns no transport,
// identity discovery, control, fallback, or compatibility projection.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	storage "github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/storage"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const (
	growattStoragePackID      semreg.DefinitionID    = "helianthus.pack.storage"
	growattStoragePackVersion semreg.SemanticVersion = "1.1.0"
)

type growattStoragePublication struct {
	mu                  sync.RWMutex
	assetID             semreg.AssetID
	sourceID            semreg.SourceID
	kernel              *semreg.PublicationKernel
	current             json.RawMessage
	currentReceivedAt   time.Time
	publicationSequence uint64
}

func validGrowattSemanticIdentity(value string) bool {
	return validGrowattAssetID(value) && validGrowattSourceID(value)
}

func validGrowattAssetID(value string) bool {
	return value == strings.TrimSpace(value) && semreg.AssetID(value).Validate() == nil
}

func validGrowattSourceID(value string) bool {
	return value == strings.TrimSpace(value) && semreg.SourceID(value).Validate() == nil
}

func newGrowattStoragePublication(asset, source string) (*growattStoragePublication, error) {
	if !validGrowattAssetID(asset) || !validGrowattSourceID(source) || asset == source {
		return nil, errors.New("growatt BMS semantic asset/source identity is invalid")
	}
	kernel, err := semreg.NewPublicationKernel(semreg.AssetID(asset), storage.New())
	if err != nil {
		return nil, err
	}
	return &growattStoragePublication{assetID: semreg.AssetID(asset), sourceID: semreg.SourceID(source), kernel: kernel}, nil
}

// Publish commits one sealed source-bound batch only after all field and
// evidence checks have completed. Any error occurs before the live kernel is
// replaced, preserving the last known good public projection.
func (p *growattStoragePublication) Publish(status modbusreg.GrowattBMSTypedReadOnlyStatus, evidence GrowattBMSRS485ObservationEvidence) (any, error) {
	if p == nil || p.kernel == nil {
		return nil, errors.New("growatt BMS storage publication unavailable")
	}
	if err := validateGrowattStorageInput(status, evidence, p.assetID, p.sourceID); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	staged, err := p.kernel.Fork()
	if err != nil {
		return nil, err
	}
	current, _, exists := staged.Current()
	sequence := p.publicationSequence + 1
	batch, manifest, requested, dispositions, err := growattStorageBatch(p.assetID, p.sourceID, status, evidence, semreg.Uint64(strconv.FormatUint(sequence, 10)), current, exists)
	if err != nil {
		return nil, err
	}
	snapshot, canonical, err := staged.Apply(batch, semreg.MonotonicPoint{ClockEpochID: semreg.ClockEpochID(evidence.ClockEpoch), Nanoseconds: semreg.Uint64(strconv.FormatInt(evidence.ReceiptMonotonic.Nanoseconds(), 10))})
	if err != nil {
		return nil, err
	}
	evaluation, err := semreg.EvaluateSnapshot(snapshot, semreg.EvaluationContext{EvaluatedAt: growattWall(evidence.ReceiptWall), EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: semreg.ClockEpochID(evidence.ClockEpoch), Nanoseconds: semreg.Uint64(strconv.FormatInt(evidence.ReceiptMonotonic.Nanoseconds(), 10))}})
	if err != nil {
		return nil, err
	}
	report, err := projection.Project(snapshot, manifest, requested, dispositions, nil)
	if err != nil {
		return nil, err
	}
	// Canonical bytes validate the staged snapshot locally; public consumers get
	// only the fixed detached SemReg projection shape shared by MCP, GraphQL,
	// and Portal.
	_ = canonical
	public := map[string]any{"snapshot": snapshot, "evaluation": evaluation, "selections": []semreg.Selection{}, "projection": report}
	encoded, err := json.Marshal(public)
	if err != nil {
		return nil, err
	}
	p.kernel, p.current, p.currentReceivedAt, p.publicationSequence = staged, append(json.RawMessage(nil), encoded...), evidence.ReceiptWall.UTC(), sequence
	return json.RawMessage(append([]byte(nil), encoded...)), nil
}

func (p *growattStoragePublication) Current(asset string) (json.RawMessage, bool) {
	if p == nil || semreg.AssetID(asset) != p.assetID {
		return nil, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.current) == 0 {
		return nil, false
	}
	return append(json.RawMessage(nil), p.current...), true
}

type growattStorageCurrent struct {
	Snapshot   semreg.Snapshot
	Evaluation semreg.EvaluationView
	Projection projection.ProjectionReport
}

// CurrentAt returns a detached current Storage tuple reevaluated at the one
// supplied scrape instant. It reads no transport and never changes publication
// state, revision, or the stored current bytes.
func (p *growattStoragePublication) CurrentAt(asset string, at time.Time) (growattStorageCurrent, bool) {
	if p == nil || semreg.AssetID(asset) != p.assetID || at.IsZero() {
		return growattStorageCurrent{}, false
	}
	p.mu.RLock()
	raw, received := append(json.RawMessage(nil), p.current...), p.currentReceivedAt
	p.mu.RUnlock()
	if len(raw) == 0 || received.IsZero() || at.Before(received) {
		return growattStorageCurrent{}, false
	}
	var current growattStorageCurrent
	if json.Unmarshal(raw, &current) != nil || current.Snapshot.SnapshotID == "" {
		return growattStorageCurrent{}, false
	}
	base, err := strconv.ParseInt(string(current.Snapshot.EvaluateMonotonic.Nanoseconds), 10, 64)
	if err != nil {
		return growattStorageCurrent{}, false
	}
	elapsed := at.Sub(received)
	if elapsed < 0 || elapsed == time.Duration(math.MaxInt64) || base < 0 || base > math.MaxInt64-int64(elapsed) {
		return growattStorageCurrent{}, false
	}
	context := semreg.EvaluationContext{
		EvaluatedAt:       growattWall(at),
		EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: current.Snapshot.EvaluateMonotonic.ClockEpochID, Nanoseconds: semreg.Uint64(strconv.FormatInt(base+int64(elapsed), 10))},
	}
	evaluation, err := semreg.EvaluateSnapshot(current.Snapshot, context)
	if err != nil {
		return growattStorageCurrent{}, false
	}
	current.Evaluation = evaluation
	return current, current.Projection.SnapshotID == current.Snapshot.SnapshotID
}

func validateGrowattStorageInput(status modbusreg.GrowattBMSTypedReadOnlyStatus, evidence GrowattBMSRS485ObservationEvidence, asset semreg.AssetID, source semreg.SourceID) error {
	// Precedence deliberately matches the accepted public mapping gate.
	if evidence.ObservationID == "" || evidence.ReceiptWall.IsZero() || len(evidence.Slices) != 4 {
		return errors.New("native_evidence_missing")
	}
	if status.Revision != growattBMSRS485Revision || status.OutboundAllowed() || evidence.OutboundAllowed || evidence.Qualification != "qualified" {
		return errors.New("revision_or_unit_or_slice_invalid")
	}
	if !validGrowattAssetID(string(asset)) || !validGrowattSourceID(string(source)) || asset == semreg.AssetID(source) {
		return errors.New("identity_missing_or_invalid")
	}
	if evidence.SourceID != string(source) || evidence.SourceEpoch == "" || evidence.DriverGeneration == 0 || evidence.ClockEpoch == "" || evidence.ReceiptMonotonic < 0 {
		return errors.New("lifecycle_missing")
	}
	for index, expected := range []struct{ offset, quantity uint16 }{{1, 7}, {13, 29}, {256, 12}, {269, 2}} {
		s := evidence.Slices[index]
		if s.UnitID == 0 || s.UnitID > 247 || uint16(s.Offset) != expected.offset || s.Quantity != expected.quantity || len(s.Words) != int(expected.quantity) || s.RequestID == 0 || s.TransportGeneration == 0 || s.RequestADUHex == "" || s.ResponseADUHex == "" {
			return errors.New("revision_or_unit_or_slice_invalid")
		}
	}
	if status.NativeObservation().UnitID() != evidence.Slices[0].UnitID {
		return errors.New("revision_or_unit_or_slice_invalid")
	}
	return nil
}

func growattStorageBatch(asset semreg.AssetID, source semreg.SourceID, status modbusreg.GrowattBMSTypedReadOnlyStatus, evidence GrowattBMSRS485ObservationEvidence, sequence semreg.Uint64, current semreg.Snapshot, exists bool) (semreg.PublicationBatch, projection.ProjectionManifest, []projection.RequestedItem, []projection.ProjectionDisposition, error) {
	bindingID := semreg.NativeBindingID("binding:semantic-storage:" + growattStorageHash(string(asset), string(source), "growatt.bms.rs485.1xsxxp.v2_02.readonly.v1")[:32])
	epoch := semreg.SourceEpochID(evidence.SourceEpoch)
	generation := semreg.Uint64(strconv.FormatUint(evidence.DriverGeneration, 10))
	expected := semreg.Uint64("0")
	if exists {
		expected = current.Revisions.Semantic
		if sequence == "0" {
			return semreg.PublicationBatch{}, projection.ProjectionManifest{}, nil, nil, errors.New("lifecycle_missing")
		}
	}
	evidenceRef := growattStorageEvidence("native.growatt.bms.rs485.v202.observation", evidence)
	registryRef := growattStorageDigestEvidence("registry.growatt.bms.rs485.v202.mapping", "storage.mapping.growatt.rs485.1xsxxp.v202")
	nativeRef := growattStorageDigestEvidence("native.growatt.bms.rs485.v202.binding", string(bindingID))
	wall := growattWall(evidence.ReceiptWall)
	mono := semreg.MonotonicPoint{ClockEpochID: semreg.ClockEpochID(evidence.ClockEpoch), Nanoseconds: semreg.Uint64(strconv.FormatInt(evidence.ReceiptMonotonic.Nanoseconds(), 10))}
	manifest := projection.ProjectionManifest{TargetID: "target:gateway-semantic-storage", TargetVersion: "1.0.0", KernelVersion: semreg.ContractKernelV1, PackVersions: []semreg.PackRef{{ID: growattStoragePackID, Version: growattStoragePackVersion}}, MappingRevision: "1"}
	requested, dispositions := growattStorageDispositions(status, asset, bindingID, source, epoch, generation, evidenceRef, wall, mono)
	facts := make([]semreg.FactCandidate, 0, len(dispositions))
	for _, d := range dispositions {
		facts = append(facts, d.candidate...)
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].CandidateID < facts[j].CandidateID })
	sort.Slice(requested, func(i, j int) bool { return requested[i].ItemID < requested[j].ItemID })
	sort.Slice(dispositions, func(i, j int) bool { return dispositions[i].item.ItemID < dispositions[j].item.ItemID })
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:semantic-storage:" + growattStorageHash(evidence.ObservationID)[:32]), AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: sequence, ExpectedSemanticRevision: expected, ObservedAt: wall,
		SourceUpserts:       []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "modbus_rtu", ProfileID: "growatt.bms.rs485.1xsxxp.v2_02.readonly.v1", ProfileVersion: "1", RegistryEvidence: registryRef, StartedAt: wall, State: semreg.SourceCurrent, Revision: "1"}},
		BindingUpserts:      []semreg.NativeBinding{{BindingID: bindingID, AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: nativeRef, State: semreg.BindingCurrent, Revision: "1"}},
		IdentityLinkUpserts: []semreg.IdentityLink{{AssetID: asset, BindingID: bindingID, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidenceRef}, Revision: "1"}}, FactUpserts: facts,
		ServiceUpserts:    []semreg.ServiceInstance{{InstanceID: semreg.ServiceInstanceID("service:semantic-storage:" + growattStorageHash(string(asset), string(bindingID), "pack")[:32]), AssetID: asset, Definition: semreg.DefinitionRef{Pack: semreg.PackRef{ID: growattStoragePackID, Version: growattStoragePackVersion}, ID: "storage.service.pack", Version: growattStoragePackVersion}, BindingID: bindingID, SourceEpochID: epoch, DriverGeneration: generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "1"}},
		CapabilityUpserts: []semreg.CapabilityInstance{}, SourceRetirements: []semreg.SourceEpochID{}, FactWithdrawals: []semreg.CandidateID{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	// Every object carried by a later observation advances with the observation
	// revision. A repeated fixed revision would let a concurrent caller replace
	// a newer lifecycle state, so the SemReg kernel correctly rejects it.
	for i := range batch.SourceUpserts {
		batch.SourceUpserts[i].Revision = sequence
	}
	for i := range batch.BindingUpserts {
		batch.BindingUpserts[i].Revision = sequence
	}
	for i := range batch.IdentityLinkUpserts {
		batch.IdentityLinkUpserts[i].Revision = sequence
	}
	for i := range batch.FactUpserts {
		batch.FactUpserts[i].Revision = sequence
	}
	for i := range batch.ServiceUpserts {
		batch.ServiceUpserts[i].Revision = sequence
	}
	if exists {
		// Identity and capability topology are stable for one configured asset.
		// Refreshes carry only newer source-bound facts; this avoids presenting a
		// source/binding replacement for an unchanged native observation route.
		batch.SourceUpserts = []semreg.SourceDescriptor{}
		batch.BindingUpserts = []semreg.NativeBinding{}
		batch.IdentityLinkUpserts = []semreg.IdentityLink{}
		batch.ServiceUpserts = []semreg.ServiceInstance{}
	}
	if status.OperatingState == modbusreg.GrowattBMSStateSoftStarting && exists {
		for _, envelope := range current.Facts {
			if envelope.Key.FactID != "storage.status.operating" {
				continue
			}
			for _, candidate := range envelope.Candidates {
				batch.FactWithdrawals = append(batch.FactWithdrawals, candidate.CandidateID)
			}
		}
		sort.Slice(batch.FactWithdrawals, func(i, j int) bool { return batch.FactWithdrawals[i] < batch.FactWithdrawals[j] })
	}
	digest, err := batch.ComputedDigest()
	if err != nil {
		return semreg.PublicationBatch{}, projection.ProjectionManifest{}, nil, nil, err
	}
	batch.BatchDigest = digest
	return batch, manifest, requested, growattStorageProjectionDispositions(dispositions), nil
}

type growattStorageDisposition struct {
	item      projection.ProjectionDisposition
	candidate []semreg.FactCandidate
}

func growattStorageDispositions(status modbusreg.GrowattBMSTypedReadOnlyStatus, asset semreg.AssetID, binding semreg.NativeBindingID, source semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64, ev semreg.EvidenceRef, wall semreg.TimePoint, mono semreg.MonotonicPoint) ([]projection.RequestedItem, []growattStorageDisposition) {
	type field struct {
		id          semreg.DefinitionID
		nativeID    semreg.DefinitionID
		value       float64
		unit        semreg.DefinitionID
		outcome     projection.ProjectionOutcome
		loss        projection.LossKind
		description string
	}
	fields := []field{
		{"storage.capacity.charge", "native.growatt.bms.rs485.v202.cumulative_charge_amp_hours", status.CumulativeChargeAmpHours, "unit.ampere_hour", projection.ProjectionTransformed, projection.LossPolicy, "counter continuity/reset/wrap remains native evidence"},
		{"storage.capacity.discharge", "native.growatt.bms.rs485.v202.cumulative_discharge_amp_hours", status.CumulativeDischargeAmpHours, "unit.ampere_hour", projection.ProjectionTransformed, projection.LossPolicy, "counter continuity/reset/wrap remains native evidence"},
		{"storage.pack.current", "native.growatt.bms.rs485.v202.pack_current_amps", status.PackCurrentAmps, "unit.ampere", projection.ProjectionTransformed, projection.LossProvenance, "native current sign reference is retained as provenance"},
		{"storage.pack.voltage", "", status.PackVoltageVolts, "unit.volt", projection.ProjectionExact, "", ""},
		{"storage.state.soc", "", float64(status.SOCPercent), "unit.percent", projection.ProjectionExact, "", ""},
		{"storage.temperature.pack", "", float64(status.TemperatureCelsius), "unit.celsius", projection.ProjectionExact, "", ""},
	}
	requested := make([]projection.RequestedItem, 0, 7)
	out := make([]growattStorageDisposition, 0, 7)
	for _, f := range fields {
		requested = append(requested, projection.RequestedItem{Kind: projection.ItemFact, ItemID: f.id})
		candidate := growattStorageCandidate(asset, binding, source, epoch, generation, ev, wall, mono, f.id, f.value, f.unit)
		out = append(out, growattStorageDisposition{item: projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: f.id, Outcome: f.outcome, SourceKeys: []semreg.FactKey{candidate.Key}, Loss: growattStorageLoss(f.loss, f.nativeID, f.description)}, candidate: []semreg.FactCandidate{candidate}})
	}
	requested = append(requested, projection.RequestedItem{Kind: projection.ItemFact, ItemID: "storage.status.operating"})
	if status.OperatingState == modbusreg.GrowattBMSStateSoftStarting {
		reason := semreg.DefinitionID("unsupported_or_withheld")
		out = append(out, growattStorageDisposition{item: projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: "storage.status.operating", Outcome: projection.ProjectionWithheld, Reason: &reason, SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{{Kind: projection.LossSymbol, SourceItems: []semreg.DefinitionID{"native.growatt.bms.rs485.v202.operating_state"}, Description: "soft_starting is withheld"}}}})
	} else {
		token := "active"
		if status.OperatingState == modbusreg.GrowattBMSStateStandby {
			token = "standby"
		}
		candidate := growattStorageSymbolCandidate(asset, binding, source, epoch, generation, ev, wall, mono, "storage.status.operating", token)
		out = append(out, growattStorageDisposition{item: projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: "storage.status.operating", Outcome: projection.ProjectionTransformed, SourceKeys: []semreg.FactKey{candidate.Key}, Loss: []projection.LossDetail{{Kind: projection.LossSymbol, SourceItems: []semreg.DefinitionID{"native.growatt.bms.rs485.v202.operating_state"}, Description: "charging and discharging collapse to active"}}}, candidate: []semreg.FactCandidate{candidate}})
	}
	return requested, out
}

func growattStorageProjectionDispositions(in []growattStorageDisposition) []projection.ProjectionDisposition {
	out := make([]projection.ProjectionDisposition, 0, len(in))
	for _, value := range in {
		out = append(out, value.item)
	}
	return out
}
func growattStorageLoss(kind projection.LossKind, nativeID semreg.DefinitionID, description string) []projection.LossDetail {
	if kind == "" {
		return []projection.LossDetail{}
	}
	return []projection.LossDetail{{Kind: kind, SourceItems: []semreg.DefinitionID{nativeID}, Description: description}}
}

func growattStorageCandidate(asset semreg.AssetID, binding semreg.NativeBindingID, source semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64, ev semreg.EvidenceRef, wall semreg.TimePoint, mono semreg.MonotonicPoint, id semreg.DefinitionID, value float64, unit semreg.DefinitionID) semreg.FactCandidate {
	number := growattDecimal(value)
	key := growattStorageKey(asset, id)
	v := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: number, Unit: unit}}
	return growattStorageFact(asset, binding, source, epoch, generation, ev, wall, mono, id, key, v)
}
func growattStorageSymbolCandidate(asset semreg.AssetID, binding semreg.NativeBindingID, source semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64, ev semreg.EvidenceRef, wall semreg.TimePoint, mono semreg.MonotonicPoint, id semreg.DefinitionID, token string) semreg.FactCandidate {
	key := growattStorageKey(asset, id)
	v := semreg.Value{Kind: semreg.ValueSymbol, Symbol: &semreg.Symbol{Namespace: id, Token: token, Known: true}}
	return growattStorageFact(asset, binding, source, epoch, generation, ev, wall, mono, id, key, v)
}
func growattStorageKey(asset semreg.AssetID, id semreg.DefinitionID) semreg.FactKey {
	dimension := string(asset)
	return semreg.FactKey{PackID: growattStoragePackID, PackVersion: growattStoragePackVersion, FactID: id, Dimensions: []semreg.Dimension{{ID: "storage.dimension.pack", Value: semreg.Value{Kind: semreg.ValueText, Text: &dimension}}}}
}
func growattStorageFact(asset semreg.AssetID, binding semreg.NativeBindingID, source semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64, ev semreg.EvidenceRef, wall semreg.TimePoint, mono semreg.MonotonicPoint, id semreg.DefinitionID, key semreg.FactKey, value semreg.Value) semreg.FactCandidate {
	hash := growattStorageHash(string(asset), string(binding), string(id))
	return semreg.FactCandidate{CandidateID: semreg.CandidateID("candidate:semantic-storage:" + hash[:32]), Key: key, Value: &value, Quality: semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}}, Times: semreg.Times{ReceivedAt: wall, ReceiptMonotonic: mono, EvaluatedAt: wall, EvaluateMonotonic: mono}, FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:gateway-storage-native-receipt", Version: "1.0.0", FreshForNS: "60000000000", RetainForNS: "300000000000", MaxWallUncertaintyNS: "0"}, BindingID: &binding, SourceEpochID: &epoch, DriverGeneration: &generation, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:semantic-storage:" + hash[:32]), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{ev}}, Evidence: []semreg.EvidenceRef{ev}, Revision: "1"}
}
func growattWall(value time.Time) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(value.UTC().UnixNano(), 10)), ClockID: "wall.utc", UncertaintyNS: "0"}
}
func growattDecimal(value float64) semreg.Decimal {
	raw := strconv.FormatFloat(value, 'f', -1, 64)
	negative := strings.HasPrefix(raw, "-")
	raw = strings.TrimPrefix(raw, "-")
	parts := strings.Split(raw, ".")
	coefficient := strings.TrimLeft(parts[0], "0")
	exponent := int32(0)
	if len(parts) == 2 {
		coefficient += parts[1]
		exponent = -int32(len(parts[1]))
	}
	coefficient = strings.TrimLeft(coefficient, "0")
	if coefficient == "" {
		return semreg.Decimal{Coefficient: "0", Exponent10: 0}
	}
	if negative {
		coefficient = "-" + coefficient
	}
	return semreg.Decimal{Coefficient: coefficient, Exponent10: exponent}
}
func growattStorageHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}
func growattStorageEvidence(kind semreg.DefinitionID, value any) semreg.EvidenceRef {
	encoded, _ := json.Marshal(value)
	return growattStorageDigestEvidence(kind, string(encoded))
}
func growattStorageDigestEvidence(kind semreg.DefinitionID, value string) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: growattStoragePackID, Kind: kind, Digest: semreg.Digest("sha256:" + growattStorageHash(value)), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

var _ = mcp.GrowattBMSRS485V202Observation{}
var _ = fmt.Sprintf
