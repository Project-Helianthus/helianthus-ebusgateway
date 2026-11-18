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

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semgraph"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-modbusreg/growatt"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	storage "github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/storage"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const (
	growattStoragePackID      semreg.DefinitionID    = "helianthus.pack.storage"
	growattStoragePackVersion semreg.SemanticVersion = "1.1.0"
)

const (
	growattStorageDriver  = "growatt_storage"
	growattStorageProfile = "growatt.bms.rs485.1xsxxp.v2_02.readonly.v1"
)

// growattStoragePackKind is the one resource kind the storage gate publishes:
// every fact it maps has the battery pack as its subject.
const growattStoragePackKind semreg.DefinitionID = "resource.battery_pack"

type growattStoragePublication struct {
	mu                  sync.RWMutex
	assetID             semreg.AssetID
	sourceID            semreg.SourceID
	hub                 *semhub.Hub
	handle              *semhub.Handle
	resources           *semanticResourceAllocator
	current             json.RawMessage
	currentReceivedAt   time.Time
	currentPublishedAt  time.Time
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

// growattStorageAssetSpec is the one declaration of the configured Storage
// asset; the startup preflight and the runtime declare the same spec.
func growattStorageAssetSpec(asset, source string) semhub.AssetSpec {
	return semhub.AssetSpec{AssetID: semreg.AssetID(asset), SourceID: semreg.SourceID(source), Driver: growattStorageDriver, Validators: []semreg.PackValidator{storage.New()}, Graph: semanticFamilyGraph(semanticFamilyStorage)}
}

// newGrowattStoragePublication binds one admitted Storage source generation
// to its declared asset. It registers the reprojection that withdraws the
// public view after an accepted control commit.
func newGrowattStoragePublication(declaration *semhub.Declaration, handle *semhub.Handle) (*growattStoragePublication, error) {
	if declaration == nil || handle == nil || handle.Source() != declaration.Source() {
		return nil, errors.New("growatt BMS semantic asset/source identity is invalid")
	}
	resources, err := semanticResourceAllocatorFor(declaration.Hub())
	if err != nil {
		return nil, err
	}
	p := &growattStoragePublication{assetID: declaration.Asset(), sourceID: declaration.Source(), hub: declaration.Hub(), handle: handle, resources: resources}
	declaration.SetReprojection(p.onControl)
	return p, nil
}

// growattStoragePackSlot is the operator slot of the configured pack. The
// family lists no identification scheme and the admitted reads carry no pack
// token, so the gate anchors the pack on an operator label: the label the
// operator configured for this storage. The slot is a domain-separated digest
// of that label, so the published anchor carries no configured text and stays
// within the slot bound whatever the label's length. Only the allocator's
// identifier ever reaches a fact key.
func growattStoragePackSlot(label semreg.AssetID) string {
	return growattStorageHash("helianthus.gateway.storage.pack_slot/v1", string(label))[:32]
}

// pack resolves the battery-pack resource of the configured label through the
// process's one resource allocator, homed in the publication's asset. The
// identifier depends on the label only, so a driver restart, a new source
// epoch, another source identifier or another unit number keep it.
func (p *growattStoragePublication) pack() (semreg.Resource, error) {
	if p.resources == nil {
		return semreg.Resource{}, errors.New("growatt BMS storage resource allocator unavailable")
	}
	slot := growattStoragePackSlot(p.assetID)
	evidence := growattStorageDigestEvidence("operator.storage.pack_label", slot)
	anchor := semgraph.Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: slot, Evidence: []semreg.EvidenceRef{evidence}}
	id, record, err := p.resources.resolve(growattStoragePackKind, anchor, p.assetID)
	if err != nil {
		return semreg.Resource{}, err
	}
	return semreg.Resource{ResourceID: id, Kind: growattStoragePackKind, Anchor: record, State: semreg.ResourceCurrent, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:semantic-storage-pack:" + growattStorageHash(string(growattStoragePackKind), slot)[:32]), Kind: semreg.OriginOperator, Evidence: []semreg.EvidenceRef{evidence}}, Revision: "1"}, nil
}

func (p *growattStoragePublication) onControl(semhub.ControlEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current, p.currentReceivedAt = nil, time.Time{}
	return nil
}

// Publish commits one sealed source-bound batch only after all field and
// evidence checks have completed. Any error occurs before the live kernel is
// replaced, preserving the last known good public projection.
func (p *growattStoragePublication) Publish(status growatt.BMSTypedReadOnlyStatus, evidence GrowattBMSRS485ObservationEvidence) (any, error) {
	if p == nil || p.hub == nil || p.handle == nil {
		return nil, errors.New("growatt BMS storage publication unavailable")
	}
	if err := validateGrowattStorageInput(status, evidence, p.assetID, p.sourceID); err != nil {
		return nil, err
	}
	pack, err := p.pack()
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	tx, err := p.hub.Begin(p.handle, p.assetID)
	if err != nil {
		return nil, err
	}
	defer tx.Abort()
	current, exists := tx.Current()
	sequence := tx.NextSequence()
	batch, manifest, requested, dispositions, err := growattStorageBatch(tx.Incarnation(), pack, status, evidence, sequence, current, exists, tx.SourceRetirements())
	if err != nil {
		return nil, err
	}
	snapshot, canonical, err := tx.Apply(batch, semreg.MonotonicPoint{ClockEpochID: semreg.ClockEpochID(evidence.ClockEpoch), Nanoseconds: semreg.Uint64(strconv.FormatInt(evidence.ReceiptMonotonic.Nanoseconds(), 10))})
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
	if _, err := tx.Commit(func(semhub.Committed) {
		p.current, p.currentReceivedAt, p.currentPublishedAt = append(json.RawMessage(nil), encoded...), evidence.ReceiptWall, time.Now()
		p.publicationSequence, _ = strconv.ParseUint(string(sequence), 10, 64)
	}); err != nil {
		return nil, err
	}
	return json.RawMessage(append([]byte(nil), encoded...)), nil
}

func (p *growattStoragePublication) Current(asset string) (json.RawMessage, bool) {
	if p == nil || semreg.AssetID(asset) != p.assetID {
		return nil, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.current) == 0 || !p.hub.Admitted(p.assetID) {
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
	return p.currentAtWithElapsed(asset, at, 0, false)
}

// currentAtWithElapsed is the CurrentAt implementation. The explicit elapsed
// seam lets the deterministic tests model Go's independent monotonic clock
// after a wall-clock step without introducing a second production clock.
func (p *growattStoragePublication) currentAtWithElapsed(asset string, at time.Time, elapsedOverride time.Duration, useElapsedOverride bool) (growattStorageCurrent, bool) {
	if p == nil || semreg.AssetID(asset) != p.assetID || at.IsZero() {
		return growattStorageCurrent{}, false
	}
	p.mu.RLock()
	raw, received := append(json.RawMessage(nil), p.current...), p.currentReceivedAt
	readable := p.hub != nil && p.hub.Admitted(p.assetID)
	p.mu.RUnlock()
	if !readable {
		return growattStorageCurrent{}, false
	}
	if len(raw) == 0 || received.IsZero() {
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
	// Both points retain Go's independent monotonic coordinate. Age starts at
	// the native evidence receipt, so publication delay cannot prolong freshness.
	elapsed := at.Sub(received)
	if useElapsedOverride {
		elapsed = elapsedOverride
	}
	if elapsed < 0 || elapsed == time.Duration(math.MaxInt64) || base < 0 || base > math.MaxInt64-int64(elapsed) {
		return growattStorageCurrent{}, false
	}
	// elapsed is deliberately computed from Go's monotonic coordinates above.
	// Serialize a comparable wall coordinate separately: a backward wall step
	// must not place evaluation before the immutable receipt wall floor.
	evaluationWall := at.UTC()
	receiptWall := received.UTC()
	if evaluationWall.UnixNano() < receiptWall.UnixNano() {
		evaluationWall = receiptWall
	}
	context := semreg.EvaluationContext{
		EvaluatedAt:       growattWall(evaluationWall),
		EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: current.Snapshot.EvaluateMonotonic.ClockEpochID, Nanoseconds: semreg.Uint64(strconv.FormatInt(base+int64(elapsed), 10))},
	}
	evaluation, err := semreg.EvaluateSnapshot(current.Snapshot, context)
	if err != nil {
		return growattStorageCurrent{}, false
	}
	current.Evaluation = evaluation
	return current, current.Projection.SnapshotID == current.Snapshot.SnapshotID
}

func validateGrowattStorageInput(status growatt.BMSTypedReadOnlyStatus, evidence GrowattBMSRS485ObservationEvidence, asset semreg.AssetID, source semreg.SourceID) error {
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

// growattStorageFirstIdentifiers returns the binding and service identifiers
// the first incarnation of a Growatt Storage source publishes. They depend
// only on the configured asset and source, so the startup preflight reserves
// them.
func growattStorageFirstIdentifiers(asset, source string) (semreg.NativeBindingID, semreg.ServiceInstanceID) {
	minter, err := semhub.NewLegacyMinter(semhub.LegacyGrowattStorage)
	if err != nil {
		return "", ""
	}
	binding := minter.Binding(semhub.Incarnation{Asset: semreg.AssetID(asset), Source: semreg.SourceID(source), First: true}, growattStorageProfile)
	return binding, growattStorageServiceID(semreg.AssetID(asset), binding)
}

// growattStorageServiceID is the Storage pack service identifier of one
// binding.
func growattStorageServiceID(asset semreg.AssetID, binding semreg.NativeBindingID) semreg.ServiceInstanceID {
	return semreg.ServiceInstanceID("service:semantic-storage:" + growattStorageHash(string(asset), string(binding), "pack")[:32])
}

// growattStorageBatch builds one publication of the configured pack. Every
// fact key, the identity link and the service name the pack resource; the
// resource record is carried only while the asset does not hold it current.
func growattStorageBatch(inc semhub.Incarnation, pack semreg.Resource, status growatt.BMSTypedReadOnlyStatus, evidence GrowattBMSRS485ObservationEvidence, sequence semreg.Uint64, current semreg.Snapshot, exists bool, retirements []semreg.SourceEpochID) (semreg.PublicationBatch, projection.ProjectionManifest, []projection.RequestedItem, []projection.ProjectionDisposition, error) {
	asset, source := inc.Asset, inc.Source
	if pack.Kind != growattStoragePackKind || pack.ResourceID.Validate() != nil {
		return semreg.PublicationBatch{}, projection.ProjectionManifest{}, nil, nil, errors.New("identity_missing_or_invalid")
	}
	packID := pack.ResourceID
	minter, err := semhub.NewLegacyMinter(semhub.LegacyGrowattStorage)
	if err != nil {
		return semreg.PublicationBatch{}, projection.ProjectionManifest{}, nil, nil, err
	}
	bindingID := minter.Binding(inc, growattStorageProfile)
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
	requested, dispositions := growattStorageDispositions(status, asset, packID, bindingID, source, epoch, generation, evidenceRef, wall, mono)
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
		IdentityLinkUpserts: []semreg.IdentityLink{{AssetID: asset, BindingID: bindingID, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidenceRef}, Revision: "1", Resource: &packID}}, FactUpserts: facts,
		ServiceUpserts:    []semreg.ServiceInstance{{InstanceID: growattStorageServiceID(asset, bindingID), AssetID: asset, Definition: semreg.DefinitionRef{Pack: semreg.PackRef{ID: growattStoragePackID, Version: growattStoragePackVersion}, ID: "storage.service.pack", Version: growattStoragePackVersion}, BindingID: bindingID, SourceEpochID: epoch, DriverGeneration: generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "1", Resource: &packID}},
		CapabilityUpserts: []semreg.CapabilityInstance{}, SourceRetirements: append([]semreg.SourceEpochID{}, retirements...), FactWithdrawals: []semreg.CandidateID{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
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
	if exists && growattStorageHasSourceEpoch(current, source, epoch) {
		// One source epoch has one descriptor, upserted by its first publication.
		// A successor generation of the same epoch (RESTART, STOP then START)
		// mints a new binding but must not upsert the descriptor again: its start
		// would be the successor's receipt, and SemReg accepts a changed object
		// only at its current revision plus one, never at the new cursor's
		// sequence. A retired epoch stays refused by SemReg either way.
		batch.SourceUpserts = []semreg.SourceDescriptor{}
	}
	if exists && growattStorageHasBinding(current, bindingID) {
		// Identity and capability topology are stable for one configured asset.
		// Refreshes carry only newer source-bound facts; this avoids presenting a
		// binding replacement for an unchanged native observation route.
		batch.BindingUpserts = []semreg.NativeBinding{}
		batch.IdentityLinkUpserts = []semreg.IdentityLink{}
		batch.ServiceUpserts = []semreg.ServiceInstance{}
	}
	resources, err := growattStorageResourceUpserts(current, pack)
	if err != nil {
		return semreg.PublicationBatch{}, projection.ProjectionManifest{}, nil, nil, err
	}
	batch.ResourceUpserts = resources
	if status.OperatingState == growatt.BMSStateSoftStarting && exists {
		for _, envelope := range current.Facts {
			if envelope.Key.FactID != "storage.status.operating" {
				continue
			}
			// Another contributor's candidate of the same fact is not this
			// publisher's to withdraw: SemReg would reject the whole batch.
			for _, candidate := range envelope.Candidates {
				if semregCandidateOfSource(current, candidate, source) {
					batch.FactWithdrawals = append(batch.FactWithdrawals, candidate.CandidateID)
				}
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

// growattStorageResourceUpserts returns the pack record the batch must carry:
// none while the asset holds it current, the record at its next revision with
// its recorded anchor when the asset holds it otherwise, and the new record
// when the asset does not hold it. A current record is never rewritten.
func growattStorageResourceUpserts(current semreg.Snapshot, pack semreg.Resource) ([]semreg.Resource, error) {
	for _, held := range current.Resources {
		if held.ResourceID != pack.ResourceID {
			continue
		}
		if held.State == semreg.ResourceCurrent {
			return nil, nil
		}
		revision, err := strconv.ParseUint(string(held.Revision), 10, 64)
		if err != nil || revision == math.MaxUint64 {
			return nil, errors.New("growatt BMS storage pack revision is exhausted")
		}
		pack.Anchor, pack.Supersedes, pack.Revision = held.Anchor, held.Supersedes, semreg.Uint64(strconv.FormatUint(revision+1, 10))
		return []semreg.Resource{pack}, nil
	}
	return []semreg.Resource{pack}, nil
}

func growattStorageHasSourceEpoch(snapshot semreg.Snapshot, source semreg.SourceID, epoch semreg.SourceEpochID) bool {
	for _, existing := range snapshot.Sources {
		if existing.SourceID == source && existing.SourceEpochID == epoch {
			return true
		}
	}
	return false
}

// semregCandidateOfSource reports whether a candidate of snapshot belongs to
// source. Its binding is resolved against the snapshot's bindings and its
// source epoch against the snapshot's source descriptors. The candidate
// belongs to source only when at least one of them resolves and every one
// that resolves names source, so a candidate whose source cannot be resolved
// is never claimed.
func semregCandidateOfSource(snapshot semreg.Snapshot, candidate semreg.FactCandidate, source semreg.SourceID) bool {
	resolved := false
	if candidate.BindingID != nil {
		for _, binding := range snapshot.Bindings {
			if binding.BindingID == *candidate.BindingID {
				if binding.SourceID != source {
					return false
				}
				resolved = true
			}
		}
	}
	if candidate.SourceEpochID != nil {
		for _, descriptor := range snapshot.Sources {
			if descriptor.SourceEpochID == *candidate.SourceEpochID {
				if descriptor.SourceID != source {
					return false
				}
				resolved = true
			}
		}
	}
	return resolved
}

func growattStorageHasBinding(snapshot semreg.Snapshot, binding semreg.NativeBindingID) bool {
	for _, existing := range snapshot.Bindings {
		if existing.BindingID == binding {
			return true
		}
	}
	return false
}

type growattStorageDisposition struct {
	item      projection.ProjectionDisposition
	candidate []semreg.FactCandidate
}

func growattStorageDispositions(status growatt.BMSTypedReadOnlyStatus, asset semreg.AssetID, pack semreg.ResourceID, binding semreg.NativeBindingID, source semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64, ev semreg.EvidenceRef, wall semreg.TimePoint, mono semreg.MonotonicPoint) ([]projection.RequestedItem, []growattStorageDisposition) {
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
		candidate := growattStorageCandidate(asset, pack, binding, source, epoch, generation, ev, wall, mono, f.id, f.value, f.unit)
		out = append(out, growattStorageDisposition{item: projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: f.id, Outcome: f.outcome, SourceKeys: []semreg.FactKey{candidate.Key}, Loss: growattStorageLoss(f.loss, f.nativeID, f.description)}, candidate: []semreg.FactCandidate{candidate}})
	}
	requested = append(requested, projection.RequestedItem{Kind: projection.ItemFact, ItemID: "storage.status.operating"})
	if status.OperatingState == growatt.BMSStateSoftStarting {
		reason := semreg.DefinitionID("unsupported_or_withheld")
		out = append(out, growattStorageDisposition{item: projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: "storage.status.operating", Outcome: projection.ProjectionWithheld, Reason: &reason, SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{{Kind: projection.LossSymbol, SourceItems: []semreg.DefinitionID{"native.growatt.bms.rs485.v202.operating_state"}, Description: "soft_starting is withheld"}}}})
	} else {
		token := "active"
		if status.OperatingState == growatt.BMSStateStandby {
			token = "standby"
		}
		candidate := growattStorageSymbolCandidate(asset, pack, binding, source, epoch, generation, ev, wall, mono, "storage.status.operating", token)
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

func growattStorageCandidate(asset semreg.AssetID, pack semreg.ResourceID, binding semreg.NativeBindingID, source semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64, ev semreg.EvidenceRef, wall semreg.TimePoint, mono semreg.MonotonicPoint, id semreg.DefinitionID, value float64, unit semreg.DefinitionID) semreg.FactCandidate {
	number := growattDecimal(value)
	key := growattStorageKey(pack, id)
	v := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: number, Unit: unit}}
	return growattStorageFact(asset, binding, source, epoch, generation, ev, wall, mono, id, key, v)
}
func growattStorageSymbolCandidate(asset semreg.AssetID, pack semreg.ResourceID, binding semreg.NativeBindingID, source semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64, ev semreg.EvidenceRef, wall semreg.TimePoint, mono semreg.MonotonicPoint, id semreg.DefinitionID, token string) semreg.FactCandidate {
	key := growattStorageKey(pack, id)
	v := semreg.Value{Kind: semreg.ValueSymbol, Symbol: &semreg.Symbol{Namespace: id, Token: token, Known: true}}
	return growattStorageFact(asset, binding, source, epoch, generation, ev, wall, mono, id, key, v)
}

// growattStorageKey is the key of one pack fact: its pack dimension value is
// the allocated identifier of the pack resource, never an asset, source or
// binding identifier.
func growattStorageKey(pack semreg.ResourceID, id semreg.DefinitionID) semreg.FactKey {
	dimension := string(pack)
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
	for strings.HasSuffix(coefficient, "0") && exponent < 18 {
		coefficient = strings.TrimSuffix(coefficient, "0")
		exponent++
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
	// Canonicalize only the digest copy; retained native evidence keeps its clock.
	if evidence, ok := value.(GrowattBMSRS485ObservationEvidence); ok {
		evidence.ReceiptWall = evidence.ReceiptWall.UTC()
		value = evidence
	}
	encoded, _ := json.Marshal(value)
	return growattStorageDigestEvidence(kind, string(encoded))
}
func growattStorageDigestEvidence(kind semreg.DefinitionID, value string) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: growattStoragePackID, Kind: kind, Digest: semreg.Digest("sha256:" + growattStorageHash(value)), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

var _ = mcp.GrowattBMSRS485V202Observation{}
var _ = fmt.Sprintf
