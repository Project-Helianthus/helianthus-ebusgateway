package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semgraph"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-modbusreg/huawei"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	elementcatalog "github.com/Project-Helianthus/helianthus-semreg/semreg/v1/graph"
	pv "github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
)

var huaweiMonotonicOrigin = time.Now()

const huaweiPVPack semreg.DefinitionID = "helianthus.pack.pv"

// The configured label, class and routed unit are the complete asset key.
// Every field, including the domain, ends in NUL under the public contract.
func huaweiAssetID(label, class string, unit byte) semreg.AssetID {
	return semreg.AssetID(class + "-asset-" + huaweiHash("helianthus.modbus.asset.v1", label, class, strconv.Itoa(int(unit)))[:32])
}
func huaweiHash(fields ...string) string {
	hash := sha256.New()
	for _, field := range fields {
		_, _ = hash.Write([]byte(field))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}
func huaweiEvidence(kind string, value any) semreg.EvidenceRef {
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return semreg.EvidenceRef{Owner: huaweiPVPack, Kind: semreg.DefinitionID(kind), Digest: semreg.Digest("sha256:" + hex.EncodeToString(sum[:])), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

// Native observations remain in memory with their original group evidence.
// The public status adapter exposes only categorical routing and asset IDs.
type huaweiPVPublication struct {
	mu     sync.Mutex
	hub    *semhub.Hub
	class  *semhub.Class
	handle *semhub.Handle
	label  string
	active func() bool
	fenced bool
	// retireMu serializes epoch retirement. Stop and a failed startup's
	// retirement can fence one generation concurrently, and Stop retries the
	// fence; retired records the single successful retirement they share.
	retireMu sync.Mutex
	retired  bool
	origin   time.Time
	native   huawei.ProviderCycle
	// latest is the most recent cycle offered to Publish. It is stored under
	// mu with the published assets so status reads one coherent pair.
	latest       huawei.ProviderCycle
	published    map[byte]semreg.AssetID
	incarnations map[byte]uint64
	// refused names, per routed unit, why the publication refused a child the
	// provider admitted; a later successful admission clears the entry.
	refused map[byte]string
	// resources is the process's resource allocator.
	resources *semanticResourceAllocator
	// replaced marks a child the native owner withheld because another unit
	// answered at it; its next admission is a new inverter.
	replaced map[byte]bool
	// predecessor is the epoch this publication succeeded, the only earlier
	// generation whose retained observations an asset keeps.
	predecessor semreg.SourceEpochID
	// capacity records each explicit withdrawal of an older generation's
	// retained observations, in commit order, for the driver status.
	capacity []huaweiRetainedWithdrawal
	// inverters holds, per routed unit, the inverter resource whose child
	// batch committed and is still published: the only far ends the logger
	// may relay.
	inverters map[byte]semreg.ResourceID
	// relaysReason names why the logger's relays instances are withheld; it
	// is empty while they are published.
	relaysReason string
}

// huaweiRetainedGenerations is the number of generations an asset keeps:
// the current generation and the one before it.
const huaweiRetainedGenerations = 2

// huaweiRetainedWithdrawal is one committed withdrawal of an older
// generation's retained observations from one asset.
type huaweiRetainedWithdrawal struct {
	AssetID       semreg.AssetID
	SourceEpochID semreg.SourceEpochID
	Generation    semreg.Uint64
	Count         int
	Reason        string
}

func (p *huaweiPVPublication) retainedWithdrawals() []huaweiRetainedWithdrawal {
	return append([]huaweiRetainedWithdrawal(nil), p.capacity...)
}

// capacityWithdrawals selects the retained observations of the older
// generations that must go for the publishing generation to fit the
// per-asset retention window. Every retained generation of the asset counts
// toward the window, whichever source holds it, because the reservation is
// shared by every contributor. Only this publisher's own generations are ever
// withdrawn: another contributor's retained observations are not its to
// withdraw, and SemReg would reject the batch. When the own generations
// cannot free enough room the admission is refused and nothing is withdrawn.
// Admitting a generation inside the window is never done by raising the
// reservation.
func (p *huaweiPVPublication) capacityWithdrawals(asset semreg.AssetID, current semreg.Snapshot, epoch semreg.SourceEpochID, generation semreg.Uint64) ([]semreg.CandidateID, []huaweiRetainedWithdrawal, error) {
	type key struct {
		epoch      semreg.SourceEpochID
		generation semreg.Uint64
	}
	type group struct {
		own   []semreg.CandidateID
		other int
	}
	all := map[key]*group{}
	for _, record := range current.Retained {
		candidate := record.Candidate
		if candidate.SourceEpochID == nil || candidate.DriverGeneration == nil {
			continue
		}
		k := key{*candidate.SourceEpochID, *candidate.DriverGeneration}
		if k.epoch == epoch && k.generation == generation {
			continue
		}
		g := all[k]
		if g == nil {
			g = &group{}
			all[k] = g
		}
		if semregCandidateOfSource(current, candidate, p.handle.Source()) {
			g.own = append(g.own, candidate.CandidateID)
		} else {
			g.other++
		}
	}
	excess := len(all) + 1 - huaweiRetainedGenerations
	if excess <= 0 {
		return nil, nil, nil
	}
	groups := map[key][]semreg.CandidateID{}
	for k, g := range all {
		if len(g.own) > 0 && g.other == 0 {
			groups[k] = g.own
		}
	}
	if len(groups) < excess {
		return nil, nil, fmt.Errorf("%w: %d retained generations, %d withdrawable", errHuaweiRetainedCapacityExhausted, len(all)+1, len(groups))
	}
	started := map[semreg.SourceEpochID]int64{}
	for _, source := range current.Sources {
		if source.SourceID == p.handle.Source() {
			started[source.SourceEpochID], _ = strconv.ParseInt(string(source.StartedAt.UnixNanoseconds), 10, 64)
		}
	}
	order := make([]key, 0, len(groups))
	for k := range groups {
		order = append(order, k)
	}
	// Newest first: the predecessor epoch, then later epoch start, then the
	// higher generation inside one epoch.
	sort.Slice(order, func(i, j int) bool {
		a, b := order[i], order[j]
		if (a.epoch == p.predecessor) != (b.epoch == p.predecessor) {
			return a.epoch == p.predecessor
		}
		if started[a.epoch] != started[b.epoch] {
			return started[a.epoch] > started[b.epoch]
		}
		if a.epoch != b.epoch {
			return a.epoch > b.epoch
		}
		ga, _ := strconv.ParseUint(string(a.generation), 10, 64)
		gb, _ := strconv.ParseUint(string(b.generation), 10, 64)
		return ga > gb
	})
	var ids []semreg.CandidateID
	var records []huaweiRetainedWithdrawal
	seen := map[semreg.CandidateID]bool{}
	for _, k := range order[len(order)-excess:] {
		count := 0
		for _, id := range groups[k] {
			count++
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		records = append(records, huaweiRetainedWithdrawal{AssetID: asset, SourceEpochID: k.epoch, Generation: k.generation, Count: count, Reason: "capacity"})
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].SourceEpochID != records[j].SourceEpochID {
			return records[i].SourceEpochID < records[j].SourceEpochID
		}
		return records[i].Generation < records[j].Generation
	})
	return ids, records, nil
}

// huaweiMaxAssetSources is the pinned kernel's per-asset bound on source
// descriptors. A retired epoch keeps its descriptor, so every managed epoch
// that published on an asset holds one slot of it for the asset's lifetime.
const huaweiMaxAssetSources = 32

var errHuaweiSourceCapacityExhausted = errors.New("huawei semantic source epoch capacity exhausted")

// huaweiMaxAssetBindings and huaweiMaxAssetIdentityLinks are the pinned
// kernel's per-asset bounds. Every re-admission of a child whose identity was
// lost adds one binding incarnation and one identity link, and neither is
// removed within the epoch.
const (
	huaweiMaxAssetBindings      = 128
	huaweiMaxAssetIdentityLinks = 128
)

var errHuaweiRetainedCapacityExhausted = errors.New("huawei semantic retained generation capacity exhausted")

var errHuaweiBindingCapacityExhausted = errors.New("huawei semantic binding capacity exhausted")

// huaweiRefusalBindingCapacity names, in the instance status, a child the
// publication refused because its re-admission would exceed the asset's bound.
const huaweiRefusalBindingCapacity = "binding_capacity"

// huaweiRefusalRetainedCapacity names a child whose successor publication could
// not free retained-generation room among the publisher's own generations.
const huaweiRefusalRetainedCapacity = "retained_capacity"

// huaweiSourceCapacity refuses a new epoch when an asset of this source already
// holds the kernel's source-descriptor bound: that epoch could never publish
// on the asset, so the START fails instead of running without publication.
func huaweiSourceCapacity(hub *semhub.Hub, source semreg.SourceID) error {
	for _, id := range hub.AssetIDs() {
		view, err := hub.Inspect(id)
		if err != nil {
			continue
		}
		if err := huaweiAssetSourceCapacity(view.Snapshot, source); err != nil {
			return err
		}
	}
	return nil
}

// huaweiAssetSourceCapacity refuses an asset that holds a descriptor of source
// and the kernel's source-descriptor bound. The bound is per asset and shared
// by every contributor, so every descriptor counts, whatever its source.
func huaweiAssetSourceCapacity(snapshot semreg.Snapshot, source semreg.SourceID) error {
	for _, descriptor := range snapshot.Sources {
		if descriptor.SourceID == source {
			if len(snapshot.Sources) >= huaweiMaxAssetSources {
				return errHuaweiSourceCapacityExhausted
			}
			break
		}
	}
	return nil
}

// huaweiIncarnationBindings is the number of bindings, and of identity links,
// one incarnation adds: the inverter path and the system path.
const huaweiIncarnationBindings = 2

// huaweiBindingCapacity refuses a new binding incarnation on an asset that
// cannot hold its bindings and identity links within the kernel's bounds.
// Both bounds are per asset and shared by every contributor, so every record
// counts, whatever its source.
func huaweiBindingCapacity(current semreg.Snapshot) error {
	if len(current.Bindings)+huaweiIncarnationBindings > huaweiMaxAssetBindings || len(current.IdentityLinks)+huaweiIncarnationBindings > huaweiMaxAssetIdentityLinks {
		return errHuaweiBindingCapacityExhausted
	}
	return nil
}

func newHuaweiPVPublication(hub *semhub.Hub, driver, source, label string, previous *semhub.Handle, active func() bool) (*huaweiPVPublication, error) {
	if hub == nil || !validModbusTCPLabel(label) || semhub.ValidateOpaque(source) != nil || active == nil {
		return nil, errors.New("huawei semantic identity unavailable")
	}
	if err := huaweiSourceCapacity(hub, semreg.SourceID(source)); err != nil {
		return nil, err
	}
	class, err := hub.DeclareClass(semhub.ClassSpec{Name: "huawei_pv", Driver: driver, Validators: func() []semreg.PackValidator { return []semreg.PackValidator{pv.New()} }, OnControl: func(semhub.ControlEvent) error { return nil }, Graph: semanticFamilyGraph(semanticFamilyLoggerPV)})
	if err != nil {
		return nil, err
	}
	resources, err := semanticResourceAllocatorFor(hub)
	if err != nil {
		return nil, err
	}
	var handle *semhub.Handle
	if previous == nil {
		handle, err = hub.AdmitSource(semhub.Admission{Source: semreg.SourceID(source), Driver: driver})
	} else {
		if previous.Source() != semreg.SourceID(source) {
			return nil, errors.New("huawei semantic source changed")
		}
		// A restart replaces the epoch: Fence retired the predecessor's epoch,
		// so a hub-native successor is admitted.
		handle, err = hub.AdmitSuccessor(semhub.Succession{Previous: previous, Operation: "lifecycle-op:" + huaweiHash(string(previous.Epoch()))[:32]})
	}
	if err != nil {
		return nil, err
	}
	publication := &huaweiPVPublication{hub: hub, class: class, handle: handle, label: label, active: active, origin: time.Now(), published: map[byte]semreg.AssetID{}, incarnations: map[byte]uint64{}, refused: map[byte]string{}, resources: resources, replaced: map[byte]bool{}, inverters: map[byte]semreg.ResourceID{}}
	if previous != nil {
		publication.predecessor = previous.Epoch()
	}
	return publication, nil
}
func (p *huaweiPVPublication) point(at time.Time) semreg.MonotonicPoint {
	elapsed := at.Sub(huaweiMonotonicOrigin)
	if elapsed < 0 {
		elapsed = 0
	}
	return semreg.MonotonicPoint{ClockEpochID: semreg.ClockEpochID("clock:huawei:" + huaweiHash(string(p.handle.Epoch()))[:32]), Nanoseconds: semreg.Uint64(strconv.FormatInt(elapsed.Nanoseconds(), 10))}
}
func (p *huaweiPVPublication) Fence() error {
	p.retireMu.Lock()
	defer p.retireMu.Unlock()
	p.mu.Lock()
	p.fenced = true
	observed := time.Now()
	if p.native.CompletedAt.After(observed) {
		observed = p.native.CompletedAt
	}
	p.mu.Unlock()
	if p.retired {
		return nil
	}
	// Retiring the stopped epoch withdraws its assets and lets a restart be
	// admitted under a successor epoch rather than inheriting this one.
	_, err := p.hub.RetireEpoch(semhub.RetireRequest{Source: p.handle.Source(), Epoch: p.handle.Epoch(), Reason: "lifecycle.source_stopped", Evidence: []semreg.EvidenceRef{huaweiEvidence("native.huawei.lifecycle", "managed epoch retirement")}, ObservedAt: growattWall(observed), Monotonic: p.point(observed)})
	if err == nil {
		p.retired = true
	}
	return err
}
func (p *huaweiPVPublication) Publish(cycle huawei.ProviderCycle) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latest = cycle
	if p.fenced || !p.active() {
		return errors.New("huawei publication generation withdrawn")
	}
	if cycle.Source != p.label {
		return errors.New("huawei instance source mismatch")
	}
	if cycle.Cycle == 0 || cycle.Generation == 0 || cycle.CompletedAt.IsZero() || cycle.StartedAt.After(cycle.CompletedAt) {
		return errors.New("huawei cycle lifecycle incomplete")
	}
	if cycle.Path != huawei.ProviderPathSmartLogger && cycle.Path != huawei.ProviderPathSDongle {
		return errors.New("huawei provider path not admitted")
	}
	if p.native.Generation != 0 && (cycle.Generation < p.native.Generation || cycle.Generation == p.native.Generation && cycle.Cycle <= p.native.Cycle) {
		return errors.New("huawei cycle replay refused")
	}
	seen := map[byte]bool{}
	var failures []error
	for _, child := range cycle.Children {
		if seen[child.UnitID] {
			return errors.New("huawei duplicate routed child")
		}
		seen[child.UnitID] = true
	}
	// Retain the exact typed native cycle even when a semantic field is withheld.
	p.native = cycle
	for _, child := range cycle.Children {
		admitted := child.Evidence != huawei.ProviderEvidenceWithheld && child.Reading.ProfileID() == child.ProfileID && child.Reading.UnitID() == child.UnitID && child.IdentifiedGeneration == cycle.Generation && child.Catalog != nil && child.Catalog.Outcome() == huawei.SUN2000CatalogSelected
		admitted = admitted && (cycle.Path == huawei.ProviderPathSmartLogger && child.ProfileID == huawei.SUN2000M3MonitoringProfileID || cycle.Path == huawei.ProviderPathSDongle && child.ProfileID == huawei.SUN2000M1MonitoringProfileID)
		if !admitted {
			if child.Reason == huawei.ProviderReasonIdentityMismatch && p.incarnations[child.UnitID] > 0 {
				p.replaced[child.UnitID] = true
			}
			if id, ok := p.published[child.UnitID]; ok {
				if err := p.publishChild(id, child, cycle, true); err != nil {
					failures = append(failures, err)
				} else {
					delete(p.published, child.UnitID)
				}
			}
			continue
		}
		id := huaweiAssetID(p.label, "pv", child.UnitID)
		if _, err := p.class.Declare(id, p.handle.Source()); err != nil {
			failures = append(failures, err)
			continue
		}
		if err := p.publishChild(id, child, cycle, false); err != nil {
			failures = append(failures, err)
			continue
		}
		p.published[child.UnitID] = id
	}
	// Only a completed enumeration can establish absence. Incomplete cycles
	// preserve last-known state and never convert an unanswered listing to empty.
	if cycle.Outcome == huawei.ProviderCycleComplete {
		var units []int
		for unit := range p.published {
			if !seen[unit] {
				units = append(units, int(unit))
			}
		}
		sort.Ints(units)
		for _, unit := range units {
			u := byte(unit)
			if err := p.publishChild(p.published[u], huawei.ProviderChildObservation{UnitID: u}, cycle, true); err != nil {
				failures = append(failures, err)
			} else {
				delete(p.published, u)
			}
		}
	}
	// The logger is published after its children, so it relays only the
	// inverters whose batches committed in this cycle or before.
	if err := p.publishLogger(cycle); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}
func huaweiNext(value semreg.Uint64) semreg.Uint64 {
	n, _ := strconv.ParseUint(string(value), 10, 64)
	return semreg.Uint64(strconv.FormatUint(n+1, 10))
}

// admitRetained frees retained-generation room for a unit and, when the own
// generations cannot, records the refusal so the instance status names it.
func (p *huaweiPVPublication) admitRetained(unit byte, asset semreg.AssetID, current semreg.Snapshot, epoch semreg.SourceEpochID, generation semreg.Uint64) ([]semreg.CandidateID, []huaweiRetainedWithdrawal, error) {
	ids, records, err := p.capacityWithdrawals(asset, current, epoch, generation)
	if err != nil {
		if errors.Is(err, errHuaweiRetainedCapacityExhausted) {
			p.refused[unit] = huaweiRefusalRetainedCapacity
		}
		return nil, nil, fmt.Errorf("unit %d: %w", unit, err)
	}
	return ids, records, nil
}

func (p *huaweiPVPublication) publishChild(asset semreg.AssetID, child huawei.ProviderChildObservation, cycle huawei.ProviderCycle, withdraw bool) error {
	if !p.active() {
		return errors.New("huawei publication generation withdrawn")
	}
	tx, err := p.hub.Begin(p.handle, asset)
	if err != nil {
		return err
	}
	defer tx.Abort()
	current, exists := tx.Current()
	expected := semreg.Uint64("0")
	if exists {
		expected = current.Revisions.Semantic
	}
	wall := growattWall(cycle.CompletedAt)
	mono := p.point(cycle.CompletedAt)
	evidence := huaweiEvidence("native.huawei.provider.observation", struct {
		Cycle      uint64
		Generation uint64
		Child      huawei.ProviderChildObservation
		Facts      []huawei.SUN2000Fact
		Groups     []huawei.SUN2000GroupReading
	}{cycle.Cycle, cycle.Generation, child, child.Reading.Facts(), child.Reading.Groups()})
	source, epoch, generation := p.handle.Source(), p.handle.Epoch(), p.handle.GenerationID()
	incarnation := p.incarnations[child.UnitID]
	if _, published := p.published[child.UnitID]; !withdraw && !published {
		incarnation++
	}
	// Identity withdrawal is terminal for a binding. Re-admission gets a new
	// native-binding incarnation while retaining the configured asset identity.
	// The binding of the inverter path observes the inverter and its inputs;
	// the binding of the system path observes the PV system the gate gives the
	// child, whose lifetime energy it carries.
	binding, err := semhub.NativeBindingID("huawei_pv", asset, source, child.ProfileID+":"+strconv.FormatUint(incarnation, 10), epoch, p.handle.Generation())
	if err != nil && !withdraw {
		return err
	}
	systemBinding, err := semhub.NativeBindingID("huawei_pv", asset, source, child.ProfileID+":"+strconv.FormatUint(incarnation, 10)+":"+huaweiSystemPath, epoch, p.handle.Generation())
	if err != nil && !withdraw {
		return err
	}
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:huawei-pv:" + huaweiHash(string(asset), string(epoch), string(generation), string(tx.NextSequence()))[:32]), AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: tx.NextSequence(), ExpectedSemanticRevision: expected, ObservedAt: wall,
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: append([]semreg.SourceEpochID{}, tx.SourceRetirements()...), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	for _, old := range tx.GenerationFences() {
		batch.GenerationFences = append(batch.GenerationFences, semreg.GenerationFence{SourceID: source, SourceEpochID: epoch, DriverGeneration: old, Reason: "lifecycle.driver_generation_replaced", Evidence: []semreg.EvidenceRef{evidence}, Revision: "1"})
	}
	var retainedIDs []semreg.CandidateID
	var capacity []huaweiRetainedWithdrawal
	var graph huaweiPVGraph
	replaced := false
	if !withdraw {
		if !growattStorageHasSourceEpoch(current, source, epoch) {
			batch.SourceUpserts = append(batch.SourceUpserts, semreg.SourceDescriptor{SourceID: source, SourceEpochID: epoch, ProtocolID: "modbus_tcp", ProfileID: "huawei.readonly.pv", ProfileVersion: "1", RegistryEvidence: huaweiEvidence("registry.huawei.mapping", "huawei-sun2000-pv-v1"), StartedAt: growattWall(p.origin), State: semreg.SourceCurrent, Revision: "1"})
		}
		replaced = p.replaced[child.UnitID]
		graph, err = p.graph(asset, child, binding, epoch, replaced)
		if err != nil {
			return err
		}
		paths := []struct {
			id       semreg.NativeBindingID
			path     string
			resource semreg.ResourceID
		}{{binding, huaweiInverterPath, graph.inverter}, {systemBinding, huaweiSystemPath, graph.system}}
		for _, path := range paths {
			hasBinding := false
			for _, old := range current.Bindings {
				if old.BindingID == path.id {
					if old.DriverGeneration != generation || old.SourceEpochID != epoch || old.State != semreg.BindingCurrent {
						return errors.New("huawei binding incarnation unavailable")
					}
					hasBinding = true
				}
			}
			if !hasBinding {
				// A new incarnation must fit the kernel's bounds; refuse before any
				// batch is applied so the asset and the siblings are left untouched.
				if len(batch.BindingUpserts) == 0 {
					if err := huaweiBindingCapacity(current); err != nil {
						p.refused[child.UnitID] = huaweiRefusalBindingCapacity
						return fmt.Errorf("unit %d: %w", child.UnitID, err)
					}
				}
				batch.BindingUpserts = append(batch.BindingUpserts, semreg.NativeBinding{BindingID: path.id, AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: huaweiEvidence("native.huawei.binding", struct {
					Asset   semreg.AssetID
					Source  semreg.SourceID
					Profile string
					Path    string
				}{asset, source, child.ProfileID, path.path}), State: semreg.BindingCurrent, Revision: "1"})
			}
			linkRevision := semreg.Uint64("1")
			updateLink := true
			for _, old := range current.IdentityLinks {
				if old.BindingID == path.id {
					linkRevision = huaweiNext(old.Revision)
					// An operator declaration replaces the publisher's own link;
					// the hub's pending set alone changes it.
					updateLink = !huaweiDeclaredLink(old) && (old.State != semreg.LinkQualified || old.Resource == nil || *old.Resource != path.resource)
				}
			}
			if updateLink {
				resource := path.resource
				batch.IdentityLinkUpserts = append(batch.IdentityLinkUpserts, semreg.IdentityLink{AssetID: asset, BindingID: path.id, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: linkRevision, Resource: &resource})
			}
		}
		sort.Slice(batch.BindingUpserts, func(i, j int) bool { return batch.BindingUpserts[i].BindingID < batch.BindingUpserts[j].BindingID })
		sort.Slice(batch.IdentityLinkUpserts, func(i, j int) bool {
			return batch.IdentityLinkUpserts[i].BindingID < batch.IdentityLinkUpserts[j].BindingID
		})
		batch.FactUpserts, err = p.facts(graph, binding, systemBinding, child, cycle, evidence, current)
		if err != nil {
			return err
		}
		retainedIDs, capacity, err = p.admitRetained(child.UnitID, asset, current, epoch, generation)
		if err != nil {
			return err
		}
		batch.FactWithdrawals = append(batch.FactWithdrawals, retainedIDs...)
		if replaced {
			// A changed unit fences every earlier reading of this source on the
			// asset: none of them is about the unit now at the child.
			for _, envelope := range current.Facts {
				for _, old := range envelope.Candidates {
					if semregCandidateOfSource(current, old, source) {
						batch.FactWithdrawals = append(batch.FactWithdrawals, old.CandidateID)
					}
				}
			}
		}
	}
	if withdraw {
		for _, old := range current.IdentityLinks {
			if old.State == semreg.LinkQualified && !huaweiDeclaredLink(old) && huaweiCurrentBinding(current, old.BindingID, epoch, generation) {
				old.State = semreg.LinkWithdrawn
				old.Revision = huaweiNext(old.Revision)
				batch.IdentityLinkUpserts = append(batch.IdentityLinkUpserts, old)
			}
		}
	}
	// The hub's pending operator identity declarations travel in this batch:
	// each replaces the publisher's own link of the binding it names. A
	// declaration that names a resource this binding does not observe, such
	// as an inverter retired by identity replacement, is not installed. The
	// hub accepts a batch that carries no operator link and leaves that
	// declaration pending, so the publisher's own link stays the one that
	// observes the resource the facts name.
	observed := map[semreg.NativeBindingID]semreg.ResourceID{}
	if !withdraw {
		observed[binding] = graph.inverter
		observed[systemBinding] = graph.system
	}
	batch.IdentityLinkUpserts = huaweiWithOperatorLinks(batch.IdentityLinkUpserts, huaweiApplicableOperatorLinks(tx.OperatorLinks(batch.BindingUpserts), observed))
	keep := map[semreg.CandidateID]bool{}
	for _, fact := range batch.FactUpserts {
		keep[fact.CandidateID] = true
	}
	for _, envelope := range current.Facts {
		for _, old := range envelope.Candidates {
			if !keep[old.CandidateID] && old.SourceEpochID != nil && *old.SourceEpochID == epoch && old.DriverGeneration != nil && *old.DriverGeneration == generation {
				batch.FactWithdrawals = append(batch.FactWithdrawals, old.CandidateID)
			}
		}
	}
	batch.FactWithdrawals = huaweiUniqueCandidates(batch.FactWithdrawals, keep)
	if err = p.relations(&batch, current, graph, binding, systemBinding, withdraw); err != nil {
		return err
	}
	if withdraw {
		batch.ResourceUpserts = huaweiResourceWithdrawals(current, batch)
	} else {
		batch.ResourceUpserts = huaweiResourceUpserts(current, graph, batch)
	}
	sort.Slice(batch.FactUpserts, func(i, j int) bool { return batch.FactUpserts[i].CandidateID < batch.FactUpserts[j].CandidateID })
	batch.BatchDigest, err = batch.ComputedDigest()
	if err != nil {
		return err
	}
	if _, _, err = tx.Apply(batch, mono); err != nil {
		return err
	}
	if !p.active() {
		return errors.New("huawei publication generation withdrawn")
	}
	_, err = tx.Commit(nil)
	if err == nil && !withdraw {
		p.incarnations[child.UnitID] = incarnation
		delete(p.refused, child.UnitID)
		delete(p.replaced, child.UnitID)
		p.resources.keepSlotOrdinal(huaweiKindInverter, huaweiChildSlot(p.label, child.UnitID), graph.ordinal)
		p.capacity = append(p.capacity, capacity...)
		p.inverters[child.UnitID] = graph.inverter
	}
	if err == nil && withdraw {
		delete(p.inverters, child.UnitID)
	}
	return err
}

// The resource kinds, relation definitions and binding paths of the
// logger-attached PV family under its re-based gate.
const (
	huaweiKindSystem   semreg.DefinitionID = "resource.pv_system"
	huaweiKindInverter semreg.DefinitionID = "resource.inverter"
	huaweiKindInput    semreg.DefinitionID = "resource.dc_input"
	huaweiInverterPath                     = "inverter"
	huaweiSystemPath                       = "pv_system"
)

var (
	huaweiRelationSystemInverter = semreg.DefinitionRef{Pack: semreg.PackRef{ID: huaweiPVPack, Version: "1.0.0"}, ID: "pv.relation.system_inverter", Version: "1.0.0"}
	huaweiRelationInverterInput  = semreg.DefinitionRef{Pack: semreg.PackRef{ID: huaweiPVPack, Version: "1.0.0"}, ID: "pv.relation.inverter_input", Version: "1.0.0"}
)

// huaweiPVGraph is the resource graph of one admitted child: the PV system
// the gate gives it, its inverter and the DC input at each published native
// string index. records holds every resource the child's home asset carries.
type huaweiPVGraph struct {
	system, inverter semreg.ResourceID
	ordinal          uint64
	inputs           map[uint16]semreg.ResourceID
	records          []semreg.Resource
}

// huaweiChildSlot is the operator slot base of a child: the configured
// instance label and the child entry the operator configured for it.
func huaweiChildSlot(label string, unit byte) string {
	return label + "/" + strconv.Itoa(int(unit))
}

// huaweiInputs lists the native string indexes whose voltage and current pair
// shows a present input. A zero pair is native evidence, not membership.
func huaweiInputs(child huawei.ProviderChildObservation) []uint16 {
	keyPrefix := "huawei.sun2000.m3."
	if child.ProfileID == huawei.SUN2000M1MonitoringProfileID {
		keyPrefix = "huawei.sun2000.m1."
	}
	seen := map[uint16]bool{}
	var out []uint16
	for _, native := range child.Reading.Facts() {
		index := uint16(native.Index)
		if native.Index <= 0 || seen[index] {
			continue
		}
		voltage, vok := child.Reading.Fact(keyPrefix+"pv_string_voltage", native.Index)
		current, cok := child.Reading.Fact(keyPrefix+"pv_string_current", native.Index)
		if !vok || !cok || voltage.Value.Coefficient == 0 && current.Value.Coefficient == 0 {
			continue
		}
		seen[index] = true
		out = append(out, index)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// graph allocates the child's resources. The system and the inverter are
// anchored on operator slots: the family has no listed identification scheme
// and its admitted read plans read no child token. A replaced unit moves the
// inverter slot to its next ordinal, so the inverter and its inputs get new
// identifiers while the system keeps its own.
func (p *huaweiPVPublication) graph(asset semreg.AssetID, child huawei.ProviderChildObservation, binding semreg.NativeBindingID, epoch semreg.SourceEpochID, replaced bool) (huaweiPVGraph, error) {
	base := huaweiChildSlot(p.label, child.UnitID)
	ordinal, err := p.resources.slotOrdinal(huaweiKindInverter, semreg.AnchorEquipment, base, replaced)
	if err != nil {
		return huaweiPVGraph{}, err
	}
	out := huaweiPVGraph{ordinal: ordinal, inputs: map[uint16]semreg.ResourceID{}}
	operator := func(kind semreg.DefinitionID, class semreg.AnchorClass, slot string) (semreg.ResourceID, error) {
		anchor := semgraph.Anchor{Class: class, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: slot, Evidence: []semreg.EvidenceRef{huaweiEvidence("operator.huawei.child_slot", slot)}}
		id, record, err := p.resources.resolve(kind, anchor, asset)
		if err != nil {
			return "", err
		}
		out.records = append(out.records, semreg.Resource{ResourceID: id, Kind: kind, Anchor: record, State: semreg.ResourceCurrent, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:huawei-pv-operator:" + huaweiHash(string(kind), slot)[:32]), Kind: semreg.OriginOperator, Evidence: []semreg.EvidenceRef{huaweiEvidence("operator.huawei.instance", p.label)}}, Revision: "1"})
		return id, nil
	}
	if out.system, err = operator(huaweiKindSystem, semreg.AnchorFunction, base); err != nil {
		return huaweiPVGraph{}, err
	}
	if out.inverter, err = operator(huaweiKindInverter, semreg.AnchorEquipment, semanticOrdinalSlot(base, ordinal)); err != nil {
		return huaweiPVGraph{}, err
	}
	source := p.handle.Source()
	for _, index := range huaweiInputs(child) {
		slot := strconv.Itoa(int(index))
		anchor := semgraph.Anchor{Class: semreg.AnchorComponent, Basis: semreg.AnchorNativeSlot, Source: semgraph.SourceNativePosition, Parent: out.inverter, Slot: slot, Evidence: []semreg.EvidenceRef{huaweiEvidence("native.huawei.string_index", slot)}}
		id, record, err := p.resources.resolve(huaweiKindInput, anchor, asset)
		if err != nil {
			return huaweiPVGraph{}, err
		}
		bindingID := binding
		out.inputs[index] = id
		out.records = append(out.records, semreg.Resource{ResourceID: id, Kind: huaweiKindInput, Anchor: record, State: semreg.ResourceCurrent, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:huawei-pv-input:" + huaweiHash(string(id), string(binding))[:32]), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &bindingID, Evidence: []semreg.EvidenceRef{huaweiEvidence("native.huawei.string_index", slot)}}, Revision: "1"})
	}
	return out, nil
}

// huaweiResourceUpserts returns the resource records the batch must carry: a
// resource the asset does not hold, one it holds withdrawn, and the
// retirement of a replaced inverter and its inputs once nothing that stays in
// the asset names them. A current record is never rewritten.
func huaweiResourceUpserts(current semreg.Snapshot, graph huaweiPVGraph, batch semreg.PublicationBatch) []semreg.Resource {
	held := map[semreg.ResourceID]semreg.Resource{}
	for _, resource := range current.Resources {
		held[resource.ResourceID] = resource
	}
	out := []semreg.Resource{}
	desired := map[semreg.ResourceID]bool{}
	for _, record := range graph.records {
		desired[record.ResourceID] = true
		old, ok := held[record.ResourceID]
		if ok && old.State == semreg.ResourceCurrent {
			continue
		}
		if ok {
			record.Anchor, record.Revision = old.Anchor, huaweiNext(old.Revision)
		}
		out = append(out, record)
	}
	named := huaweiNamedResources(current, batch)
	for _, old := range current.Resources {
		// A replaced inverter withdrawn while its child was withheld is
		// retired as well as one that is still current.
		if old.State == semreg.ResourceRetired || desired[old.ResourceID] || named[old.ResourceID] {
			continue
		}
		replacedInverter := old.Kind == huaweiKindInverter
		replacedInput := old.Kind == huaweiKindInput && old.Anchor.Parent != nil && *old.Anchor.Parent != graph.inverter
		if !replacedInverter && !replacedInput {
			continue
		}
		old.State, old.Revision = semreg.ResourceRetired, huaweiNext(old.Revision)
		out = append(out, old)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ResourceID < out[j].ResourceID })
	return out
}

// huaweiResourceWithdrawals withdraws the child's resources when the native
// owner withholds the child or a complete listing no longer names it: each
// current system, inverter and input record that nothing staying in the asset
// names becomes withdrawn under its identifier. The record states that the
// resource is known and not observed now; it is not an absence statement,
// which the gate does not state. Re-admission makes it current again.
func huaweiResourceWithdrawals(current semreg.Snapshot, batch semreg.PublicationBatch) []semreg.Resource {
	named := huaweiNamedResources(current, batch)
	out := []semreg.Resource{}
	for _, old := range current.Resources {
		if old.State != semreg.ResourceCurrent || named[old.ResourceID] {
			continue
		}
		if old.Kind != huaweiKindSystem && old.Kind != huaweiKindInverter && old.Kind != huaweiKindInput {
			continue
		}
		old.State, old.Revision = semreg.ResourceWithdrawn, huaweiNext(old.Revision)
		out = append(out, old)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ResourceID < out[j].ResourceID })
	return out
}

// huaweiNamedResources is every resource a fact or a relation that stays in
// the asset after batch names.
func huaweiNamedResources(current semreg.Snapshot, batch semreg.PublicationBatch) map[semreg.ResourceID]bool {
	named := map[semreg.ResourceID]bool{}
	withdrawn := map[semreg.CandidateID]bool{}
	for _, id := range batch.FactWithdrawals {
		withdrawn[id] = true
	}
	name := func(key semreg.FactKey) {
		for _, dimension := range key.Dimensions {
			if dimension.Value.Text != nil {
				named[semreg.ResourceID(*dimension.Value.Text)] = true
			}
		}
	}
	for _, envelope := range current.Facts {
		for _, candidate := range envelope.Candidates {
			if !withdrawn[candidate.CandidateID] {
				name(candidate.Key)
			}
		}
	}
	for _, fact := range batch.FactUpserts {
		name(fact.Key)
	}
	ended := map[string]bool{}
	for _, key := range batch.RelationWithdrawals {
		ended[huaweiRelationKey(key)] = true
	}
	for _, relation := range current.Relations {
		if !ended[huaweiRelationKey(semreg.RelationKey{Definition: relation.Definition, From: relation.From, To: relation.To})] {
			named[relation.From], named[relation.To] = true, true
		}
	}
	for _, relation := range batch.RelationUpserts {
		named[relation.From], named[relation.To] = true, true
	}
	return named
}

// relations adds the gate's relation records to batch: the system groups the
// child's inverter, and the inverter contains each published input. Each
// record's origin is the binding of the path that observes its starting end.
// Every record is checked against the gate before it is offered, so a record
// the gate does not state, or one to a resource the child does not publish,
// refuses the batch. A record of this source that the child no longer
// supports is withdrawn, and a withdrawn child withdraws them all.
func (p *huaweiPVPublication) relations(batch *semreg.PublicationBatch, current semreg.Snapshot, graph huaweiPVGraph, binding, systemBinding semreg.NativeBindingID, withdraw bool) error {
	source, epoch := p.handle.Source(), p.handle.Epoch()
	desired := []semreg.RelationInstance{}
	add := func(definition semreg.DefinitionRef, from, to semreg.ResourceID, origin semreg.NativeBindingID) {
		bindingID := origin
		basis := huaweiGateBasis(definition.ID)
		desired = append(desired, semreg.RelationInstance{Definition: definition, From: from, To: to, State: semreg.LinkQualified, BasisClass: semreg.RelationGateRule, Basis: []semreg.EvidenceRef{basis}, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:huawei-pv-relation:" + huaweiHash(string(definition.ID), string(from), string(to), string(origin))[:32]), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &bindingID, Evidence: []semreg.EvidenceRef{basis}}, Revision: "1"})
	}
	if !withdraw {
		add(huaweiRelationSystemInverter, graph.system, graph.inverter, systemBinding)
		for _, input := range graph.inputs {
			add(huaweiRelationInverterInput, graph.inverter, input, binding)
		}
	}
	ends := map[semreg.ResourceID]semreg.DefinitionID{}
	for _, record := range graph.records {
		ends[record.ResourceID] = record.Kind
	}
	observers := huaweiObservers(current, batch, source)
	for _, relation := range desired {
		if err := huaweiCheckRelation(relation, ends, observers); err != nil {
			return err
		}
	}
	huaweiRelationDiff(batch, current, desired, source)
	return nil
}

// huaweiObservers maps each binding of source that the asset holds current
// once batch applies to the resource its qualified identity link names. The
// batch's bindings and links replace the asset's; a withdrawn link, a link
// without a resource and a binding of another source observe nothing.
func huaweiObservers(current semreg.Snapshot, batch *semreg.PublicationBatch, source semreg.SourceID) map[semreg.NativeBindingID]semreg.ResourceID {
	bindings := map[semreg.NativeBindingID]semreg.NativeBinding{}
	for _, binding := range current.Bindings {
		bindings[binding.BindingID] = binding
	}
	for _, binding := range batch.BindingUpserts {
		bindings[binding.BindingID] = binding
	}
	links := map[semreg.NativeBindingID]semreg.IdentityLink{}
	for _, link := range current.IdentityLinks {
		links[link.BindingID] = link
	}
	for _, link := range batch.IdentityLinkUpserts {
		links[link.BindingID] = link
	}
	out := map[semreg.NativeBindingID]semreg.ResourceID{}
	for id, link := range links {
		binding, ok := bindings[id]
		if !ok || binding.SourceID != source || binding.State != semreg.BindingCurrent || link.State != semreg.LinkQualified || link.Resource == nil {
			continue
		}
		out[id] = *link.Resource
	}
	return out
}

// huaweiRelationDiff adds to batch the records of desired that the asset does
// not hold as they are, with the next revision of a changed record, and the
// withdrawal of every record of source that desired no longer names.
func huaweiRelationDiff(batch *semreg.PublicationBatch, current semreg.Snapshot, records []semreg.RelationInstance, source semreg.SourceID) {
	desired := map[string]semreg.RelationInstance{}
	for _, relation := range records {
		desired[huaweiRelationKey(semreg.RelationKey{Definition: relation.Definition, From: relation.From, To: relation.To})] = relation
	}
	for _, old := range current.Relations {
		key := huaweiRelationKey(semreg.RelationKey{Definition: old.Definition, From: old.From, To: old.To})
		if next, ok := desired[key]; ok {
			if reflect.DeepEqual(next.Origin, old.Origin) && reflect.DeepEqual(next.Basis, old.Basis) && old.State == next.State {
				delete(desired, key)
			} else {
				next.Revision = huaweiNext(old.Revision)
				desired[key] = next
			}
			continue
		}
		if old.Origin.BindingID != nil && huaweiBindingOfSource(current, *old.Origin.BindingID, source) {
			batch.RelationWithdrawals = append(batch.RelationWithdrawals, semreg.RelationKey{Definition: old.Definition, From: old.From, To: old.To})
		}
	}
	for _, relation := range desired {
		batch.RelationUpserts = append(batch.RelationUpserts, relation)
	}
	sort.Slice(batch.RelationUpserts, func(i, j int) bool {
		return huaweiRelationKey(semreg.RelationKey{Definition: batch.RelationUpserts[i].Definition, From: batch.RelationUpserts[i].From, To: batch.RelationUpserts[i].To}) < huaweiRelationKey(semreg.RelationKey{Definition: batch.RelationUpserts[j].Definition, From: batch.RelationUpserts[j].From, To: batch.RelationUpserts[j].To})
	})
	sort.Slice(batch.RelationWithdrawals, func(i, j int) bool {
		return huaweiRelationKey(batch.RelationWithdrawals[i]) < huaweiRelationKey(batch.RelationWithdrawals[j])
	})
}

// huaweiRelationKey orders relation keys as the kernel does: the canonical
// definition bytes, then the two endpoints.
func huaweiRelationKey(key semreg.RelationKey) string {
	definition, _ := semreg.CanonicalJSON(key.Definition)
	return string(definition) + "\x00" + string(key.From) + "\x00" + string(key.To)
}

func huaweiBindingOfSource(snapshot semreg.Snapshot, id semreg.NativeBindingID, source semreg.SourceID) bool {
	for _, binding := range snapshot.Bindings {
		if binding.BindingID == id {
			return binding.SourceID == source && binding.State == semreg.BindingCurrent
		}
	}
	return false
}

// huaweiUniqueCandidates sorts withdrawals, removes duplicates and drops a
// candidate the batch upserts.
func huaweiUniqueCandidates(ids []semreg.CandidateID, upserted map[semreg.CandidateID]bool) []semreg.CandidateID {
	seen := map[semreg.CandidateID]bool{}
	out := []semreg.CandidateID{}
	for _, id := range ids {
		if seen[id] || upserted[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (p *huaweiPVPublication) facts(graph huaweiPVGraph, binding, systemBinding semreg.NativeBindingID, child huawei.ProviderChildObservation, cycle huawei.ProviderCycle, evidence semreg.EvidenceRef, current semreg.Snapshot) ([]semreg.FactCandidate, error) {
	out := []semreg.FactCandidate{}
	registry, err := semreg.NewRegistry(pv.New())
	if err != nil {
		return nil, err
	}
	for _, native := range child.Reading.Facts() {
		if native.Availability != huawei.FactValue {
			continue
		}
		id, dimension, unit, group := semreg.DefinitionID(""), "", "", uint16(0)
		number := semreg.Decimal{Coefficient: strconv.FormatInt(native.Value.Coefficient, 10), Exponent10: int32(native.Value.Exponent)}
		keyPrefix := "huawei.sun2000.m3."
		if child.ProfileID == huawei.SUN2000M1MonitoringProfileID {
			keyPrefix = "huawei.sun2000.m1."
		}
		switch strings.TrimPrefix(native.Key, keyPrefix) {
		case "pv_string_voltage":
			id, dimension, unit, group = "pv.dc.voltage", "pv.dimension.input", "unit.volt", 32016
		case "pv_string_current":
			id, dimension, unit, group = "pv.dc.current", "pv.dimension.input", "unit.ampere", 32016
		case "input_power":
			id, dimension, unit, group = "pv.dc.aggregate_power", "pv.dimension.inverter", "unit.watt", 32064
			number.Exponent10 += 3
		case "active_power":
			if native.Value.Coefficient < 0 {
				continue
			}
			id, dimension, unit, group = "pv.ac.aggregate_active_power", "pv.dimension.inverter", "unit.watt", 32064
			number.Exponent10 += 3
		case "status":
			if native.State != "grid_connected" {
				continue
			}
			id, dimension, group = "pv.status.operating", "pv.dimension.inverter", 32064
		case "energy_accumulated":
			id, dimension, unit, group = "pv.energy.generated", "pv.dimension.system", "unit.kilowatt_hour", 32106
		default:
			continue
		}
		for number.Coefficient != "0" && strings.HasSuffix(number.Coefficient, "0") && number.Exponent10 < 18 {
			number.Coefficient = strings.TrimSuffix(number.Coefficient, "0")
			number.Exponent10++
		}
		if number.Coefficient == "0" {
			number.Exponent10 = 0
		}
		// The resource dimension value is the identifier of the fact's subject.
		// Zero pairs do not prove input membership: such an input has no
		// resource, and its words stay native evidence.
		subject, factBinding := graph.inverter, binding
		switch dimension {
		case "pv.dimension.input":
			input, ok := graph.inputs[uint16(native.Index)]
			if native.Index <= 0 || !ok {
				continue
			}
			subject = input
		case "pv.dimension.system":
			subject, factBinding = graph.system, systemBinding
		}
		var receipt time.Time
		groupStale := native.Retained
		for _, g := range child.Groups {
			if g.Offset != group || g.Cycle != native.Cycle {
				continue
			}
			// A retained group keeps the cycle and transport generation at which
			// it was last answered, so after a reconnect it carries an earlier
			// generation than the cycle and is published stale. A group that is
			// not retained must belong to the cycle's generation.
			if g.Generation != cycle.Generation && (!g.Retained || g.Generation >= cycle.Generation) {
				continue
			}
			receipt = g.ReadAt
			groupStale = groupStale || g.Retained || g.Cycle != cycle.Cycle
		}
		if receipt.IsZero() || receipt.After(cycle.CompletedAt) {
			return nil, errors.New("huawei native receipt lineage missing")
		}
		dimensionValue := string(subject)
		key := semreg.FactKey{PackID: huaweiPVPack, PackVersion: "1.0.0", FactID: id, Dimensions: []semreg.Dimension{{ID: semreg.DefinitionID(dimension), Value: semreg.Value{Kind: semreg.ValueText, Text: &dimensionValue}}}}
		value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: number, Unit: semreg.DefinitionID(unit)}}
		if id == "pv.status.operating" {
			value = semreg.Value{Kind: semreg.ValueSymbol, Symbol: &semreg.Symbol{Namespace: id, Token: "generating", Known: true}}
		}
		epoch, source, generation := p.handle.Epoch(), p.handle.Source(), p.handle.GenerationID()
		hash := huaweiHash(string(factBinding), string(epoch), string(generation), string(id), dimensionValue)
		revision := semreg.Uint64("1")
		candidateID := semreg.CandidateID("candidate:huawei-pv:" + hash[:32])
		for _, envelope := range current.Facts {
			for _, old := range envelope.Candidates {
				if old.CandidateID == candidateID {
					revision = huaweiNext(old.Revision)
				}
			}
		}
		quality := semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}}
		// A later group failure does not age facts acquired earlier in this
		// cycle. Each fact keeps the freshness and receipt of its own group.
		if groupStale {
			quality.Freshness = semreg.FreshnessStale
			quality.Reasons = append(quality.Reasons, "native.huawei.child_stale")
		}
		bindingID := factBinding
		fact := semreg.FactCandidate{CandidateID: candidateID, Key: key, Value: &value, Quality: quality, Times: semreg.Times{ReceivedAt: growattWall(receipt), ReceiptMonotonic: p.point(receipt), EvaluatedAt: growattWall(cycle.CompletedAt), EvaluateMonotonic: p.point(cycle.CompletedAt)}, FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "pv.telemetry.fast.v1", Version: "1.0.0", FreshForNS: "60000000000", RetainForNS: "300000000000", MaxWallUncertaintyNS: "0"}, BindingID: &bindingID, SourceEpochID: &epoch, DriverGeneration: &generation, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:huawei-pv:" + hash[:32]), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &bindingID, Evidence: []semreg.EvidenceRef{evidence}}, Evidence: []semreg.EvidenceRef{huaweiEvidence("native.huawei.grade", native.Grade), evidence}, Revision: revision}
		if err := registry.ValidateFactCandidate(fact); err != nil {
			continue
		} // Out-of-pack values are withheld, never clamped.
		out = append(out, fact)
	}
	return out, nil
}

func huaweiCurrentBinding(snapshot semreg.Snapshot, id semreg.NativeBindingID, epoch semreg.SourceEpochID, generation semreg.Uint64) bool {
	for _, binding := range snapshot.Bindings {
		if binding.BindingID == id {
			return binding.SourceEpochID == epoch && binding.DriverGeneration == generation && binding.State == semreg.BindingCurrent
		}
	}
	return false
}

// The logger is the gateway path its children's profiles name: generic
// equipment, homed in an asset of its own.
const (
	huaweiKindEquipment semreg.DefinitionID = "resource.equipment"
	huaweiRelaysID      semreg.DefinitionID = "relation.relays"
	huaweiGatewayPath                       = "gateway_path"
)

// huaweiRelaysUnresolved names, in the publication, relays instances that are
// withheld because the kernel cannot name the core relays definition.
const huaweiRelaysUnresolved = "relays_definition_unresolved"

var (
	errHuaweiFalseJoin        = errors.New("huawei relation is not stated by the gate")
	errHuaweiDanglingEndpoint = errors.New("huawei relation endpoint is not published")
	errHuaweiAbsenceNotStated = errors.New("huawei absence statement is not stated by the gate")
)

// huaweiLoggerAssetID is the home asset of the logger of one configured
// instance. The configured label is its whole key; its own domain keeps it
// apart from every child's asset.
func huaweiLoggerAssetID(label string) semreg.AssetID {
	return semreg.AssetID("pv-asset-" + huaweiHash("helianthus.modbus.gateway_path.v1", label)[:32])
}

// huaweiPublishedLoggerAsset is the logger asset when this publication has
// published it. The operator-origin equipment record stays current after the
// epoch that wrote it is retired, so that record alone is not publication.
// The asset must also carry a current source descriptor or a current binding
// of this publication's source and epoch. The caller holds the publication
// lock.
func huaweiPublishedLoggerAsset(p *huaweiPVPublication) (semreg.AssetID, bool) {
	if p == nil || p.hub == nil || p.handle == nil || p.label == "" {
		return "", false
	}
	id := huaweiLoggerAssetID(p.label)
	view, err := p.hub.Inspect(id)
	if err != nil || !view.Present {
		return "", false
	}
	if !huaweiPublicationEpochEvidence(view.Snapshot, p.handle.Source(), p.handle.Epoch()) {
		return "", false
	}
	for _, resource := range view.Snapshot.Resources {
		if resource.Kind == huaweiKindEquipment && resource.State == semreg.ResourceCurrent {
			return id, true
		}
	}
	return "", false
}

// huaweiPublicationEpochEvidence reports whether snapshot carries a current
// source descriptor or a current binding of source at epoch.
func huaweiPublicationEpochEvidence(snapshot semreg.Snapshot, source semreg.SourceID, epoch semreg.SourceEpochID) bool {
	for _, descriptor := range snapshot.Sources {
		if descriptor.SourceID == source && descriptor.SourceEpochID == epoch && descriptor.State == semreg.SourceCurrent {
			return true
		}
	}
	for _, binding := range snapshot.Bindings {
		if binding.SourceID == source && binding.SourceEpochID == epoch && binding.State == semreg.BindingCurrent {
			return true
		}
	}
	return false
}

// huaweiRelaysRef returns the reference that names the core relays definition
// in a relation record, and whether the element catalog compiled into the
// process resolves it as a member. The kernel names a relation definition by
// a pack reference, so a core-owned definition has no reference until SemReg
// gives it one; the relays instances are withheld until then.
func huaweiRelaysRef() (semreg.DefinitionRef, bool) {
	ref, ok := elementcatalog.CoreElement(huaweiRelaysID, "1.0.0").DefinitionRef()
	if !ok {
		return semreg.DefinitionRef{}, false
	}
	catalog, err := elementcatalog.Embedded()
	if err != nil {
		return semreg.DefinitionRef{}, false
	}
	definition, ok := catalog.RelationDefinition(ref)
	return ref, ok && definition.Membership.Disposition == semreg.GraphMember
}

// huaweiGateRelation is one row of the gate's relation table: the definition,
// its basis class, the kinds of both ends, the scope of the rule and the
// absence sides the gate states, none for every row.
type huaweiGateRelation struct {
	definition semreg.DefinitionID
	pack       bool
	basis      semreg.RelationBasis
	from, to   semreg.DefinitionID
	rule       string
	absence    []semreg.AbsenceSide
}

var huaweiGateRelations = []huaweiGateRelation{
	{definition: huaweiRelaysID, basis: semreg.RelationNativeObservation, from: huaweiKindEquipment, to: huaweiKindInverter, rule: "each_admitted_child_is_read_through_the_gateway_path_its_profile_names"},
	{definition: huaweiRelationSystemInverter.ID, pack: true, basis: semreg.RelationGateRule, from: huaweiKindSystem, to: huaweiKindInverter, rule: "one_pv_system_per_admitted_inverter_child"},
	{definition: huaweiRelationInverterInput.ID, pack: true, basis: semreg.RelationGateRule, from: huaweiKindInverter, to: huaweiKindInput, rule: "native_string_index_1_to_8_with_a_nonzero_voltage_or_current_word"},
}

// ref is the reference a record of the row names; a core row has one only
// when the kernel resolves it.
func (row huaweiGateRelation) ref() (semreg.DefinitionRef, bool) {
	if row.pack {
		return semreg.DefinitionRef{Pack: semreg.PackRef{ID: huaweiPVPack, Version: "1.0.0"}, ID: row.definition, Version: "1.0.0"}, true
	}
	return huaweiRelaysRef()
}

// huaweiGateBasisMatches reports whether basis carries the gate basis of
// definition: the owner, kind and rule scope huaweiGateBasis records. A
// native string index, a configured label or another rule does not.
func huaweiGateBasisMatches(basis []semreg.EvidenceRef, definition semreg.DefinitionID) bool {
	gate := huaweiGateBasis(definition)
	for _, ref := range basis {
		if ref.Owner == gate.Owner && ref.Kind == gate.Kind && ref.Digest == gate.Digest {
			return true
		}
	}
	return false
}

// huaweiGateBasis is the basis reference of a record of the gate row of
// definition: the definition and the scope of its rule.
func huaweiGateBasis(definition semreg.DefinitionID) semreg.EvidenceRef {
	rule := ""
	for _, row := range huaweiGateRelations {
		if row.definition == definition {
			rule = row.rule
		}
	}
	return huaweiEvidence("gate.huawei.pv.relation", struct {
		Definition semreg.DefinitionID
		Rule       string
	}{definition, rule})
}

// huaweiCheckRelation refuses a relation record the gate does not state. Both
// ends must be resources this publication has published, given in ends with
// their kinds (a dangling endpoint otherwise). The record must then match a
// gate row in its definition, basis class and end kinds, carry that row's
// gate basis (its owner, kind and rule scope) and a native origin whose
// binding observes the From end: observers maps each current binding of this
// source to the resource its qualified identity link names (a false join
// otherwise). An index, a label, a role or an address is never a basis, so a
// record built from one matches no row, and a binding that observes the To
// end or another resource does not originate the record.
func huaweiCheckRelation(relation semreg.RelationInstance, ends map[semreg.ResourceID]semreg.DefinitionID, observers map[semreg.NativeBindingID]semreg.ResourceID) error {
	from, fromOK := ends[relation.From]
	to, toOK := ends[relation.To]
	if !fromOK || !toOK {
		return fmt.Errorf("%w: %s", errHuaweiDanglingEndpoint, relation.Definition.ID)
	}
	if !huaweiGateBasisMatches(relation.Basis, relation.Definition.ID) || relation.Origin.Kind != semreg.OriginNativeObservation || relation.Origin.BindingID == nil {
		return fmt.Errorf("%w: %s basis", errHuaweiFalseJoin, relation.Definition.ID)
	}
	if observed, ok := observers[*relation.Origin.BindingID]; !ok || observed != relation.From {
		return fmt.Errorf("%w: %s origin does not observe its from end", errHuaweiFalseJoin, relation.Definition.ID)
	}
	for _, row := range huaweiGateRelations {
		ref, ok := row.ref()
		if ok && ref == relation.Definition && row.basis == relation.BasisClass && row.from == from && row.to == to {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", errHuaweiFalseJoin, relation.Definition.ID)
}

// huaweiCheckAbsence refuses an absence statement on a side the gate does not
// state. The gate states none: a zero pair does not show that an input is
// absent, and a withheld child is not absent.
func huaweiCheckAbsence(absence semreg.RelationAbsence) error {
	for _, row := range huaweiGateRelations {
		ref, ok := row.ref()
		if !ok || ref != absence.Definition || row.basis != absence.BasisClass {
			continue
		}
		for _, side := range row.absence {
			if side == absence.Side {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: %s", errHuaweiAbsenceNotStated, absence.Definition.ID)
}

// huaweiDeclaredLink reports whether a published identity link is a live
// operator identity declaration.
func huaweiDeclaredLink(link semreg.IdentityLink) bool {
	if link.State == semreg.LinkWithdrawn {
		return false
	}
	for _, ref := range link.Basis {
		if ref.Owner == semhub.OperatorEvidenceOwner {
			return true
		}
	}
	return false
}

// huaweiApplicableOperatorLinks is the pending operator set to install.
// A qualified link for a binding this batch observes is installed only when
// it names that resource. A link that names another resource, including a
// retired inverter, is not installed, and neither is the rest of the pending
// set: the hub requires a batch that carries any operator link to carry
// exactly the pending set, and it accepts a batch that carries none. Links
// for bindings this batch does not observe, and withdrawals, stay in the set.
func huaweiApplicableOperatorLinks(pending []semreg.IdentityLink, observed map[semreg.NativeBindingID]semreg.ResourceID) []semreg.IdentityLink {
	for _, link := range pending {
		want, ok := observed[link.BindingID]
		if !ok || link.State == semreg.LinkWithdrawn {
			continue
		}
		if link.Resource == nil || *link.Resource != want {
			return nil
		}
	}
	return pending
}

// huaweiWithOperatorLinks replaces the publisher's own link of each binding
// the hub returns with the hub's pending operator link, in binding order.
func huaweiWithOperatorLinks(own, pending []semreg.IdentityLink) []semreg.IdentityLink {
	byBinding := map[semreg.NativeBindingID]semreg.IdentityLink{}
	for _, link := range own {
		byBinding[link.BindingID] = link
	}
	for _, link := range pending {
		byBinding[link.BindingID] = link
	}
	out := make([]semreg.IdentityLink, 0, len(byBinding))
	for _, link := range byBinding {
		out = append(out, link)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BindingID < out[j].BindingID })
	return out
}

// relaysWithheld returns why the logger's relays instances are withheld, or
// an empty reason while they are published.
func (p *huaweiPVPublication) relaysWithheld() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.relaysReason
}

// relays builds one relays instance from the logger to each inverter whose
// child batch committed, observed by the logger's binding. The caller holds
// the publication lock.
func (p *huaweiPVPublication) relays(ref semreg.DefinitionRef, logger semreg.ResourceID, binding semreg.NativeBindingID) []semreg.RelationInstance {
	source, epoch := p.handle.Source(), p.handle.Epoch()
	units := make([]int, 0, len(p.inverters))
	for unit := range p.inverters {
		units = append(units, int(unit))
	}
	sort.Ints(units)
	out := make([]semreg.RelationInstance, 0, len(units))
	for _, unit := range units {
		inverter := p.inverters[byte(unit)]
		bindingID := binding
		basis := huaweiGateBasis(huaweiRelaysID)
		out = append(out, semreg.RelationInstance{Definition: ref, From: logger, To: inverter, State: semreg.LinkQualified, BasisClass: semreg.RelationNativeObservation, Basis: []semreg.EvidenceRef{basis}, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:huawei-pv-relation:" + huaweiHash(string(huaweiRelaysID), string(logger), string(inverter), string(binding))[:32]), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &bindingID, Evidence: []semreg.EvidenceRef{basis}}, Revision: "1"})
	}
	return out
}

// checkLoggerRelations checks the logger's records against the gate: their
// only ends are the logger and the inverters whose child batches committed.
// Each origin binding must observe the logger, as observers states. The
// caller holds the publication lock.
func (p *huaweiPVPublication) checkLoggerRelations(relations []semreg.RelationInstance, logger semreg.ResourceID, observers map[semreg.NativeBindingID]semreg.ResourceID) ([]semreg.RelationInstance, error) {
	ends := map[semreg.ResourceID]semreg.DefinitionID{logger: huaweiKindEquipment}
	for _, inverter := range p.inverters {
		ends[inverter] = huaweiKindInverter
	}
	for _, relation := range relations {
		if err := huaweiCheckRelation(relation, ends, observers); err != nil {
			return nil, err
		}
	}
	return relations, nil
}

// publishLogger publishes the logger of the configured instance in its own
// asset, declared by the family's class: generic equipment on the operator
// slot of the instance label, observed by a binding of the gateway path, and
// the gate's relays instance to each published inverter when the kernel can
// name the definition. Nothing is published before a child has committed, and
// a batch that would change nothing is not applied.
func (p *huaweiPVPublication) publishLogger(cycle huawei.ProviderCycle) error {
	if len(p.inverters) == 0 && !p.loggerDue() {
		return nil
	}
	if !p.active() {
		return errors.New("huawei publication generation withdrawn")
	}
	asset := huaweiLoggerAssetID(p.label)
	source, epoch, generation := p.handle.Source(), p.handle.Epoch(), p.handle.GenerationID()
	if _, err := p.class.Declare(asset, source); err != nil {
		return err
	}
	tx, err := p.hub.Begin(p.handle, asset)
	if err != nil {
		return err
	}
	defer tx.Abort()
	current, exists := tx.Current()
	expected := semreg.Uint64("0")
	if exists {
		expected = current.Revisions.Semantic
	}
	operator := huaweiEvidence("operator.huawei.instance", p.label)
	anchor := semgraph.Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: p.label, Evidence: []semreg.EvidenceRef{operator}}
	logger, record, err := p.resources.resolve(huaweiKindEquipment, anchor, asset)
	if err != nil {
		return err
	}
	binding, err := semhub.NativeBindingID("huawei_pv", asset, source, huaweiGatewayPath, epoch, p.handle.Generation())
	if err != nil {
		return err
	}
	path := huaweiEvidence("native.huawei.binding", struct {
		Asset  semreg.AssetID
		Source semreg.SourceID
		Path   huawei.ProviderPath
	}{asset, source, cycle.Path})
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:huawei-pv-logger:" + huaweiHash(string(asset), string(epoch), string(generation), string(tx.NextSequence()))[:32]), AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: tx.NextSequence(), ExpectedSemanticRevision: expected, ObservedAt: growattWall(cycle.CompletedAt),
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: append([]semreg.SourceEpochID{}, tx.SourceRetirements()...), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}, ResourceUpserts: []semreg.Resource{}}
	for _, old := range tx.GenerationFences() {
		batch.GenerationFences = append(batch.GenerationFences, semreg.GenerationFence{SourceID: source, SourceEpochID: epoch, DriverGeneration: old, Reason: "lifecycle.driver_generation_replaced", Evidence: []semreg.EvidenceRef{path}, Revision: "1"})
	}
	if !growattStorageHasSourceEpoch(current, source, epoch) {
		batch.SourceUpserts = append(batch.SourceUpserts, semreg.SourceDescriptor{SourceID: source, SourceEpochID: epoch, ProtocolID: "modbus_tcp", ProfileID: "huawei.readonly.pv", ProfileVersion: "1", RegistryEvidence: huaweiEvidence("registry.huawei.mapping", "huawei-sun2000-pv-v1"), StartedAt: growattWall(p.origin), State: semreg.SourceCurrent, Revision: "1"})
	}
	held := false
	for _, old := range current.Bindings {
		if old.BindingID == binding {
			if old.DriverGeneration != generation || old.SourceEpochID != epoch || old.State != semreg.BindingCurrent {
				return errors.New("huawei logger binding unavailable")
			}
			held = true
		}
	}
	if !held {
		if err := huaweiBindingCapacity(current); err != nil {
			return fmt.Errorf("logger: %w", err)
		}
		batch.BindingUpserts = append(batch.BindingUpserts, semreg.NativeBinding{BindingID: binding, AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: path, State: semreg.BindingCurrent, Revision: "1"})
	}
	link := semreg.IdentityLink{AssetID: asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{path}, Revision: "1", Resource: &logger}
	for _, old := range current.IdentityLinks {
		if old.BindingID != binding {
			continue
		}
		link.Revision = huaweiNext(old.Revision)
		if huaweiDeclaredLink(old) || old.State == semreg.LinkQualified && old.Resource != nil && *old.Resource == logger {
			link = semreg.IdentityLink{}
		}
	}
	if link.BindingID != "" {
		batch.IdentityLinkUpserts = append(batch.IdentityLinkUpserts, link)
	}
	// A pending declaration for the logger's binding is installed only when it
	// names the logger; otherwise the pending set stays with the hub and the
	// publisher's own link keeps observing the logger.
	batch.IdentityLinkUpserts = huaweiWithOperatorLinks(batch.IdentityLinkUpserts, huaweiApplicableOperatorLinks(tx.OperatorLinks(batch.BindingUpserts), map[semreg.NativeBindingID]semreg.ResourceID{binding: logger}))
	resource := semreg.Resource{ResourceID: logger, Kind: huaweiKindEquipment, Anchor: record, State: semreg.ResourceCurrent, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:huawei-pv-operator:" + huaweiHash(string(huaweiKindEquipment), p.label)[:32]), Kind: semreg.OriginOperator, Evidence: []semreg.EvidenceRef{operator}}, Revision: "1"}
	listed := false
	for _, old := range current.Resources {
		if old.ResourceID != logger {
			continue
		}
		listed = true
		if old.State != semreg.ResourceCurrent {
			resource.Anchor, resource.Revision = old.Anchor, huaweiNext(old.Revision)
			batch.ResourceUpserts = append(batch.ResourceUpserts, resource)
		}
	}
	if !listed {
		batch.ResourceUpserts = append(batch.ResourceUpserts, resource)
	}
	records := []semreg.RelationInstance{}
	if ref, ok := huaweiRelaysRef(); ok {
		p.relaysReason = ""
		if records, err = p.checkLoggerRelations(p.relays(ref, logger, binding), logger, huaweiObservers(current, &batch, source)); err != nil {
			return err
		}
	} else {
		p.relaysReason = huaweiRelaysUnresolved
	}
	huaweiRelationDiff(&batch, current, records, source)
	if len(batch.SourceUpserts)+len(batch.SourceRetirements)+len(batch.BindingUpserts)+len(batch.IdentityLinkUpserts)+len(batch.GenerationFences)+len(batch.ResourceUpserts)+len(batch.RelationUpserts)+len(batch.RelationWithdrawals) == 0 {
		return nil
	}
	batch.BatchDigest, err = batch.ComputedDigest()
	if err != nil {
		return err
	}
	if _, _, err = tx.Apply(batch, p.point(cycle.CompletedAt)); err != nil {
		return err
	}
	if !p.active() {
		return errors.New("huawei publication generation withdrawn")
	}
	_, err = tx.Commit(nil)
	return err
}

// loggerDue reports whether the logger asset was declared by this source, so
// a cycle that publishes no child still withdraws the logger's relays.
func (p *huaweiPVPublication) loggerDue() bool {
	view, err := p.hub.Inspect(huaweiLoggerAssetID(p.label))
	return err == nil && view.Present && growattStorageHasSourceEpoch(view.Snapshot, p.handle.Source(), p.handle.Epoch())
}
