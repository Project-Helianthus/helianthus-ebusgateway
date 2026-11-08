package main

import (
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
	native   modbusreg.HuaweiProviderCycle
	// latest is the most recent cycle offered to Publish. It is stored under
	// mu with the published assets so status reads one coherent pair.
	latest       modbusreg.HuaweiProviderCycle
	published    map[byte]semreg.AssetID
	incarnations map[byte]uint64
	// refused names, per routed unit, why the publication refused a child the
	// provider admitted; a later successful admission clears the entry.
	refused map[byte]string
	// predecessor is the epoch this publication succeeded, the only earlier
	// generation whose retained observations an asset keeps.
	predecessor semreg.SourceEpochID
	// capacity records each explicit withdrawal of an older generation's
	// retained observations, in commit order, for the driver status.
	capacity []huaweiRetainedWithdrawal
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

// capacityWithdrawals selects the retained observations of every generation
// older than the one before the publishing generation. Admitting a third
// generation inside the retention window would otherwise exceed the
// per-asset retained reservation, so those observations are withdrawn
// explicitly in the admitting batch; the reservation is never raised.
func (p *huaweiPVPublication) capacityWithdrawals(asset semreg.AssetID, current semreg.Snapshot, epoch semreg.SourceEpochID, generation semreg.Uint64) ([]semreg.CandidateID, []huaweiRetainedWithdrawal) {
	type key struct {
		epoch      semreg.SourceEpochID
		generation semreg.Uint64
	}
	groups := map[key][]semreg.CandidateID{}
	for _, record := range current.Retained {
		candidate := record.Candidate
		if candidate.SourceEpochID == nil || candidate.DriverGeneration == nil {
			continue
		}
		k := key{*candidate.SourceEpochID, *candidate.DriverGeneration}
		if k.epoch == epoch && k.generation == generation {
			continue
		}
		groups[k] = append(groups[k], candidate.CandidateID)
	}
	if len(groups)+1 <= huaweiRetainedGenerations {
		return nil, nil
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
	for _, k := range order[huaweiRetainedGenerations-1:] {
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
	return ids, records
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

var errHuaweiBindingCapacityExhausted = errors.New("huawei semantic binding capacity exhausted")

// huaweiRefusalBindingCapacity names, in the instance status, a child the
// publication refused because its re-admission would exceed the asset's bound.
const huaweiRefusalBindingCapacity = "binding_capacity"

// huaweiSourceCapacity refuses a new epoch when an asset of this source already
// holds the kernel's source-descriptor bound: that epoch could never publish
// on the asset, so the START fails instead of running without publication.
func huaweiSourceCapacity(hub *semhub.Hub, source semreg.SourceID) error {
	for _, id := range hub.AssetIDs() {
		view, err := hub.Inspect(id)
		if err != nil {
			continue
		}
		for _, descriptor := range view.Snapshot.Sources {
			if descriptor.SourceID == source {
				if len(view.Snapshot.Sources) >= huaweiMaxAssetSources {
					return errHuaweiSourceCapacityExhausted
				}
				break
			}
		}
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
	class, err := hub.DeclareClass(semhub.ClassSpec{Name: "huawei_pv", Driver: driver, Validators: func() []semreg.PackValidator { return []semreg.PackValidator{pv.New()} }, OnControl: func(semhub.ControlEvent) error { return nil }})
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
	publication := &huaweiPVPublication{hub: hub, class: class, handle: handle, label: label, active: active, origin: time.Now(), published: map[byte]semreg.AssetID{}, incarnations: map[byte]uint64{}, refused: map[byte]string{}}
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
func (p *huaweiPVPublication) Publish(cycle modbusreg.HuaweiProviderCycle) error {
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
	if cycle.Path != modbusreg.HuaweiProviderPathSmartLogger && cycle.Path != modbusreg.HuaweiProviderPathSDongle {
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
		admitted := child.Evidence != modbusreg.HuaweiProviderEvidenceWithheld && child.Reading.ProfileID() == child.ProfileID && child.Reading.UnitID() == child.UnitID && child.IdentifiedGeneration == cycle.Generation && child.Catalogue != nil && child.Catalogue.Outcome() == modbusreg.HuaweiSUN2000CatalogueSelected
		admitted = admitted && (cycle.Path == modbusreg.HuaweiProviderPathSmartLogger && child.ProfileID == modbusreg.HuaweiSUN2000M3MonitoringProfileID || cycle.Path == modbusreg.HuaweiProviderPathSDongle && child.ProfileID == modbusreg.HuaweiSUN2000M1MonitoringProfileID)
		if !admitted {
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
	if cycle.Outcome == modbusreg.HuaweiProviderCycleComplete {
		var units []int
		for unit := range p.published {
			if !seen[unit] {
				units = append(units, int(unit))
			}
		}
		sort.Ints(units)
		for _, unit := range units {
			u := byte(unit)
			if err := p.publishChild(p.published[u], modbusreg.HuaweiProviderChildObservation{UnitID: u}, cycle, true); err != nil {
				failures = append(failures, err)
			} else {
				delete(p.published, u)
			}
		}
	}
	return errors.Join(failures...)
}
func huaweiNext(value semreg.Uint64) semreg.Uint64 {
	n, _ := strconv.ParseUint(string(value), 10, 64)
	return semreg.Uint64(strconv.FormatUint(n+1, 10))
}

func (p *huaweiPVPublication) publishChild(asset semreg.AssetID, child modbusreg.HuaweiProviderChildObservation, cycle modbusreg.HuaweiProviderCycle, withdraw bool) error {
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
		Child      modbusreg.HuaweiProviderChildObservation
		Facts      []modbusreg.HuaweiSUN2000Fact
		Groups     []modbusreg.HuaweiSUN2000GroupReading
	}{cycle.Cycle, cycle.Generation, child, child.Reading.Facts(), child.Reading.Groups()})
	source, epoch, generation := p.handle.Source(), p.handle.Epoch(), p.handle.GenerationID()
	incarnation := p.incarnations[child.UnitID]
	if _, published := p.published[child.UnitID]; !withdraw && !published {
		incarnation++
	}
	// Identity withdrawal is terminal for a binding. Re-admission gets a new
	// native-binding incarnation while retaining the configured asset identity.
	binding, err := semhub.NativeBindingID("huawei_pv", asset, source, child.ProfileID+":"+strconv.FormatUint(incarnation, 10), epoch, p.handle.Generation())
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
	if !withdraw {
		if !growattStorageHasSourceEpoch(current, source, epoch) {
			batch.SourceUpserts = append(batch.SourceUpserts, semreg.SourceDescriptor{SourceID: source, SourceEpochID: epoch, ProtocolID: "modbus_tcp", ProfileID: "huawei.readonly.pv", ProfileVersion: "1", RegistryEvidence: huaweiEvidence("registry.huawei.mapping", "huawei-sun2000-pv-v1"), StartedAt: growattWall(p.origin), State: semreg.SourceCurrent, Revision: "1"})
		}
		hasBinding := false
		for _, old := range current.Bindings {
			if old.BindingID == binding {
				if old.DriverGeneration != generation || old.SourceEpochID != epoch || old.State != semreg.BindingCurrent {
					return errors.New("huawei binding incarnation unavailable")
				}
				hasBinding = true
			}
		}
		if !hasBinding {
			// A new incarnation must fit the kernel's bounds; refuse before any
			// batch is applied so the asset and the siblings are left untouched.
			if len(current.Bindings) >= huaweiMaxAssetBindings || len(current.IdentityLinks) >= huaweiMaxAssetIdentityLinks {
				p.refused[child.UnitID] = huaweiRefusalBindingCapacity
				return fmt.Errorf("unit %d: %w", child.UnitID, errHuaweiBindingCapacityExhausted)
			}
			batch.BindingUpserts = append(batch.BindingUpserts, semreg.NativeBinding{BindingID: binding, AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: huaweiEvidence("native.huawei.binding", struct {
				Asset   semreg.AssetID
				Source  semreg.SourceID
				Profile string
			}{asset, source, child.ProfileID}), State: semreg.BindingCurrent, Revision: "1"})
		}
		linkRevision := semreg.Uint64("1")
		updateLink := true
		for _, old := range current.IdentityLinks {
			if old.BindingID == binding {
				linkRevision = huaweiNext(old.Revision)
				updateLink = old.State != semreg.LinkQualified
			}
		}
		if updateLink {
			batch.IdentityLinkUpserts = append(batch.IdentityLinkUpserts, semreg.IdentityLink{AssetID: asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: linkRevision})
		}
		batch.FactUpserts, err = p.facts(asset, binding, child, cycle, evidence, current)
		if err != nil {
			return err
		}
		retainedIDs, capacity = p.capacityWithdrawals(asset, current, epoch, generation)
		batch.FactWithdrawals = append(batch.FactWithdrawals, retainedIDs...)
	}
	if withdraw {
		for _, old := range current.IdentityLinks {
			if old.State == semreg.LinkQualified && huaweiCurrentBinding(current, old.BindingID, epoch, generation) {
				old.State = semreg.LinkWithdrawn
				old.Revision = huaweiNext(old.Revision)
				batch.IdentityLinkUpserts = append(batch.IdentityLinkUpserts, old)
			}
		}
	}
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
	sort.Slice(batch.FactUpserts, func(i, j int) bool { return batch.FactUpserts[i].CandidateID < batch.FactUpserts[j].CandidateID })
	sort.Slice(batch.FactWithdrawals, func(i, j int) bool { return batch.FactWithdrawals[i] < batch.FactWithdrawals[j] })
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
		p.capacity = append(p.capacity, capacity...)
	}
	return err
}

func (p *huaweiPVPublication) facts(asset semreg.AssetID, binding semreg.NativeBindingID, child modbusreg.HuaweiProviderChildObservation, cycle modbusreg.HuaweiProviderCycle, evidence semreg.EvidenceRef, current semreg.Snapshot) ([]semreg.FactCandidate, error) {
	out := []semreg.FactCandidate{}
	registry, err := semreg.NewRegistry(pv.New())
	if err != nil {
		return nil, err
	}
	for _, native := range child.Reading.Facts() {
		if native.Availability != modbusreg.HuaweiFactValue {
			continue
		}
		id, dimension, unit, group := semreg.DefinitionID(""), "", "", uint16(0)
		number := semreg.Decimal{Coefficient: strconv.FormatInt(native.Value.Coefficient, 10), Exponent10: int32(native.Value.Exponent)}
		keyPrefix := "huawei.sun2000.m3."
		if child.ProfileID == modbusreg.HuaweiSUN2000M1MonitoringProfileID {
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
		case "device_status":
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
		// Zero pairs do not prove input membership. Keep them as native evidence.
		if native.Index > 0 {
			voltage, vok := child.Reading.Fact(keyPrefix+"pv_string_voltage", native.Index)
			current, cok := child.Reading.Fact(keyPrefix+"pv_string_current", native.Index)
			if !vok || !cok || voltage.Value.Coefficient == 0 && current.Value.Coefficient == 0 {
				continue
			}
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
		dimensionValue := strings.TrimPrefix(dimension, "pv.dimension.") + ":" + strings.TrimPrefix(string(asset), "pv-asset-")
		if native.Index > 0 {
			dimensionValue = fmt.Sprintf("input:%d", native.Index)
		}
		key := semreg.FactKey{PackID: huaweiPVPack, PackVersion: "1.0.0", FactID: id, Dimensions: []semreg.Dimension{{ID: semreg.DefinitionID(dimension), Value: semreg.Value{Kind: semreg.ValueText, Text: &dimensionValue}}}}
		value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: number, Unit: semreg.DefinitionID(unit)}}
		if id == "pv.status.operating" {
			value = semreg.Value{Kind: semreg.ValueSymbol, Symbol: &semreg.Symbol{Namespace: id, Token: "generating", Known: true}}
		}
		epoch, source, generation := p.handle.Epoch(), p.handle.Source(), p.handle.GenerationID()
		hash := huaweiHash(string(asset), string(binding), string(epoch), string(generation), string(id), dimensionValue)
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
		fact := semreg.FactCandidate{CandidateID: candidateID, Key: key, Value: &value, Quality: quality, Times: semreg.Times{ReceivedAt: growattWall(receipt), ReceiptMonotonic: p.point(receipt), EvaluatedAt: growattWall(cycle.CompletedAt), EvaluateMonotonic: p.point(cycle.CompletedAt)}, FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "pv.telemetry.fast.v1", Version: "1.0.0", FreshForNS: "60000000000", RetainForNS: "300000000000", MaxWallUncertaintyNS: "0"}, BindingID: &binding, SourceEpochID: &epoch, DriverGeneration: &generation, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:huawei-pv:" + hash[:32]), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}}, Evidence: []semreg.EvidenceRef{huaweiEvidence("native.huawei.grade", native.Grade), evidence}, Revision: revision}
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
