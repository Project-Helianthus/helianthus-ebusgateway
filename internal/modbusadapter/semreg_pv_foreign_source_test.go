package modbusadapter

import (
	"reflect"
	"strconv"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// foreignSourcePVContributor adds a second source's descriptor, binding,
// identity link, services, capabilities, one current candidate per fact
// envelope and one retained observation to a copy of snapshot, each copied
// from the publisher's own record under synthetic foreign identifiers.
func foreignSourcePVContributor(t *testing.T, snapshot semreg.Snapshot) semreg.Snapshot {
	t.Helper()
	const (
		source  semreg.SourceID        = "source:synthetic-foreign"
		epoch   semreg.SourceEpochID   = "source-epoch:synthetic-foreign"
		binding semreg.NativeBindingID = "binding:synthetic-foreign"
	)
	foreignEpoch, foreignBinding, generation := epoch, binding, semreg.Uint64("1")
	if len(snapshot.Sources) == 0 || len(snapshot.Bindings) == 0 || len(snapshot.Facts) == 0 {
		t.Fatal("fixture publication holds no complete topology")
	}
	descriptor := snapshot.Sources[0]
	descriptor.SourceID, descriptor.SourceEpochID = source, epoch
	snapshot.Sources = append(append([]semreg.SourceDescriptor{}, snapshot.Sources...), descriptor)
	nativeBinding := snapshot.Bindings[0]
	nativeBinding.BindingID, nativeBinding.SourceID, nativeBinding.SourceEpochID, nativeBinding.DriverGeneration = binding, source, epoch, generation
	snapshot.Bindings = append(append([]semreg.NativeBinding{}, snapshot.Bindings...), nativeBinding)
	snapshot.IdentityLinks = append(append([]semreg.IdentityLink{}, snapshot.IdentityLinks...), semreg.IdentityLink{AssetID: snapshot.AssetID, BindingID: binding, State: semreg.LinkQualified, Revision: "1"})
	services := append([]semreg.ServiceInstance{}, snapshot.Services...)
	for i, service := range snapshot.Services {
		service.InstanceID, service.BindingID, service.SourceEpochID, service.DriverGeneration = semreg.ServiceInstanceID("service:synthetic-foreign-"+strconv.Itoa(i)), binding, epoch, generation
		services = append(services, service)
	}
	snapshot.Services = services
	capabilities := append([]semreg.CapabilityInstance{}, snapshot.Capabilities...)
	for i, capability := range snapshot.Capabilities {
		capability.InstanceID, capability.BindingID, capability.SourceEpochID, capability.DriverGeneration = semreg.CapabilityInstanceID("capability:synthetic-foreign-"+strconv.Itoa(i)), binding, epoch, generation
		capability.ServiceInstance = "service:synthetic-foreign-0"
		capabilities = append(capabilities, capability)
	}
	snapshot.Capabilities = capabilities
	facts := make([]semreg.FactEnvelope, 0, len(snapshot.Facts))
	for i, envelope := range snapshot.Facts {
		candidate := envelope.Candidates[0]
		candidate.CandidateID = semreg.CandidateID("candidate:synthetic-foreign-" + strconv.Itoa(i))
		candidate.BindingID, candidate.SourceEpochID, candidate.DriverGeneration = &foreignBinding, &foreignEpoch, &generation
		candidate.Origin.BindingID, candidate.Origin.SourceEpochID, candidate.Origin.SourceID = &foreignBinding, &foreignEpoch, nil
		envelope.Candidates = append(append([]semreg.FactCandidate{}, envelope.Candidates...), candidate)
		facts = append(facts, envelope)
		if i == 0 {
			retained := candidate
			retained.CandidateID = "candidate:synthetic-foreign-retained"
			snapshot.Retained = append(append([]semreg.RetainedObservationRecord{}, snapshot.Retained...), semreg.RetainedObservationRecord{Contract: semreg.ContractKernelV1, Candidate: retained, Removal: semreg.RetainedRemovalGenerationFence})
		}
	}
	snapshot.Facts = facts
	return snapshot
}

// foreignSourcePVForeignIDs lists every identifier foreign holds that single
// does not.
func foreignSourcePVForeignIDs(single, foreign semreg.Snapshot) map[string]bool {
	own := map[string]bool{}
	for _, source := range single.Sources {
		own[string(source.SourceID)], own[string(source.SourceEpochID)] = true, true
	}
	for _, binding := range single.Bindings {
		own[string(binding.BindingID)] = true
	}
	for _, envelope := range single.Facts {
		for _, candidate := range envelope.Candidates {
			own[string(candidate.CandidateID)] = true
		}
	}
	for _, record := range single.Retained {
		own[string(record.Candidate.CandidateID)] = true
	}
	for _, service := range single.Services {
		own[string(service.InstanceID)] = true
	}
	for _, capability := range single.Capabilities {
		own[string(capability.InstanceID)] = true
	}
	out := map[string]bool{}
	mark := func(id string) {
		if !own[id] {
			out[id] = true
		}
	}
	for _, source := range foreign.Sources {
		mark(string(source.SourceID))
		mark(string(source.SourceEpochID))
	}
	for _, binding := range foreign.Bindings {
		mark(string(binding.BindingID))
	}
	for _, envelope := range foreign.Facts {
		for _, candidate := range envelope.Candidates {
			mark(string(candidate.CandidateID))
		}
	}
	for _, record := range foreign.Retained {
		mark(string(record.Candidate.CandidateID))
	}
	for _, service := range foreign.Services {
		mark(string(service.InstanceID))
	}
	for _, capability := range foreign.Capabilities {
		mark(string(capability.InstanceID))
	}
	return out
}

// foreignSourcePVBatchNames lists every identifier batch upserts, revises,
// fences, retires or withdraws.
func foreignSourcePVBatchNames(batch semreg.PublicationBatch) []string {
	var named []string
	for _, source := range batch.SourceUpserts {
		named = append(named, string(source.SourceID), string(source.SourceEpochID))
	}
	for _, epoch := range batch.SourceRetirements {
		named = append(named, string(epoch))
	}
	for _, binding := range batch.BindingUpserts {
		named = append(named, string(binding.BindingID), string(binding.SourceID))
	}
	for _, link := range batch.IdentityLinkUpserts {
		named = append(named, string(link.BindingID))
	}
	for _, fact := range batch.FactUpserts {
		named = append(named, string(fact.CandidateID))
		if fact.BindingID != nil {
			named = append(named, string(*fact.BindingID))
		}
	}
	for _, id := range batch.FactWithdrawals {
		named = append(named, string(id))
	}
	for _, service := range batch.ServiceUpserts {
		named = append(named, string(service.InstanceID), string(service.BindingID))
	}
	for _, id := range batch.ServiceWithdrawals {
		named = append(named, string(id))
	}
	for _, capability := range batch.CapabilityUpserts {
		named = append(named, string(capability.InstanceID), string(capability.BindingID))
	}
	for _, id := range batch.CapabilityWithdrawals {
		named = append(named, string(id))
	}
	for _, fence := range batch.GenerationFences {
		named = append(named, string(fence.SourceID), string(fence.SourceEpochID))
	}
	return named
}

// TestForeignSourceSunSpecPVBatchNamesOnlyOwnRecords is a regression test:
// the SunSpec PV builder already matches its own source, binding, candidate,
// service and capability identifiers. With a second contributor's
// descriptor, binding, identity link, services, capabilities, current
// candidates on every fact and a retained observation, a same-generation
// refresh, a driver-generation replacement and an epoch replacement build the
// same batch as on the single-source asset and name no foreign identifier.
func TestForeignSourceSunSpecPVBatchNamesOnlyOwnRecords(t *testing.T) {
	core, err := newTestPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	words := observedFroniusFloatControlsWords()
	first := pvCoreDraft(t, words, "source-epoch:pv:foreign-a", 1, 2_000)
	if _, err := core.ingest(first); err != nil {
		t.Fatal(err)
	}
	view, err := core.currentForValidation(first.assetID)
	if err != nil {
		t.Fatal(err)
	}
	single := view.snapshot
	foreign := foreignSourcePVContributor(t, single)
	foreignIDs := foreignSourcePVForeignIDs(single, foreign)
	if len(foreignIDs) == 0 {
		t.Fatal("fixture holds no foreign identifier")
	}
	refreshed := append([]uint16(nil), words...)
	pvCoreSetFloat(refreshed, 20, 4_321.5)
	replacedEpoch := pvCoreDraft(t, refreshed, "source-epoch:pv:foreign-b", 1, 5_000)
	replacedEpoch.lifecycle.sourceStartedAt = pvCoreWall(4_500)
	replacedEpoch.source.StartedAt = replacedEpoch.lifecycle.sourceStartedAt
	for _, tc := range []struct {
		name        string
		draft       pvPublicationDraft
		retirements []semreg.SourceEpochID
		fenced      []semreg.Uint64
	}{
		{name: "same-generation refresh", draft: pvCoreDraft(t, refreshed, "source-epoch:pv:foreign-a", 1, 3_000), retirements: []semreg.SourceEpochID{}},
		{name: "driver-generation replacement", draft: pvCoreDraft(t, refreshed, "source-epoch:pv:foreign-a", 2, 4_000), retirements: []semreg.SourceEpochID{}, fenced: []semreg.Uint64{"1"}},
		{name: "epoch replacement", draft: replacedEpoch, retirements: []semreg.SourceEpochID{"source-epoch:pv:foreign-a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := pvPublicationBatch(tc.draft, single, true, nil, "2", tc.retirements, tc.fenced)
			if err != nil {
				t.Fatal(err)
			}
			got, err := pvPublicationBatch(tc.draft, foreign, true, nil, "2", tc.retirements, tc.fenced)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range foreignSourcePVBatchNames(got) {
				if foreignIDs[id] {
					t.Fatalf("batch names the foreign identifier %s", id)
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("a second contributor changed the batch:\n got %+v\nwant %+v", got, want)
			}
		})
	}
}
