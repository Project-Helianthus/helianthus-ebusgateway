package mcp

import (
	"reflect"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// foreignSourceTeslaContributor adds a second source's descriptor, binding,
// identity link, services, capabilities, one current candidate per fact
// envelope and one retained observation to a copy of snapshot, each copied
// from the publisher's own record under synthetic foreign identifiers.
func foreignSourceTeslaContributor(t *testing.T, snapshot semreg.Snapshot) semreg.Snapshot {
	t.Helper()
	const (
		source  semreg.SourceID        = "source:synthetic-foreign"
		epoch   semreg.SourceEpochID   = "source-epoch:synthetic-foreign"
		binding semreg.NativeBindingID = "binding:synthetic-foreign"
	)
	foreignEpoch, foreignBinding, generation := epoch, binding, semreg.Uint64("1")
	if len(snapshot.Sources) == 0 || len(snapshot.Bindings) == 0 || len(snapshot.Facts) == 0 || len(snapshot.Services) == 0 || len(snapshot.Capabilities) == 0 {
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
		service.InstanceID, service.BindingID, service.SourceEpochID, service.DriverGeneration = semreg.ServiceInstanceID("service:synthetic-foreign-"+string(rune('a'+i))), binding, epoch, generation
		services = append(services, service)
	}
	snapshot.Services = services
	capabilities := append([]semreg.CapabilityInstance{}, snapshot.Capabilities...)
	for i, capability := range snapshot.Capabilities {
		capability.InstanceID, capability.BindingID, capability.SourceEpochID, capability.DriverGeneration = semreg.CapabilityInstanceID("capability:synthetic-foreign-"+string(rune('a'+i))), binding, epoch, generation
		capability.ServiceInstance = "service:synthetic-foreign-a"
		capabilities = append(capabilities, capability)
	}
	snapshot.Capabilities = capabilities
	facts := make([]semreg.FactEnvelope, 0, len(snapshot.Facts))
	for i, envelope := range snapshot.Facts {
		candidate := envelope.Candidates[0]
		candidate.CandidateID = semreg.CandidateID("candidate:synthetic-foreign-" + string(rune('a'+i)))
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

// foreignSourceTeslaAssertNoForeign fails when batch upserts, revises or
// withdraws any identifier of the foreign contributor.
func foreignSourceTeslaAssertNoForeign(t *testing.T, batch semreg.PublicationBatch, single, foreign semreg.Snapshot) {
	t.Helper()
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
	for _, service := range single.Services {
		own[string(service.InstanceID)] = true
	}
	for _, capability := range single.Capabilities {
		own[string(capability.InstanceID)] = true
	}
	foreignIDs := map[string]bool{}
	mark := func(id string) {
		if !own[id] {
			foreignIDs[id] = true
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
	if len(foreignIDs) == 0 {
		t.Fatal("fixture holds no foreign identifier")
	}
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
	for _, id := range named {
		if foreignIDs[id] {
			t.Fatalf("batch names the foreign identifier %s", id)
		}
	}
}

// TestForeignSourceTeslaEVSEBatchNamesOnlyOwnRecords is a regression test:
// the Tesla EVSE builder already matches its own binding and candidate
// identifiers. With a second contributor's descriptor, binding, identity
// link, services, capabilities, current candidates on every fact and a
// retained observation, the refresh batch (allocated current upserted and
// revised) and the withdrawal batch (allocated current withdrawn) are
// identical to the single-source batches and name no foreign identifier.
func TestForeignSourceTeslaEVSEBatchNamesOnlyOwnRecords(t *testing.T) {
	base := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		provisional bool
	}{
		{name: "allocated current revised", provisional: true},
		{name: "allocated current withdrawn"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := newTestTeslaGen3EVSESemanticPublication(teslaSemanticConfig())
			if err != nil {
				t.Fatal(err)
			}
			p.now = func() time.Time { return base.Add(time.Second) }
			p.readClock = func() (uint64, error) { return 0, nil }
			if err := p.Publish(teslaGen3EVSECurrentLimitV1FixtureSource(t), TeslaGen3EVSESemanticEvidence{ObservationID: "observation:foreign-source-first", ObservedAt: base, EvaluatedAt: base, MonotonicNS: 1, EvaluatedMonotonicNS: 1, Sequence: 1}); err != nil {
				t.Fatal(err)
			}
			if !teslaGen3EVSESnapshotHasFact(p.current, "evse.limit.allocated_current") {
				t.Fatal("fixture publication holds no allocated current")
			}
			next := teslaGen3EVSECurrentLimitV1FixtureSource(t)
			if !tc.provisional {
				next.Provisional = nil
			}
			evaluated := base.Add(time.Second)
			evidence := TeslaGen3EVSESemanticEvidence{ObservationID: "observation:foreign-source-next", ObservedAt: evaluated, EvaluatedAt: evaluated, MonotonicNS: 2, EvaluatedMonotonicNS: 2, Sequence: 2}
			mono := teslaGen3EVSEMonotonic(p.cfg.ClockEpoch, 2)
			single, _, _, _, err := p.batch(next, evidence, p.current, true, []semreg.SourceEpochID{}, evaluated, mono)
			if err != nil {
				t.Fatal(err)
			}
			if tc.provisional && len(single.FactUpserts) != 2 || !tc.provisional && len(single.FactWithdrawals) != 1 {
				t.Fatalf("fixture batch upserts=%d withdrawals=%d", len(single.FactUpserts), len(single.FactWithdrawals))
			}
			foreign := foreignSourceTeslaContributor(t, p.current)
			batch, _, _, _, err := p.batch(next, evidence, foreign, true, []semreg.SourceEpochID{}, evaluated, mono)
			if err != nil {
				t.Fatal(err)
			}
			foreignSourceTeslaAssertNoForeign(t, batch, p.current, foreign)
			if !reflect.DeepEqual(batch, single) {
				t.Fatalf("a second contributor changed the batch:\n got %+v\nwant %+v", batch, single)
			}
		})
	}
}
