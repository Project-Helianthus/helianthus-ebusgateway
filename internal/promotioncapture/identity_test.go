package promotioncapture

import (
	"encoding/json"
	"testing"
)

func TestIssue784CapturedIdentitiesBindCompletePrivateSelectors(t *testing.T) {
	registry, err := DefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	candidate, ok := registry.Candidate("m7-candidate-0018")
	if !ok || candidate.EBusSelector == nil || candidate.EEBusSource == nil {
		t.Fatal("outside-air candidate is incomplete")
	}
	ebusIdentity, err := NewB524Identity(*candidate.EBusSelector, 0xfd)
	if err != nil {
		t.Fatal(err)
	}
	if ebusIdentity.TargetPseudonym[:7] != "target-" || !digestPattern.MatchString(ebusIdentity.SelectorHash) {
		t.Fatalf("invalid eBUS identity: %+v", ebusIdentity)
	}
	eebusIdentity, err := NewEEBusIdentity(
		*candidate.EEBusSource,
		"b1b7197b064084e4cfef2365105d8d36ff185e5b",
		"d:synthetic-vr940",
		[]uint64{6},
		11,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !digestPattern.MatchString(eebusIdentity.SourceProfileHash) ||
		!digestPattern.MatchString(eebusIdentity.IdentityHash) ||
		eebusIdentity.EntitySlot != "outside_sensor" ||
		eebusIdentity.DeclaredConstraints.Step != (Decimal{Number: 5, Scale: -1}) {
		t.Fatalf("invalid eeBUS identity: %+v", eebusIdentity)
	}

	// Caller mutations cannot rewrite the catalog-owned profile held by the identity.
	candidate.EEBusSource.DescriptionFunctions[0] = "rewritten"
	if eebusIdentity.DescriptionFunctions[0] != "measurementDescriptionListData" {
		t.Fatal("identity aliases the source profile")
	}
}

func TestIssue784WindowCheckpointHashIsDeterministicAndSelfExcluding(t *testing.T) {
	checkpoint := WindowCheckpoint{
		Contract: "helianthus.internal.leaf-promotion-window-checkpoint.v1", SchemaVersion: 1,
		CampaignID: "campaign-1", ProcessInstanceID: "process-11111111111111111111111111111111",
		TrustStateID: "trust-1", PeerBindingID: "peer-1",
		Window: Window{
			WindowID: "campaign-1-pre", Phase: PhasePreRestart,
			StartedAt: "2026-08-11T10:00:00Z", EndedAt: "2026-08-11T10:00:01Z",
			CaptureGeneration: "capture-1", ProcessInstanceHash: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			LocalIdentityHash: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			TrustStateHash:    "sha256:3333333333333333333333333333333333333333333333333333333333333333",
			PeerBindingHash:   "sha256:4444444444444444444444444444444444444444444444444444444444444444",
			AdmittedSource:    0xfd, EEBusRuntimeEpoch: 2, ConnectionGeneration: 94,
			EBusPollGeneration: "poll-1", M8NoDrift: true, RollbackExact: true,
		},
		Candidates: []CapturedCandidateWindow{}, CapturedAt: "2026-08-11T10:00:01Z",
	}
	if err := BindCheckpointHash(&checkpoint); err != nil {
		t.Fatal(err)
	}
	first := checkpoint.CheckpointHash
	checkpoint.CheckpointHash = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if err := BindCheckpointHash(&checkpoint); err != nil {
		t.Fatal(err)
	}
	if checkpoint.CheckpointHash != first {
		t.Fatalf("self-field changed hash: %s != %s", checkpoint.CheckpointHash, first)
	}
	raw, err := json.Marshal(checkpoint)
	if err != nil || !json.Valid(raw) {
		t.Fatalf("checkpoint is not JSON: %v", err)
	}
}
