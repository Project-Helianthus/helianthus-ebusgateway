package promotioncapture

import (
	"reflect"
	"strings"
	"testing"
)

func TestAssembleCampaignPromotesOnlyTwoWindowMatches(t *testing.T) {
	registry := mustRegistry(t)
	pre := assemblyCheckpoint(t, registry, PhasePreRestart)
	post := assemblyCheckpoint(t, registry, PhasePostRestart)
	manifest := assemblyManifest(pre.CampaignID)

	first, err := AssembleCampaign(registry, manifest, pre, post)
	if err != nil {
		t.Fatalf("AssembleCampaign: %v", err)
	}
	second, err := AssembleCampaign(registry, manifest, pre, post)
	if err != nil {
		t.Fatalf("AssembleCampaign second pass: %v", err)
	}
	firstRaw, err := CanonicalJSON(first)
	if err != nil {
		t.Fatalf("CanonicalJSON(first): %v", err)
	}
	secondRaw, err := CanonicalJSON(second)
	if err != nil {
		t.Fatalf("CanonicalJSON(second): %v", err)
	}
	if !reflect.DeepEqual(firstRaw, secondRaw) {
		t.Fatal("campaign assembly is not deterministic")
	}

	wantPromoted := []string{
		"m7-candidate-0009",
		"m7-candidate-0010",
		"m7-candidate-0011",
		"m7-candidate-0012",
		"m7-candidate-0014",
		"m7-candidate-0015",
		"m7-candidate-0016",
	}
	var promoted []string
	for _, candidate := range first.Candidates {
		if candidate.Decision == DecisionPromoted {
			promoted = append(promoted, candidate.CandidateID)
			if candidate.DossierHash == nil || candidate.Visibility != VisibilityLockedNotExposed ||
				candidate.TerminalState != nil || len(candidate.Assessments) != 2 {
				t.Fatalf("promoted candidate malformed: %+v", candidate)
			}
			continue
		}
		if candidate.DossierHash != nil || candidate.Visibility != VisibilityRawDebugOnly {
			t.Fatalf("withheld candidate malformed: %+v", candidate)
		}
	}
	if !reflect.DeepEqual(promoted, wantPromoted) {
		t.Fatalf("promoted = %v, want %v", promoted, wantPromoted)
	}
	for candidateID, terminal := range map[string]Outcome{
		"m7-candidate-0005": OutcomeStale,
		"m7-candidate-0006": OutcomeMissing,
		"m7-candidate-0007": OutcomeMissing,
		"m7-candidate-0018": OutcomeMismatch,
	} {
		candidate := campaignCandidate(t, first, candidateID)
		if candidate.TerminalState == nil || *candidate.TerminalState != terminal {
			t.Fatalf("%s terminal = %v, want %s", candidateID, candidate.TerminalState, terminal)
		}
	}
	if first.CampaignHash != "sha256:458ce116327b359a3d7e3fa9e1f06751960444beaa0009e55f6289301b9859c6" {
		t.Fatalf("campaign hash = %s", first.CampaignHash)
	}
	if first.SourceBindings.ReplayHash != "sha256:854f999bd6adb13cb2b25965debd2a5de8505c8131fd44e29d1aee1c788c9151" {
		t.Fatalf("replay hash = %s", first.SourceBindings.ReplayHash)
	}
	wantDossiers := map[string]string{
		"m7-candidate-0009": "sha256:3e1683606ca38c3f226b0b3f6b164d8ba46cc013cad5c304ef40b4dc0c2263a6",
		"m7-candidate-0010": "sha256:afcc8ae66170f411eca44b0492470e0442ea19da4458b747c6877577652977d7",
		"m7-candidate-0011": "sha256:3a138cd748bce86279a9dce52b9aef44f2e919738ceec2ec1b90758c43f0938f",
		"m7-candidate-0012": "sha256:1f108adf07f7c7c6e813fdf85ed6fe3b276f8b2b46bb2371f7aae51a2afcaf9e",
		"m7-candidate-0014": "sha256:8bc2146d007f9cc29630e7c9c178ecffec13835313c053f958659852382ccc5d",
		"m7-candidate-0015": "sha256:36d21be845236c4467a954b333908e45a5085b024f2309b4eea0ed6accd61c74",
		"m7-candidate-0016": "sha256:478c4c571cf9eb837bd070977c1ea69e1e1cea84a79f349612a0887411519d96",
	}
	for candidateID, want := range wantDossiers {
		candidate := campaignCandidate(t, first, candidateID)
		if candidate.DossierHash == nil || *candidate.DossierHash != want {
			t.Fatalf("%s dossier hash = %v, want %s", candidateID, candidate.DossierHash, want)
		}
	}
}

func TestAssembleCampaignRejectsOutcomeRewrittenAgainstSamples(t *testing.T) {
	registry := mustRegistry(t)
	pre := assemblyCheckpoint(t, registry, PhasePreRestart)
	post := assemblyCheckpoint(t, registry, PhasePostRestart)
	for index := range pre.Candidates {
		candidate := &pre.Candidates[index]
		if candidate.CandidateID == "m7-candidate-0018" {
			candidate.Evaluation.Outcome = OutcomeMatch
			candidate.Evaluation.Assessment.Comparator.Outcome = OutcomeMatch
		}
	}
	pre.CheckpointHash = ""
	if err := BindCheckpointHash(&pre); err != nil {
		t.Fatalf("BindCheckpointHash: %v", err)
	}
	if _, err := AssembleCampaign(registry, assemblyManifest(pre.CampaignID), pre, post); err == nil {
		t.Fatal("rewritten outcome was accepted")
	}
}

func assemblyManifest(campaignID string) CampaignAssemblyManifest {
	commit := strings.Repeat("a", 40)
	return CampaignAssemblyManifest{
		EvidenceMode: EvidenceModeLiveCapture,
		Provenance: CampaignProvenance{
			Class: EvidenceModeLiveCapture, CaptureCampaignID: &campaignID,
			CaptureReceipts:        []string{assemblyDigest("1"), assemblyDigest("2")},
			DeploymentSourceCommit: &commit,
			DeploymentSourceHash:   stringPointer(assemblyDigest("3")),
			DeploymentBinaryHash:   stringPointer(assemblyDigest("4")),
		},
		SourceBindings: CampaignSourceBindings{
			RegistrySHA256: RegistrySHA256, DocsEEBusCommit: DocsEEBusCommit,
			M7GraphID: "dcfgv1:live", M7GraphHash: assemblyDigest("5"), M7GraphBytesHash: assemblyDigest("6"),
			M7ReplayID: "dcfrv1:live", M7ReplayHash: assemblyDigest("7"), M7ReplayBytesHash: assemblyDigest("8"),
			M7StatusID: "dcfpsv1:live", M7StatusHash: assemblyDigest("9"), M7StatusBytesHash: assemblyDigest("a"),
			M8EvidenceID: "mrcv1:live", M8EvidenceHash: assemblyDigest("b"), M8EvidenceBytesHash: assemblyDigest("c"),
			M8ReportID: "mrcrv1:live", M8ReportHash: assemblyDigest("d"), M8ReportBytesHash: assemblyDigest("e"),
		},
	}
}

func assemblyCheckpoint(t *testing.T, registry *Registry, phase WindowPhase) WindowCheckpoint {
	t.Helper()
	window := assemblyWindow(phase)
	candidates := make([]CapturedCandidateWindow, 0, 18)
	for index, definition := range registry.Candidates() {
		var ebusIdentity *B524Identity
		var eebusIdentity *EEBusIdentity
		if definition.EBusSelector != nil {
			identity, err := NewB524Identity(*definition.EBusSelector, window.AdmittedSource)
			if err != nil {
				t.Fatalf("NewB524Identity(%s): %v", definition.CandidateID, err)
			}
			ebusIdentity = &identity
		}
		if definition.EEBusSource != nil {
			identity, err := NewEEBusIdentity(
				*definition.EEBusSource, "service-live", "device-live",
				[]uint64{uint64(index + 1)}, uint64(index+101),
			)
			if err != nil {
				t.Fatalf("NewEEBusIdentity(%s): %v", definition.CandidateID, err)
			}
			eebusIdentity = &identity
		}

		input := WindowAssessmentInput{}
		if definition.ProtocolEligibility == ProtocolEligible {
			input = assemblyInput(t, definition, window, ebusIdentity, eebusIdentity)
		}
		evaluation, err := registry.EvaluateWindow(definition.CandidateID, input)
		if err != nil {
			t.Fatalf("EvaluateWindow(%s, %s): %v", definition.CandidateID, phase, err)
		}
		candidates = append(candidates, CapturedCandidateWindow{
			CandidateID: definition.CandidateID, FactHash: definition.FactHash,
			SourceStatus: definition.SourceStatus, SemanticPath: definition.SemanticPath,
			ComparatorClass: definition.ComparatorClass, ProtocolEligibility: definition.ProtocolEligibility,
			EBusIdentity: ebusIdentity, EEBusIdentity: eebusIdentity, Evaluation: evaluation,
		})
	}
	processID := "process-pre"
	if phase == PhasePostRestart {
		processID = "process-post"
	}
	checkpoint := WindowCheckpoint{
		Contract: CheckpointContractV1, SchemaVersion: 1, CampaignID: "campaign-live",
		ProcessInstanceID: processID, TrustStateID: window.TrustStateHash,
		PeerBindingID: window.PeerBindingHash, Window: window, Candidates: candidates,
		CapturedAt: window.EndedAt,
	}
	if err := BindCheckpointHash(&checkpoint); err != nil {
		t.Fatalf("BindCheckpointHash: %v", err)
	}
	return checkpoint
}

func assemblyInput(
	t *testing.T,
	candidate CandidateDefinition,
	window Window,
	ebusIdentity *B524Identity,
	eebusIdentity *EEBusIdentity,
) WindowAssessmentInput {
	t.Helper()
	constraints := candidate.EEBusSource.DeclaredConstraints
	value := Decimal{}
	if constraints != nil {
		value = constraints.Minimum
	}
	input := validInput(t, candidate, value, value)
	input.Window = window
	input.ExpectedEBusIdentityHash = ebusIdentity.SelectorHash
	input.ExpectedEEBusIdentityHash = eebusIdentity.IdentityHash
	input.ObservedEBusIdentityHash = stringPointer(ebusIdentity.SelectorHash)
	input.ObservedEEBusIdentityHash = stringPointer(eebusIdentity.IdentityHash)
	assemblyRebindSample(input.EBusSample, window, "2026-08-11T10:00:09Z")
	assemblyRebindSample(input.EEBusSample, window, "2026-08-11T10:00:09.100000000Z")
	if window.Phase == PhasePostRestart {
		assemblyRebindSample(input.EBusSample, window, "2026-08-11T10:05:09Z")
		assemblyRebindSample(input.EEBusSample, window, "2026-08-11T10:05:09.100000000Z")
	}

	switch candidate.CandidateID {
	case "m7-candidate-0005":
		if window.Phase == PhasePostRestart {
			input.EBusSample.ObservedAt = "2026-08-11T10:04:00Z"
			input.EEBusSample.ObservedAt = "2026-08-11T10:04:00.100000000Z"
		}
	case "m7-candidate-0006":
		input.EBusSample = nil
		input.ObservedEBusIdentityHash = nil
	case "m7-candidate-0007":
		if window.Phase == PhasePostRestart {
			input.EBusSample = nil
			input.ObservedEBusIdentityHash = nil
		}
	case "m7-candidate-0018":
		if window.Phase == PhasePreRestart {
			input = validInput(t, candidate, constraints.Minimum, constraints.Maximum)
			input.Window = window
			input.ExpectedEBusIdentityHash = ebusIdentity.SelectorHash
			input.ExpectedEEBusIdentityHash = eebusIdentity.IdentityHash
			input.ObservedEBusIdentityHash = stringPointer(ebusIdentity.SelectorHash)
			input.ObservedEEBusIdentityHash = stringPointer(eebusIdentity.IdentityHash)
			assemblyRebindSample(input.EBusSample, window, "2026-08-11T10:00:09Z")
			assemblyRebindSample(input.EEBusSample, window, "2026-08-11T10:00:09.100000000Z")
		}
	}
	return input
}

func assemblyRebindSample(sample *Sample, window Window, observedAt string) {
	if sample == nil {
		return
	}
	sample.ObservedAt = observedAt
	sample.CaptureGeneration = window.CaptureGeneration
	if sample.Source == SourceEBus {
		sample.PollGeneration = stringPointer(window.EBusPollGeneration)
		return
	}
	sample.RuntimeEpoch = int64Pointer(window.EEBusRuntimeEpoch)
	sample.ConnectionGeneration = int64Pointer(window.ConnectionGeneration)
}

func assemblyWindow(phase WindowPhase) Window {
	window := testWindow()
	window.Phase = phase
	if phase == PhasePreRestart {
		window.WindowID = "window-pre"
		window.StartedAt = "2026-08-11T10:00:00Z"
		window.EndedAt = "2026-08-11T10:00:10Z"
		return window
	}
	window.WindowID = "window-post"
	window.StartedAt = "2026-08-11T10:05:00Z"
	window.EndedAt = "2026-08-11T10:05:10Z"
	window.CaptureGeneration = "capture-2"
	window.ProcessInstanceHash = assemblyDigest("2")
	window.ConnectionGeneration++
	window.EBusPollGeneration = "poll-generation-2"
	return window
}

func campaignCandidate(t *testing.T, campaign Campaign, candidateID string) CampaignCandidate {
	t.Helper()
	for _, candidate := range campaign.Candidates {
		if candidate.CandidateID == candidateID {
			return candidate
		}
	}
	t.Fatalf("candidate %s not found", candidateID)
	return CampaignCandidate{}
}

func assemblyDigest(value string) string {
	return "sha256:" + strings.Repeat(value, 64)
}
