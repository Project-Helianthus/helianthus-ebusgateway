package promotioncapture

import (
	"reflect"
	"strings"
	"testing"
)

func TestAssembleCampaignPromotesOnlyTwoWindowMatches(t *testing.T) {
	registry := mustRegistry(t)
	pre := assemblyCheckpoint(t, registry, PhasePreRestart, true)
	post := assemblyCheckpoint(t, registry, PhasePostRestart, true)
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
		"m7-candidate-0008",
		"m7-candidate-0009",
		"m7-candidate-0010",
		"m7-candidate-0011",
		"m7-candidate-0012",
		"m7-candidate-0013",
		"m7-candidate-0014",
		"m7-candidate-0015",
		"m7-candidate-0016",
		"m7-candidate-0017",
		"m7-candidate-0019",
		"m7-candidate-0020",
		"m7-candidate-0021",
		"m7-candidate-0022",
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
	if first.CampaignHash != "sha256:d773b1fc2296a1aaa62bcb746a44353f3f1fd58f6c0caf97b6e4a5779aa31940" {
		t.Fatalf("campaign hash = %s", first.CampaignHash)
	}
	if first.SourceBindings.ReplayHash != "sha256:f801e9be69a45e96d1ee816d3ffc318bf52b75c8404557818356d1c4cd8fc3f4" {
		t.Fatalf("replay hash = %s", first.SourceBindings.ReplayHash)
	}
	wantDossiers := map[string]string{
		"m7-candidate-0008": "sha256:60ae47f983fba5bc87df650dc1298e59a53caf26a5a9dfbb79903e05f4b98195",
		"m7-candidate-0009": "sha256:9455d79766fcb5c5f0ecdc2d6b7e0cd1f1a2b9c14e2503def1dd0fb881d4bd64",
		"m7-candidate-0010": "sha256:46e4fe683d8ec4e1c630089be7c0e0031778ef0bfc10cb64e9c71be3adefb91a",
		"m7-candidate-0011": "sha256:35f199012777b88a54bab3a51d2c27e85545c2525189ec698cac827c4724058f",
		"m7-candidate-0012": "sha256:d229e594511ed36d1be3ba44f7d530640ef4bfdbce1d3aba49ba252da76f83c3",
		"m7-candidate-0013": "sha256:b51a9fd3096e6121fb6b63bb042ca658caa1acee168f1df616f5a52a8809d455",
		"m7-candidate-0014": "sha256:306ed77f2db67b9b59372902c3fc33eb6be1467b46597cd8f685973eae44bcaf",
		"m7-candidate-0015": "sha256:23d8a739d045ed51e6195de22a60c22e41f816303cbec1ef22ca239c8a17477a",
		"m7-candidate-0016": "sha256:dc76344ab1fea2840826d70d823d32c3d009a126080914d99b60d0e2fe7ac035",
		"m7-candidate-0017": "sha256:fdcbbb6159f13b19234fa48b9050fd586852284385ed016c2122319fb76ff1e2",
		"m7-candidate-0019": "sha256:7fe867f55ed343c9ef28b5fb0f85be34c5919fa327340fa8a1cb3102a2cb6b92",
		"m7-candidate-0020": "sha256:8fc03200bcf240bf169af270f0c414d97af18a902bb01dabf92ad1bf41ceee85",
		"m7-candidate-0021": "sha256:75057eaa3afa6625fdf0c90f4fe7b1ac99e9bf11c76a067ba84787f8ac509c3a",
		"m7-candidate-0022": "sha256:a0982a0168f480f05fd95223463cb514820dc81e582fd96269f2d314018637e4",
	}
	actualDossiers := make(map[string]string)
	for _, candidate := range first.Candidates {
		if candidate.DossierHash != nil {
			actualDossiers[candidate.CandidateID] = *candidate.DossierHash
		}
	}
	if !reflect.DeepEqual(actualDossiers, wantDossiers) {
		t.Fatalf("dossier hashes = %#v, want %#v", actualDossiers, wantDossiers)
	}
}

func TestAssembleCampaignRejectsOutcomeRewrittenAgainstSamples(t *testing.T) {
	registry := mustRegistry(t)
	pre := assemblyCheckpoint(t, registry, PhasePreRestart, true)
	post := assemblyCheckpoint(t, registry, PhasePostRestart, true)
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

func assemblyCheckpoint(t *testing.T, registry *Registry, phase WindowPhase, withFailures bool) WindowCheckpoint {
	t.Helper()
	window := assemblyWindow(phase)
	candidates := make([]CapturedCandidateWindow, 0, 22)
	for index, definition := range registry.Candidates() {
		var ebusIdentity *EBusIdentity
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
		switch definition.ProtocolEligibility {
		case ProtocolCrossProtocol:
			input = assemblyInput(t, definition, window, ebusIdentity, eebusIdentity, withFailures)
		case ProtocolEEBusNative:
			value := BooleanValue(false)
			if definition.ComparatorClass == ComparatorString {
				value = StringValue("stable-" + definition.CandidateID)
			}
			var previous *TypedValue
			if phase == PhasePostRestart {
				previous = &value
			}
			input = nativeInput(t, definition, window, value, previous)
			input.ExpectedEEBusIdentityHash = eebusIdentity.IdentityHash
			input.ObservedEEBusIdentityHash = stringPointer(eebusIdentity.IdentityHash)
		}
		evaluation, err := registry.EvaluateWindow(definition.CandidateID, input)
		if err != nil {
			t.Fatalf("EvaluateWindow(%s, %s): %v", definition.CandidateID, phase, err)
		}
		candidates = append(candidates, CapturedCandidateWindow{
			CandidateID: definition.CandidateID, FactHash: definition.FactHash,
			SourceStatus: definition.SourceStatus, RetirementState: definition.RetirementState,
			SemanticPath: definition.SemanticPath, ValidationMode: definition.ValidationMode,
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
	withFailures bool,
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

	if !withFailures {
		return input
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
