package promotioncapture

import (
	"reflect"
	"strings"
	"testing"
)

func TestAssembleCampaignPublishesDeclaredLeavesAndRecordsVerification(t *testing.T) {
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

	wantFindings := map[string]struct {
		level   VerificationLevel
		finding Outcome
	}{
		"m7-candidate-0005": {VerificationNotRun, OutcomeStale},
		"m7-candidate-0006": {VerificationNotRun, OutcomeMissing},
		"m7-candidate-0007": {VerificationNotRun, OutcomeMissing},
		"m7-candidate-0018": {VerificationCaptureMismatch, OutcomeMismatch},
	}
	var published []string
	for _, candidate := range first.Candidates {
		if candidate.RetirementState != nil {
			if candidate.Decision != DecisionWithheld || candidate.Visibility != VisibilityRawDebugOnly ||
				candidate.VerificationLevel != VerificationNotRun || candidate.DossierHash != nil {
				t.Fatalf("retired candidate malformed: %+v", candidate)
			}
			continue
		}
		published = append(published, candidate.CandidateID)
		if candidate.Decision != DecisionPromoted || candidate.Visibility != VisibilityPublished ||
			candidate.SourceStatus != SourceStatusSpecificationDeclared || candidate.DossierHash == nil ||
			len(candidate.Assessments) != 2 {
			t.Fatalf("declared leaf malformed: %+v", candidate)
		}
		want, recordedFinding := wantFindings[candidate.CandidateID]
		if !recordedFinding {
			if candidate.VerificationLevel != VerificationByCapture || candidate.TerminalState != nil {
				t.Fatalf("%s level = %s finding = %v, want VERIFIED_BY_CAPTURE", candidate.CandidateID, candidate.VerificationLevel, candidate.TerminalState)
			}
		} else if candidate.VerificationLevel != want.level || candidate.TerminalState == nil || *candidate.TerminalState != want.finding {
			t.Fatalf("%s level = %s finding = %v, want %s %s", candidate.CandidateID, candidate.VerificationLevel, candidate.TerminalState, want.level, want.finding)
		}
		dossierHash, err := digestWithoutField(DossierDomain, candidate, "dossier_hash")
		if err != nil || *candidate.DossierHash != dossierHash {
			t.Fatalf("%s dossier hash = %s, want %s (%v)", candidate.CandidateID, *candidate.DossierHash, dossierHash, err)
		}
	}
	if len(published) != 18 {
		t.Fatalf("published = %v, want the 18 declared leaves", published)
	}

	replayBindings, err := objectWithoutField(first.SourceBindings, "replay_hash")
	if err != nil {
		t.Fatalf("objectWithoutField: %v", err)
	}
	replayHash, err := CanonicalDigest(ReplayDomain, map[string]any{
		"source_bindings": replayBindings, "windows": first.Windows, "candidates": first.Candidates,
	})
	if err != nil || first.SourceBindings.ReplayHash != replayHash {
		t.Fatalf("replay hash = %s, want %s (%v)", first.SourceBindings.ReplayHash, replayHash, err)
	}
	campaignHash, err := digestWithoutField(CampaignDomain, first, "campaign_hash")
	if err != nil || first.CampaignHash != campaignHash {
		t.Fatalf("campaign hash = %s, want %s (%v)", first.CampaignHash, campaignHash, err)
	}
}

func TestAssembleCampaignBindsVerificationLevelIntoDossierHash(t *testing.T) {
	registry := mustRegistry(t)
	pre := assemblyCheckpoint(t, registry, PhasePreRestart, true)
	post := assemblyCheckpoint(t, registry, PhasePostRestart, true)
	campaign, err := AssembleCampaign(registry, assemblyManifest(pre.CampaignID), pre, post)
	if err != nil {
		t.Fatalf("AssembleCampaign: %v", err)
	}
	candidate := campaignCandidate(t, campaign, "m7-candidate-0018")
	candidate.VerificationLevel = VerificationByCapture
	candidate.TerminalState = nil
	rewritten, err := digestWithoutField(DossierDomain, candidate, "dossier_hash")
	if err != nil {
		t.Fatalf("digestWithoutField: %v", err)
	}
	if rewritten == *candidate.DossierHash {
		t.Fatal("rewriting a recorded mismatch as verified keeps the dossier hash")
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
