package promotioncapture

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestIssue790RegistryClosesTwentyTwoRecordsAndEighteenLeaves(t *testing.T) {
	if RegistrySHA256 != "sha256:00ceefc05439e9aec5830b640661cdc6be2b503f9365eed437e3dbffdf6d0678" {
		t.Fatalf("RegistrySHA256 = %q", RegistrySHA256)
	}
	if DocsContractCommit != "c4cea33a3f6262e31801cad35d663e08317de4dd" {
		t.Fatalf("DocsContractCommit = %q", DocsContractCommit)
	}
	if DocsEEBusCommit != "ed5354421ddf0a2005f496e3fd65675990032b5e" {
		t.Fatalf("DocsEEBusCommit = %q", DocsEEBusCommit)
	}

	registry := mustRegistry(t)
	candidates := registry.Candidates()
	if len(candidates) != 22 {
		t.Fatalf("candidate count = %d, want 22", len(candidates))
	}

	counts := map[ProtocolEligibility]int{}
	paths := map[string]bool{}
	for index, candidate := range candidates {
		wantID := "m7-candidate-" + leftPadFour(index+1)
		if candidate.CandidateID != wantID {
			t.Fatalf("candidate[%d] = %q, want %q", index, candidate.CandidateID, wantID)
		}
		counts[candidate.ProtocolEligibility]++
		if index < 4 {
			if candidate.RetirementState == nil || *candidate.RetirementState != RetirementTerminalNotALeaf ||
				candidate.SemanticPath != nil || candidate.ValidationMode != nil {
				t.Fatalf("retired candidate malformed: %+v", candidate)
			}
			continue
		}
		if candidate.RetirementState != nil || candidate.SemanticPath == nil || candidate.ValidationMode == nil {
			t.Fatalf("real leaf malformed: %+v", candidate)
		}
		if paths[*candidate.SemanticPath] {
			t.Fatalf("duplicate semantic path %q", *candidate.SemanticPath)
		}
		paths[*candidate.SemanticPath] = true
	}
	if counts[ProtocolTerminal] != 4 || counts[ProtocolCrossProtocol] != 11 || counts[ProtocolEEBusNative] != 7 || len(paths) != 18 {
		t.Fatalf("eligibility counts = %#v, paths = %d", counts, len(paths))
	}
	for _, id := range []string{"m7-candidate-0008", "m7-candidate-0013", "m7-candidate-0017"} {
		candidate := mustCandidate(t, registry, id)
		if *candidate.ValidationMode != ValidationEEBusNativeCapability {
			t.Fatalf("%s validation mode = %q", id, *candidate.ValidationMode)
		}
	}
	for _, id := range []string{"m7-candidate-0019", "m7-candidate-0020", "m7-candidate-0021", "m7-candidate-0022"} {
		candidate := mustCandidate(t, registry, id)
		if *candidate.ValidationMode != ValidationEEBusNativeMetadata || candidate.ComparatorClass != ComparatorString {
			t.Fatalf("%s native metadata contract malformed", id)
		}
	}
}

func TestIssue790StringTypedValueIsExclusive(t *testing.T) {
	value := StringValue("Vaillant")
	if value.Kind != ValueString || value.String == nil || *value.String != "Vaillant" {
		t.Fatalf("StringValue = %#v", value)
	}
	if err := value.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	value.Boolean = boolPointer(true)
	if err := value.Validate(); err == nil {
		t.Fatal("multi-valued STRING was accepted")
	}
	if err := StringValue("").Validate(); err == nil {
		t.Fatal("empty STRING was accepted")
	}
}

func TestIssue790NativeMetadataRequiresExactRestartStability(t *testing.T) {
	registry := mustRegistry(t)
	candidate := mustCandidate(t, registry, "m7-candidate-0019")
	preValue := StringValue("Vaillant")
	preInput := nativeInput(t, candidate, testWindow(), preValue, nil)
	pre := mustEvaluate(t, registry, candidate.CandidateID, preInput)
	if pre.Outcome != OutcomeNativeValid || pre.Assessment == nil || pre.Assessment.EBusSample != nil ||
		pre.Assessment.ObservedEBusIdentityHash != nil || pre.Assessment.SkewNS != nil {
		t.Fatalf("PRE native evaluation = %#v", pre)
	}

	postWindow := testWindow()
	postWindow.Phase = PhasePostRestart
	postWindow.WindowID = "window-post"
	postWindow.CaptureGeneration = "capture-post"
	postWindow.EBusPollGeneration = "poll-post"
	postWindow.ProcessInstanceHash = "sha256:" + strings.Repeat("7", 64)
	postValue := StringValue("Vaillant Group")
	post := mustEvaluate(t, registry, candidate.CandidateID, nativeInput(t, candidate, postWindow, postValue, &preValue))
	if post.Outcome != OutcomeNativeDrift || post.Assessment == nil || post.Assessment.Comparator.Outcome != OutcomeNativeDrift {
		t.Fatalf("POST native drift evaluation = %#v", post)
	}
}

func TestIssue790NativeCapabilityUsesExactBooleanMapping(t *testing.T) {
	registry := mustRegistry(t)
	candidate := mustCandidate(t, registry, "m7-candidate-0008")
	value := BooleanValue(false)
	result := mustEvaluate(t, registry, candidate.CandidateID, nativeInput(t, candidate, testWindow(), value, nil))
	if result.Outcome != OutcomeNativeValid || result.Assessment == nil || result.Assessment.Comparator.MappingHash == nil {
		t.Fatalf("native capability evaluation = %#v", result)
	}

	bad := StringValue("false")
	result = mustEvaluate(t, registry, candidate.CandidateID, nativeInput(t, candidate, testWindow(), bad, nil))
	if result.Outcome != OutcomeNativeDrift {
		t.Fatalf("mistyped native capability outcome = %q, want NATIVE_DRIFT", result.Outcome)
	}
}

func TestIssue790B555FallbackIdentityIsNotSerializedAsB524(t *testing.T) {
	registry := mustRegistry(t)
	candidate := mustCandidate(t, registry, "m7-candidate-0006")
	if candidate.EBusFallback == nil {
		t.Fatal("candidate 0006 has no B555 fallback")
	}
	identity, err := NewB555Identity(*candidate.EBusFallback, 0x15, 0xfd)
	if err != nil {
		t.Fatalf("NewB555Identity: %v", err)
	}
	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if object["family"] != "B555" || object["operation"] != "TIMER_READ" || object["slot_index"] != float64(0) {
		t.Fatalf("B555 identity = %s", raw)
	}
	for _, forbidden := range []string{"opcode", "GG", "II", "RR", "group_meaning", "instance_gate", "register_category"} {
		if _, found := object[forbidden]; found {
			t.Fatalf("B555 identity contains B524 field %q: %s", forbidden, raw)
		}
	}
	if !strings.HasPrefix(identity.SelectorHash, "sha256:") {
		t.Fatalf("selector hash = %q", identity.SelectorHash)
	}
}

func TestIssue790EEBusIdentityContainsOnlySourceProfileAndNativeSelector(t *testing.T) {
	registry := mustRegistry(t)
	candidate := mustCandidate(t, registry, "m7-candidate-0007")
	identity, err := NewEEBusIdentity(*candidate.EEBusSource, "service", "device", []uint64{1}, 2)
	if err != nil {
		t.Fatalf("NewEEBusIdentity: %v", err)
	}
	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for _, forbidden := range []string{"conversion", "mapping_profile"} {
		if _, found := object[forbidden]; found {
			t.Fatalf("eeBUS identity contains candidate-owned %q", forbidden)
		}
	}
	if !reflect.DeepEqual(object["field_path"], candidate.EEBusSource.FieldPath) {
		t.Fatalf("field_path = %#v", object["field_path"])
	}
}

func TestIssue790AllEighteenValidLeavesRemainLockedNotExposed(t *testing.T) {
	registry := mustRegistry(t)
	pre := assemblyCheckpoint(t, registry, PhasePreRestart, false)
	post := assemblyCheckpoint(t, registry, PhasePostRestart, false)
	campaign, err := AssembleCampaign(registry, assemblyManifest(pre.CampaignID), pre, post)
	if err != nil {
		t.Fatalf("AssembleCampaign: %v", err)
	}
	realLeaves := 0
	retired := 0
	for _, candidate := range campaign.Candidates {
		if candidate.RetirementState != nil {
			retired++
			if candidate.Decision != DecisionWithheld || len(candidate.Assessments) != 0 {
				t.Fatalf("retired candidate entered promotion denominator: %+v", candidate)
			}
			continue
		}
		realLeaves++
		if candidate.Decision != DecisionPromoted || candidate.Visibility != VisibilityLockedNotExposed ||
			candidate.TerminalState != nil || candidate.DossierHash == nil || len(candidate.Assessments) != 2 {
			t.Fatalf("valid real leaf is not locked: %+v", candidate)
		}
	}
	if len(campaign.Candidates) != 22 || realLeaves != 18 || retired != 4 {
		t.Fatalf("records=%d real=%d retired=%d", len(campaign.Candidates), realLeaves, retired)
	}
}

func nativeInput(t *testing.T, candidate CandidateDefinition, window Window, value TypedValue, previous *TypedValue) WindowAssessmentInput {
	t.Helper()
	sample := sample(t, SourceEEBus, window, window.EndedAt, value, value, candidate.EEBusSource.Unit)
	return WindowAssessmentInput{
		Window:                    window,
		ExpectedEEBusIdentityHash: testEEBusIdentityHash,
		ObservedEEBusIdentityHash: stringPointer(testEEBusIdentityHash),
		EEBusSample:               &sample,
		PreviousNativeValue:       previous,
	}
}

func leftPadFour(value int) string {
	return fmt.Sprintf("%04d", value)
}

func boolPointer(value bool) *bool { return &value }
