package promotioncapture

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestIssue790RegistryClosesTwentyTwoRecordsAndEighteenLeaves(t *testing.T) {
	if RegistrySHA256 != fmt.Sprintf("sha256:%x", sha256.Sum256(EmbeddedRegistryBytes())) {
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

func TestIssue1694EighteenLeavesAreDeclaredFromSpecification(t *testing.T) {
	var document map[string]any
	if err := json.Unmarshal(EmbeddedRegistryBytes(), &document); err != nil {
		t.Fatalf("Unmarshal registry: %v", err)
	}
	if document["positive_visibility"] != "PUBLISHED" || document["withheld_visibility"] != "RAW_DEBUG_ONLY" ||
		document["required_windows"] != float64(2) {
		t.Fatalf("visibility contract = positive %v, withheld %v, windows %v",
			document["positive_visibility"], document["withheld_visibility"], document["required_windows"])
	}
	if got, want := document["verification_levels"], []any{"VERIFIED_BY_CAPTURE", "CAPTURE_MISMATCH", "NOT_RUN"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("verification_levels = %#v, want %#v", got, want)
	}

	registry := mustRegistry(t)
	declared, retired := 0, 0
	for _, candidate := range registry.Candidates() {
		if candidate.ProtocolEligibility == ProtocolTerminal {
			retired++
			if candidate.SourceStatus != "WITHHELD" || candidate.TerminalState == nil ||
				candidate.RetirementState == nil || *candidate.RetirementState != RetirementTerminalNotALeaf {
				t.Fatalf("retired candidate changed: %+v", candidate)
			}
			continue
		}
		declared++
		if candidate.SourceStatus != "SPECIFICATION_DECLARED_PUBLISHED" {
			t.Fatalf("%s source status = %q, want SPECIFICATION_DECLARED_PUBLISHED", candidate.CandidateID, candidate.SourceStatus)
		}
	}
	if declared != 18 || retired != 4 {
		t.Fatalf("declared=%d retired=%d, want 18 and 4", declared, retired)
	}
}

func TestIssue1694CaptureRecordsVerificationLevelWithoutLockingExposure(t *testing.T) {
	registry := mustRegistry(t)
	type recorded struct {
		level   string
		finding any
	}
	for _, withFailures := range []bool{false, true} {
		want := map[string]recorded{}
		for _, candidate := range registry.Candidates() {
			if candidate.ProtocolEligibility != ProtocolTerminal {
				want[candidate.CandidateID] = recorded{level: "VERIFIED_BY_CAPTURE"}
			}
		}
		if withFailures {
			want["m7-candidate-0005"] = recorded{level: "NOT_RUN", finding: "STALE"}
			want["m7-candidate-0006"] = recorded{level: "NOT_RUN", finding: "MISSING"}
			want["m7-candidate-0007"] = recorded{level: "NOT_RUN", finding: "MISSING"}
			want["m7-candidate-0018"] = recorded{level: "CAPTURE_MISMATCH", finding: "MISMATCH"}
		}
		pre := assemblyCheckpoint(t, registry, PhasePreRestart, withFailures)
		post := assemblyCheckpoint(t, registry, PhasePostRestart, withFailures)
		campaign, err := AssembleCampaign(registry, assemblyManifest(pre.CampaignID), pre, post)
		if err != nil {
			t.Fatalf("AssembleCampaign(withFailures=%t): %v", withFailures, err)
		}
		raw, err := CanonicalJSON(campaign)
		if err != nil {
			t.Fatalf("CanonicalJSON: %v", err)
		}
		var document struct {
			Candidates []map[string]any `json:"candidates"`
		}
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatalf("Unmarshal campaign: %v", err)
		}
		published := 0
		for _, candidate := range document.Candidates {
			id, _ := candidate["candidate_id"].(string)
			expected, eligible := want[id]
			assessments, _ := candidate["assessments"].([]any)
			if !eligible {
				if candidate["decision"] != "WITHHELD" || candidate["visibility"] != "RAW_DEBUG_ONLY" ||
					candidate["verification_level"] != "NOT_RUN" || candidate["dossier_hash"] != nil ||
					candidate["terminal_state"] == nil || len(assessments) != 0 {
					t.Fatalf("retired candidate %s = %#v", id, candidate)
				}
				continue
			}
			published++
			if candidate["decision"] != "PROMOTED" || candidate["visibility"] != "PUBLISHED" ||
				candidate["verification_level"] != expected.level || candidate["terminal_state"] != expected.finding ||
				candidate["dossier_hash"] == nil || len(assessments) != 2 {
				t.Fatalf("withFailures=%t leaf %s = decision %v visibility %v level %v finding %v dossier %v assessments %d; want PROMOTED PUBLISHED %s %v",
					withFailures, id, candidate["decision"], candidate["visibility"], candidate["verification_level"],
					candidate["terminal_state"], candidate["dossier_hash"], len(assessments), expected.level, expected.finding)
			}
		}
		if published != 18 || len(document.Candidates) != 22 {
			t.Fatalf("withFailures=%t published leaves = %d, want 18", withFailures, published)
		}
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
