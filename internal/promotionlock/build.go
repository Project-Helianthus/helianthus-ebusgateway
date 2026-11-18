package promotionlock

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
)

const (
	resultHashDomainV1             = "HELIANTHUS:LEAF-PROMOTION-LOCK-RESULT:V1"
	capturedAssessmentHashDomainV1 = "HELIANTHUS:LEAF-PROMOTION-CAPTURED-ASSESSMENT:V1"
)

func Build(inputs InputsV1) ([]byte, error) {
	inputs = cloneInputsV1(inputs)
	if err := validateRegistryV1(inputs.Registry); err != nil {
		return nil, err
	}
	switch inputs.Profile {
	case ProfileSyntheticConformanceV1:
		if err := validateSyntheticInputs(inputs); err != nil {
			return nil, err
		}
		return mustContractArtifactV1("docs/platform/fixtures/leaf-promotion-dossier/v1/positive/result.json"), nil
	case ProfileDeclaredFromSpecificationV1:
		sources, err := validateCapturedSources(inputs)
		if err != nil {
			return nil, err
		}
		result, err := buildManifest(sources)
		if err != nil {
			return nil, err
		}
		return encodeCanonicalV1(result)
	default:
		return nil, fail("captured.input")
	}
}

func buildManifest(sources verifiedSourcesV1) (ResultV1, error) {
	assessment, err := buildCapturedAssessment(sources)
	if err != nil {
		return ResultV1{}, err
	}
	public := make([]PublicAssessmentV1, len(assessment.Assessments))
	for index, item := range assessment.Assessments {
		public[index] = PublicAssessmentV1{
			CandidateID:        item.CandidateID,
			FactHash:           item.FactHash,
			SourceStatus:       item.SourceStatus,
			TerminalState:      cloneString(item.TerminalState),
			Decision:           item.Decision,
			WithholdingReasons: append([]string{}, item.WithholdingReasons...),
			RetestTrigger:      cloneRetest(item.RetestTrigger),
		}
	}
	counts := CountsV1{Total: uint64(len(public)), Promoted: uint64(len(public))}
	result := ResultV1{
		Contract:       ContractV1,
		SchemaVersion:  SchemaVersionV1,
		Profile:        ProfileDeclaredFromSpecificationV1,
		ExportTier:     ExportTierPublicRedactedV1,
		SourceBindings: cloneSourceBindings(&assessment.SourceBindings),
		ReplayTool:     replayToolV1,
		ReplayVersion:  SchemaVersionV1,
		Counts:         counts,
		DossierCount:   0,
		Assessments:    public,
		M9ConsumerGate: consumerGateV1(counts),
		Verdict:        VerdictValidPromotionLockV1,
	}
	result.ResultHash = hashWithoutFieldV1(resultHashDomainV1, result, "result_hash")
	return result, nil
}

// buildCapturedAssessment derives the declared assessment: every leaf the
// capture registry declares from the cited specification counts as promoted.
// The verified M7 and M8 sources only bind the result; a source fact that names
// a declared leaf must carry the same fact hash.
func buildCapturedAssessment(sources verifiedSourcesV1) (capturedAssessmentV1, error) {
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		return capturedAssessmentV1{}, fail("registry.binding")
	}
	declared := make([]promotioncapture.CandidateDefinition, 0, declaredLeafCountV1)
	byID := make(map[string]promotioncapture.CandidateDefinition, declaredLeafCountV1)
	for _, candidate := range registry.Candidates() {
		if candidate.ProtocolEligibility == promotioncapture.ProtocolTerminal {
			continue
		}
		if candidate.SourceStatus != promotioncapture.SourceStatusSpecificationDeclared || candidate.SemanticPath == nil {
			return capturedAssessmentV1{}, fail("promotion.declared")
		}
		declared = append(declared, candidate)
		byID[candidate.CandidateID] = candidate
	}
	if len(declared) != declaredLeafCountV1 || len(byID) != declaredLeafCountV1 {
		return capturedAssessmentV1{}, fail("assessment.derivation")
	}
	seenFacts := make(map[string]struct{}, len(sources.graph.Facts))
	for _, fact := range sources.graph.Facts {
		if _, duplicate := seenFacts[fact.CandidateID]; duplicate {
			return capturedAssessmentV1{}, fail("assessment.ordering")
		}
		seenFacts[fact.CandidateID] = struct{}{}
		if candidate, ok := byID[fact.CandidateID]; ok && candidate.FactHash != fact.FactHash {
			return capturedAssessmentV1{}, fail("assessment.derivation")
		}
	}
	sort.Slice(declared, func(left, right int) bool {
		if *declared[left].SemanticPath == *declared[right].SemanticPath {
			return declared[left].CandidateID < declared[right].CandidateID
		}
		return *declared[left].SemanticPath < *declared[right].SemanticPath
	})
	coexistencePass := sources.report.Verdict == "PASS"
	assessments := make([]privateAssessmentV1, len(declared))
	for index, candidate := range declared {
		assessments[index] = assessDeclaredLeaf(candidate, coexistencePass)
	}
	sourceBindings := PublicSourceBindingsV1{
		M7GatewaySourceCommit:  m7GatewaySourceCommitV1,
		M7DocsSourceCommit:     m7DocsSourceCommitV1,
		M7GraphID:              sources.graph.GraphID,
		M7GraphHash:            sources.graph.GraphHash,
		M7ReplayID:             sources.replay.ReplayID,
		M7ReplayHash:           sources.replay.ReplayHash,
		M7StatusProjectionID:   m7StatusProjectionIDV1,
		M7StatusProjectionHash: m7StatusProjectionHashV1,
		M8GatewaySourceCommit:  m8GatewaySourceCommitV1,
		M8DocsSourceCommit:     m8DocsSourceCommitV1,
		M8EvidenceID:           sources.evidence.EvidenceID,
		M8EvidenceHash:         sources.evidence.EvidenceHash,
		M8ReportID:             sources.report.ReportID,
		M8ReportHash:           sources.report.ReportHash,
		CoexistenceVerdict:     sources.report.Verdict,
	}
	assessment := capturedAssessmentV1{
		Contract:       capturedAssessmentContractV1,
		SchemaVersion:  SchemaVersionV1,
		Profile:        ProfileDeclaredFromSpecificationV1,
		ExportTier:     exportTierPrivateOperatorV1,
		SourceBindings: sourceBindings,
		Assessments:    assessments,
		Dossiers:       []string{},
		M9ConsumerGate: M9ReadyV1,
	}
	assessment.AssessmentHash = hashWithoutFieldV1(capturedAssessmentHashDomainV1, assessment, "assessment_hash")
	return assessment, nil
}

// assessDeclaredLeaf records a declared leaf. The eligibility flags record what
// the specification declares and what the lock observed; none of them
// conditions the promotion. The lock never runs the captured comparison, so the
// comparator flags stay false and the retest trigger names the two capture
// windows that record the leaf's verification level.
func assessDeclaredLeaf(candidate promotioncapture.CandidateDefinition, coexistencePass bool) privateAssessmentV1 {
	sourceKinds := []string{"EEBUS"}
	if candidate.ProtocolEligibility == promotioncapture.ProtocolCrossProtocol {
		sourceKinds = []string{"EBUS", "EEBUS"}
	}
	return privateAssessmentV1{
		CandidateID:   candidate.CandidateID,
		SemanticPath:  *candidate.SemanticPath,
		FactHash:      candidate.FactHash,
		SourceStatus:  candidate.SourceStatus,
		TerminalState: nil,
		Eligibility: eligibilityV1{
			ExactEBusIdentity: candidate.EBusSelector != nil, ExactEEBusPath: candidate.EEBusSource != nil,
			CoexistenceNoDrift: coexistencePass,
		},
		Decision:           decisionPromotedV1,
		WithholdingReasons: []string{},
		RetestTrigger: RetestV1{
			Trigger: "NEW_SYNCHRONIZED_BUNDLE", RequiredSourceKinds: sourceKinds, MinimumNewSamples: verificationWindowsV1,
		},
	}
}

// consumerGateV1 states whether consumers may read the result's leaves: a
// result with promoted leaves opens the gate; one without stays blocked.
func consumerGateV1(counts CountsV1) string {
	if counts.Promoted == 0 {
		return M9BlockedZeroPromotedLeavesV1
	}
	return M9ReadyV1
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneRetest(value RetestV1) RetestV1 {
	return RetestV1{
		Trigger: value.Trigger, RequiredSourceKinds: append([]string(nil), value.RequiredSourceKinds...),
		MinimumNewSamples: value.MinimumNewSamples,
	}
}

func cloneSourceBindings(value *PublicSourceBindingsV1) *PublicSourceBindingsV1 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func hashWithoutFieldV1(domain string, value any, field string) string {
	raw, err := json.Marshal(value)
	if err != nil {
		panic("promotionlock: hash encoding failed: " + err.Error())
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		panic("promotionlock: hash normalization failed: " + err.Error())
	}
	delete(object, field)
	canonical, err := json.Marshal(object)
	if err != nil {
		panic("promotionlock: hash canonicalization failed: " + err.Error())
	}
	digest := sha256.Sum256(append(append([]byte(domain), 0), canonical...))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func hashObjectWithoutFieldV1(domain string, object map[string]any, field string) string {
	view := make(map[string]any, len(object)-1)
	for key, value := range object {
		if key != field {
			view[key] = value
		}
	}
	canonical, err := json.Marshal(view)
	if err != nil {
		panic("promotionlock: result hash canonicalization failed: " + err.Error())
	}
	digest := sha256.Sum256(append(append([]byte(domain), 0), canonical...))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func encodeCanonicalV1(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fail("captured.result")
	}
	var normalized any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return nil, fail("captured.result")
	}
	canonical, err := json.Marshal(normalized)
	if err != nil {
		return nil, fail("captured.result")
	}
	return append(canonical, '\n'), nil
}
