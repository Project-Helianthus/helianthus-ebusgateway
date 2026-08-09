package promotionlock

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/candidatefacts"
)

const (
	resultHashDomainV1             = "HELIANTHUS:LEAF-PROMOTION-LOCK-RESULT:V1"
	capturedAssessmentHashDomainV1 = "HELIANTHUS:LEAF-PROMOTION-CAPTURED-ASSESSMENT:V1"
)

var withholdingReasonOrderV1 = []string{
	"SOURCE_STATUS_WITHHELD",
	"SOURCE_STATUS_RAW_ONLY",
	"EXACT_EBUS_IDENTITY_MISSING",
	"EXACT_EBUS_IDENTITY_INELIGIBLE",
	"EXACT_EEBUS_PATH_MISSING",
	"COMPARATOR_NOT_MATCHED",
	"CAPTURED_EVIDENCE_INELIGIBLE",
	"COEXISTENCE_PROOF_MISSING",
}

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
	case ProfileCapturedZeroPromotionV1:
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
			WithholdingReasons: append([]string(nil), item.WithholdingReasons...),
			RetestTrigger:      cloneRetest(item.RetestTrigger),
		}
	}
	result := ResultV1{
		Contract:       ContractV1,
		SchemaVersion:  SchemaVersionV1,
		Profile:        ProfileCapturedZeroPromotionV1,
		ExportTier:     ExportTierPublicRedactedV1,
		SourceBindings: cloneSourceBindings(&assessment.SourceBindings),
		ReplayTool:     replayToolV1,
		ReplayVersion:  SchemaVersionV1,
		Counts:         CountsV1{Total: uint64(len(public)), Withheld: uint64(len(public))},
		DossierCount:   0,
		Assessments:    public,
		M9ConsumerGate: M9BlockedZeroPromotedLeavesV1,
		Verdict:        VerdictValidZeroPromotionV1,
	}
	result.ResultHash = hashWithoutFieldV1(resultHashDomainV1, result, "result_hash")
	return result, nil
}

func buildCapturedAssessment(sources verifiedSourcesV1) (capturedAssessmentV1, error) {
	facts := append([]candidatefacts.FactV1(nil), sources.graph.Facts...)
	sort.Slice(facts, func(left, right int) bool {
		if facts[left].ProposedPath == facts[right].ProposedPath {
			return facts[left].CandidateID < facts[right].CandidateID
		}
		return facts[left].ProposedPath < facts[right].ProposedPath
	})
	if len(facts) != 18 {
		return capturedAssessmentV1{}, fail("assessment.derivation")
	}
	assessments := make([]privateAssessmentV1, len(facts))
	statusCounts := map[string]int{}
	seenIDs := make(map[string]struct{}, len(facts))
	for index, fact := range facts {
		if _, duplicate := seenIDs[fact.CandidateID]; duplicate {
			return capturedAssessmentV1{}, fail("assessment.ordering")
		}
		seenIDs[fact.CandidateID] = struct{}{}
		if fact.Status != "RAW_ONLY" && fact.Status != "WITHHELD" {
			return capturedAssessmentV1{}, fail("promotion.forbidden")
		}
		statusCounts[fact.Status]++
		assessments[index] = assessCandidate(fact, sources.report.Verdict == "PASS")
	}
	if statusCounts["RAW_ONLY"] != 14 || statusCounts["WITHHELD"] != 4 {
		return capturedAssessmentV1{}, fail("assessment.derivation")
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
		Profile:        ProfileCapturedZeroPromotionV1,
		ExportTier:     exportTierPrivateOperatorV1,
		SourceBindings: sourceBindings,
		Assessments:    assessments,
		Dossiers:       []string{},
		M9ConsumerGate: M9BlockedZeroPromotedLeavesV1,
	}
	assessment.AssessmentHash = hashWithoutFieldV1(capturedAssessmentHashDomainV1, assessment, "assessment_hash")
	return assessment, nil
}

func assessCandidate(fact candidatefacts.FactV1, coexistencePass bool) privateAssessmentV1 {
	hasEBus, exactEBus := exactEBusIdentity(fact)
	exactEEBus := hasExactEEBusIdentity(fact.Provenance.EEBus)
	comparatorMatch := fact.Comparator.Outcome == "MATCH"
	capturedEligible := exactEBus && exactEEBus && comparatorMatch && fact.Status == "CANDIDATE"
	reasons := map[string]bool{
		"CAPTURED_EVIDENCE_INELIGIBLE": !capturedEligible,
	}
	if fact.Status == "WITHHELD" {
		reasons["SOURCE_STATUS_WITHHELD"] = true
	} else {
		reasons["SOURCE_STATUS_RAW_ONLY"] = true
	}
	if !hasEBus {
		reasons["EXACT_EBUS_IDENTITY_MISSING"] = true
	} else if !exactEBus {
		reasons["EXACT_EBUS_IDENTITY_INELIGIBLE"] = true
	}
	if !exactEEBus {
		reasons["EXACT_EEBUS_PATH_MISSING"] = true
	}
	if !comparatorMatch {
		reasons["COMPARATOR_NOT_MATCHED"] = true
	}
	if !coexistencePass {
		reasons["COEXISTENCE_PROOF_MISSING"] = true
	}
	orderedReasons := make([]string, 0, len(reasons))
	for _, reason := range withholdingReasonOrderV1 {
		if reasons[reason] {
			orderedReasons = append(orderedReasons, reason)
		}
	}
	return privateAssessmentV1{
		CandidateID:   fact.CandidateID,
		SemanticPath:  fact.ProposedPath,
		FactHash:      fact.FactHash,
		SourceStatus:  fact.Status,
		TerminalState: cloneString(fact.TerminalNegativeState),
		Eligibility: eligibilityV1{
			ExactEBusIdentity: exactEBus, ExactEEBusPath: exactEEBus, ComparatorMatch: comparatorMatch,
			CapturedEvidenceEligible: capturedEligible, CoexistenceNoDrift: coexistencePass,
		},
		Decision:           "WITHHELD",
		WithholdingReasons: orderedReasons,
		RetestTrigger: RetestV1{
			Trigger: fact.RetestTrigger.TriggerCode, RequiredSourceKinds: append([]string(nil), fact.RetestTrigger.RequiredSourceKinds...),
			MinimumNewSamples: fact.RetestTrigger.MinimumNewSamples,
		},
	}
}

func exactEBusIdentity(fact candidatefacts.FactV1) (bool, bool) {
	identity := fact.Provenance.EBus
	if identity == nil && fact.Provenance.SourceTerminal != nil {
		identity = &fact.Provenance.SourceTerminal.EBusIdentity
	}
	if identity == nil {
		return false, false
	}
	return true, member(identity.Family, "B509", "B524", "B555")
}

func hasExactEEBusIdentity(identity *candidatefacts.EEBusIdentityV1) bool {
	if identity == nil || identity.Entity == "" || identity.Service == "" || identity.Feature == "" || len(identity.FeaturePath) < 3 {
		return false
	}
	return identity.FeaturePath[0].Kind == "SERVICE" && identity.FeaturePath[0].Selector == identity.Service &&
		identity.FeaturePath[1].Kind == "ENTITY" && identity.FeaturePath[1].Selector == identity.Entity &&
		identity.FeaturePath[2].Kind == "FEATURE" && identity.FeaturePath[2].Selector == identity.Feature
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
