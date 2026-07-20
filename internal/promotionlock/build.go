package promotionlock

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/candidatefacts"
)

const manifestHashDomainV1 = "HELIANTHUS:LEAF-PROMOTION-LOCK-MANIFEST:V1"

func Build(inputs InputsV1) ([]byte, error) {
	sources, err := validateSources(inputs)
	if err != nil {
		return nil, err
	}
	manifest, err := buildManifest(sources)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, fail("manifest.encode")
	}
	return append(raw, '\n'), nil
}

func buildManifest(sources verifiedSourcesV1) (ManifestV1, error) {
	assessments := make([]CandidateAssessmentV1, 0, len(sources.graph.Facts))
	for _, fact := range sources.graph.Facts {
		assessment := assessCandidate(fact)
		if assessment.ExactEBusIdentity && assessment.ExactEEBusIdentity && assessment.ComparatorPassed {
			return ManifestV1{}, fail("promotion.dossier_required")
		}
		assessments = append(assessments, assessment)
	}
	sort.Slice(assessments, func(left, right int) bool {
		if assessments[left].SemanticPath == assessments[right].SemanticPath {
			return assessments[left].CandidateID < assessments[right].CandidateID
		}
		return assessments[left].SemanticPath < assessments[right].SemanticPath
	})

	binding := PinnedContractV1()
	manifest := ManifestV1{
		Contract:      ContractV1,
		SchemaVersion: SchemaVersionV1,
		ContractBinding: ContractReferenceV1{
			OwnerRepository: binding.OwnerRepository,
			OwnerCommit:     binding.OwnerCommit,
			OwnerTree:       binding.OwnerTree,
		},
		SourceBindings: SourceBindingsV1{
			M7GraphID:       sources.graph.GraphID,
			M7GraphHash:     sources.graph.GraphHash,
			M7ReplayID:      sources.replay.ReplayID,
			M7ReplayHash:    sources.replay.ReplayHash,
			M8EvidenceID:    sources.evidence.EvidenceID,
			M8EvidenceHash:  sources.evidence.EvidenceHash,
			M8ReportID:      sources.report.ReportID,
			M8ReportHash:    sources.report.ReportHash,
			EvidenceClass:   sources.evidence.EvidenceClass,
			LiveVR940Claim:  sources.evidence.Scope.LiveVR940Claim,
			CoexistencePass: sources.report.Verdict == "PASS",
		},
		PromotionState: "LOCKED_ZERO_PROMOTION",
		Counts: CountsV1{
			Candidates: uint64(len(assessments)),
			Dossiers:   0,
			Promoted:   0,
			Withheld:   uint64(len(assessments)),
		},
		Assessments:          assessments,
		PromotedPaths:        make([]string, 0),
		LockedDossierIDs:     make([]string, 0),
		M9ConsumerGate:       "BLOCKED_ZERO_PROMOTED_LEAVES",
		Verdict:              "VALID_ZERO_PROMOTION",
		StableSurfaceChanges: false,
	}
	manifest.ManifestHash = hashManifest(manifest)
	manifest.ManifestID = "lplmv1:" + manifest.ManifestHash
	return manifest, nil
}

func assessCandidate(fact candidatefacts.FactV1) CandidateAssessmentV1 {
	exactEBus := fact.Provenance.EBus != nil
	exactEEBus := hasExactEEBusIdentity(fact.Provenance.EEBus)
	passed := fact.Comparator.Outcome == "MATCH"
	reason := "COMPARATOR_NOT_PASSED"
	if fact.TerminalNegativeState != nil {
		reason = "TERMINAL_NEGATIVE_STATE"
	} else if !exactEBus {
		reason = "MISSING_EBUS_IDENTITY"
	} else if !exactEEBus {
		reason = "MISSING_EEBUS_ENTITY_FEATURE_PATH"
	}
	return CandidateAssessmentV1{
		CandidateID:        fact.CandidateID,
		SemanticPath:       fact.ProposedPath,
		CandidateStatus:    fact.Status,
		CandidateHash:      fact.FactHash,
		TerminalState:      cloneString(fact.TerminalNegativeState),
		ExactEBusIdentity:  exactEBus,
		ExactEEBusIdentity: exactEEBus,
		ComparatorPassed:   passed,
		Decision:           "WITHHELD",
		Visibility:         "RAW_DEBUG_ONLY",
		DossierState:       "NOT_CREATED",
		ReasonCode:         reason,
		RetestTrigger:      fact.RetestTrigger.TriggerCode,
		MinimumNewSamples:  fact.RetestTrigger.MinimumNewSamples,
	}
}

func hasExactEEBusIdentity(identity *candidatefacts.EEBusIdentityV1) bool {
	if identity == nil || identity.Entity == "" || identity.Service == "" || identity.Feature == "" ||
		len(identity.FeaturePath) < 3 {
		return false
	}
	return identity.FeaturePath[0].Kind == "ENTITY" && identity.FeaturePath[0].Selector == identity.Entity &&
		identity.FeaturePath[1].Kind == "SERVICE" && identity.FeaturePath[1].Selector == identity.Service &&
		identity.FeaturePath[2].Kind == "FEATURE" && identity.FeaturePath[2].Selector == identity.Feature
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func hashManifest(manifest ManifestV1) string {
	manifest.ManifestID = ""
	manifest.ManifestHash = ""
	raw, err := json.Marshal(manifest)
	if err != nil {
		panic("promotionlock: manifest hash encoding failed: " + err.Error())
	}
	digest := sha256.Sum256(append([]byte(manifestHashDomainV1+"\x00"), raw...))
	return "sha256:" + hex.EncodeToString(digest[:])
}
