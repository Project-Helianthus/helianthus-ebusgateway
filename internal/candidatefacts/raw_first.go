package candidatefacts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const m625SourceContractV1 = "helianthus.eebus.m625.public-redacted-evidence.v1"

type rawFirstFactV1 struct {
	sortKey string
	fact    FactV1
}

// BuildRawFirstV1 derives only reviewable raw and terminal facts from an
// already captured synchronized-evidence bundle. It never evaluates a
// cross-runtime comparator or creates a promotable semantic claim.
func BuildRawFirstV1(sourceBundleRaw, sourceReplayRaw []byte) ([]byte, []byte, error) {
	bundle, _, err := verifySourceInputs(sourceBundleRaw, sourceReplayRaw)
	if err != nil {
		return nil, nil, err
	}
	bundleID, ok := stringValue(bundle["bundle_id"])
	if !ok {
		return nil, nil, fail("provenance.binding")
	}

	drafts, err := rawFirstFacts(bundle, bundleID)
	if err != nil {
		return nil, nil, err
	}
	if len(drafts) == 0 || len(drafts) > 9999 {
		return nil, nil, fail("provenance.binding")
	}
	sort.SliceStable(drafts, func(i, j int) bool { return drafts[i].sortKey < drafts[j].sortKey })
	facts := make([]FactV1, len(drafts))
	for index := range drafts {
		facts[index] = drafts[index].fact
		facts[index].CandidateID = fmt.Sprintf("m7-candidate-%04d", index+1)
	}

	graph, err := Build(BuildInputV1{
		SourceBundle:     sourceBundleRaw,
		SourceReplay:     sourceReplayRaw,
		ComparatorDrafts: []ComparatorDraftV1{rawFirstComparator(bundle)},
		Facts:            facts,
	})
	if err != nil {
		return nil, nil, err
	}
	replay, err := Replay(graph, sourceBundleRaw, sourceReplayRaw)
	if err != nil {
		return nil, nil, err
	}
	return graph, replay, nil
}

func rawFirstFacts(bundle map[string]any, bundleID string) ([]rawFirstFactV1, error) {
	facts := make([]rawFirstFactV1, 0)
	terminalCounts := map[string]int{}
	sources, _ := arrayValue(bundle["sources"])
	for _, raw := range sources {
		source, _ := objectValue(raw)
		binding, bindingOK := objectValue(source["source_binding"])
		bindingKind, bindingKindOK := stringValue(binding["source_kind"])
		artifactIDs, artifactIDsOK := arrayValue(source["artifact_ids"])
		if !bindingOK || !bindingKindOK || !artifactIDsOK || len(artifactIDs) != 0 ||
			source["source_kind"] != "EBUS" || source["state"] != "UNAVAILABLE" ||
			source["error_category"] != "BACKEND_UNAVAILABLE" ||
			!member(bindingKind, "EBUS_B509", "EBUS_B524", "EBUS_B555") ||
			sourceHasArtifact(bundle, asString(source["source_id"])) {
			continue
		}
		identity, err := decodeEBusIdentity(source["ebus_identity"])
		if err != nil {
			return nil, err
		}
		refs, err := decodeEvidenceRefs(source["evidence_refs"])
		if err != nil {
			return nil, err
		}
		version, ok := unsigned(source["source_schema_version"])
		if !ok {
			return nil, fail("provenance.binding")
		}
		family := strings.ToLower(identity.Family)
		terminalCounts[family]++
		path := fmt.Sprintf("/candidates/ebus/%s/source_terminal_%04d", family, terminalCounts[family])
		terminalState := "NOT_TESTED"
		facts = append(facts, rawFirstFactV1{
			sortKey: path + "\x00" + asString(source["source_id"]),
			fact: FactV1{
				ProposedPath: path,
				Status:       "WITHHELD", TerminalNegativeState: &terminalState,
				Confidence: ConfidenceV1{Level: "LOW", Basis: "INSUFFICIENT", ScoreMilli: 0},
				Provenance: ProvenanceV1{
					SourceBundleID: bundleID, NativeEvidenceRefs: refs,
					SourceTerminal: &SourceTerminalV1{
						SourceID: asString(source["source_id"]), SourceKind: "EBUS",
						BindingSourceKind: bindingKind, SourceContract: asString(source["source_contract"]),
						SourceSchemaVersion: version, Phase: asString(source["phase"]),
						State: "UNAVAILABLE", ErrorCategory: "BACKEND_UNAVAILABLE",
						EBusIdentity: identity, EvidenceRefs: append([]EvidenceRefV1(nil), refs...),
					},
				},
				Comparator:    ComparatorEvaluationV1{DraftID: "NUMERIC_WINDOW_V1_DRAFT", Samples: []ComparatorSampleV1{}, Outcome: "NOT_EVALUATED"},
				Falsifier:     FalsifierV1{ConditionCode: "PROVENANCE_BREAKS", ExpectedTerminalState: "NOT_TESTED", Description: "The native source remains unavailable in the bounded capture."},
				RetestTrigger: RetestTriggerV1{TriggerCode: "SOURCE_RECOVERED", RequiredSourceKinds: []string{"EBUS"}, MinimumNewSamples: 1},
				DebugOnly:     true,
			},
		})
	}

	artifacts, _ := arrayValue(bundle["artifacts"])
	eebusFacts := make([]rawFirstFactV1, 0)
	cloudFacts := make([]rawFirstFactV1, 0)
	for _, raw := range artifacts {
		artifact, _ := objectValue(raw)
		refs, err := decodeEvidenceRefs(artifact["evidence_refs"])
		if err != nil {
			return nil, err
		}
		switch artifact["source_kind"] {
		case "EEBUS":
			if artifact["source_contract"] != m625SourceContractV1 {
				continue
			}
			normalized, ok := objectValue(artifact["normalized_evidence"])
			if !ok || normalized["contract"] != m625SourceContractV1 {
				return nil, fail("provenance.binding")
			}
			paths, pathsOK := arrayValue(normalized["feature_paths"])
			observations, observationsOK := arrayValue(normalized["observations"])
			if !pathsOK || !observationsOK {
				return nil, fail("provenance.binding")
			}
			observed := make(map[uint64]bool)
			for _, rawObservation := range observations {
				observation, ok := objectValue(rawObservation)
				pathIndex, indexOK := unsigned(observation["path_index"])
				if !ok || !indexOK || pathIndex >= uint64(len(paths)) {
					return nil, fail("provenance.binding")
				}
				if observation["quality"] == "OBSERVED" && observation["terminal_classification"] == "SUCCESS" {
					observed[pathIndex] = true
				}
			}
			for pathIndex := range observed {
				identity, err := decodeEEBusIdentity(paths[pathIndex])
				if err != nil {
					return nil, err
				}
				identityKey, err := canonicalKey(paths[pathIndex])
				if err != nil {
					return nil, fail("provenance.binding")
				}
				terminal := (*string)(nil)
				sourceID := asString(artifact["source_id"])
				artifactID := asString(artifact["artifact_id"])
				service := identity.Service
				eebusFacts = append(eebusFacts, rawFirstFactV1{
					sortKey: identityKey + "\x00" + sourceID + "\x00" + artifactID,
					fact: FactV1{
						Status: "RAW_ONLY", TerminalNegativeState: terminal,
						Confidence: ConfidenceV1{Level: "LOW", Basis: "OBSERVED", ScoreMilli: 200},
						Provenance: ProvenanceV1{
							SourceBundleID: bundleID, NativeEvidenceRefs: append([]EvidenceRefV1(nil), refs...),
							EEBusSourceID: &sourceID, EEBusArtifactID: &artifactID,
							EEBusService: &service, EEBus: &identity,
						},
						Comparator:    ComparatorEvaluationV1{DraftID: "NUMERIC_WINDOW_V1_DRAFT", Samples: []ComparatorSampleV1{}, Outcome: "NOT_EVALUATED"},
						Falsifier:     FalsifierV1{ConditionCode: "IDENTITY_CHANGES", ExpectedTerminalState: "NOT_TESTED", Description: "A changed native path invalidates this raw observation binding."},
						RetestTrigger: RetestTriggerV1{TriggerCode: "NEW_SYNCHRONIZED_BUNDLE", RequiredSourceKinds: []string{"EEBUS"}, MinimumNewSamples: 1},
						DebugOnly:     true,
					},
				})
			}
		case "CLOUD_APP":
			if len(refs) == 0 {
				return nil, fail("provenance.binding")
			}
			sort.SliceStable(refs, func(i, j int) bool { return refs[i].Digest < refs[j].Digest })
			sourceID := asString(artifact["source_id"])
			artifactID := asString(artifact["artifact_id"])
			evidenceID := "public-evidence:" + refs[0].Digest
			terminal := "CLOUD_ONLY"
			cloudFacts = append(cloudFacts, rawFirstFactV1{
				sortKey: sourceID + "\x00" + artifactID,
				fact: FactV1{
					Status: "WITHHELD", TerminalNegativeState: &terminal,
					Confidence: ConfidenceV1{Level: "LOW", Basis: "INSUFFICIENT", ScoreMilli: 100},
					Provenance: ProvenanceV1{
						SourceBundleID: bundleID, NativeEvidenceRefs: append([]EvidenceRefV1(nil), refs...),
						Cloud: &CloudProvenanceV1{SourceID: sourceID, ArtifactID: artifactID, EvidenceID: evidenceID},
					},
					Comparator:    ComparatorEvaluationV1{DraftID: "NUMERIC_WINDOW_V1_DRAFT", Samples: []ComparatorSampleV1{}, Outcome: "NOT_EVALUATED"},
					Falsifier:     FalsifierV1{ConditionCode: "SIGNAL_DISAPPEARS", ExpectedTerminalState: "CLOUD_ONLY", Description: "Native bus evidence remains absent in the bounded capture."},
					RetestTrigger: RetestTriggerV1{TriggerCode: "SOURCE_RECOVERED", RequiredSourceKinds: []string{"EBUS", "EEBUS"}, MinimumNewSamples: 2},
					DebugOnly:     true,
				},
			})
		}
	}

	sort.SliceStable(eebusFacts, func(i, j int) bool { return eebusFacts[i].sortKey < eebusFacts[j].sortKey })
	for index := range eebusFacts {
		eebusFacts[index].fact.ProposedPath = fmt.Sprintf("/candidates/eebus/feature_path_%04d", index+1)
		eebusFacts[index].sortKey = eebusFacts[index].fact.ProposedPath
	}
	sort.SliceStable(cloudFacts, func(i, j int) bool { return cloudFacts[i].sortKey < cloudFacts[j].sortKey })
	for index := range cloudFacts {
		cloudFacts[index].fact.ProposedPath = fmt.Sprintf("/candidates/cloud_only/observation_%04d", index+1)
		cloudFacts[index].sortKey = cloudFacts[index].fact.ProposedPath
	}
	facts = append(facts, eebusFacts...)
	facts = append(facts, cloudFacts...)
	return facts, nil
}

func rawFirstComparator(bundle map[string]any) ComparatorDraftV1 {
	window, _ := objectValue(bundle["capture_window"])
	pre, _ := objectValue(window["pre"])
	post, _ := objectValue(window["post"])
	start, _ := unsigned(pre["start_offset_ns"])
	end, _ := unsigned(post["end_offset_ns"])
	return ComparatorDraftV1{
		DraftID: "NUMERIC_WINDOW_V1_DRAFT", Type: "NUMERIC_WINDOW",
		Parameters: ComparatorParametersV1{
			Window:         ComparatorWindowV1{StartOffsetNS: start, EndOffsetNS: end},
			Tolerance:      ComparatorToleranceV1{AbsoluteDecimal: "0", RelativePPM: 0},
			UnitConversion: ComparatorUnitConversionV1{Mode: "IDENTITY", SourceUnit: "unitless", TargetUnit: "unitless", ScaleDecimal: "1", OffsetDecimal: "0"},
			Rounding:       ComparatorRoundingV1{Mode: "NONE", DecimalPlaces: nil},
			MinimumSamples: 1, MaximumMissingSamples: 0, StaleCutoffNS: 1,
			ConflictThreshold: ComparatorConflictThresholdV1{AbsoluteDecimal: "0", ConsecutiveSamples: 1},
		},
	}
}

func decodeEvidenceRefs(value any) ([]EvidenceRefV1, error) {
	var refs []EvidenceRefV1
	if err := decodeRawFirstValue(value, &refs); err != nil || len(refs) == 0 {
		return nil, fail("provenance.binding")
	}
	return refs, nil
}

func decodeEBusIdentity(value any) (EBusIdentityV1, error) {
	var identity EBusIdentityV1
	if err := decodeRawFirstValue(value, &identity); err != nil {
		return EBusIdentityV1{}, fail("identity.native")
	}
	return identity, nil
}

func decodeEEBusIdentity(value any) (EEBusIdentityV1, error) {
	var identity EEBusIdentityV1
	if err := decodeRawFirstValue(value, &identity); err != nil {
		return EEBusIdentityV1{}, fail("identity.native")
	}
	return identity, nil
}

func decodeRawFirstValue(value any, target any) error {
	canonical, err := canonicalJSON(value)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}
