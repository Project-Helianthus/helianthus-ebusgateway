package promotioncapture

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sync"
)

//go:embed contracts/leaf-promotion-captured-multi-leaf-registry-v1.json
var contractFiles embed.FS

const registryPath = "contracts/leaf-promotion-captured-multi-leaf-registry-v1.json"

var (
	digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	registryOnce  sync.Once
	registryV1    *Registry
	registryErr   error
)

type Registry struct {
	candidates    []CandidateDefinition
	byID          map[string]int
	captureLimits CaptureLimits
}

type registryDocument struct {
	Contract              string                `json:"contract"`
	SchemaVersion         int                   `json:"schema_version"`
	Profile               string                `json:"profile"`
	PrivateSchema         string                `json:"private_schema"`
	PublicSchema          string                `json:"public_schema"`
	M7PublicStatus        string                `json:"m7_public_status"`
	DocsEEBusSourceCommit string                `json:"docs_eebus_source_commit"`
	CaptureLimits         CaptureLimits         `json:"capture_limits"`
	SanitizedProvenance   json.RawMessage       `json:"sanitized_provenance"`
	CandidateCatalog      []CandidateDefinition `json:"candidate_catalog"`
	WindowPhases          []WindowPhase         `json:"window_phases"`
	ComparatorClasses     []ComparatorClass     `json:"comparator_classes"`
	TerminalStates        []Outcome             `json:"terminal_states"`
	PositiveVisibility    string                `json:"positive_visibility"`
	WithheldVisibility    string                `json:"withheld_visibility"`
	NumericMatchRule      string                `json:"numeric_match_rule"`
	RequiredWindows       int                   `json:"required_windows"`
	PublicForbiddenKeys   []string              `json:"public_forbidden_keys"`
}

func EmbeddedRegistryBytes() []byte {
	raw, err := contractFiles.ReadFile(registryPath)
	if err != nil {
		panic(err)
	}
	return append([]byte(nil), raw...)
}

func DefaultRegistry() (*Registry, error) {
	registryOnce.Do(func() {
		registryV1, registryErr = parseEmbeddedRegistry()
	})
	return registryV1, registryErr
}

func (registry *Registry) Candidates() []CandidateDefinition {
	if registry == nil {
		return nil
	}
	result := make([]CandidateDefinition, len(registry.candidates))
	for index, candidate := range registry.candidates {
		result[index] = cloneCandidate(candidate)
	}
	return result
}

func (registry *Registry) Candidate(candidateID string) (CandidateDefinition, bool) {
	if registry == nil {
		return CandidateDefinition{}, false
	}
	index, ok := registry.byID[candidateID]
	if !ok {
		return CandidateDefinition{}, false
	}
	return cloneCandidate(registry.candidates[index]), true
}

func parseEmbeddedRegistry() (*Registry, error) {
	raw := EmbeddedRegistryBytes()
	digest := sha256.Sum256(raw)
	if "sha256:"+hex.EncodeToString(digest[:]) != RegistrySHA256 {
		return nil, fmt.Errorf("registry binding: embedded bytes do not match %s", RegistrySHA256)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var document registryDocument
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("registry binding: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("registry binding: trailing JSON")
	}
	if err := validateRegistryDocument(document); err != nil {
		return nil, err
	}
	registry := &Registry{
		candidates:    append([]CandidateDefinition(nil), document.CandidateCatalog...),
		byID:          make(map[string]int, len(document.CandidateCatalog)),
		captureLimits: document.CaptureLimits,
	}
	for index, candidate := range registry.candidates {
		registry.byID[candidate.CandidateID] = index
	}
	return registry, nil
}

func validateRegistryDocument(document registryDocument) error {
	if document.Contract != "helianthus.platform.leaf-promotion-captured-multi-leaf-registry.v1" ||
		document.SchemaVersion != 1 || document.Profile != "CAPTURED_RUNTIME_MULTI_LEAF_V1" ||
		document.DocsEEBusSourceCommit != DocsEEBusCommit || document.RequiredWindows != 2 ||
		document.NumericMatchRule != "ABS_DELTA_LE_DECLARED_SPINE_STEP_INCLUSIVE" ||
		document.CaptureLimits.MaxSkewNS <= 0 || document.CaptureLimits.MaxAgeNS <= 0 {
		return fmt.Errorf("registry binding: header mismatch")
	}
	if !equalWindowPhases(document.WindowPhases, []WindowPhase{PhasePreRestart, PhasePostRestart}) ||
		!equalComparatorClasses(document.ComparatorClasses, []ComparatorClass{ComparatorNone, ComparatorNumeric, ComparatorEnum, ComparatorBoolean}) ||
		len(document.CandidateCatalog) != 18 {
		return fmt.Errorf("registry binding: closed catalog metadata mismatch")
	}
	seen := make(map[string]struct{}, len(document.CandidateCatalog))
	for index, candidate := range document.CandidateCatalog {
		expectedID := fmt.Sprintf("m7-candidate-%04d", index+1)
		if candidate.CandidateID != expectedID {
			return fmt.Errorf("registry binding: candidate[%d] = %q, want %q", index, candidate.CandidateID, expectedID)
		}
		if _, duplicate := seen[candidate.CandidateID]; duplicate {
			return fmt.Errorf("registry binding: duplicate candidate %s", candidate.CandidateID)
		}
		seen[candidate.CandidateID] = struct{}{}
		if !digestPattern.MatchString(candidate.FactHash) {
			return fmt.Errorf("registry binding: invalid fact hash for %s", candidate.CandidateID)
		}
		if err := validateCandidateDefinition(candidate); err != nil {
			return fmt.Errorf("registry binding: %s: %w", candidate.CandidateID, err)
		}
	}
	return nil
}

func validateCandidateDefinition(candidate CandidateDefinition) error {
	switch candidate.ProtocolEligibility {
	case ProtocolTerminal:
		if candidate.ComparatorClass != ComparatorNone || candidate.TerminalState == nil ||
			candidate.EBusSelector != nil || candidate.EEBusSource != nil || candidate.SemanticPath != nil {
			return fmt.Errorf("malformed terminal candidate")
		}
		return nil
	case ProtocolWithholdNoEBusCapability:
		if candidate.ComparatorClass != ComparatorBoolean || candidate.TerminalState != nil ||
			candidate.EBusSelector != nil || candidate.EEBusSource == nil || candidate.SemanticPath == nil {
			return fmt.Errorf("malformed noncomparable candidate")
		}
		if candidate.EEBusSource.ExactMapping == nil || candidate.EEBusSource.MappingProfile != nil ||
			candidate.EEBusSource.Unit != nil || candidate.EEBusSource.DeclaredConstraints != nil ||
			candidate.EEBusSource.Conversion != nil {
			return fmt.Errorf("malformed noncomparable source")
		}
		return nil
	case ProtocolEligible:
		if candidate.TerminalState != nil || candidate.EBusSelector == nil ||
			candidate.EEBusSource == nil || candidate.SemanticPath == nil {
			return fmt.Errorf("malformed eligible candidate")
		}
		if candidate.EBusSelector.Family != "B524" || candidate.EBusSelector.TargetAddress != 0x15 {
			return fmt.Errorf("unexpected eBUS selector")
		}
	default:
		return fmt.Errorf("unknown protocol eligibility %q", candidate.ProtocolEligibility)
	}

	source := candidate.EEBusSource
	if source.FeatureRole != "server" || len(source.DescriptionFunctions) == 0 || len(source.ValueFunctions) == 0 || len(source.Descriptor) == 0 {
		return fmt.Errorf("incomplete eeBUS source")
	}
	switch candidate.ComparatorClass {
	case ComparatorNumeric:
		if source.Unit == nil || source.DeclaredConstraints == nil || source.Conversion == nil ||
			source.ExactMapping != nil || source.MappingProfile != nil {
			return fmt.Errorf("malformed numeric source")
		}
		if _, err := CompareNumeric(source.DeclaredConstraints.Minimum, source.DeclaredConstraints.Minimum, *source.DeclaredConstraints, *source.Conversion); err != nil {
			return fmt.Errorf("invalid numeric declaration: %w", err)
		}
	case ComparatorEnum, ComparatorBoolean:
		if source.Unit != nil || source.DeclaredConstraints != nil || source.Conversion != nil ||
			source.ExactMapping == nil || source.MappingProfile == nil ||
			len(source.ExactMapping.Pairs) < 2 || len(source.MappingProfile.Pairs) == 0 {
			return fmt.Errorf("malformed mapped source")
		}
	default:
		return fmt.Errorf("unexpected comparator class %q", candidate.ComparatorClass)
	}
	return nil
}

func cloneCandidate(candidate CandidateDefinition) CandidateDefinition {
	raw, err := json.Marshal(candidate)
	if err != nil {
		panic(err)
	}
	var clone CandidateDefinition
	if err := json.Unmarshal(raw, &clone); err != nil {
		panic(err)
	}
	return clone
}

func equalWindowPhases(left, right []WindowPhase) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalComparatorClasses(left, right []ComparatorClass) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
