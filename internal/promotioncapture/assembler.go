package promotioncapture

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

const (
	CampaignContractV1  = "helianthus.platform.leaf-promotion-captured-multi-leaf.v1"
	CampaignProfileV1   = "CAPTURED_RUNTIME_MULTI_LEAF_V1"
	PrivateOperatorTier = "PRIVATE_OPERATOR"

	CampaignDomain = "HELIANTHUS:LEAF-PROMOTION:CAPTURED-MULTI-LEAF:V1\x00"
	DossierDomain  = "HELIANTHUS:LEAF-PROMOTION:CAPTURED-DOSSIER:V1\x00"
	ReplayDomain   = "HELIANTHUS:LEAF-PROMOTION:CAPTURED-REPLAY:V1\x00"
)

var commitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

func AssembleCampaign(
	registry *Registry,
	manifest CampaignAssemblyManifest,
	pre WindowCheckpoint,
	post WindowCheckpoint,
) (Campaign, error) {
	if registry == nil {
		return Campaign{}, fmt.Errorf("%w: nil registry", ErrInvalidEvidence)
	}
	if err := validateAssemblyManifest(manifest, pre.CampaignID); err != nil {
		return Campaign{}, err
	}
	if err := validateCheckpoint(registry, pre, PhasePreRestart); err != nil {
		return Campaign{}, fmt.Errorf("pre-restart checkpoint: %w", err)
	}
	if err := validateCheckpoint(registry, post, PhasePostRestart); err != nil {
		return Campaign{}, fmt.Errorf("post-restart checkpoint: %w", err)
	}
	if err := validateRestartPair(pre, post); err != nil {
		return Campaign{}, err
	}

	campaign := Campaign{
		Contract: CampaignContractV1, SchemaVersion: 1, Profile: CampaignProfileV1,
		EvidenceMode: manifest.EvidenceMode, ExportTier: PrivateOperatorTier,
		Provenance: cloneValue(manifest.Provenance), SourceBindings: cloneValue(manifest.SourceBindings),
		Windows:    []Window{cloneValue(pre.Window), cloneValue(post.Window)},
		Candidates: make([]CampaignCandidate, 0, len(pre.Candidates)),
	}
	for index, definition := range registry.candidates {
		candidate, err := assembleCandidate(definition, pre.Candidates[index], post.Candidates[index])
		if err != nil {
			return Campaign{}, fmt.Errorf("%s: %w", definition.CandidateID, err)
		}
		campaign.Candidates = append(campaign.Candidates, candidate)
	}

	replayBindings, err := objectWithoutField(campaign.SourceBindings, "replay_hash")
	if err != nil {
		return Campaign{}, err
	}
	replayDigest, err := CanonicalDigest(ReplayDomain, map[string]any{
		"source_bindings": replayBindings,
		"windows":         campaign.Windows,
		"candidates":      campaign.Candidates,
	})
	if err != nil {
		return Campaign{}, err
	}
	campaign.SourceBindings.ReplayHash = replayDigest
	campaignDigest, err := digestWithoutField(CampaignDomain, campaign, "campaign_hash")
	if err != nil {
		return Campaign{}, err
	}
	campaign.CampaignHash = campaignDigest
	return campaign, nil
}

func validateAssemblyManifest(manifest CampaignAssemblyManifest, campaignID string) error {
	if manifest.EvidenceMode != EvidenceModeLiveCapture || manifest.Provenance.Class != manifest.EvidenceMode {
		return fmt.Errorf("%w: campaign requires LIVE_CAPTURE provenance", ErrInvalidEvidence)
	}
	provenance := manifest.Provenance
	if provenance.FixtureID != nil || provenance.Generator != nil || provenance.CaptureCampaignID == nil ||
		*provenance.CaptureCampaignID != campaignID || len(provenance.CaptureReceipts) != 2 ||
		provenance.CaptureReceipts[0] == provenance.CaptureReceipts[1] ||
		provenance.DeploymentSourceCommit == nil || provenance.DeploymentSourceHash == nil ||
		provenance.DeploymentBinaryHash == nil || !commitPattern.MatchString(*provenance.DeploymentSourceCommit) {
		return fmt.Errorf("%w: incomplete live provenance", ErrInvalidEvidence)
	}
	for _, digest := range append(append([]string(nil), provenance.CaptureReceipts...),
		*provenance.DeploymentSourceHash, *provenance.DeploymentBinaryHash) {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("%w: malformed provenance digest", ErrInvalidEvidence)
		}
	}
	bindings := manifest.SourceBindings
	if bindings.RegistrySHA256 != RegistrySHA256 || bindings.DocsEEBusCommit != DocsEEBusCommit || bindings.ReplayHash != "" {
		return fmt.Errorf("%w: source binding pin mismatch", ErrInvalidEvidence)
	}
	for _, token := range []string{bindings.M7GraphID, bindings.M7ReplayID, bindings.M7StatusID, bindings.M8EvidenceID, bindings.M8ReportID} {
		if !validToken(token) {
			return fmt.Errorf("%w: malformed source binding id", ErrInvalidEvidence)
		}
	}
	for _, digest := range []string{
		bindings.M7GraphHash, bindings.M7GraphBytesHash, bindings.M7ReplayHash, bindings.M7ReplayBytesHash,
		bindings.M7StatusHash, bindings.M7StatusBytesHash, bindings.M8EvidenceHash, bindings.M8EvidenceBytesHash,
		bindings.M8ReportHash, bindings.M8ReportBytesHash,
	} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("%w: malformed source binding digest", ErrInvalidEvidence)
		}
	}
	return nil
}

func validateCheckpoint(registry *Registry, checkpoint WindowCheckpoint, phase WindowPhase) error {
	if checkpoint.Contract != CheckpointContractV1 || checkpoint.SchemaVersion != 1 ||
		!validToken(checkpoint.CampaignID) || !validToken(checkpoint.ProcessInstanceID) ||
		!validToken(checkpoint.TrustStateID) || !validToken(checkpoint.PeerBindingID) ||
		!timestampPattern.MatchString(checkpoint.CapturedAt) {
		return fmt.Errorf("%w: checkpoint contract or metadata mismatch", ErrInvalidEvidence)
	}
	if checkpoint.Window.Phase != phase || !checkpoint.Window.M8NoDrift || !checkpoint.Window.RollbackExact {
		return fmt.Errorf("%w: checkpoint phase or coexistence gates mismatch", ErrInvalidEvidence)
	}
	if err := validateWindow(checkpoint.Window); err != nil {
		return err
	}
	capturedAt, _ := parseTimestamp(checkpoint.CapturedAt)
	windowEnd, _ := parseTimestamp(checkpoint.Window.EndedAt)
	if capturedAt.Before(windowEnd) || capturedAt.Sub(windowEnd) > 30_000_000_000 {
		return fmt.Errorf("%w: checkpoint timestamp outside capture boundary", ErrInvalidEvidence)
	}
	for _, digest := range []string{
		checkpoint.Window.ProcessInstanceHash, checkpoint.Window.LocalIdentityHash,
		checkpoint.Window.TrustStateHash, checkpoint.Window.PeerBindingHash,
	} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("%w: malformed window digest", ErrInvalidEvidence)
		}
	}
	wantHash, err := digestWithoutField(CheckpointDomain, checkpoint, "checkpoint_hash")
	if err != nil || checkpoint.CheckpointHash != wantHash {
		return fmt.Errorf("%w: checkpoint hash mismatch", ErrInvalidEvidence)
	}
	definitions := registry.candidates
	if len(checkpoint.Candidates) != len(definitions) {
		return fmt.Errorf("%w: checkpoint candidate count mismatch", ErrInvalidEvidence)
	}
	for index, definition := range definitions {
		captured := checkpoint.Candidates[index]
		if captured.CandidateID != definition.CandidateID || captured.FactHash != definition.FactHash ||
			captured.SourceStatus != definition.SourceStatus || captured.ComparatorClass != definition.ComparatorClass ||
			captured.ProtocolEligibility != definition.ProtocolEligibility ||
			!reflect.DeepEqual(captured.SemanticPath, definition.SemanticPath) {
			return fmt.Errorf("%w: %s catalog metadata mismatch", ErrInvalidEvidence, definition.CandidateID)
		}
		if err := validateCapturedIdentity(definition, checkpoint.Window, captured); err != nil {
			return fmt.Errorf("%s: %w", definition.CandidateID, err)
		}
		reevaluated, err := reevaluateCaptured(registry, definition, checkpoint.Window, captured)
		if err != nil {
			return fmt.Errorf("%s: %w", definition.CandidateID, err)
		}
		if !canonicalEqual(reevaluated, captured.Evaluation) {
			return fmt.Errorf("%w: %s evaluation does not match samples", ErrInvalidEvidence, definition.CandidateID)
		}
	}
	return nil
}

func validateCapturedIdentity(definition CandidateDefinition, window Window, captured CapturedCandidateWindow) error {
	switch definition.ProtocolEligibility {
	case ProtocolTerminal:
		if captured.EBusIdentity != nil || captured.EEBusIdentity != nil {
			return fmt.Errorf("%w: terminal candidate carries protocol identity", ErrInvalidEvidence)
		}
		return nil
	case ProtocolWithholdNoEBusCapability:
		if captured.EBusIdentity != nil || captured.EEBusIdentity == nil {
			return fmt.Errorf("%w: capability-only identity mismatch", ErrInvalidEvidence)
		}
	case ProtocolEligible:
		if captured.EBusIdentity == nil || captured.EEBusIdentity == nil || definition.EBusSelector == nil {
			return fmt.Errorf("%w: eligible identity missing", ErrInvalidEvidence)
		}
		want, err := NewB524Identity(*definition.EBusSelector, window.AdmittedSource)
		if err != nil || !canonicalEqual(want, *captured.EBusIdentity) {
			return fmt.Errorf("%w: eBUS selector mismatch", ErrInvalidEvidence)
		}
	default:
		return fmt.Errorf("%w: unknown protocol eligibility", ErrInvalidEvidence)
	}
	if definition.EEBusSource == nil || captured.EEBusIdentity == nil {
		return fmt.Errorf("%w: eeBUS identity missing", ErrInvalidEvidence)
	}
	want, err := NewEEBusIdentity(
		*definition.EEBusSource, captured.EEBusIdentity.ServiceID, captured.EEBusIdentity.DeviceAddress,
		captured.EEBusIdentity.EntityAddress, captured.EEBusIdentity.FeatureAddress,
	)
	if err != nil || !canonicalEqual(want, *captured.EEBusIdentity) {
		return fmt.Errorf("%w: eeBUS source identity mismatch", ErrInvalidEvidence)
	}
	return nil
}

func reevaluateCaptured(
	registry *Registry,
	definition CandidateDefinition,
	window Window,
	captured CapturedCandidateWindow,
) (WindowEvaluation, error) {
	if _, fixed := definition.FixedOutcome(); fixed {
		return registry.EvaluateWindow(definition.CandidateID, WindowAssessmentInput{})
	}
	if captured.Evaluation.Assessment == nil || captured.EBusIdentity == nil || captured.EEBusIdentity == nil {
		return WindowEvaluation{}, fmt.Errorf("%w: eligible assessment missing", ErrInvalidEvidence)
	}
	assessment := captured.Evaluation.Assessment
	return registry.EvaluateWindow(definition.CandidateID, WindowAssessmentInput{
		Window:                    window,
		ExpectedEBusIdentityHash:  captured.EBusIdentity.SelectorHash,
		ExpectedEEBusIdentityHash: captured.EEBusIdentity.IdentityHash,
		ObservedEBusIdentityHash:  assessment.ObservedEBusIdentityHash,
		ObservedEEBusIdentityHash: assessment.ObservedEEBusIdentityHash,
		EBusSample:                assessment.EBusSample,
		EEBusSample:               assessment.EEBusSample,
		ConflictSamples:           assessment.ConflictSamples,
	})
}

func validateRestartPair(pre, post WindowCheckpoint) error {
	preEnd, _ := parseTimestamp(pre.Window.EndedAt)
	postStart, _ := parseTimestamp(post.Window.StartedAt)
	if pre.CampaignID != post.CampaignID || pre.Window.WindowID == post.Window.WindowID ||
		!preEnd.Before(postStart) || pre.ProcessInstanceID == post.ProcessInstanceID ||
		pre.Window.ProcessInstanceHash == post.Window.ProcessInstanceHash ||
		pre.TrustStateID != post.TrustStateID || pre.PeerBindingID != post.PeerBindingID ||
		pre.Window.LocalIdentityHash != post.Window.LocalIdentityHash ||
		pre.Window.TrustStateHash != post.Window.TrustStateHash ||
		pre.Window.PeerBindingHash != post.Window.PeerBindingHash ||
		pre.Window.AdmittedSource != post.Window.AdmittedSource {
		return fmt.Errorf("%w: restart continuity mismatch", ErrInvalidEvidence)
	}
	for index := range pre.Candidates {
		if !canonicalEqual(pre.Candidates[index].EBusIdentity, post.Candidates[index].EBusIdentity) ||
			!canonicalEqual(pre.Candidates[index].EEBusIdentity, post.Candidates[index].EEBusIdentity) {
			return fmt.Errorf("%w: %s identity changed across restart", ErrInvalidEvidence, pre.Candidates[index].CandidateID)
		}
	}
	return nil
}

func assembleCandidate(
	definition CandidateDefinition,
	pre CapturedCandidateWindow,
	post CapturedCandidateWindow,
) (CampaignCandidate, error) {
	candidate := CampaignCandidate{
		CandidateID: definition.CandidateID, FactHash: definition.FactHash,
		SourceStatus: definition.SourceStatus, SemanticPath: cloneStringPointer(definition.SemanticPath),
		ComparatorClass: definition.ComparatorClass, EBusIdentity: cloneJSONPointer(pre.EBusIdentity),
		EEBusIdentity: cloneJSONPointer(pre.EEBusIdentity), Assessments: []Assessment{},
		Decision: DecisionWithheld, Visibility: VisibilityRawDebugOnly,
	}
	if fixed, ok := definition.FixedOutcome(); ok {
		terminal := fixed
		candidate.TerminalState = &terminal
		return candidate, nil
	}
	if pre.Evaluation.Assessment == nil || post.Evaluation.Assessment == nil {
		return CampaignCandidate{}, fmt.Errorf("%w: eligible assessment missing", ErrInvalidEvidence)
	}
	candidate.Assessments = []Assessment{
		cloneValue(*pre.Evaluation.Assessment),
		cloneValue(*post.Evaluation.Assessment),
	}
	for _, outcome := range []Outcome{pre.Evaluation.Outcome, post.Evaluation.Outcome} {
		if outcome != OutcomeMatch {
			if outcome == OutcomeNotEvaluated || outcome == OutcomeNotComparable {
				return CampaignCandidate{}, fmt.Errorf("%w: invalid eligible terminal outcome %s", ErrInvalidEvidence, outcome)
			}
			terminal := outcome
			candidate.TerminalState = &terminal
			return candidate, nil
		}
	}
	candidate.Decision = DecisionPromoted
	candidate.Visibility = VisibilityLockedNotExposed
	digest, err := digestWithoutField(DossierDomain, candidate, "dossier_hash")
	if err != nil {
		return CampaignCandidate{}, err
	}
	candidate.DossierHash = &digest
	return candidate, nil
}

func validToken(value string) bool {
	return value != "" && len(value) <= 256 && strings.TrimSpace(value) == value
}

func canonicalEqual(left, right any) bool {
	leftRaw, leftErr := CanonicalJSON(left)
	rightRaw, rightErr := CanonicalJSON(right)
	return leftErr == nil && rightErr == nil && string(leftRaw) == string(rightRaw)
}

func cloneValue[T any](value T) T {
	cloned := cloneJSONPointer(&value)
	return *cloned
}
