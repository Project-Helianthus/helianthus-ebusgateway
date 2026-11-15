// Package semselect holds the gateway's presentation selection policy for a
// fact key that carries candidates of several sources.
//
// The SemReg kernel keeps every candidate of a key side by side and never
// chooses between them; a presentation surface that shows one value per key
// chooses through this policy. The choice is presentation only: it publishes
// nothing, grants no operation authority and never hides an open conflict,
// which stays visible on the envelope; a consumer never reports a conflicted
// key as exact, whichever candidate it presents. Consumers apply the policy
// only when an envelope holds more than one candidate, so every
// single-candidate output and every family policy in use for one candidate
// stays as it is.
package semselect

import (
	"errors"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// PolicyID and PolicyVersion identify the owner-first presentation policy in
// every Selection it produces.
const (
	PolicyID      semreg.PolicyID        = "policy:gateway-owner-first"
	PolicyVersion semreg.SemanticVersion = "1.0.0"
)

var errNoSelection = errors.New("no presentable candidate")

// Policy selects, in order: the single qualified candidate observed by the
// owner source, else the single qualified candidate of the key, else none.
// The owner is the source of the asset's owning driver; an empty owner states
// that no owner is known and leaves only the second step.
type Policy struct {
	owner semreg.SourceID
}

var _ semreg.SelectionPolicy = Policy{}

// New returns the policy for an asset whose owning source is owner.
func New(owner semreg.SourceID) Policy { return Policy{owner: owner} }

// PolicyID implements semreg.SelectionPolicy.
func (Policy) PolicyID() semreg.PolicyID { return PolicyID }

// Version implements semreg.SelectionPolicy.
func (Policy) Version() semreg.SemanticVersion { return PolicyVersion }

// Select implements semreg.SelectionPolicy. It reports an error when the
// policy selects no candidate.
func (p Policy) Select(envelope semreg.FactEnvelope, evaluated []semreg.EvaluatedFact) (semreg.CandidateID, error) {
	selected, ok := Choose(envelope, evaluated, p.owner)
	if !ok {
		return "", errNoSelection
	}
	return selected, nil
}

// Selectable reports whether a candidate may be presented: it carries a value,
// is qualified, promoted and good, and its evaluation at the same revision is
// available and fresh.
func Selectable(candidate semreg.FactCandidate, evaluated semreg.EvaluatedFact) bool {
	q := candidate.Quality
	return candidate.Value != nil &&
		q.Qualification == semreg.QualificationQualified &&
		q.Promotion == semreg.PromotionPromoted &&
		q.Validity == semreg.ValidityGood &&
		evaluated.CandidateID == candidate.CandidateID &&
		evaluated.CandidateRevision == candidate.Revision &&
		evaluated.EffectiveAvailability == semreg.AvailabilityAvailable &&
		evaluated.Freshness == semreg.FreshnessFresh
}

// observedBy reports whether the candidate is a native observation of source.
func observedBy(candidate semreg.FactCandidate, source semreg.SourceID) bool {
	return source != "" &&
		candidate.Origin.Kind == semreg.OriginNativeObservation &&
		candidate.Origin.SourceID != nil &&
		*candidate.Origin.SourceID == source
}

// Choose is the pure form of the policy: it returns the selected candidate and
// true, or false when the policy selects none. The result does not depend on
// the order of candidates or evaluations.
func Choose(envelope semreg.FactEnvelope, evaluated []semreg.EvaluatedFact, owner semreg.SourceID) (semreg.CandidateID, bool) {
	byID := make(map[semreg.CandidateID]semreg.EvaluatedFact, len(evaluated))
	for _, fact := range evaluated {
		byID[fact.CandidateID] = fact
	}
	var owned, qualified []semreg.CandidateID
	for _, candidate := range envelope.Candidates {
		fact, found := byID[candidate.CandidateID]
		if !found || !Selectable(candidate, fact) {
			continue
		}
		qualified = append(qualified, candidate.CandidateID)
		if observedBy(candidate, owner) {
			owned = append(owned, candidate.CandidateID)
		}
	}
	switch {
	case len(owned) == 1:
		return owned[0], true
	case len(qualified) == 1:
		return qualified[0], true
	}
	return "", false
}

// Evaluated returns the evaluations of the envelope's candidates, in candidate
// order. A candidate without an evaluation is omitted, so it is never selected.
func Evaluated(envelope semreg.FactEnvelope, view semreg.EvaluationView) []semreg.EvaluatedFact {
	byID := make(map[semreg.CandidateID]semreg.EvaluatedFact, len(view.Facts))
	for _, fact := range view.Facts {
		byID[fact.CandidateID] = fact
	}
	out := make([]semreg.EvaluatedFact, 0, len(envelope.Candidates))
	for _, candidate := range envelope.Candidates {
		if fact, found := byID[candidate.CandidateID]; found {
			out = append(out, fact)
		}
	}
	return out
}

// Present returns the kernel-validated presentation Selection of the
// envelope's key under the policy for owner, or false when the policy selects
// no candidate. The snapshot and evaluation must be the immutable pair the
// envelope was read from.
func Present(snapshot semreg.Snapshot, evaluation semreg.EvaluationView, envelope semreg.FactEnvelope, owner semreg.SourceID) (semreg.Selection, bool, error) {
	if _, ok := Choose(envelope, Evaluated(envelope, evaluation), owner); !ok {
		return semreg.Selection{}, false, nil
	}
	kernel, err := semreg.NewSelectionKernel(New(owner))
	if err != nil {
		return semreg.Selection{}, false, err
	}
	selection, err := kernel.SelectPresentation(snapshot, evaluation, envelope.Key, PolicyID, PolicyVersion)
	if err != nil {
		return semreg.Selection{}, false, err
	}
	return selection, true, nil
}
