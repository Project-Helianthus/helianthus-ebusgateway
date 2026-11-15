package semselect

import (
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

const (
	testOwner   semreg.SourceID = "source:owner"
	testForeign semreg.SourceID = "source:contributor"
)

func testKey() semreg.FactKey {
	text := "system:test"
	return semreg.FactKey{PackID: "helianthus.pack.pv", PackVersion: "1.0.0", FactID: "pv.ac.aggregate_active_power", Dimensions: []semreg.Dimension{{ID: "pv.dimension.system", Value: semreg.Value{Kind: semreg.ValueText, Text: &text}}}}
}

// testCandidate returns a selectable native observation of source.
func testCandidate(id semreg.CandidateID, source semreg.SourceID, coefficient string) semreg.FactCandidate {
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: coefficient}, Unit: "unit.watt"}}
	origin := source
	return semreg.FactCandidate{
		CandidateID: id,
		Key:         testKey(),
		Value:       &value,
		Quality:     semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Origin:      semreg.OriginRef{OriginID: semreg.OriginID("origin:" + string(id)), Kind: semreg.OriginNativeObservation, SourceID: &origin},
		Revision:    "1",
	}
}

func testEvaluated(c semreg.FactCandidate) semreg.EvaluatedFact {
	return semreg.EvaluatedFact{CandidateID: c.CandidateID, CandidateRevision: c.Revision, Freshness: semreg.FreshnessFresh, EffectiveAvailability: semreg.AvailabilityAvailable}
}

func testEnvelope(candidates ...semreg.FactCandidate) (semreg.FactEnvelope, []semreg.EvaluatedFact) {
	evaluated := make([]semreg.EvaluatedFact, 0, len(candidates))
	for _, c := range candidates {
		evaluated = append(evaluated, testEvaluated(c))
	}
	return semreg.FactEnvelope{Key: testKey(), Candidates: candidates, Conflicts: []semreg.Conflict{}, Revision: "1"}, evaluated
}

func TestPolicyIdentity(t *testing.T) {
	policy := New(testOwner)
	if policy.PolicyID() != PolicyID || policy.Version() != PolicyVersion {
		t.Fatalf("policy identity = %q %q", policy.PolicyID(), policy.Version())
	}
	if err := PolicyID.Validate(); err != nil {
		t.Fatalf("policy id: %v", err)
	}
	if err := PolicyVersion.Validate(); err != nil {
		t.Fatalf("policy version: %v", err)
	}
	var _ semreg.SelectionPolicy = policy
	if _, err := semreg.NewSelectionKernel(policy); err != nil {
		t.Fatalf("register policy: %v", err)
	}
}

// TestSelectTwoCandidates covers the three steps of the policy with two
// candidates of two sources: the owner's qualified candidate, else the single
// qualified candidate, else none.
func TestSelectTwoCandidates(t *testing.T) {
	stale := func(e *semreg.EvaluatedFact) { e.Freshness = semreg.FreshnessStale }
	for _, tc := range []struct {
		name          string
		owner         semreg.SourceID
		ownerStale    bool
		foreignStale  bool
		foreignAsOwn  bool
		want          semreg.CandidateID
		wantSelection bool
	}{
		{name: "owner qualified, foreign qualified", owner: testOwner, want: "candidate:a-owner", wantSelection: true},
		{name: "owner stale, single qualified foreign", owner: testOwner, ownerStale: true, want: "candidate:b-foreign", wantSelection: true},
		{name: "foreign stale, owner qualified", owner: testOwner, foreignStale: true, want: "candidate:a-owner", wantSelection: true},
		{name: "both stale", owner: testOwner, ownerStale: true, foreignStale: true},
		{name: "no owner declared, two qualified", owner: ""},
		{name: "no owner declared, single qualified", owner: "", ownerStale: true, want: "candidate:b-foreign", wantSelection: true},
		{name: "unknown owner, two qualified", owner: "source:absent"},
		{name: "owner has two qualified candidates", owner: testOwner, foreignAsOwn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			foreignSource := testForeign
			if tc.foreignAsOwn {
				foreignSource = testOwner
			}
			envelope, evaluated := testEnvelope(testCandidate("candidate:a-owner", testOwner, "40"), testCandidate("candidate:b-foreign", foreignSource, "40"))
			if tc.ownerStale {
				stale(&evaluated[0])
			}
			if tc.foreignStale {
				stale(&evaluated[1])
			}
			got, ok := Choose(envelope, evaluated, tc.owner)
			if ok != tc.wantSelection || got != tc.want {
				t.Fatalf("Choose = %q %v, want %q %v", got, ok, tc.want, tc.wantSelection)
			}
			selected, err := New(tc.owner).Select(envelope, evaluated)
			if tc.wantSelection && (err != nil || selected != tc.want) {
				t.Fatalf("Select = %q %v, want %q", selected, err, tc.want)
			}
			if !tc.wantSelection && (err == nil || selected != "") {
				t.Fatalf("Select = %q %v, want no selection", selected, err)
			}
		})
	}
}

// TestSelectOrderIndependent keeps the choice independent of candidate and
// evaluation order, so the owner wins whichever identifier sorts first.
func TestSelectOrderIndependent(t *testing.T) {
	envelope, evaluated := testEnvelope(testCandidate("candidate:a-foreign", testForeign, "40"), testCandidate("candidate:b-owner", testOwner, "40"))
	reversed := []semreg.EvaluatedFact{evaluated[1], evaluated[0]}
	for _, facts := range [][]semreg.EvaluatedFact{evaluated, reversed} {
		if got, ok := Choose(envelope, facts, testOwner); !ok || got != "candidate:b-owner" {
			t.Fatalf("Choose = %q %v, want the owner's candidate", got, ok)
		}
	}
}

// TestSelectableFields varies every member that makes a candidate selectable
// or attributes it to the owner. Each case names the production mutation it
// catches.
func TestSelectableFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*semreg.FactCandidate, *semreg.EvaluatedFact)
		// ownerOnly is true when the mutated owner candidate stays selectable but
		// is no longer attributed to the owner.
		ownerOnly bool
	}{
		// Value: dropping the value check presents an empty value.
		{name: "value absent", mutate: func(c *semreg.FactCandidate, _ *semreg.EvaluatedFact) { c.Value = nil }},
		// Quality.Qualification: dropping it presents a candidate value.
		{name: "qualification candidate", mutate: func(c *semreg.FactCandidate, _ *semreg.EvaluatedFact) {
			c.Quality.Qualification = semreg.QualificationCandidate
		}},
		// Quality.Promotion: dropping it presents an unpromoted value.
		{name: "promotion unpromoted", mutate: func(c *semreg.FactCandidate, _ *semreg.EvaluatedFact) {
			c.Quality.Promotion = semreg.PromotionUnpromoted
		}},
		// Quality.Validity: accepting suspect or bad presents a doubtful value.
		{name: "validity suspect", mutate: func(c *semreg.FactCandidate, _ *semreg.EvaluatedFact) { c.Quality.Validity = semreg.ValiditySuspect }},
		{name: "validity bad", mutate: func(c *semreg.FactCandidate, _ *semreg.EvaluatedFact) { c.Quality.Validity = semreg.ValidityBad }},
		// EvaluatedFact.EffectiveAvailability: reading the stored availability
		// instead of the evaluated one presents a degraded value.
		{name: "effective availability degraded", mutate: func(_ *semreg.FactCandidate, e *semreg.EvaluatedFact) {
			e.EffectiveAvailability = semreg.AvailabilityDegraded
		}},
		// EvaluatedFact.Freshness: reading the stored freshness presents an
		// expired value.
		{name: "freshness expired", mutate: func(_ *semreg.FactCandidate, e *semreg.EvaluatedFact) { e.Freshness = semreg.FreshnessExpired }},
		// EvaluatedFact.CandidateRevision: ignoring it binds a stale evaluation.
		{name: "evaluated revision differs", mutate: func(_ *semreg.FactCandidate, e *semreg.EvaluatedFact) { e.CandidateRevision = "2" }},
		// EvaluatedFact.CandidateID: a missing evaluation must never select.
		{name: "evaluation missing", mutate: func(_ *semreg.FactCandidate, e *semreg.EvaluatedFact) { e.CandidateID = "candidate:other" }},
		// Origin.SourceID: matching any source as the owner picks a foreign one.
		{name: "origin source differs", ownerOnly: true, mutate: func(c *semreg.FactCandidate, _ *semreg.EvaluatedFact) {
			other := semreg.SourceID("source:third")
			c.Origin.SourceID = &other
		}},
		// Origin.SourceID absent: dereferencing it would panic or match "".
		{name: "origin source absent", ownerOnly: true, mutate: func(c *semreg.FactCandidate, _ *semreg.EvaluatedFact) { c.Origin.SourceID = nil }},
		// Origin.Kind: a derived record naming the owner is not its observation.
		{name: "origin kind derived", ownerOnly: true, mutate: func(c *semreg.FactCandidate, _ *semreg.EvaluatedFact) { c.Origin.Kind = semreg.OriginDerived }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner, foreign := testCandidate("candidate:a-owner", testOwner, "40"), testCandidate("candidate:b-foreign", testForeign, "40")
			envelope, evaluated := testEnvelope(owner, foreign)
			tc.mutate(&envelope.Candidates[0], &evaluated[0])
			got, ok := Choose(envelope, evaluated, testOwner)
			if !tc.ownerOnly {
				// The owner's candidate is not selectable, so the foreign one is the
				// single qualified candidate.
				if !ok || got != "candidate:b-foreign" {
					t.Fatalf("Choose = %q %v, want the single qualified foreign candidate", got, ok)
				}
				single, _ := testEnvelope(owner)
				singleEvaluated := []semreg.EvaluatedFact{testEvaluated(owner)}
				tc.mutate(&single.Candidates[0], &singleEvaluated[0])
				if got, ok := Choose(single, singleEvaluated, testOwner); ok {
					t.Fatalf("Choose on the mutated single candidate = %q, want none", got)
				}
				return
			}
			// Both stay selectable and neither is the owner's: no selection.
			if ok {
				t.Fatalf("Choose = %q, want none without an owner candidate", got)
			}
		})
	}
}

// TestSelectConflict: a presentation choice under an open conflict follows the
// same steps. The conflict stays on the envelope; the consumers, which never
// report a conflicted key as exact, are tested in their own packages.
func TestSelectConflict(t *testing.T) {
	envelope, evaluated := testEnvelope(testCandidate("candidate:a-owner", testOwner, "40"), testCandidate("candidate:b-foreign", testForeign, "41"))
	envelope.Conflicts = []semreg.Conflict{{ConflictID: "conflict:value", Kind: semreg.ConflictValue, Candidates: []semreg.CandidateID{"candidate:a-owner", "candidate:b-foreign"}, Evidence: []semreg.EvidenceRef{}, State: semreg.ConflictOpen}}
	if got, ok := Choose(envelope, evaluated, testOwner); !ok || got != "candidate:a-owner" {
		t.Fatalf("Choose = %q %v, want the owner's candidate", got, ok)
	}
	if got, ok := Choose(envelope, evaluated, ""); ok {
		t.Fatalf("Choose without an owner = %q, want none between two qualified candidates", got)
	}
	if len(envelope.Conflicts) != 1 {
		t.Fatal("the choice must not touch the envelope's conflict")
	}
}

// TestSelectOneCandidateUnchanged: with one candidate the policy selects
// exactly the candidate today's single-candidate consumers export: qualified,
// promoted, good, carrying a value, evaluated available and fresh. The
// candidate's source is irrelevant.
func TestSelectOneCandidateUnchanged(t *testing.T) {
	today := func(c semreg.FactCandidate, e semreg.EvaluatedFact) bool {
		return c.Value != nil && c.Quality.Qualification == semreg.QualificationQualified && c.Quality.Promotion == semreg.PromotionPromoted && c.Quality.Validity == semreg.ValidityGood && e.EffectiveAvailability == semreg.AvailabilityAvailable && e.Freshness == semreg.FreshnessFresh
	}
	qualifications := []semreg.Qualification{semreg.QualificationCandidate, semreg.QualificationQualified, semreg.QualificationUnsupported, semreg.QualificationUnknown, semreg.QualificationRejected}
	promotions := []semreg.Promotion{semreg.PromotionPromoted, semreg.PromotionUnpromoted}
	validities := []semreg.Validity{semreg.ValidityGood, semreg.ValiditySuspect, semreg.ValidityBad, semreg.ValidityUnknown}
	availabilities := []semreg.Availability{semreg.AvailabilityAvailable, semreg.AvailabilityDegraded, semreg.AvailabilityUnavailable, semreg.AvailabilityWithdrawn}
	freshnesses := []semreg.Freshness{semreg.FreshnessFresh, semreg.FreshnessStale, semreg.FreshnessExpired, semreg.FreshnessUnknown}
	for _, source := range []semreg.SourceID{testOwner, testForeign} {
		for _, q := range qualifications {
			for _, p := range promotions {
				for _, v := range validities {
					for _, a := range availabilities {
						for _, f := range freshnesses {
							c := testCandidate("candidate:one", source, "40")
							c.Quality.Qualification, c.Quality.Promotion, c.Quality.Validity = q, p, v
							e := testEvaluated(c)
							e.EffectiveAvailability, e.Freshness = a, f
							envelope, _ := testEnvelope(c)
							got, ok := Choose(envelope, []semreg.EvaluatedFact{e}, testOwner)
							if want := today(c, e); ok != want || (ok && got != c.CandidateID) {
								t.Fatalf("%s %s %s %s %s %s: Choose = %q %v, want %v", source, q, p, v, a, f, got, ok, want)
							}
						}
					}
				}
			}
		}
	}
}

func TestSelectEmptyEnvelope(t *testing.T) {
	if got, ok := Choose(semreg.FactEnvelope{Key: testKey(), Candidates: []semreg.FactCandidate{}, Conflicts: []semreg.Conflict{}}, nil, testOwner); ok {
		t.Fatalf("Choose on an empty envelope = %q", got)
	}
}
