package canonicalpvshadow

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

type donor struct {
	Contract string        `json:"contract"`
	Status   string        `json:"status"`
	Sources  []donorSource `json:"sources"`
	Clock    struct {
		Receipt string `json:"receipt_ns"`
		Fresh   string `json:"fresh_for_ns"`
		Retain  string `json:"retain_for_ns"`
	} `json:"clock"`
	Provenance struct {
		Asset        string `json:"asset_ref"`
		Protocol     string `json:"protocol"`
		Profile      string `json:"semantic_profile_id"`
		Version      string `json:"profile_version"`
		Registry     string `json:"registry_digest"`
		Observation  string `json:"observation_digest"`
		Shadow       string `json:"shadow_digest"`
		Evidence     string `json:"evidence_digest"`
		EnergyWordHi uint16 `json:"energy_word_hi"`
		EnergyWordLo uint16 `json:"energy_word_lo"`
	} `json:"provenance"`
	Facts     []donorFact     `json:"facts"`
	Withheld  []donorWithheld `json:"withheld"`
	Counters  []counter       `json:"counters"`
	Negatives []counter       `json:"counter_negatives"`
	policies  map[string]semregPolicyFixture
}
type donorSource struct {
	Repository string `json:"repository"`
	Commit     string `json:"commit"`
	Path       string `json:"path"`
	Blob       string `json:"blob_sha1"`
	SHA256     string `json:"sha256"`
}
type donorFact struct {
	RequestRef          string `json:"request_ref"`
	RequestNativeID     string `json:"request_native_id"`
	LegacyKey           string `json:"legacy_key"`
	Target              string `json:"target"`
	Dimension           string `json:"dimension"`
	DimensionValue      string `json:"dimension_value"`
	Coefficient         string `json:"coefficient"`
	Scale               int32  `json:"scale"`
	Symbol              string `json:"symbol"`
	SemanticUnit        string `json:"semantic_unit"`
	SemanticCoefficient string `json:"semantic_coefficient"`
	SemanticExponent    int32  `json:"semantic_exponent10"`
	SemanticSymbol      string `json:"semantic_symbol"`
	Outcome             string `json:"outcome"`
	Loss                string `json:"loss"`
}
type donorWithheld struct {
	RequestRef      string `json:"request_ref"`
	RequestNativeID string `json:"request_native_id"`
	Outcome         string `json:"outcome"`
}
type semregPolicyFixture struct {
	ID                   string `json:"id"`
	Version              string `json:"version"`
	FreshForNS           string `json:"fresh_for_ns"`
	RetainForNS          string `json:"retain_for_ns"`
	MaxWallUncertaintyNS string `json:"max_wall_uncertainty_ns"`
}

type semregComparator struct {
	Contract string `json:"contract"`
	Facts    []struct {
		Legacy          string              `json:"legacy"`
		Target          string              `json:"target"`
		Dimension       string              `json:"dimension"`
		DimensionValue  string              `json:"dimension_value"`
		Symbol          string              `json:"symbol"`
		FreshnessPolicy semregPolicyFixture `json:"freshness_policy"`
	} `json:"facts"`
}
type semregDispositions struct {
	Contract     string `json:"contract"`
	Dispositions []struct {
		Legacy  string `json:"legacy"`
		Outcome string `json:"outcome"`
	} `json:"dispositions"`
}
type counter struct {
	ID        string  `json:"id"`
	Previous  *string `json:"previous"`
	Current   string  `json:"current"`
	Event     string  `json:"event"`
	Modulus   string  `json:"modulus"`
	Boundary  bool    `json:"boundary_verified"`
	Digest    string  `json:"evidence_digest"`
	Expected  string  `json:"expected_evidence_digest"`
	Want      string  `json:"want"`
	WantError string  `json:"want_error"`
}

func TestCanonicalPVShadowPipeline(t *testing.T) {
	d := load(t)
	vector, dispositions := loadSemRegComparator(t, d)
	if err := comparatorBindingError(d, vector, dispositions); err != nil {
		t.Fatal(err)
	}
	batch := batchFor(t, d, "1", "0")
	kernel, err := semreg.NewPublicationKernel(semreg.AssetID(d.Provenance.Asset), pv.New())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, canonical, err := kernel.Apply(batch, mono(d.Clock.Receipt))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Facts) != 11 || len(snapshot.Services) != 0 || len(snapshot.Capabilities) != 0 || len(snapshot.Bindings) != 1 || len(snapshot.IdentityLinks) != 1 {
		t.Fatalf("runtime state facts=%d services=%d capabilities=%d", len(snapshot.Facts), len(snapshot.Services), len(snapshot.Capabilities))
	}
	if encoded, err := semreg.CanonicalJSON(snapshot); err != nil || !bytes.Equal(encoded, canonical) {
		t.Fatalf("canonical snapshot: %v", err)
	}
	if decoded, err := semreg.Decode[semreg.Snapshot](canonical); err != nil || decoded.SnapshotID != snapshot.SnapshotID {
		t.Fatalf("canonical decode: %v", err)
	}
	for _, check := range []struct{ at string }{{d.Clock.Receipt}, {"30000000100"}, {"60000000100"}, {"300000000100"}, {"600000000100"}, {"900000000100"}, {"86400000000100"}} {
		view, err := semreg.EvaluateSnapshot(snapshot, semreg.EvaluationContext{EvaluatedAt: wall(check.at), EvaluateMonotonic: mono(check.at)})
		if err != nil || len(view.Facts) != 11 {
			t.Fatalf("evaluation %s: %v", check.at, err)
		}
		for _, fact := range view.Facts {
			want := semreg.FreshnessFresh
			isAccumulator := fact.CandidateID == "candidate:canonical-pv:c"
			isStatus := fact.CandidateID == "candidate:canonical-pv:e"
			if check.at == "30000000100" && !isAccumulator && !isStatus {
				want = semreg.FreshnessStale
			}
			if check.at == "300000000100" && !isAccumulator {
				want = semreg.FreshnessExpired
			}
			if check.at == "300000000100" && isStatus {
				want = semreg.FreshnessStale
			}
			if check.at == "60000000100" && !isAccumulator {
				want = semreg.FreshnessStale
			}
			if (check.at == "600000000100" || check.at == "900000000100" || check.at == "86400000000100") && !isAccumulator {
				want = semreg.FreshnessExpired
			}
			if check.at == "900000000100" && isAccumulator {
				want = semreg.FreshnessStale
			}
			if check.at == "86400000000100" && isAccumulator {
				want = semreg.FreshnessExpired
			}
			if fact.Freshness != want {
				t.Fatalf("freshness %s candidate=%s got=%s want=%s", check.at, fact.CandidateID, fact.Freshness, want)
			}
		}
	}
	report := reportFor(t, snapshot, d)
	if len(report.Requested) != 14 || len(report.Dispositions) != 14 {
		t.Fatalf("projection accounting %d/%d", len(report.Requested), len(report.Dispositions))
	}
	if _, err := semreg.CanonicalJSON(report); err != nil {
		t.Fatal(err)
	}
	assertReportAssociations(t, report, d)
	assertPublishedEnergyConversion(t, snapshot, d)
}

func TestCanonicalPVShadowComparatorBindingRejectsSemanticMutation(t *testing.T) {
	d := load(t)
	vector, dispositions := loadSemRegComparator(t, d)
	for name, mutate := range map[string]func(*donor){
		"target":                func(d *donor) { d.Facts[0].Target = "pv.ac.frequency" },
		"dimension":             func(d *donor) { d.Facts[0].DimensionValue = "inverter:mutated" },
		"disposition":           func(d *donor) { d.Facts[0].Outcome = "transformed" },
		"operating_state_token": func(d *donor) { d.Facts[4].SemanticSymbol = "standby" },
		"request_identity": func(d *donor) {
			d.Facts[0].RequestNativeID = d.Facts[1].RequestNativeID
			d.Facts[0].Target = d.Facts[1].Target
			d.Facts[0].Dimension = d.Facts[1].Dimension
			d.Facts[0].DimensionValue = d.Facts[1].DimensionValue
			d.Facts[0].Outcome = d.Facts[1].Outcome
		},
		"withheld_request_identity": func(d *donor) { d.Withheld[0].RequestNativeID = d.Withheld[1].RequestNativeID },
		"withheld_outcome":          func(d *donor) { d.Withheld[0].Outcome = "exact" },
	} {
		mutated := d
		mutated.Facts = append([]donorFact(nil), d.Facts...)
		mutated.Withheld = append([]donorWithheld(nil), d.Withheld...)
		mutate(&mutated)
		if err := comparatorBindingError(mutated, vector, dispositions); err == nil {
			t.Fatalf("semantic %s mutation was accepted", name)
		}
	}
	mutatedVector := vector
	mutatedVector.Facts = append(mutatedVector.Facts[:0:0], vector.Facts...)
	mutatedVector.Facts[0].FreshnessPolicy.FreshForNS = "1"
	if err := comparatorBindingError(d, mutatedVector, dispositions); err == nil {
		t.Fatal("accepted lifecycle policy mutation was accepted")
	}
}

func TestCanonicalPVShadowRejectsMutationAndReplay(t *testing.T) {
	d := load(t)
	initial := batchFor(t, d, "1", "0")
	kernel, err := semreg.NewPublicationKernel(semreg.AssetID(d.Provenance.Asset), pv.New())
	if err != nil {
		t.Fatal(err)
	}
	clock := mono(d.Clock.Receipt)
	first, firstRaw, err := kernel.Apply(initial, clock)
	if err != nil {
		t.Fatal(err)
	}
	unit := batchFor(t, d, "2", "1")
	value := *unit.FactUpserts[0].Value
	quantity := *value.Quantity
	quantity.Unit = "unit.kilowatt_hour"
	value.Quantity = &quantity
	unit.FactUpserts[0].Value = &value
	rejected(t, kernel, unit, semreg.InvalidValue, clock)
	replay, raw, err := kernel.Apply(initial, clock)
	if err != nil || replay.SnapshotID != first.SnapshotID || !bytes.Equal(raw, firstRaw) {
		t.Fatalf("idempotence: %v", err)
	}
	conflict := initial
	conflict.BatchID = "batch:canonical-pv:conflict"
	conflict.ObservedAt = wall("101")
	seal(t, &conflict)
	rejected(t, kernel, conflict, semreg.SequenceConflict, clock)
	partial := base("2", "1", d)
	updated := initial.FactUpserts[0]
	updated.Revision = "2"
	updated.Value = &semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "1", Exponent10: 0}, Unit: "unit.watt"}}
	partial.FactUpserts = []semreg.FactCandidate{updated}
	seal(t, &partial)
	second, secondRaw, err := kernel.Apply(partial, clock)
	if err != nil || len(second.Facts) != 11 {
		t.Fatalf("partial retention: %v", err)
	}
	assertPartialSnapshot(t, first, second, updated)
	bad := partial
	bad.BatchID = "batch:canonical-pv:bad"
	bad.FactUpserts = append([]semreg.FactCandidate(nil), partial.FactUpserts...)
	bad.FactUpserts[0].Key.Dimensions = append([]semreg.Dimension(nil), partial.FactUpserts[0].Key.Dimensions...)
	bad.FactUpserts[0].Key.Dimensions = append(bad.FactUpserts[0].Key.Dimensions, bad.FactUpserts[0].Key.Dimensions[0])
	rejected(t, kernel, bad, semreg.DuplicateKey, clock)
	fresh, err := semreg.NewPublicationKernel(semreg.AssetID(d.Provenance.Asset), pv.New())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := fresh.Apply(initial, clock); err != nil {
		t.Fatal(err)
	}
	if _, rebuilt, err := fresh.Apply(partial, clock); err != nil || !bytes.Equal(rebuilt, secondRaw) {
		t.Fatalf("ordered replay: %v", err)
	}
}

func TestCanonicalPVShadowCounterEvidence(t *testing.T) {
	d := load(t)
	initial := batchFor(t, d, "1", "0")
	energy := candidateForFact(t, initial, "pv.energy.generated")
	for _, c := range append(d.Counters, d.Negatives...) {
		state, evidence, failure := counterState(c)
		if c.WantError != "" {
			if state != "discontinuity" || failure != c.WantError || evidence.Digest != "" {
				t.Fatalf("counter %s=%s/%s", c.ID, state, failure)
			}
			continue
		}
		if state != c.Want || failure != "" {
			t.Fatalf("counter %s=%s/%s", c.ID, state, failure)
		}
		if c.Digest == "" {
			continue
		}
		carried := energy
		carried.Evidence = append(carried.Evidence, evidence)
		carried.Origin.Evidence = append(carried.Origin.Evidence, evidence)
		raw, err := semreg.CanonicalJSON(carried)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := semreg.DecodeFactCandidate(raw)
		if err != nil || !has(decoded.Evidence, evidence.Digest) || !has(decoded.Origin.Evidence, evidence.Digest) {
			t.Fatalf("counter %s evidence: %v", c.ID, err)
		}
		if reencoded, err := semreg.CanonicalJSON(decoded); err != nil || !bytes.Equal(raw, reencoded) {
			t.Fatalf("counter %s canonical: %v", c.ID, err)
		}
	}
	for _, c := range d.Counters {
		if c.Event != "reset" && c.Event != "rollover" {
			continue
		}
		state, evidence, failure := counterState(c)
		if state != c.Want || failure != "" || evidence.Digest == "" {
			t.Fatalf("accepted counter %s=%s/%s", c.ID, state, failure)
		}
		initial, energy := counterInitialBatch(t, d, c)
		kernel, err := semreg.NewPublicationKernel(semreg.AssetID(d.Provenance.Asset), pv.New())
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := kernel.Apply(initial, mono(d.Clock.Receipt)); err != nil {
			t.Fatalf("counter %s initial: %v", c.ID, err)
		}
		if c.Previous != nil {
			stored := envelopeFor(t, mustCurrent(t, kernel), energy.Key)
			if len(stored.Candidates) != 1 || stored.Candidates[0].Value == nil || stored.Candidates[0].Value.Quantity == nil || stored.Candidates[0].Value.Quantity.Number.Coefficient != *c.Previous {
				t.Fatalf("counter %s initial value did not use declared previous %s", c.ID, *c.Previous)
			}
		}
		update := energy
		value := *update.Value
		quantity := *value.Quantity
		quantity.Number.Coefficient = c.Current
		value.Quantity = &quantity
		update.Value = &value
		update.Revision = "2"
		update.Evidence = append(update.Evidence, evidence)
		update.Origin.Evidence = append(update.Origin.Evidence, evidence)
		batch := base("2", "1", d)
		batch.BatchID = semreg.BatchID("batch:canonical-pv:counter-" + c.ID)
		batch.FactUpserts = []semreg.FactCandidate{update}
		seal(t, &batch)
		accepted, _, err := kernel.Apply(batch, mono(d.Clock.Receipt))
		if err != nil {
			t.Fatalf("counter %s apply: %v", c.ID, err)
		}
		stored := envelopeFor(t, accepted, update.Key)
		if len(stored.Candidates) != 1 || !sameFactCandidate(t, stored.Candidates[0], update) || !has(stored.Candidates[0].Evidence, evidence.Digest) || !has(stored.Candidates[0].Origin.Evidence, evidence.Digest) {
			t.Fatalf("counter %s accepted evidence lost: %+v", c.ID, stored)
		}
	}
}

func counterInitialBatch(t *testing.T, d donor, c counter) (semreg.PublicationBatch, semreg.FactCandidate) {
	t.Helper()
	batch := batchFor(t, d, "1", "0")
	energy := candidateForFact(t, batch, "pv.energy.generated")
	if c.Previous == nil {
		return batch, energy
	}
	value := *energy.Value
	quantity := *value.Quantity
	quantity.Number.Coefficient = *c.Previous
	value.Quantity = &quantity
	energy.Value = &value
	for index := range batch.FactUpserts {
		if batch.FactUpserts[index].CandidateID == energy.CandidateID {
			batch.FactUpserts[index] = energy
			break
		}
	}
	seal(t, &batch)
	return batch, energy
}

func mustCurrent(t *testing.T, kernel *semreg.PublicationKernel) semreg.Snapshot {
	t.Helper()
	snapshot, _, ok := kernel.Current()
	if !ok {
		t.Fatal("counter initial snapshot absent")
	}
	return snapshot
}

func load(t *testing.T) donor {
	t.Helper()
	raw, err := os.ReadFile("testdata/donor.json")
	if err != nil {
		t.Fatal(err)
	}
	var d donor
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	if d.Contract != "helianthus.semantic.compatibility.canonical-pv/v1" || d.Status != "test_only_non_runtime" || len(d.Sources) != 3 || len(d.Facts) != 11 || len(d.Withheld) != 3 || len(d.Counters) != 6 || len(d.Negatives) != 3 {
		t.Fatal("invalid donor fixture")
	}
	if d.Sources[0].Commit != "f5cd9c51c60bdf422e8fc1b5690fbde52a393be3" || d.Sources[1].Commit != "eafda745434971ea05f658322b7e64c04888d1c2" || d.Sources[2].Commit != "eafda745434971ea05f658322b7e64c04888d1c2" || d.Sources[1].Path != "fixtures/v1/compatibility/canonical-pv-v1/comparator-vectors.json" || d.Sources[2].Path != "fixtures/v1/compatibility/canonical-pv-v1/dispositions.json" {
		t.Fatal("source pin drift")
	}
	vector, _ := loadSemRegComparator(t, d)
	d.policies = make(map[string]semregPolicyFixture, len(vector.Facts))
	for _, fact := range vector.Facts {
		policy := semreg.FreshnessPolicy{PolicyID: semreg.PolicyID(fact.FreshnessPolicy.ID), Version: semreg.SemanticVersion(fact.FreshnessPolicy.Version), FreshForNS: semreg.Uint64(fact.FreshnessPolicy.FreshForNS), RetainForNS: semreg.Uint64(fact.FreshnessPolicy.RetainForNS), MaxWallUncertaintyNS: semreg.Uint64(fact.FreshnessPolicy.MaxWallUncertaintyNS)}
		if err := policy.Validate(); err != nil {
			t.Fatalf("accepted lifecycle policy %s: %v", fact.Legacy, err)
		}
		if _, duplicate := d.policies[fact.Legacy]; duplicate {
			t.Fatalf("duplicate accepted lifecycle identity %s", fact.Legacy)
		}
		d.policies[fact.Legacy] = fact.FreshnessPolicy
	}
	return d
}
func loadSemRegComparator(t *testing.T, d donor) (semregComparator, semregDispositions) {
	t.Helper()
	moduleVersion := semregModuleVersion(t)
	vectorRaw := readSemRegFixture(t, moduleVersion, d.Sources[1])
	dispositionsRaw := readSemRegFixture(t, moduleVersion, d.Sources[2])
	var vector semregComparator
	if err := json.Unmarshal(vectorRaw, &vector); err != nil {
		t.Fatal(err)
	}
	var dispositions semregDispositions
	if err := json.Unmarshal(dispositionsRaw, &dispositions); err != nil {
		t.Fatal(err)
	}
	if vector.Contract != d.Contract || dispositions.Contract != d.Contract || len(vector.Facts) != 11 || len(dispositions.Dispositions) != 14 {
		t.Fatalf("accepted SemReg comparator fixture shape vector=%d dispositions=%d", len(vector.Facts), len(dispositions.Dispositions))
	}
	return vector, dispositions
}
func semregModuleVersion(t *testing.T) string {
	t.Helper()
	output, err := exec.Command("go", "list", "-m", "-json", "github.com/Project-Helianthus/helianthus-semreg").Output()
	if err != nil {
		t.Fatal(err)
	}
	var module struct {
		Path    string
		Version string
		Replace *struct{}
	}
	if err := json.Unmarshal(output, &module); err != nil {
		t.Fatal(err)
	}
	if module.Path != "github.com/Project-Helianthus/helianthus-semreg" || module.Replace != nil || module.Version != "v0.0.0-20260907015800-eafda7454349" {
		t.Fatalf("unexpected SemReg module pin %+v", module)
	}
	return module.Version
}
func readSemRegFixture(t *testing.T, moduleVersion string, source donorSource) []byte {
	t.Helper()
	cache, err := exec.Command("go", "env", "GOMODCACHE").Output()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(strings.TrimSpace(string(cache)), "github.com", "!project-!helianthus", "helianthus-semreg@"+moduleVersion, filepath.FromSlash(source.Path))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := fmt.Sprintf("%x", sum); got != source.SHA256 {
		t.Fatalf("SemReg fixture digest %s=%s want=%s", source.Path, got, source.SHA256)
	}
	return raw
}
func comparatorBindingError(d donor, vector semregComparator, dispositions semregDispositions) error {
	facts := make(map[string]struct {
		target, dimension, dimensionValue, symbol string
		policy                                    semregPolicyFixture
	}, len(vector.Facts))
	for _, fact := range vector.Facts {
		if _, duplicate := facts[fact.Legacy]; duplicate {
			return fmt.Errorf("duplicate accepted comparator fact %q", fact.Legacy)
		}
		facts[fact.Legacy] = struct {
			target, dimension, dimensionValue, symbol string
			policy                                    semregPolicyFixture
		}{fact.Target, fact.Dimension, fact.DimensionValue, fact.Symbol, fact.FreshnessPolicy}
	}
	outcomes := make(map[string]string, len(dispositions.Dispositions))
	for _, disposition := range dispositions.Dispositions {
		if _, duplicate := outcomes[disposition.Legacy]; duplicate {
			return fmt.Errorf("duplicate accepted comparator disposition %q", disposition.Legacy)
		}
		outcomes[disposition.Legacy] = disposition.Outcome
	}
	if len(facts) != len(d.Facts) {
		return fmt.Errorf("accepted comparator facts=%d donor facts=%d", len(facts), len(d.Facts))
	}
	for _, fact := range d.Facts {
		accepted, ok := facts[fact.RequestNativeID]
		acceptedPolicy, policyOK := d.policies[fact.RequestNativeID]
		if fact.RequestNativeID == "" || fact.RequestRef != canonicalPVRequestRef(fact.RequestNativeID) || !ok || !policyOK || accepted.policy != acceptedPolicy || fact.Target != accepted.target || fact.Dimension != accepted.dimension || fact.DimensionValue != accepted.dimensionValue || (fact.RequestNativeID == "inverter.operating_state" && (fact.Symbol != "OPERATING" || fact.SemanticSymbol != accepted.symbol || accepted.symbol != "generating")) {
			return fmt.Errorf("donor semantic target or dimensions drifted for %s", fact.LegacyKey)
		}
		outcome, ok := outcomes[fact.RequestNativeID]
		if !ok || outcome != fact.Outcome {
			return fmt.Errorf("donor disposition drifted for %s: %q want %q", fact.RequestNativeID, fact.Outcome, outcome)
		}
	}
	seenDispositions := make(map[string]bool, len(outcomes))
	for _, fact := range d.Facts {
		if seenDispositions[fact.RequestNativeID] {
			return fmt.Errorf("duplicate donor requested identity %q", fact.RequestNativeID)
		}
		seenDispositions[fact.RequestNativeID] = true
	}
	for _, withheld := range d.Withheld {
		outcome, ok := outcomes[withheld.RequestNativeID]
		if withheld.RequestNativeID == "" || withheld.RequestRef != canonicalPVRequestRef(withheld.RequestNativeID) || !ok || withheld.Outcome != "withheld" || outcome != withheld.Outcome || seenDispositions[withheld.RequestNativeID] {
			return fmt.Errorf("donor withheld disposition drifted for %s", withheld.RequestNativeID)
		}
		seenDispositions[withheld.RequestNativeID] = true
	}
	if len(seenDispositions) != len(outcomes) {
		return fmt.Errorf("accepted disposition accounting seen=%d want=%d", len(seenDispositions), len(outcomes))
	}
	return nil
}
func canonicalPVRequestRef(nativeFieldID string) string {
	payload := append(append([]byte("canonical-pv-request-v1"), 0), []byte(nativeFieldID)...)
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("sha256:%x", sum)
}
func evidence(digest string) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: "helianthus.compatibility", Kind: "canonical_pv.fixture", Digest: semreg.Digest(digest), Contract: "helianthus.canonical-pv/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
}
func wall(ns string) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(ns), ClockID: "clock.utc", UncertaintyNS: "0"}
}
func mono(ns string) semreg.MonotonicPoint {
	return semreg.MonotonicPoint{ClockEpochID: "clock-epoch:canonical-pv", Nanoseconds: semreg.Uint64(ns)}
}
func base(sequence, expected semreg.Uint64, d donor) semreg.PublicationBatch {
	return semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:canonical-pv:" + string(sequence)), AssetID: semreg.AssetID(d.Provenance.Asset), SourceID: "source:sunspec:fixture", SourceEpochID: "source-epoch:sunspec:fixture", DriverGeneration: "1", Sequence: sequence, ExpectedSemanticRevision: expected, ObservedAt: wall(d.Clock.Receipt), SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: []semreg.SourceEpochID{}, BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
}
func batchFor(t *testing.T, d donor, sequence, expected semreg.Uint64) semreg.PublicationBatch {
	t.Helper()
	b := base(sequence, expected, d)
	source, epoch, binding, generation := semreg.SourceID("source:sunspec:fixture"), semreg.SourceEpochID("source-epoch:sunspec:fixture"), semreg.NativeBindingID("binding:sunspec:fixture"), semreg.Uint64("1")
	b.SourceUpserts = []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: semreg.DefinitionID(d.Provenance.Protocol), ProfileID: semreg.DefinitionID(d.Provenance.Profile), ProfileVersion: semreg.VersionLabel(d.Provenance.Version), RegistryEvidence: evidence(d.Provenance.Registry), StartedAt: wall(d.Clock.Receipt), State: semreg.SourceCurrent, Revision: "1"}}
	b.BindingUpserts = []semreg.NativeBinding{{BindingID: binding, AssetID: semreg.AssetID(d.Provenance.Asset), SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: evidence(d.Provenance.Shadow), State: semreg.BindingCurrent, Revision: "1"}}
	b.IdentityLinkUpserts = []semreg.IdentityLink{{AssetID: semreg.AssetID(d.Provenance.Asset), BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence(d.Provenance.Evidence)}, Revision: "1"}}
	for i, f := range d.Facts {
		b.FactUpserts = append(b.FactUpserts, candidate(t, d, f, i, source, epoch, binding, generation))
	}
	sort.Slice(b.FactUpserts, func(i, j int) bool { return b.FactUpserts[i].CandidateID < b.FactUpserts[j].CandidateID })
	seal(t, &b)
	return b
}
func candidate(t *testing.T, d donor, f donorFact, i int, source semreg.SourceID, epoch semreg.SourceEpochID, binding semreg.NativeBindingID, generation semreg.Uint64) semreg.FactCandidate {
	t.Helper()
	dimension := f.DimensionValue
	coefficient, exponent := f.Coefficient, f.Scale
	if f.Target == "pv.energy.generated" {
		if f.SemanticUnit != "unit.kilowatt_hour" || f.SemanticCoefficient != f.Coefficient || f.SemanticExponent != f.Scale-3 {
			t.Fatalf("energy donor transform is not an exact Wh-to-kWh exponent adjustment: %+v", f)
		}
		// The shared legacy observation remains in Wh. SemReg receives the same
		// coefficient with only the exact base-10 exponent adjusted for kWh.
		exponent -= 3
	}
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: coefficient, Exponent10: exponent}, Unit: semreg.DefinitionID(f.SemanticUnit)}}
	if f.Symbol != "" {
		value = semreg.Value{Kind: semreg.ValueSymbol, Symbol: &semreg.Symbol{Namespace: semreg.DefinitionID(f.Target), Token: f.SemanticSymbol, Known: true}}
	}
	key := semreg.FactKey{PackID: "helianthus.pack.pv", PackVersion: "1.0.0", FactID: semreg.DefinitionID(f.Target), Dimensions: []semreg.Dimension{{ID: semreg.DefinitionID(f.Dimension), Value: semreg.Value{Kind: semreg.ValueText, Text: &dimension}}}}
	if err := pv.New().ValidateFact(key, &value); err != nil {
		t.Fatalf("PV validation %s: %v", f.LegacyKey, err)
	}
	accepted, ok := d.policies[f.RequestNativeID]
	if !ok {
		t.Fatalf("accepted lifecycle policy absent for %s", f.RequestNativeID)
	}
	policy := semreg.FreshnessPolicy{PolicyID: semreg.PolicyID(accepted.ID), Version: semreg.SemanticVersion(accepted.Version), FreshForNS: semreg.Uint64(accepted.FreshForNS), RetainForNS: semreg.Uint64(accepted.RetainForNS), MaxWallUncertaintyNS: semreg.Uint64(accepted.MaxWallUncertaintyNS)}
	if err := policy.Validate(); err != nil {
		t.Fatalf("accepted lifecycle policy %s: %v", f.RequestNativeID, err)
	}
	return semreg.FactCandidate{CandidateID: semreg.CandidateID("candidate:canonical-pv:" + string(rune('a'+i))), Key: key, Value: &value, Quality: semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationCandidate, Promotion: semreg.PromotionUnpromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}}, Times: semreg.Times{ReceivedAt: wall(d.Clock.Receipt), ReceiptMonotonic: mono(d.Clock.Receipt), EvaluatedAt: wall(d.Clock.Receipt), EvaluateMonotonic: mono(d.Clock.Receipt)}, FreshnessPolicy: policy, BindingID: &binding, SourceEpochID: &epoch, DriverGeneration: &generation, Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:canonical-pv:" + string(rune('a'+i))), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence(d.Provenance.Observation)}}, Evidence: []semreg.EvidenceRef{evidence(d.Provenance.Evidence)}, Revision: "1"}
}

func assertPublishedEnergyConversion(t *testing.T, snapshot semreg.Snapshot, d donor) {
	t.Helper()
	var source donorFact
	for _, f := range d.Facts {
		if f.Target == "pv.energy.generated" {
			source = f
			break
		}
	}
	if source.Target == "" {
		t.Fatal("immutable donor has no energy observation")
	}
	for _, envelope := range snapshot.Facts {
		if envelope.Key.FactID != "pv.energy.generated" {
			continue
		}
		if len(envelope.Candidates) != 1 || envelope.Candidates[0].Value == nil || envelope.Candidates[0].Value.Quantity == nil {
			t.Fatalf("published energy candidate=%+v", envelope)
		}
		got := envelope.Candidates[0].Value.Quantity.Number
		if envelope.Candidates[0].Value.Quantity.Unit != "unit.kilowatt_hour" || got.Coefficient != source.SemanticCoefficient || got.Exponent10 != source.SemanticExponent {
			t.Fatalf("published energy=%+v; donor semantic=%sE%d kWh", got, source.SemanticCoefficient, source.SemanticExponent)
		}
		if rat(source.Coefficient, source.Scale).Cmp(new(big.Rat).Mul(rat(got.Coefficient, got.Exponent10), big.NewRat(1000, 1))) != 0 {
			t.Fatalf("published energy %sE%d kWh is not exact source %sE%d Wh", got.Coefficient, got.Exponent10, source.Coefficient, source.Scale)
		}
		return
	}
	t.Fatal("published energy fact absent")
}
func seal(t *testing.T, b *semreg.PublicationBatch) {
	t.Helper()
	digest, err := b.ComputedDigest()
	if err != nil {
		t.Fatal(err)
	}
	b.BatchDigest = digest
}
func rejected(t *testing.T, k *semreg.PublicationKernel, b semreg.PublicationBatch, want semreg.ErrorID, clock semreg.MonotonicPoint) {
	t.Helper()
	before, raw, ok := k.Current()
	_, _, err := k.Apply(b, clock)
	if semreg.ErrorIdentifier(err) != want {
		t.Fatalf("error=%s want=%s", semreg.ErrorIdentifier(err), want)
	}
	after, afterRaw, afterOK := k.Current()
	if ok != afterOK || before.SnapshotID != after.SnapshotID || !bytes.Equal(raw, afterRaw) {
		t.Fatal("rejection changed snapshot")
	}
}
func reportFor(t *testing.T, s semreg.Snapshot, d donor) projection.ProjectionReport {
	t.Helper()
	requested := make([]projection.RequestedItem, 0, 14)
	dispositions := make([]projection.ProjectionDisposition, 0, 14)
	exact, transformed := 0, 0
	for _, f := range d.Facts {
		outcome := projection.ProjectionOutcome(f.Outcome)
		itemID := requestItemID(t, f.RequestRef)
		requested = append(requested, projection.RequestedItem{Kind: projection.ItemFact, ItemID: itemID})
		dimension := f.DimensionValue
		key := semreg.FactKey{PackID: "helianthus.pack.pv", PackVersion: "1.0.0", FactID: semreg.DefinitionID(f.Target), Dimensions: []semreg.Dimension{{ID: semreg.DefinitionID(f.Dimension), Value: semreg.Value{Kind: semreg.ValueText, Text: &dimension}}}}
		dis := projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: itemID, Outcome: outcome, SourceKeys: []semreg.FactKey{key}, Loss: []projection.LossDetail{}}
		if outcome == projection.ProjectionExact {
			exact++
		} else {
			transformed++
			kind := projection.LossUnit
			if strings.HasPrefix(f.Loss, "symbol:") {
				kind = projection.LossSymbol
			}
			if strings.HasPrefix(f.Loss, "provenance:") {
				kind = projection.LossProvenance
			}
			dis.Loss = []projection.LossDetail{{Kind: kind, SourceItems: []semreg.DefinitionID{semreg.DefinitionID(f.Target)}, Description: f.Loss, Reversible: kind == projection.LossUnit}}
		}
		dispositions = append(dispositions, dis)
	}
	for _, withheld := range d.Withheld {
		reason := semreg.DefinitionID("compatibility.legacy_path_required")
		itemID := requestItemID(t, withheld.RequestRef)
		requested = append(requested, projection.RequestedItem{Kind: projection.ItemFact, ItemID: itemID})
		dispositions = append(dispositions, projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: itemID, Outcome: projection.ProjectionWithheld, SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{{Kind: projection.LossProvenance, SourceItems: []semreg.DefinitionID{semreg.DefinitionID(withheld.RequestNativeID)}, Description: "provenance: retained legacy output"}}, Reason: &reason})
	}
	if exact != 8 || transformed != 3 {
		t.Fatalf("dispositions=%d/%d", exact, transformed)
	}
	sort.Slice(requested, func(i, j int) bool { return requested[i].ItemID < requested[j].ItemID })
	sort.Slice(dispositions, func(i, j int) bool { return dispositions[i].ItemID < dispositions[j].ItemID })
	r, err := projection.Project(s, projection.ProjectionManifest{TargetID: "target:canonical-pv-comparator", TargetVersion: "1.0.0", KernelVersion: semreg.ContractKernelV1, PackVersions: []semreg.PackRef{{ID: "helianthus.pack.pv", Version: "1.0.0"}}, MappingRevision: "1"}, requested, dispositions, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func requestItemID(t *testing.T, requestRef string) semreg.DefinitionID {
	t.Helper()
	const digestPrefix = "sha256:"
	digest := strings.TrimPrefix(requestRef, digestPrefix)
	if requestRef != digestPrefix+digest || len(digest) != 64 {
		t.Fatalf("invalid donor request ref %q", requestRef)
	}
	itemID := semreg.DefinitionID("projection.pv.fixture.request." + digest)
	if err := itemID.Validate(); err != nil {
		t.Fatalf("request item identity %q: %v", itemID, err)
	}
	return itemID
}
func assertReportAssociations(t *testing.T, report projection.ProjectionReport, d donor) {
	t.Helper()
	type expected struct {
		outcome  projection.ProjectionOutcome
		key      semreg.FactKey
		mapped   bool
		nativeID string
	}
	expectedByItem := make(map[semreg.DefinitionID]expected, 14)
	for _, f := range d.Facts {
		itemID := requestItemID(t, f.RequestRef)
		dimension := f.DimensionValue
		key := semreg.FactKey{PackID: "helianthus.pack.pv", PackVersion: "1.0.0", FactID: semreg.DefinitionID(f.Target), Dimensions: []semreg.Dimension{{ID: semreg.DefinitionID(f.Dimension), Value: semreg.Value{Kind: semreg.ValueText, Text: &dimension}}}}
		if _, duplicate := expectedByItem[itemID]; duplicate {
			t.Fatalf("duplicate mapped request item %s", itemID)
		}
		expectedByItem[itemID] = expected{outcome: projection.ProjectionOutcome(f.Outcome), key: key, mapped: true}
	}
	for _, withheld := range d.Withheld {
		itemID := requestItemID(t, withheld.RequestRef)
		if withheld.RequestNativeID == "" {
			t.Fatalf("withheld request %s has no native identity", itemID)
		}
		if _, duplicate := expectedByItem[itemID]; duplicate {
			t.Fatalf("duplicate withheld request item %s", itemID)
		}
		expectedByItem[itemID] = expected{outcome: projection.ProjectionWithheld, nativeID: withheld.RequestNativeID}
	}
	if len(expectedByItem) != 14 {
		t.Fatalf("donor request associations=%d", len(expectedByItem))
	}
	seenRequested := make(map[semreg.DefinitionID]bool, 14)
	for _, requested := range report.Requested {
		if requested.Kind != projection.ItemFact || seenRequested[requested.ItemID] {
			t.Fatalf("invalid or duplicate report request %+v", requested)
		}
		if _, ok := expectedByItem[requested.ItemID]; !ok {
			t.Fatalf("unexpected report request %s", requested.ItemID)
		}
		seenRequested[requested.ItemID] = true
	}
	if len(seenRequested) != len(expectedByItem) {
		t.Fatalf("report requested associations=%d want=%d", len(seenRequested), len(expectedByItem))
	}
	seenDisposition := make(map[semreg.DefinitionID]bool, 14)
	for _, disposition := range report.Dispositions {
		want, ok := expectedByItem[disposition.ItemID]
		if !ok || seenDisposition[disposition.ItemID] || disposition.Kind != projection.ItemFact || disposition.Outcome != want.outcome {
			t.Fatalf("report disposition association=%+v", disposition)
		}
		seenDisposition[disposition.ItemID] = true
		if want.mapped {
			if len(disposition.SourceKeys) != 1 || !sameFactKey(t, disposition.SourceKeys[0], want.key) {
				t.Fatalf("mapped report disposition %s source keys=%+v", disposition.ItemID, disposition.SourceKeys)
			}
			continue
		}
		if len(disposition.SourceKeys) != 0 || len(disposition.Loss) != 1 || len(disposition.Loss[0].SourceItems) != 1 || disposition.Loss[0].SourceItems[0] != semreg.DefinitionID(want.nativeID) {
			t.Fatalf("withheld report disposition %s=%+v", disposition.ItemID, disposition)
		}
	}
	if len(seenDisposition) != len(expectedByItem) {
		t.Fatalf("report disposition associations=%d want=%d", len(seenDisposition), len(expectedByItem))
	}
}
func sameFactKey(t *testing.T, left, right semreg.FactKey) bool {
	t.Helper()
	leftRaw, err := semreg.CanonicalJSON(left)
	if err != nil {
		t.Fatal(err)
	}
	rightRaw, err := semreg.CanonicalJSON(right)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(leftRaw, rightRaw)
}
func sameFactEnvelope(t *testing.T, left, right semreg.FactEnvelope) bool {
	t.Helper()
	leftRaw, err := semreg.CanonicalJSON(left)
	if err != nil {
		t.Fatal(err)
	}
	rightRaw, err := semreg.CanonicalJSON(right)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(leftRaw, rightRaw)
}
func sameFactCandidate(t *testing.T, left, right semreg.FactCandidate) bool {
	t.Helper()
	leftRaw, err := semreg.CanonicalJSON(left)
	if err != nil {
		t.Fatal(err)
	}
	rightRaw, err := semreg.CanonicalJSON(right)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(leftRaw, rightRaw)
}
func envelopeFor(t *testing.T, snapshot semreg.Snapshot, key semreg.FactKey) semreg.FactEnvelope {
	t.Helper()
	for _, envelope := range snapshot.Facts {
		if sameFactKey(t, envelope.Key, key) {
			return envelope
		}
	}
	t.Fatalf("snapshot fact not found: %+v", key)
	return semreg.FactEnvelope{}
}
func candidateForFact(t *testing.T, batch semreg.PublicationBatch, factID semreg.DefinitionID) semreg.FactCandidate {
	t.Helper()
	for _, candidate := range batch.FactUpserts {
		if candidate.Key.FactID == factID {
			return candidate
		}
	}
	t.Fatalf("batch fact %s not found", factID)
	return semreg.FactCandidate{}
}
func assertPartialSnapshot(t *testing.T, first, second semreg.Snapshot, updated semreg.FactCandidate) {
	t.Helper()
	if len(first.Facts) != len(second.Facts) {
		t.Fatalf("partial fact count before=%d after=%d", len(first.Facts), len(second.Facts))
	}
	retained := 0
	for _, before := range first.Facts {
		after := envelopeFor(t, second, before.Key)
		if sameFactKey(t, before.Key, updated.Key) {
			if len(after.Candidates) != 1 || !sameFactCandidate(t, after.Candidates[0], updated) {
				t.Fatalf("partial updated fact=%+v want=%+v", after, updated)
			}
			continue
		}
		retained++
		if !sameFactEnvelope(t, before, after) {
			t.Fatalf("partial update changed retained fact key=%+v before=%+v after=%+v", before.Key, before, after)
		}
	}
	if retained != len(first.Facts)-1 {
		t.Fatalf("partial retained facts=%d", retained)
	}
}
func rat(coefficient string, exponent int32) *big.Rat {
	n, ok := new(big.Int).SetString(coefficient, 10)
	if !ok {
		return nil
	}
	r := new(big.Rat).SetInt(n)
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs(exponent))), nil)
	if exponent >= 0 {
		return r.Mul(r, new(big.Rat).SetInt(scale))
	}
	return r.Quo(r, new(big.Rat).SetInt(scale))
}
func abs(v int32) int32 {
	if v < 0 {
		return -v
	}
	return v
}
func counterState(c counter) (string, semreg.EvidenceRef, string) {
	if c.Event == "none" {
		if c.Previous == nil {
			return "baseline", semreg.EvidenceRef{}, ""
		}
		if rat(c.Current, 0).Cmp(rat(*c.Previous, 0)) >= 0 {
			return "contiguous", semreg.EvidenceRef{}, ""
		}
		return "discontinuity", semreg.EvidenceRef{}, ""
	}
	e := evidence(c.Digest)
	if c.Digest == "" || e.Validate() != nil {
		return "discontinuity", semreg.EvidenceRef{}, "invalid_evidence"
	}
	if c.Expected != "" && e.Digest != semreg.Digest(c.Expected) {
		return "discontinuity", semreg.EvidenceRef{}, "digest_mismatch"
	}
	if c.Event == "reset" {
		return "reset", e, ""
	}
	if c.Event == "rollover" && c.Previous != nil && c.Boundary && rat(c.Current, 0).Cmp(rat(*c.Previous, 0)) < 0 && rat(c.Modulus, 0).Sign() > 0 && rat(*c.Previous, 0).Cmp(rat(c.Modulus, 0)) < 0 {
		return "rollover", e, ""
	}
	return "discontinuity", semreg.EvidenceRef{}, "invalid_evidence"
}
func has(items []semreg.EvidenceRef, digest semreg.Digest) bool {
	for _, item := range items {
		if item.Digest == digest {
			return true
		}
	}
	return false
}
