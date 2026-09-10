package ebusgateway

import (
	"bytes"
	"strings"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

func TestSemanticPrometheusRendersOnlyFreshQualifiedNumericFacts(t *testing.T) {
	domain := semanticMetricsFixture("pv", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
	var out bytes.Buffer
	writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
	metrics := out.String()
	if !strings.Contains(metrics, `helianthus_semantic_fact_value{dimension="",domain="pv",fact_id="pv.ac.aggregate_active_power",pack="helianthus.pack.pv",unit="unit.watt"} 42`) {
		t.Fatalf("missing accepted value:\n%s", metrics)
	}
	if strings.Contains(metrics, "asset:private") {
		t.Fatalf("forbidden asset identifier leaked:\n%s", metrics)
	}
	if !strings.Contains(metrics, `helianthus_semantic_evidence_age_seconds{dimension="",domain="pv",fact_id="pv.ac.aggregate_active_power",pack="helianthus.pack.pv"} 1`) {
		t.Fatalf("missing evidence age:\n%s", metrics)
	}
}

func TestSemanticPrometheusWithholdsStaleAndConflictedValues(t *testing.T) {
	for _, tc := range []struct {
		name      string
		freshness semreg.Freshness
		conflicts int
	}{{"stale", semreg.FreshnessStale, 0}, {"conflict", semreg.FreshnessFresh, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{semanticMetricsFixture("storage", true, tc.freshness, semreg.AvailabilityAvailable, semreg.ValidityGood, tc.conflicts)}, time.Unix(101, 0))
			metrics := out.String()
			if strings.Contains(metrics, "\nhelianthus_semantic_fact_value{") {
				t.Fatalf("unsafe value leakage:\n%s", metrics)
			}
			if !strings.Contains(metrics, "helianthus_semantic_fact_state") || !strings.Contains(metrics, "helianthus_semantic_open_conflicts") {
				t.Fatalf("missing truthful state:\n%s", metrics)
			}
		})
	}
}

func TestSemanticPrometheusRejectsIncoherentTuple(t *testing.T) {
	domain := semanticMetricsFixture("pv", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
	domain.Projection.SnapshotID = "snapshot:other"
	var out bytes.Buffer
	writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
	if strings.Contains(out.String(), "\nhelianthus_semantic_fact_value{") || !strings.Contains(out.String(), `helianthus_semantic_projection_available{domain="pv"} 0`) || !strings.Contains(out.String(), "helianthus_semantic_render_overflow 1") {
		t.Fatalf("incoherent tuple was rendered:\n%s", out.String())
	}
}

func TestSemanticPrometheusRejectsUnknownPVProjectionItems(t *testing.T) {
	domain := semanticMetricsFixture("pv", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
	for _, suffix := range []string{"attacker-unique-suffix-a", "attacker-unique-suffix-b", "attacker-unique-suffix-c"} {
		domain.Projection.Dispositions = append(domain.Projection.Dispositions, projection.ProjectionDisposition{
			ItemID:     semreg.DefinitionID("projection.gateway.pv.inverter." + suffix),
			Outcome:    projection.ProjectionExact,
			SourceKeys: domain.Projection.Dispositions[0].SourceKeys,
		})
	}
	var out bytes.Buffer
	writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
	metrics := out.String()
	if strings.Contains(metrics, "attacker-unique-suffix") {
		t.Fatalf("unbounded PV item label leaked:\n%s", metrics)
	}
	if !strings.Contains(metrics, "helianthus_semantic_render_overflow 3") {
		t.Fatalf("rejected item was not counted:\n%s", metrics)
	}
}

func TestSemanticPrometheusRejectsInvalidStateLabels(t *testing.T) {
	domain := semanticMetricsFixture("pv", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
	domain.Evaluation.Facts[0].Freshness = semreg.Freshness("asset:private")
	var out bytes.Buffer
	writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
	metrics := out.String()
	if strings.Contains(metrics, "asset:private") || strings.Contains(metrics, "\nhelianthus_semantic_fact_state{") {
		t.Fatalf("invalid semantic state leaked:\n%s", metrics)
	}
	if !strings.Contains(metrics, "helianthus_semantic_render_overflow 1") {
		t.Fatalf("invalid state was not counted:\n%s", metrics)
	}
}

func TestSemanticFactKeyRejectsInvalidKeys(t *testing.T) {
	invalid := semreg.FactKey{PackID: "not a definition id"}
	if key, ok := semanticFactKey(invalid); ok || key != "" {
		t.Fatalf("invalid fact key was canonicalized: %q, %t", key, ok)
	}
}

func semanticMetricsFixture(domain string, available bool, freshness semreg.Freshness, availability semreg.Availability, validity semreg.Validity, conflicts int) SemanticMetricsDomain {
	key := semreg.FactKey{PackID: "helianthus.pack." + semreg.DefinitionID(domain), PackVersion: "1.0.0", FactID: "pv.ac.aggregate_active_power", Dimensions: []semreg.Dimension{}}
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "42"}, Unit: "unit.watt"}}
	candidate := semreg.FactCandidate{CandidateID: "candidate:test", Key: key, Value: &value, Quality: semreg.Quality{Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: validity}, Times: semreg.Times{ReceivedAt: semreg.TimePoint{UnixNanoseconds: "100000000000", ClockID: "clock.utc"}}}
	envelope := semreg.FactEnvelope{Key: key, Candidates: []semreg.FactCandidate{candidate}}
	for i := 0; i < conflicts; i++ {
		envelope.Conflicts = append(envelope.Conflicts, semreg.Conflict{ConflictID: "conflict:test", Kind: semreg.ConflictValue, Candidates: []semreg.CandidateID{candidate.CandidateID}, State: semreg.ConflictOpen})
	}
	snapshot := semreg.Snapshot{SnapshotID: "snapshot:test", Facts: []semreg.FactEnvelope{envelope}}
	evaluation := semreg.EvaluationView{SnapshotID: snapshot.SnapshotID, Facts: []semreg.EvaluatedFact{{CandidateID: candidate.CandidateID, Freshness: freshness, EffectiveAvailability: availability}}, EvaluationDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"}
	item := semreg.DefinitionID("storage.pack.voltage")
	if domain == "pv" {
		item = "projection.gateway.pv.inverter.ac.power.active"
	}
	report := projection.ProjectionReport{SnapshotID: snapshot.SnapshotID, Dispositions: []projection.ProjectionDisposition{{ItemID: item, Outcome: projection.ProjectionExact, SourceKeys: []semreg.FactKey{key}}}}
	return SemanticMetricsDomain{Name: domain, Snapshot: snapshot, Evaluation: evaluation, Projection: report, Available: available}
}
