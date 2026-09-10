package ebusgateway

import (
	"bytes"
	"strconv"
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
	if !strings.Contains(metrics, `helianthus_semantic_fact_value{dimension="inverter",domain="pv",fact_id="pv.ac.aggregate_active_power",pack="helianthus.pack.pv",unit="unit.watt"} 42`) {
		t.Fatalf("missing accepted value:\n%s", metrics)
	}
	if strings.Contains(metrics, "asset:private") {
		t.Fatalf("forbidden asset identifier leaked:\n%s", metrics)
	}
	if !strings.Contains(metrics, `helianthus_semantic_evidence_age_seconds{dimension="inverter",domain="pv",fact_id="pv.ac.aggregate_active_power",pack="helianthus.pack.pv"} 1`) {
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

func TestSemanticPrometheusRejectsDimensionlessFacts(t *testing.T) {
	for _, domainName := range []string{"pv", "storage"} {
		t.Run(domainName, func(t *testing.T) {
			domain := semanticMetricsFixture(domainName, true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
			key := domain.Snapshot.Facts[0].Key
			key.Dimensions = nil
			domain.Snapshot.Facts[0].Key = key
			domain.Snapshot.Facts[0].Candidates[0].Key = key
			domain.Projection.Dispositions[0].SourceKeys[0] = key
			var out bytes.Buffer
			writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
			metrics := out.String()
			if strings.Contains(metrics, `dimension=""`) || strings.Contains(metrics, "\nhelianthus_semantic_fact_state{") || strings.Contains(metrics, "\nhelianthus_semantic_fact_value{") {
				t.Fatalf("dimensionless fact leaked:\n%s", metrics)
			}
			if !strings.Contains(metrics, "helianthus_semantic_render_overflow 1") {
				t.Fatalf("dimensionless fact was not counted:\n%s", metrics)
			}
		})
	}
}

func TestSemanticPrometheusRejectsCrossSchemaValues(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*SemanticMetricsDomain)
	}{
		{"boolean-power", func(domain *SemanticMetricsDomain) {
			value := true
			domain.Snapshot.Facts[0].Candidates[0].Value = &semreg.Value{Kind: semreg.ValueBoolean, Boolean: &value}
		}},
		{"wrong-unit", func(domain *SemanticMetricsDomain) {
			domain.Snapshot.Facts[0].Candidates[0].Value.Quantity.Unit = "unit.volt"
		}},
		{"future-pack", func(domain *SemanticMetricsDomain) {
			key := domain.Snapshot.Facts[0].Key
			key.PackVersion = "2.0.0"
			domain.Snapshot.Facts[0].Key = key
			domain.Snapshot.Facts[0].Candidates[0].Key = key
			domain.Projection.Dispositions[0].SourceKeys[0] = key
		}},
		{"fact-item-mismatch", func(domain *SemanticMetricsDomain) {
			key := domain.Snapshot.Facts[0].Key
			key.FactID = "pv.ac.frequency"
			domain.Snapshot.Facts[0].Key = key
			domain.Snapshot.Facts[0].Candidates[0].Key = key
			domain.Projection.Dispositions[0].SourceKeys[0] = key
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			domain := semanticMetricsFixture("pv", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
			tc.mutate(&domain)
			var out bytes.Buffer
			writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
			metrics := out.String()
			if strings.Contains(metrics, "\nhelianthus_semantic_fact_state{") || strings.Contains(metrics, "\nhelianthus_semantic_fact_value{") || strings.Contains(metrics, "unit=\"boolean\"") {
				t.Fatalf("cross-schema input leaked:\n%s", metrics)
			}
			if !strings.Contains(metrics, "helianthus_semantic_render_overflow 1") {
				t.Fatalf("cross-schema input was not counted:\n%s", metrics)
			}
		})
	}
}

func TestSemanticPrometheusEVSEExportsBoundedConfiguredAndConnectorFacts(t *testing.T) {
	domain := semanticMetricsFixture("evse", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
	addEVSEAllocatedFixture(&domain, "connector:private-a", "candidate:allocated-a")
	addEVSEAllocatedFixture(&domain, "connector:private-b", "candidate:allocated-b")
	var out bytes.Buffer
	writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
	metrics := out.String()
	for _, want := range []string{
		`fact_id="evse.limit.configured_current"`,
		`connector="connector_1"`,
		`connector="connector_2"`,
		`fact_id="evse.limit.allocated_current"`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("missing EVSE metric %q:\n%s", want, metrics)
		}
	}
	if strings.Contains(metrics, "private-a") || strings.Contains(metrics, "private-b") || strings.Contains(metrics, "asset:private") {
		t.Fatalf("EVSE identity leaked through labels:\n%s", metrics)
	}
}

func TestSemanticPrometheusEVSEWithheldAllocationDoesNotSuppressConfiguredCurrent(t *testing.T) {
	domain := semanticMetricsFixture("evse", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
	reason := semreg.DefinitionID("withheld_provisional_missing")
	domain.Projection.Dispositions = append(domain.Projection.Dispositions, projection.ProjectionDisposition{ItemID: "evse.limit.allocated_current", Outcome: projection.ProjectionWithheld, Reason: &reason, Loss: []projection.LossDetail{{Kind: projection.LossPolicy}}})
	var out bytes.Buffer
	writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
	metrics := out.String()
	if !strings.Contains(metrics, `helianthus_semantic_fact_value{dimension="evse",domain="evse",fact_id="evse.limit.configured_current",pack="helianthus.pack.evse",unit="unit.ampere"} 42`) || !strings.Contains(metrics, `item="evse.limit.allocated_current",loss_kind="policy"`) {
		t.Fatalf("withheld allocation changed configured current:\n%s", metrics)
	}
}

func TestSemanticPrometheusEVSERejectsHostileSchemaAndConnectorOverflow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*SemanticMetricsDomain)
	}{
		{"future-pack", func(domain *SemanticMetricsDomain) {
			key := domain.Snapshot.Facts[0].Key
			key.PackVersion = "2.0.0"
			domain.Snapshot.Facts[0].Key, domain.Snapshot.Facts[0].Candidates[0].Key, domain.Projection.Dispositions[0].SourceKeys[0] = key, key, key
		}},
		{"wrong-dimension", func(domain *SemanticMetricsDomain) {
			key := domain.Snapshot.Facts[0].Key
			key.Dimensions[0].ID = "evse.dimension.connector"
			domain.Snapshot.Facts[0].Key, domain.Snapshot.Facts[0].Candidates[0].Key, domain.Projection.Dispositions[0].SourceKeys[0] = key, key, key
		}},
		{"wrong-unit", func(domain *SemanticMetricsDomain) {
			domain.Snapshot.Facts[0].Candidates[0].Value.Quantity.Unit = "unit.volt"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			domain := semanticMetricsFixture("evse", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
			tc.mutate(&domain)
			var out bytes.Buffer
			writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
			metrics := out.String()
			if strings.Contains(metrics, "\nhelianthus_semantic_fact_state{") || strings.Contains(metrics, "\nhelianthus_semantic_fact_value{") || !strings.Contains(metrics, "helianthus_semantic_render_overflow 1") {
				t.Fatalf("hostile EVSE input was rendered:\n%s", metrics)
			}
		})
	}
	domain := semanticMetricsFixture("evse", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
	for index := 0; index < semanticEVSEConnectorBudget+1; index++ {
		addEVSEAllocatedFixture(&domain, "connector:hostile-"+strconv.Itoa(index), semreg.CandidateID("candidate:allocated-"+strconv.Itoa(index)))
	}
	var out bytes.Buffer
	writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
	metrics := out.String()
	if strings.Contains(metrics, "hostile-") || !strings.Contains(metrics, "helianthus_semantic_render_overflow") {
		t.Fatalf("unbounded EVSE connector labels leaked:\n%s", metrics)
	}
}

func semanticMetricsFixture(domain string, available bool, freshness semreg.Freshness, availability semreg.Availability, validity semreg.Validity, conflicts int) SemanticMetricsDomain {
	dimensionID, dimensionValue, factID := semreg.DefinitionID("pv.dimension.inverter"), "inverter:asset:private", semreg.DefinitionID("pv.ac.aggregate_active_power")
	unit := semreg.DefinitionID("unit.watt")
	if domain == "storage" {
		dimensionID, dimensionValue, factID, unit = "storage.dimension.pack", "storage:asset:private", "storage.pack.voltage", "unit.volt"
	}
	if domain == "evse" {
		dimensionID, dimensionValue, factID, unit = "evse.dimension.evse", "evse:asset:private", "evse.limit.configured_current", "unit.ampere"
	}
	version := semreg.SemanticVersion("1.0.0")
	if domain == "storage" {
		version = "1.1.0"
	}
	key := semreg.FactKey{PackID: "helianthus.pack." + semreg.DefinitionID(domain), PackVersion: version, FactID: factID, Dimensions: []semreg.Dimension{{ID: dimensionID, Value: semreg.Value{Kind: semreg.ValueText, Text: &dimensionValue}}}}
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "42"}, Unit: unit}}
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
	if domain == "evse" {
		item = "evse.limit.configured_current"
	}
	report := projection.ProjectionReport{SnapshotID: snapshot.SnapshotID, Dispositions: []projection.ProjectionDisposition{{ItemID: item, Outcome: projection.ProjectionExact, SourceKeys: []semreg.FactKey{key}}}}
	return SemanticMetricsDomain{Name: domain, Snapshot: snapshot, Evaluation: evaluation, Projection: report, Available: available}
}

func addEVSEAllocatedFixture(domain *SemanticMetricsDomain, connector string, candidateID semreg.CandidateID) {
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "16"}, Unit: "unit.ampere"}}
	key := semreg.FactKey{PackID: "helianthus.pack.evse", PackVersion: "1.0.0", FactID: "evse.limit.allocated_current", Dimensions: []semreg.Dimension{{ID: "evse.dimension.connector", Value: semreg.Value{Kind: semreg.ValueText, Text: &connector}}}}
	candidate := semreg.FactCandidate{CandidateID: candidateID, Key: key, Value: &value, Quality: semreg.Quality{Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood}, Times: semreg.Times{ReceivedAt: semreg.TimePoint{UnixNanoseconds: "100000000000", ClockID: "clock.utc"}}}
	domain.Snapshot.Facts = append(domain.Snapshot.Facts, semreg.FactEnvelope{Key: key, Candidates: []semreg.FactCandidate{candidate}})
	domain.Evaluation.Facts = append(domain.Evaluation.Facts, semreg.EvaluatedFact{CandidateID: candidateID, Freshness: semreg.FreshnessFresh, EffectiveAvailability: semreg.AvailabilityAvailable})
	domain.Projection.Dispositions = append(domain.Projection.Dispositions, projection.ProjectionDisposition{ItemID: "evse.limit.allocated_current", Outcome: projection.ProjectionExact, SourceKeys: []semreg.FactKey{key}})
}
