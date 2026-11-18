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
	if !strings.Contains(metrics, `helianthus_semantic_evidence_age{dimension="inverter",domain="pv",fact_id="pv.ac.aggregate_active_power",pack="helianthus.pack.pv",unit="seconds"} 1`) {
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

func TestSemanticPrometheusEVSEConnectorSlotsIgnoreUnrenderableCandidates(t *testing.T) {
	domain := semanticMetricsFixture("evse", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
	for index := 0; index < semanticEVSEConnectorBudget; index++ {
		candidateID := semreg.CandidateID("candidate:rejected-" + strconv.Itoa(index))
		addEVSEAllocatedFixture(&domain, "connector:blocked-"+strconv.Itoa(index), candidateID)
		for factIndex := range domain.Snapshot.Facts {
			if len(domain.Snapshot.Facts[factIndex].Candidates) == 1 && domain.Snapshot.Facts[factIndex].Candidates[0].CandidateID == candidateID {
				domain.Snapshot.Facts[factIndex].Candidates[0].Quality.Qualification = semreg.QualificationRejected
			}
		}
	}
	addEVSEAllocatedFixture(&domain, "connector:promoted", "candidate:promoted")
	var out bytes.Buffer
	writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
	metrics := out.String()
	if !strings.Contains(metrics, `helianthus_semantic_fact_value{connector="connector_1",dimension="connector",domain="evse",fact_id="evse.limit.allocated_current",pack="helianthus.pack.evse",unit="unit.ampere"} 16`) {
		t.Fatalf("renderable connector was displaced by rejected candidates:\n%s", metrics)
	}
	if strings.Contains(metrics, "blocked-") || strings.Contains(metrics, "connector:promoted") {
		t.Fatalf("connector identity leaked through labels:\n%s", metrics)
	}
}

func semanticMetricsFixture(domain string, available bool, freshness semreg.Freshness, availability semreg.Availability, validity semreg.Validity, conflicts int) SemanticMetricsDomain {
	dimensionID, dimensionValue, factID := semreg.DefinitionID("pv.dimension.inverter"), "resource:asset:private:inverter", semreg.DefinitionID("pv.ac.aggregate_active_power")
	unit := semreg.DefinitionID("unit.watt")
	if domain == "storage" {
		dimensionID, dimensionValue, factID, unit = "storage.dimension.pack", "resource:asset:private:pack", "storage.pack.voltage", "unit.volt"
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
	if domain == "pv" {
		// The PV fixture is graphed: its dimension value is the identifier of
		// the fact's subject resource.
		snapshot.Resources = []semreg.Resource{semanticResourceFixture(semreg.ResourceID(dimensionValue), "resource.inverter", "")}
	}
	if domain == "storage" {
		// So is the storage fixture: its pack value names a battery pack.
		snapshot.Resources = []semreg.Resource{semanticResourceFixture(semreg.ResourceID(dimensionValue), "resource.battery_pack", "")}
	}
	evaluation := semreg.EvaluationView{SnapshotID: snapshot.SnapshotID, Facts: []semreg.EvaluatedFact{{CandidateID: candidate.CandidateID, Freshness: freshness, EffectiveAvailability: availability}}, EvaluationDigest: semreg.Digest("sha256:" + strings.Repeat("0", 64))}
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

// semanticResourceFixture is one current resource; a phase carries its slot.
func semanticResourceFixture(id semreg.ResourceID, kind semreg.DefinitionID, slot string) semreg.Resource {
	resource := semreg.Resource{ResourceID: id, Kind: kind, Anchor: semreg.ResourceAnchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot}, State: semreg.ResourceCurrent, Revision: "1"}
	if slot != "" {
		resource.Anchor.Class, resource.Anchor.Basis, resource.Anchor.Slot = semreg.AnchorComponent, semreg.AnchorNativeSlot, &slot
	}
	return resource
}

// semanticPVFixtureAs rekeys the PV fixture's one fact as fact, dimension and
// value under item.
func semanticPVFixtureAs(domain *SemanticMetricsDomain, item, fact, dimension, value string, unit semreg.DefinitionID) {
	key := domain.Snapshot.Facts[0].Key
	key.FactID, key.Dimensions = semreg.DefinitionID(fact), []semreg.Dimension{{ID: semreg.DefinitionID(dimension), Value: semreg.Value{Kind: semreg.ValueText, Text: &value}}}
	domain.Snapshot.Facts[0].Key = key
	domain.Snapshot.Facts[0].Candidates[0].Key = key
	domain.Snapshot.Facts[0].Candidates[0].Value.Quantity.Unit = unit
	domain.Projection.Dispositions[0].ItemID = semreg.DefinitionID(item)
	domain.Projection.Dispositions[0].SourceKeys[0] = key
}

// TestSemanticPrometheusPVLabelsFollowResourceKindAndSlot: in a graphed
// snapshot the dimension label is derived from the subject resource's kind
// and, for a phase, its slot, and the identifier never leaves the renderer. A
// value naming no current resource, or one of another kind, renders no fact,
// and role text is not read in a graphed snapshot. A snapshot without
// resources keeps today's reading of its publisher's role text.
func TestSemanticPrometheusPVLabelsFollowResourceKindAndSlot(t *testing.T) {
	type rendered struct {
		label string
		ok    bool
	}
	for _, tc := range []struct {
		name, item, fact, dimension, value string
		unit                               semreg.DefinitionID
		resources                          []semreg.Resource
		want                               rendered
	}{
		{"inverter", "projection.gateway.pv.inverter.ac.power.active", "pv.ac.aggregate_active_power", "pv.dimension.inverter", "resource:test-inverter", "unit.watt", []semreg.Resource{semanticResourceFixture("resource:test-inverter", "resource.inverter", "")}, rendered{"inverter", true}},
		{"system", "projection.gateway.pv.inverter.ac.energy_lifetime", "pv.energy.generated", "pv.dimension.system", "resource:test-system", "unit.kilowatt_hour", []semreg.Resource{semanticResourceFixture("resource:test-system", "resource.pv_system", "")}, rendered{"system", true}},
		{"phase slot", "projection.gateway.pv.inverter.ac.current.phase_b", "pv.ac.current", "pv.dimension.phase", "resource:test-phase", "unit.ampere", []semreg.Resource{semanticResourceFixture("resource:test-phase", "resource.phase", "L2")}, rendered{"phase_l2", true}},
		{"phase of another slot", "projection.gateway.pv.inverter.ac.current.phase_b", "pv.ac.current", "pv.dimension.phase", "resource:test-phase", "unit.ampere", []semreg.Resource{semanticResourceFixture("resource:test-phase", "resource.phase", "L3")}, rendered{}},
		{"names no resource", "projection.gateway.pv.inverter.ac.power.active", "pv.ac.aggregate_active_power", "pv.dimension.inverter", "resource:test-missing", "unit.watt", []semreg.Resource{semanticResourceFixture("resource:test-inverter", "resource.inverter", "")}, rendered{}},
		{"resource of another kind", "projection.gateway.pv.inverter.ac.power.active", "pv.ac.aggregate_active_power", "pv.dimension.inverter", "resource:test-input", "unit.watt", []semreg.Resource{semanticResourceFixture("resource:test-input", "resource.dc_input", "1")}, rendered{}},
		{"ungraphed literal text", "projection.gateway.pv.inverter.ac.power.active", "pv.ac.aggregate_active_power", "pv.dimension.inverter", "inverter", "unit.watt", nil, rendered{"inverter", true}},
		{"ungraphed role text", "projection.gateway.pv.inverter.ac.power.active", "pv.ac.aggregate_active_power", "pv.dimension.inverter", "inverter:asset:private", "unit.watt", nil, rendered{"inverter", true}},
		{"ungraphed role of another dimension", "projection.gateway.pv.inverter.ac.power.active", "pv.ac.aggregate_active_power", "pv.dimension.inverter", "system:asset:private", "unit.watt", nil, rendered{}},
		{"graphed role text", "projection.gateway.pv.inverter.ac.power.active", "pv.ac.aggregate_active_power", "pv.dimension.inverter", "inverter:asset:private", "unit.watt", []semreg.Resource{semanticResourceFixture("resource:test-inverter", "resource.inverter", "")}, rendered{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			domain := semanticMetricsFixture("pv", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
			semanticPVFixtureAs(&domain, tc.item, tc.fact, tc.dimension, tc.value, tc.unit)
			domain.Snapshot.Resources = tc.resources
			var out bytes.Buffer
			writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
			metrics := out.String()
			if strings.Contains(metrics, "resource:test") || strings.Contains(metrics, "asset:private") {
				t.Fatalf("an identifier leaked into a label:\n%s", metrics)
			}
			want := `helianthus_semantic_fact_value{dimension="` + tc.want.label + `",domain="pv",fact_id="` + tc.fact + `",pack="helianthus.pack.pv",unit="` + string(tc.unit) + `"} 42`
			if tc.want.ok != strings.Contains(metrics, want) {
				t.Fatalf("rendered=%v, want %v (%s):\n%s", !tc.want.ok, tc.want.ok, want, metrics)
			}
			if !tc.want.ok && (strings.Contains(metrics, "\nhelianthus_semantic_fact_value{") || !strings.Contains(metrics, "helianthus_semantic_render_overflow 1")) {
				t.Fatalf("an unresolved subject was rendered or not counted:\n%s", metrics)
			}
		})
	}
	t.Run("withdrawn resource", func(t *testing.T) {
		domain := semanticMetricsFixture("pv", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
		domain.Snapshot.Resources[0].State = semreg.ResourceWithdrawn
		var out bytes.Buffer
		writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
		if strings.Contains(out.String(), "helianthus_semantic_fact_value{") {
			t.Fatalf("a fact about a withdrawn resource was rendered:\n%s", out.String())
		}
	})
}

// TestSemanticPrometheusStorageLabelFollowsResourceKind: the storage pack
// label is taken from the subject resource's kind, never from the pack value,
// which is an identifier. A pack value naming a current battery pack renders
// the label "pack"; one naming no resource, a resource of another kind or a
// withdrawn pack renders no fact, and so does a pack value in a snapshot
// without resources. No identifier reaches a label.
func TestSemanticPrometheusStorageLabelFollowsResourceKind(t *testing.T) {
	const value = `helianthus_semantic_fact_value{dimension="pack",domain="storage",fact_id="storage.pack.voltage",pack="helianthus.pack.storage",unit="unit.volt"} 42`
	pack := semanticResourceFixture("resource:asset:private:pack", "resource.battery_pack", "")
	withdrawn := pack
	withdrawn.State = semreg.ResourceWithdrawn
	for _, tc := range []struct {
		name      string
		resources []semreg.Resource
		rendered  bool
	}{
		{"battery pack", []semreg.Resource{pack}, true},
		{"names no resource", []semreg.Resource{semanticResourceFixture("resource:test-other", "resource.battery_pack", "")}, false},
		{"resource of another kind", []semreg.Resource{semanticResourceFixture("resource:asset:private:pack", "resource.inverter", "")}, false},
		{"withdrawn pack", []semreg.Resource{withdrawn}, false},
		{"asset-valued text without resources", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			domain := semanticMetricsFixture("storage", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
			domain.Snapshot.Resources = tc.resources
			var out bytes.Buffer
			writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
			metrics := out.String()
			if strings.Contains(metrics, "asset:private") || strings.Contains(metrics, "resource:") {
				t.Fatalf("an identifier leaked into a label:\n%s", metrics)
			}
			if tc.rendered != strings.Contains(metrics, value) {
				t.Fatalf("rendered=%v, want %v:\n%s", !tc.rendered, tc.rendered, metrics)
			}
			if !tc.rendered && (strings.Contains(metrics, "\nhelianthus_semantic_fact_value{") || !strings.Contains(metrics, "helianthus_semantic_render_overflow 1")) {
				t.Fatalf("an unresolved pack was rendered or not counted:\n%s", metrics)
			}
		})
	}
}

func addEVSEAllocatedFixture(domain *SemanticMetricsDomain, connector string, candidateID semreg.CandidateID) {
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "16"}, Unit: "unit.ampere"}}
	key := semreg.FactKey{PackID: "helianthus.pack.evse", PackVersion: "1.0.0", FactID: "evse.limit.allocated_current", Dimensions: []semreg.Dimension{{ID: "evse.dimension.connector", Value: semreg.Value{Kind: semreg.ValueText, Text: &connector}}}}
	candidate := semreg.FactCandidate{CandidateID: candidateID, Key: key, Value: &value, Quality: semreg.Quality{Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood}, Times: semreg.Times{ReceivedAt: semreg.TimePoint{UnixNanoseconds: "100000000000", ClockID: "clock.utc"}}}
	domain.Snapshot.Facts = append(domain.Snapshot.Facts, semreg.FactEnvelope{Key: key, Candidates: []semreg.FactCandidate{candidate}})
	domain.Evaluation.Facts = append(domain.Evaluation.Facts, semreg.EvaluatedFact{CandidateID: candidateID, Freshness: semreg.FreshnessFresh, EffectiveAvailability: semreg.AvailabilityAvailable})
	domain.Projection.Dispositions = append(domain.Projection.Dispositions, projection.ProjectionDisposition{ItemID: "evse.limit.allocated_current", Outcome: projection.ProjectionExact, SourceKeys: []semreg.FactKey{key}})
}

// addSemanticSourceCandidate attributes the fixture's candidate to first and
// adds a second candidate of the same key observed by second. It sets the
// envelope's conflict when the coefficients differ.
func addSemanticSourceCandidate(domain *SemanticMetricsDomain, first, second semreg.SourceID, coefficient string, receivedSeconds int, freshness semreg.Freshness) {
	envelope := &domain.Snapshot.Facts[0]
	original := &envelope.Candidates[0]
	original.Origin = semreg.OriginRef{OriginID: "origin:first", Kind: semreg.OriginNativeObservation, SourceID: &first}
	candidate := *original
	candidate.CandidateID = "candidate:test-second"
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: coefficient}, Unit: original.Value.Quantity.Unit}}
	candidate.Value = &value
	candidate.Origin = semreg.OriginRef{OriginID: "origin:second", Kind: semreg.OriginNativeObservation, SourceID: &second}
	candidate.Times.ReceivedAt = semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.Itoa(receivedSeconds) + "000000000"), ClockID: "clock.utc"}
	envelope.Candidates = append(envelope.Candidates, candidate)
	if coefficient != original.Value.Quantity.Number.Coefficient {
		envelope.Conflicts = append(envelope.Conflicts, semreg.Conflict{ConflictID: "conflict:test", Kind: semreg.ConflictValue, Candidates: []semreg.CandidateID{original.CandidateID, candidate.CandidateID}, State: semreg.ConflictOpen})
	}
	domain.Evaluation.Facts = append(domain.Evaluation.Facts, semreg.EvaluatedFact{CandidateID: candidate.CandidateID, Freshness: freshness, EffectiveAvailability: semreg.AvailabilityAvailable})
}

// TestSemanticPrometheusTwoCandidatesExportSelected: a key with candidates of
// two sources exports the candidate the presentation policy selects instead of
// dropping the fact; a conflict still withholds the value.
func TestSemanticPrometheusTwoCandidatesExportSelected(t *testing.T) {
	const value = `helianthus_semantic_fact_value{dimension="inverter",domain="pv",fact_id="pv.ac.aggregate_active_power",pack="helianthus.pack.pv",unit="unit.watt"} `
	const age = `helianthus_semantic_evidence_age{dimension="inverter",domain="pv",fact_id="pv.ac.aggregate_active_power",pack="helianthus.pack.pv",unit="seconds"} `
	const conflicts = `helianthus_semantic_open_conflicts{dimension="inverter",domain="pv",fact_id="pv.ac.aggregate_active_power",pack="helianthus.pack.pv"} `
	for _, tc := range []struct {
		name        string
		owner       semreg.SourceID
		coefficient string
		freshness   semreg.Freshness
		value       string // empty: no value is exported
		age         string // empty: no evidence age is exported
		conflicts   string // empty: no conflict gauge is exported
	}{
		// The second candidate is stale, so the first is the single qualified one.
		{name: "single qualified", coefficient: "42", freshness: semreg.FreshnessStale, value: "42", age: "1", conflicts: "0"},
		// Two qualified candidates and no owner: nothing is selected.
		{name: "two qualified without owner", coefficient: "42", freshness: semreg.FreshnessFresh, conflicts: "0"},
		// The owner's candidate is selected, whichever identifier sorts first:
		// the evidence age tells the two candidates apart.
		{name: "owner first", owner: "source:first", coefficient: "42", freshness: semreg.FreshnessFresh, value: "42", age: "1", conflicts: "0"},
		{name: "owner second", owner: "source:second", coefficient: "42", freshness: semreg.FreshnessFresh, value: "42", age: "10", conflicts: "0"},
		// An owner that observed neither candidate selects nothing.
		{name: "owner absent", owner: "source:third", coefficient: "42", freshness: semreg.FreshnessFresh, conflicts: "0"},
		// The owner's candidate is stale: the other one is the single qualified.
		{name: "owner stale", owner: "source:second", coefficient: "42", freshness: semreg.FreshnessStale, value: "42", age: "1", conflicts: "0"},
		// A conflict keeps the owner's state and age but never a value.
		{name: "owner under conflict", owner: "source:second", coefficient: "43", freshness: semreg.FreshnessFresh, age: "10", conflicts: "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for run := 0; run < 16; run++ {
				domain := semanticMetricsFixture("pv", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
				addSemanticSourceCandidate(&domain, "source:first", "source:second", tc.coefficient, 91, tc.freshness)
				domain.Owner = tc.owner
				var out bytes.Buffer
				writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
				metrics := out.String()
				for _, expect := range []struct{ prefix, want string }{{value, tc.value}, {age, tc.age}, {conflicts, tc.conflicts}} {
					if expect.want == "" {
						if strings.Contains(metrics, expect.prefix) {
							t.Fatalf("unexpected %q:\n%s", expect.prefix, metrics)
						}
						continue
					}
					if !strings.Contains(metrics, expect.prefix+expect.want+"\n") {
						t.Fatalf("missing %q%s:\n%s", expect.prefix, expect.want, metrics)
					}
				}
				if strings.Contains(metrics, "\nhelianthus_semantic_render_overflow ") {
					t.Fatalf("two candidates overflowed:\n%s", metrics)
				}
			}
		})
	}
}

// TestSemanticPrometheusEVSEConnectorSlotTwoCandidates: a connector whose
// allocated current has candidates of two sources receives a slot and exports
// the owner's candidate; without an owner neither candidate is selected, so
// the connector receives no slot and nothing is exported.
func TestSemanticPrometheusEVSEConnectorSlotTwoCandidates(t *testing.T) {
	const value = `helianthus_semantic_fact_value{connector="connector_1",dimension="connector",domain="evse",fact_id="evse.limit.allocated_current",pack="helianthus.pack.evse",unit="unit.ampere"} 16`
	for _, tc := range []struct {
		owner  semreg.SourceID
		export bool
	}{{"source:owner", true}, {"", false}} {
		domain := semanticMetricsFixture("evse", true, semreg.FreshnessFresh, semreg.AvailabilityAvailable, semreg.ValidityGood, 0)
		addEVSEAllocatedFixture(&domain, "connector:shared", "candidate:allocated")
		envelope := &domain.Snapshot.Facts[len(domain.Snapshot.Facts)-1]
		owner, contributor := semreg.SourceID("source:owner"), semreg.SourceID("source:contributor")
		envelope.Candidates[0].Origin = semreg.OriginRef{OriginID: "origin:owner", Kind: semreg.OriginNativeObservation, SourceID: &owner}
		second := envelope.Candidates[0]
		second.CandidateID = "candidate:allocated-contributor"
		second.Origin = semreg.OriginRef{OriginID: "origin:contributor", Kind: semreg.OriginNativeObservation, SourceID: &contributor}
		envelope.Candidates = append(envelope.Candidates, second)
		domain.Evaluation.Facts = append(domain.Evaluation.Facts, semreg.EvaluatedFact{CandidateID: second.CandidateID, Freshness: semreg.FreshnessFresh, EffectiveAvailability: semreg.AvailabilityAvailable})
		domain.Owner = tc.owner
		var out bytes.Buffer
		writeSemanticMetrics(newPrometheusWriter(&out), []SemanticMetricsDomain{domain}, time.Unix(101, 0))
		metrics := out.String()
		if strings.Contains(metrics, value) != tc.export {
			t.Fatalf("owner %q: connector value exported %v want %v:\n%s", tc.owner, !tc.export, tc.export, metrics)
		}
		if strings.Contains(metrics, "connector:shared") {
			t.Fatalf("connector identity leaked through labels:\n%s", metrics)
		}
	}
}
