package ebusgateway

// The semantic Prometheus target deliberately consumes only detached SemReg
// output. It is an observation surface, never a semantic authority.

import (
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const semanticMetricsSeriesBudget = 192

// SemanticMetricsDomain is one coherent detached snapshot/evaluation/
// projection tuple. Name is a fixed vocabulary value: pv or storage.
type SemanticMetricsDomain struct {
	Name       string
	Snapshot   semreg.Snapshot
	Evaluation semreg.EvaluationView
	Projection projection.ProjectionReport
	Available  bool
}

func writeSemanticMetrics(w *prometheusWriter, domains []SemanticMetricsDomain, now time.Time) {
	w.writeHelp("helianthus_semantic_projection_available", "Detached SemReg projection availability for this Prometheus target.")
	w.writeType("helianthus_semantic_projection_available", "gauge")
	w.writeHelp("helianthus_semantic_projection_item", "Current requested SemReg projection outcome.")
	w.writeType("helianthus_semantic_projection_item", "gauge")
	w.writeHelp("helianthus_semantic_fact_state", "Current SemReg fact quality, effective availability, and freshness state.")
	w.writeType("helianthus_semantic_fact_state", "gauge")
	w.writeHelp("helianthus_semantic_fact_value", "Current fresh qualified numeric or boolean SemReg fact value.")
	w.writeType("helianthus_semantic_fact_value", "gauge")
	w.writeHelp("helianthus_semantic_evidence_age_seconds", "Age of selected SemReg evidence when wall clocks are comparable.")
	w.writeType("helianthus_semantic_evidence_age_seconds", "gauge")
	w.writeHelp("helianthus_semantic_open_conflicts", "Open conflicts on the detached SemReg fact envelope.")
	w.writeType("helianthus_semantic_open_conflicts", "gauge")
	w.writeHelp("helianthus_semantic_projection_loss", "Loss kinds declared by the detached SemReg projection.")
	w.writeType("helianthus_semantic_projection_loss", "gauge")
	w.writeHelp("helianthus_semantic_render_overflow", "Semantic metrics omitted because the fixed renderer series budget was reached or input was invalid.")
	w.writeType("helianthus_semantic_render_overflow", "gauge")

	used, overflow := 0, 0
	seen := make(map[string]struct{})
	emit := func(name string, value float64, labels map[string]string) bool {
		key := name
		keys := make([]string, 0, len(labels))
		for label := range labels {
			keys = append(keys, label)
		}
		sort.Strings(keys)
		for _, label := range keys {
			key += "\x00" + label + "=" + labels[label]
		}
		if _, ok := seen[key]; ok {
			overflow++
			return false
		}
		seen[key] = struct{}{}
		if used >= semanticMetricsSeriesBudget {
			overflow++
			return false
		}
		used++
		w.writeGaugeSample(name, value, labels)
		return true
	}
	validDomain := map[string]bool{"pv": true, "storage": true}
	for _, d := range domains {
		if !validDomain[d.Name] {
			overflow++
			continue
		}
		if !d.Available {
			emit("helianthus_semantic_projection_available", 0, labelMap("domain", d.Name))
			continue
		}
		if d.Snapshot.SnapshotID == "" || d.Evaluation.SnapshotID != d.Snapshot.SnapshotID || d.Projection.SnapshotID != d.Snapshot.SnapshotID || d.Evaluation.Revisions != d.Snapshot.Revisions || d.Projection.Revisions != d.Snapshot.Revisions || d.Evaluation.EvaluationDigest == "" {
			emit("helianthus_semantic_projection_available", 0, labelMap("domain", d.Name))
			overflow++
			continue
		}
		emit("helianthus_semantic_projection_available", 1, labelMap("domain", d.Name))
		facts := factIndex(d.Snapshot)
		evaluated := evaluatedIndex(d.Evaluation)
		dispositions := append([]projection.ProjectionDisposition(nil), d.Projection.Dispositions...)
		sort.Slice(dispositions, func(i, j int) bool { return dispositions[i].ItemID < dispositions[j].ItemID })
		for _, disposition := range dispositions {
			outcome := string(disposition.Outcome)
			if !validOutcome(outcome) || !validSemanticItem(d.Name, string(disposition.ItemID)) {
				overflow++
				continue
			}
			emit("helianthus_semantic_projection_item", 1, labelMap("domain", d.Name, "item", string(disposition.ItemID), "outcome", outcome))
			for _, loss := range disposition.Loss {
				if validLoss(string(loss.Kind)) {
					emit("helianthus_semantic_projection_loss", 1, labelMap("domain", d.Name, "item", string(disposition.ItemID), "loss_kind", string(loss.Kind)))
				} else {
					overflow++
				}
			}
			if disposition.Outcome != projection.ProjectionExact && disposition.Outcome != projection.ProjectionTransformed {
				continue
			}
			for _, key := range disposition.SourceKeys {
				renderSemanticFact(emit, d.Name, key, facts, evaluated, now)
			}
		}
	}
	if overflow != 0 {
		w.writeGaugeSample("helianthus_semantic_render_overflow", float64(overflow), nil)
	}
}

func factIndex(snapshot semreg.Snapshot) map[string]semreg.FactEnvelope {
	out := make(map[string]semreg.FactEnvelope, len(snapshot.Facts))
	for _, f := range snapshot.Facts {
		for _, c := range f.Candidates {
			out[string(c.CandidateID)] = f
		}
	}
	return out
}
func evaluatedIndex(view semreg.EvaluationView) map[semreg.CandidateID]semreg.EvaluatedFact {
	out := make(map[semreg.CandidateID]semreg.EvaluatedFact, len(view.Facts))
	for _, f := range view.Facts {
		out[f.CandidateID] = f
	}
	return out
}

func renderSemanticFact(emit func(string, float64, map[string]string) bool, domain string, key semreg.FactKey, envelopes map[string]semreg.FactEnvelope, evaluated map[semreg.CandidateID]semreg.EvaluatedFact, now time.Time) {
	if key.Validate() != nil || !validSemanticFact(domain, string(key.PackID), string(key.FactID)) {
		return
	}
	for _, envelope := range envelopes {
		for _, candidate := range envelope.Candidates {
			if semanticFactKey(candidate.Key) != semanticFactKey(key) {
				continue
			}
			e, ok := evaluated[candidate.CandidateID]
			if !ok {
				return
			}
			if !validSemanticState(candidate.Quality.Qualification, candidate.Quality.Promotion, candidate.Quality.Validity, e.EffectiveAvailability, e.Freshness) {
				return
			}
			dimension, dimensionsOK := semanticDimensions(key)
			labels := labelMap("domain", domain, "pack", string(key.PackID), "fact_id", string(key.FactID), "dimension", dimension, "qualification", string(candidate.Quality.Qualification), "promotion", string(candidate.Quality.Promotion), "quality", string(candidate.Quality.Validity), "availability", string(e.EffectiveAvailability), "freshness", string(e.Freshness))
			emit("helianthus_semantic_fact_state", 1, labels)
			emit("helianthus_semantic_open_conflicts", float64(len(envelope.Conflicts)), labelMap("domain", domain, "pack", string(key.PackID), "fact_id", string(key.FactID), "dimension", dimension))
			if age, ok := semanticAge(candidate.Times.ReceivedAt, now); ok {
				emit("helianthus_semantic_evidence_age_seconds", age, labelMap("domain", domain, "pack", string(key.PackID), "fact_id", string(key.FactID), "dimension", dimension))
			}
			if !dimensionsOK || len(envelope.Candidates) != 1 || len(envelope.Conflicts) != 0 || candidate.Quality.Qualification != semreg.QualificationQualified || candidate.Quality.Promotion != semreg.PromotionPromoted || candidate.Quality.Validity != semreg.ValidityGood || e.EffectiveAvailability != semreg.AvailabilityAvailable || e.Freshness != semreg.FreshnessFresh {
				return
			}
			value, unit, ok := semanticNumeric(candidate.Value)
			if ok {
				emit("helianthus_semantic_fact_value", value, labelMap("domain", domain, "pack", string(key.PackID), "fact_id", string(key.FactID), "dimension", dimension, "unit", unit))
			}
			return
		}
	}
}

func validSemanticState(q semreg.Qualification, p semreg.Promotion, v semreg.Validity, a semreg.Availability, f semreg.Freshness) bool {
	return (q == semreg.QualificationCandidate || q == semreg.QualificationQualified || q == semreg.QualificationUnsupported || q == semreg.QualificationUnknown || q == semreg.QualificationRejected) && (p == semreg.PromotionPromoted || p == semreg.PromotionUnpromoted) && (v == semreg.ValidityGood || v == semreg.ValiditySuspect || v == semreg.ValidityBad || v == semreg.ValidityUnknown) && (a == semreg.AvailabilityAvailable || a == semreg.AvailabilityDegraded || a == semreg.AvailabilityUnavailable || a == semreg.AvailabilityWithdrawn) && (f == semreg.FreshnessFresh || f == semreg.FreshnessStale || f == semreg.FreshnessExpired || f == semreg.FreshnessUnknown)
}

func semanticDimensions(key semreg.FactKey) (string, bool) {
	if len(key.Dimensions) != 1 {
		return "", len(key.Dimensions) == 0
	}
	dimension := key.Dimensions[0]
	if dimension.ID == "storage.dimension.pack" {
		return "pack", true
	} // never export its asset-valued dimension.
	if dimension.ID != "pv.dimension.phase" && dimension.ID != "pv.dimension.inverter" && dimension.ID != "pv.dimension.system" {
		return "", false
	}
	if dimension.Value.Kind != semreg.ValueText || dimension.Value.Text == nil {
		return "", false
	}
	value := *dimension.Value.Text
	switch {
	case value == "phase:L1":
		return "phase_l1", true
	case value == "phase:L2":
		return "phase_l2", true
	case value == "phase:L3":
		return "phase_l3", true
	case value == "inverter" || strings.HasPrefix(value, "inverter:"):
		return "inverter", true
	case value == "system" || strings.HasPrefix(value, "system:"):
		return "system", true
	}
	return "", false
}

func semanticFactKey(key semreg.FactKey) string {
	if key.Validate() != nil {
		return ""
	}
	encoded, err := semreg.CanonicalJSON(key)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func validSemanticFact(domain, pack, fact string) bool {
	if domain == "pv" && pack == "helianthus.pack.pv" {
		switch fact {
		case "pv.ac.current", "pv.ac.voltage", "pv.ac.aggregate_active_power", "pv.ac.frequency", "pv.energy.generated", "pv.temperature.inverter", "pv.status.operating":
			return true
		}
	}
	if domain == "storage" && pack == "helianthus.pack.storage" {
		switch fact {
		case "storage.capacity.charge", "storage.capacity.discharge", "storage.pack.current", "storage.pack.voltage", "storage.state.soc", "storage.temperature.pack", "storage.status.operating":
			return true
		}
	}
	return false
}
func validSemanticItem(domain, item string) bool {
	if domain == "pv" {
		return strings.HasPrefix(item, "projection.gateway.pv.inverter.")
	}
	if domain == "storage" {
		switch item {
		case "storage.capacity.charge", "storage.capacity.discharge", "storage.pack.current", "storage.pack.voltage", "storage.state.soc", "storage.temperature.pack", "storage.status.operating":
			return true
		}
	}
	return false
}

func semanticNumeric(value *semreg.Value) (float64, string, bool) {
	if value == nil {
		return 0, "", false
	}
	if value.Kind == semreg.ValueBoolean && value.Boolean != nil {
		if *value.Boolean {
			return 1, "boolean", true
		}
		return 0, "boolean", true
	}
	if value.Kind != semreg.ValueQuantity || value.Quantity == nil {
		return 0, "", false
	}
	if !validSemanticUnit(string(value.Quantity.Unit)) {
		return 0, "", false
	}
	r, ok := new(big.Rat).SetString(string(value.Quantity.Number.Coefficient))
	if !ok {
		return 0, "", false
	}
	if value.Quantity.Number.Exponent10 >= 0 {
		r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(value.Quantity.Number.Exponent10)), nil)))
	} else {
		r.Quo(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-value.Quantity.Number.Exponent10)), nil)))
	}
	f, _ := r.Float64()
	return f, string(value.Quantity.Unit), true
}

func validSemanticUnit(unit string) bool {
	switch unit {
	case "unit.watt", "unit.ampere", "unit.volt", "unit.hertz", "unit.kilowatt_hour", "unit.ampere_hour", "unit.percent", "unit.celsius":
		return true
	}
	return false
}
func semanticAge(point semreg.TimePoint, now time.Time) (float64, bool) {
	if point.ClockID != "clock.utc" && point.ClockID != "wall.utc" {
		return 0, false
	}
	n, err := strconv.ParseInt(string(point.UnixNanoseconds), 10, 64)
	if err != nil {
		return 0, false
	}
	age := now.UTC().Sub(time.Unix(0, n)).Seconds()
	if age < 0 {
		return 0, false
	}
	return age, true
}
func validOutcome(v string) bool {
	switch v {
	case "exact", "transformed", "withheld", "unrepresentable", "unsupported", "unknown":
		return true
	}
	return false
}
func validLoss(v string) bool {
	switch v {
	case "unit", "range", "precision", "time", "symbol", "provenance", "identity", "capability", "operation", "policy":
		return true
	}
	return false
}
