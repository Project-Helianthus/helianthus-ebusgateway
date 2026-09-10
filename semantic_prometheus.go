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
	validDomain := map[string]bool{"pv": true, "storage": true, "evse": true}
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
		connectorSlots := semanticEVSEConnectorSlots(d.Name, d.Snapshot)
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
				if renderSemanticFact(emit, d.Name, string(disposition.ItemID), key, facts, evaluated, connectorSlots, now) {
					overflow++
				}
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

func renderSemanticFact(emit func(string, float64, map[string]string) bool, domain, item string, key semreg.FactKey, envelopes map[string]semreg.FactEnvelope, evaluated map[semreg.CandidateID]semreg.EvaluatedFact, connectorSlots map[string]string, now time.Time) bool {
	targetKey, targetOK := semanticFactKey(key)
	schema, schemaOK := semanticFactSchemaFor(domain, item)
	if !targetOK || !schemaOK {
		return true
	}
	invalidCandidate := false
	for _, envelope := range envelopes {
		for _, candidate := range envelope.Candidates {
			candidateKey, candidateOK := semanticFactKey(candidate.Key)
			if !candidateOK {
				invalidCandidate = true
				continue
			}
			if candidateKey != targetKey {
				continue
			}
			e, ok := evaluated[candidate.CandidateID]
			if !ok {
				return true
			}
			if !validSemanticState(candidate.Quality.Qualification, candidate.Quality.Promotion, candidate.Quality.Validity, e.EffectiveAvailability, e.Freshness) {
				return true
			}
			if !schema.matches(key, candidate.Value) {
				return true
			}
			dimension, connector, dimensionsOK := semanticDimensions(domain, item, key, connectorSlots)
			if !dimensionsOK {
				return true
			}
			labels := semanticFactLabels(domain, key, dimension, connector, "qualification", string(candidate.Quality.Qualification), "promotion", string(candidate.Quality.Promotion), "quality", string(candidate.Quality.Validity), "availability", string(e.EffectiveAvailability), "freshness", string(e.Freshness))
			emit("helianthus_semantic_fact_state", 1, labels)
			emit("helianthus_semantic_open_conflicts", float64(len(envelope.Conflicts)), semanticFactLabels(domain, key, dimension, connector))
			if age, ok := semanticAge(candidate.Times.ReceivedAt, now); ok {
				emit("helianthus_semantic_evidence_age_seconds", age, semanticFactLabels(domain, key, dimension, connector))
			}
			if len(envelope.Candidates) != 1 || len(envelope.Conflicts) != 0 || candidate.Quality.Qualification != semreg.QualificationQualified || candidate.Quality.Promotion != semreg.PromotionPromoted || candidate.Quality.Validity != semreg.ValidityGood || e.EffectiveAvailability != semreg.AvailabilityAvailable || e.Freshness != semreg.FreshnessFresh {
				return false
			}
			value, unit, ok := semanticNumeric(candidate.Value)
			if ok {
				emit("helianthus_semantic_fact_value", value, semanticFactLabels(domain, key, dimension, connector, "unit", unit))
			}
			return false
		}
	}
	return invalidCandidate
}

func validSemanticState(q semreg.Qualification, p semreg.Promotion, v semreg.Validity, a semreg.Availability, f semreg.Freshness) bool {
	return (q == semreg.QualificationCandidate || q == semreg.QualificationQualified || q == semreg.QualificationUnsupported || q == semreg.QualificationUnknown || q == semreg.QualificationRejected) && (p == semreg.PromotionPromoted || p == semreg.PromotionUnpromoted) && (v == semreg.ValidityGood || v == semreg.ValiditySuspect || v == semreg.ValidityBad || v == semreg.ValidityUnknown) && (a == semreg.AvailabilityAvailable || a == semreg.AvailabilityDegraded || a == semreg.AvailabilityUnavailable || a == semreg.AvailabilityWithdrawn) && (f == semreg.FreshnessFresh || f == semreg.FreshnessStale || f == semreg.FreshnessExpired || f == semreg.FreshnessUnknown)
}

func semanticFactLabels(domain string, key semreg.FactKey, dimension, connector string, extra ...string) map[string]string {
	labels := labelMap("domain", domain, "pack", string(key.PackID), "fact_id", string(key.FactID), "dimension", dimension)
	if connector != "" {
		labels["connector"] = connector
	}
	for index := 0; index+1 < len(extra); index += 2 {
		labels[extra[index]] = extra[index+1]
	}
	return labels
}

func semanticDimensions(domain, item string, key semreg.FactKey, connectorSlots map[string]string) (string, string, bool) {
	if len(key.Dimensions) != 1 {
		return "", "", false
	}
	dimension := key.Dimensions[0]
	if domain == "evse" {
		if dimension.Value.Kind != semreg.ValueText || dimension.Value.Text == nil || *dimension.Value.Text == "" {
			return "", "", false
		}
		switch item {
		case "evse.limit.configured_current":
			return "evse", "", dimension.ID == "evse.dimension.evse"
		case "evse.limit.allocated_current":
			key, ok := semanticFactKey(key)
			if !ok {
				return "", "", false
			}
			slot, ok := connectorSlots[key]
			return "connector", slot, dimension.ID == "evse.dimension.connector" && ok
		default:
			return "", "", false
		}
	}
	if dimension.ID == "storage.dimension.pack" {
		return "pack", "", true
	} // never export its asset-valued dimension.
	if dimension.ID != "pv.dimension.phase" && dimension.ID != "pv.dimension.inverter" && dimension.ID != "pv.dimension.system" {
		return "", "", false
	}
	if dimension.Value.Kind != semreg.ValueText || dimension.Value.Text == nil {
		return "", "", false
	}
	value := *dimension.Value.Text
	switch {
	case value == "phase:L1":
		return "phase_l1", "", true
	case value == "phase:L2":
		return "phase_l2", "", true
	case value == "phase:L3":
		return "phase_l3", "", true
	case value == "inverter" || strings.HasPrefix(value, "inverter:"):
		return "inverter", "", true
	case value == "system" || strings.HasPrefix(value, "system:"):
		return "system", "", true
	}
	return "", "", false
}

const semanticEVSEConnectorBudget = 8

// semanticEVSEConnectorSlots converts private connector identities into a
// fixed, deterministic vocabulary. The raw identifier is used only to order a
// detached snapshot; it is never emitted as a label.
func semanticEVSEConnectorSlots(domain string, snapshot semreg.Snapshot) map[string]string {
	if domain != "evse" {
		return nil
	}
	keys := make([]string, 0)
	for _, envelope := range snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			key, ok := semanticFactKey(candidate.Key)
			if !ok || candidate.Key.PackID != "helianthus.pack.evse" || candidate.Key.PackVersion != "1.0.0" || candidate.Key.FactID != "evse.limit.allocated_current" || len(candidate.Key.Dimensions) != 1 || candidate.Key.Dimensions[0].ID != "evse.dimension.connector" {
				continue
			}
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	slots := make(map[string]string, semanticEVSEConnectorBudget)
	for _, key := range keys {
		if _, exists := slots[key]; exists {
			continue
		}
		if len(slots) >= semanticEVSEConnectorBudget {
			break
		}
		slots[key] = "connector_" + strconv.Itoa(len(slots)+1)
	}
	return slots
}

func semanticFactKey(key semreg.FactKey) (string, bool) {
	if key.Validate() != nil {
		return "", false
	}
	encoded, err := semreg.CanonicalJSON(key)
	if err != nil {
		return "", false
	}
	return string(encoded), true
}

func validSemanticItem(domain, item string) bool {
	_, ok := semanticFactSchemaFor(domain, item)
	if ok {
		return true
	}
	return domain == "pv" && (item == "projection.gateway.pv.inverter.ac.current.total" || item == "projection.gateway.pv.inverter.events.1" || item == "projection.gateway.pv.inverter.events.2")
}

// semanticFactSchema is the complete finite v1 contract for a fact-backed
// projection item. It binds pack version, fact, dimension, and value shape so
// independent allowlists cannot accidentally compose a different metric.
type semanticFactSchema struct {
	domain, item, pack, version, fact, dimensionID, dimensionValue, unit string
	dimensionSuffix                                                      bool
	symbols                                                              map[string]bool
}

func (s semanticFactSchema) matches(key semreg.FactKey, value *semreg.Value) bool {
	if string(key.PackID) != s.pack || string(key.PackVersion) != s.version || string(key.FactID) != s.fact || len(key.Dimensions) != 1 || value == nil {
		return false
	}
	dimension := key.Dimensions[0]
	if string(dimension.ID) != s.dimensionID || dimension.Value.Kind != semreg.ValueText || dimension.Value.Text == nil || *dimension.Value.Text == "" {
		return false
	}
	if s.dimensionValue != "" && *dimension.Value.Text != s.dimensionValue && (!s.dimensionSuffix || !strings.HasPrefix(*dimension.Value.Text, s.dimensionValue+":")) {
		return false
	}
	if s.symbols != nil {
		return value.Kind == semreg.ValueSymbol && value.Symbol != nil && value.Symbol.Known && string(value.Symbol.Namespace) == s.fact && s.symbols[string(value.Symbol.Token)]
	}
	return value.Kind == semreg.ValueQuantity && value.Quantity != nil && string(value.Quantity.Unit) == s.unit
}

func semanticFactSchemaFor(domain, item string) (semanticFactSchema, bool) {
	const pvPack, pvVersion = "helianthus.pack.pv", "1.0.0"
	const storagePack, storageVersion = "helianthus.pack.storage", "1.1.0"
	const evsePack, evseVersion = "helianthus.pack.evse", "1.0.0"
	pv := func(item, fact, dimension, value, unit string) semanticFactSchema {
		return semanticFactSchema{domain: "pv", item: item, pack: pvPack, version: pvVersion, fact: fact, dimensionID: dimension, dimensionValue: value, dimensionSuffix: value == "inverter" || value == "system", unit: unit}
	}
	storage := func(item, unit string) semanticFactSchema {
		return semanticFactSchema{domain: "storage", item: item, pack: storagePack, version: storageVersion, fact: item, dimensionID: "storage.dimension.pack", unit: unit}
	}
	if domain == "pv" {
		switch item {
		case "projection.gateway.pv.inverter.ac.current.phase_a":
			return pv(item, "pv.ac.current", "pv.dimension.phase", "phase:L1", "unit.ampere"), true
		case "projection.gateway.pv.inverter.ac.current.phase_b":
			return pv(item, "pv.ac.current", "pv.dimension.phase", "phase:L2", "unit.ampere"), true
		case "projection.gateway.pv.inverter.ac.current.phase_c":
			return pv(item, "pv.ac.current", "pv.dimension.phase", "phase:L3", "unit.ampere"), true
		case "projection.gateway.pv.inverter.ac.voltage.phase_a":
			return pv(item, "pv.ac.voltage", "pv.dimension.phase", "phase:L1", "unit.volt"), true
		case "projection.gateway.pv.inverter.ac.voltage.phase_b":
			return pv(item, "pv.ac.voltage", "pv.dimension.phase", "phase:L2", "unit.volt"), true
		case "projection.gateway.pv.inverter.ac.voltage.phase_c":
			return pv(item, "pv.ac.voltage", "pv.dimension.phase", "phase:L3", "unit.volt"), true
		case "projection.gateway.pv.inverter.ac.power.active":
			return pv(item, "pv.ac.aggregate_active_power", "pv.dimension.inverter", "inverter", "unit.watt"), true
		case "projection.gateway.pv.inverter.ac.frequency":
			return pv(item, "pv.ac.frequency", "pv.dimension.inverter", "inverter", "unit.hertz"), true
		case "projection.gateway.pv.inverter.ac.energy_lifetime":
			return pv(item, "pv.energy.generated", "pv.dimension.system", "system", "unit.kilowatt_hour"), true
		case "projection.gateway.pv.inverter.temperature.cabinet":
			return pv(item, "pv.temperature.inverter", "pv.dimension.inverter", "inverter", "unit.celsius"), true
		case "projection.gateway.pv.inverter.operating_state":
			return semanticFactSchema{domain: "pv", item: item, pack: pvPack, version: pvVersion, fact: "pv.status.operating", dimensionID: "pv.dimension.inverter", dimensionValue: "inverter", dimensionSuffix: true, symbols: map[string]bool{"generating": true}}, true
		}
	}
	if domain == "storage" {
		switch item {
		case "storage.capacity.charge", "storage.capacity.discharge":
			return storage(item, "unit.ampere_hour"), true
		case "storage.pack.current":
			return storage(item, "unit.ampere"), true
		case "storage.pack.voltage":
			return storage(item, "unit.volt"), true
		case "storage.state.soc":
			return storage(item, "unit.percent"), true
		case "storage.temperature.pack":
			return storage(item, "unit.celsius"), true
		case "storage.status.operating":
			return semanticFactSchema{domain: "storage", item: item, pack: storagePack, version: storageVersion, fact: item, dimensionID: "storage.dimension.pack", symbols: map[string]bool{"active": true, "standby": true}}, true
		}
	}
	if domain == "evse" {
		switch item {
		case "evse.limit.configured_current":
			return semanticFactSchema{domain: "evse", item: item, pack: evsePack, version: evseVersion, fact: item, dimensionID: "evse.dimension.evse", unit: "unit.ampere"}, true
		case "evse.limit.allocated_current":
			return semanticFactSchema{domain: "evse", item: item, pack: evsePack, version: evseVersion, fact: item, dimensionID: "evse.dimension.connector", unit: "unit.ampere"}, true
		}
	}
	return semanticFactSchema{}, false
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
