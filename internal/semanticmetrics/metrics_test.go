package semanticmetrics

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/infrastructure"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
)

func m36JSON(t *testing.T, value any) []byte {
	t.Helper()
	b, e := json.Marshal(value)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func m36Base(t *testing.T) semreg.Snapshot {
	t.Helper()
	// Entirely synthetic: one source, binding, candidate and referenced fact.
	digest := sha256.Sum256([]byte("synthetic projection revision axes evidence"))
	evidence := semreg.EvidenceRef{
		Owner: "owner.test", Kind: "evidence.test", Digest: semreg.Digest(fmt.Sprintf("sha256:%x", digest)),
		Contract: "test.contract/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone,
	}
	source := semreg.SourceID("source:test")
	epoch := semreg.SourceEpochID("epoch:test")
	binding := semreg.NativeBindingID("binding:test")
	generation := semreg.Uint64("1")
	point := semreg.TimePoint{UnixNanoseconds: "1", ClockID: "clock.test", UncertaintyNS: "0"}
	monotonic := semreg.MonotonicPoint{ClockEpochID: "clock-epoch:test", Nanoseconds: "1"}
	key := semreg.FactKey{PackID: "pack.test", PackVersion: "1.0.0", FactID: "fact.test", Dimensions: []semreg.Dimension{}}
	value := true
	snapshot := semreg.Snapshot{
		Contract: semreg.ContractKernelV1, AssetID: "asset:test",
		Revisions:   semreg.RevisionVector{Semantic: "2", Identity: "3", Facts: "4", Services: "5", Capabilities: "6"},
		EvaluatedAt: point, EvaluateMonotonic: monotonic,
		Sources: []semreg.SourceDescriptor{{
			SourceID: source, SourceEpochID: epoch, ProtocolID: "protocol.test", ProfileID: "profile.test", ProfileVersion: "test-1",
			RegistryEvidence: evidence, StartedAt: point, State: semreg.SourceCurrent, Revision: "1",
		}},
		Bindings: []semreg.NativeBinding{{
			BindingID: binding, AssetID: "asset:test", SourceID: source, SourceEpochID: epoch, DriverGeneration: generation,
			NativeResource: evidence, State: semreg.BindingCurrent, Revision: "1",
		}},
		IdentityLinks: []semreg.IdentityLink{},
		Facts: []semreg.FactEnvelope{{
			AssetID: "asset:test", Key: key, Revision: "1", Conflicts: []semreg.Conflict{},
			Candidates: []semreg.FactCandidate{{
				CandidateID: "candidate:test", Key: key, Value: &semreg.Value{Kind: semreg.ValueBoolean, Boolean: &value},
				Quality: semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified,
					Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable,
					Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
				Times:           semreg.Times{ReceivedAt: point, ReceiptMonotonic: monotonic, EvaluatedAt: point, EvaluateMonotonic: monotonic},
				FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:test", Version: "1.0.0", FreshForNS: "1", RetainForNS: "2", MaxWallUncertaintyNS: "0"},
				BindingID:       &binding, SourceEpochID: &epoch, DriverGeneration: &generation,
				Origin: semreg.OriginRef{OriginID: "origin:test", Kind: semreg.OriginNativeObservation,
					SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}},
				Evidence: []semreg.EvidenceRef{evidence}, Revision: "1",
			}},
		}},
		Services: []semreg.ServiceInstance{}, Capabilities: []semreg.CapabilityInstance{}, Fences: []semreg.GenerationFence{},
		Cursors: []semreg.PublicationCursor{{SourceID: source, SourceEpochID: epoch, DriverGeneration: generation,
			LastSequence: "1", LastBatchDigest: evidence.Digest}},
	}
	var document map[string]any
	if err := json.Unmarshal(m36JSON(t, snapshot), &document); err != nil {
		t.Fatal(err)
	}
	delete(document, "snapshot_id")
	snapshotDigest := sha256.Sum256(m36JSON(t, document))
	snapshot.SnapshotID = semreg.SnapshotID(fmt.Sprintf("sha256:%x", snapshotDigest))
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("synthetic snapshot: %v", err)
	}
	return snapshot
}

func m36Seal(t *testing.T, s *semreg.Snapshot) {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(m36JSON(t, s), &doc); err != nil {
		t.Fatal(err)
	}
	delete(doc, "snapshot_id")
	digest := sha256.Sum256(m36JSON(t, doc))
	s.SnapshotID = semreg.SnapshotID(fmt.Sprintf("sha256:%x", digest))
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
}
func m36View(t *testing.T, s semreg.Snapshot) semanticview.View {
	t.Helper()
	m36Seal(t, &s)
	e, err := semreg.EvaluateSnapshot(s, semreg.EvaluationContext{EvaluatedAt: s.EvaluatedAt, EvaluateMonotonic: s.EvaluateMonotonic})
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := packs.NewMetadataRegistry()
	if err != nil {
		t.Fatal(err)
	}
	v, err := semanticview.Build(metadata, []semanticview.Input{{AssetID: s.AssetID, Snapshot: &s, Evaluation: &e}})
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func m36Fixture(t *testing.T, field string, value semreg.Value) (semreg.Snapshot, Registry) {
	t.Helper()
	s := m36Base(t)
	domain := strings.Split(field, ".")[0]
	metadata := thermal.Metadata()
	if domain == "infrastructure" {
		metadata = infrastructure.Metadata()
	}
	var f semreg.FieldMetadata
	for _, x := range metadata.Fields {
		if string(x.Ref.ID) == field {
			f = x
		}
	}
	dim := "opaque"
	key := semreg.FactKey{PackID: metadata.Pack.ID, PackVersion: metadata.Pack.Version, FactID: f.Ref.ID, Dimensions: []semreg.Dimension{{ID: f.Dimension.ID, Value: semreg.Value{Kind: semreg.ValueText, Text: &dim}}}}
	c := &s.Facts[0].Candidates[0]
	c.Key = key
	c.Value = &value
	s.Facts[0].Key = key
	serviceID := semreg.ServiceInstanceID("service:synthetic")
	// Thermal quantity dimensions are physical quantities, while thermal services
	// name resource kinds. The explicit configured tuple supplies that association.
	serviceName := domain + ".service.system"
	if domain == "infrastructure" {
		serviceName = domain + ".service." + strings.TrimPrefix(string(f.Dimension.ID), domain+".dimension.")
	}
	capName := strings.Replace(serviceName, ".service.", ".capability.read.", 1)
	ref := func(id string) semreg.DefinitionRef {
		return semreg.DefinitionRef{Pack: metadata.Pack, ID: semreg.DefinitionID(id), Version: metadata.Pack.Version}
	}
	binding := s.Bindings[0]
	s.Services = []semreg.ServiceInstance{{InstanceID: serviceID, AssetID: s.AssetID, Definition: ref(serviceName), BindingID: binding.BindingID, SourceEpochID: binding.SourceEpochID, DriverGeneration: binding.DriverGeneration, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "1"}}
	s.Capabilities = []semreg.CapabilityInstance{{InstanceID: "capability:synthetic", AssetID: s.AssetID, ServiceInstance: serviceID, Definition: ref(capName), BindingID: binding.BindingID, SourceEpochID: binding.SourceEpochID, DriverGeneration: binding.DriverGeneration, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Constraints: []semreg.TypedField{}, ActivationEvidence: []semreg.EvidenceRef{binding.NativeResource}, Revision: "1"}}
	r := Registry{Validated: true, Bindings: []Binding{{Domain: domain, Slot: 0, AssetID: s.AssetID, SourceID: binding.SourceID, SourceEpochID: binding.SourceEpochID, BindingID: binding.BindingID, DriverGeneration: binding.DriverGeneration, ServiceInstance: serviceID, Dimensions: key.Dimensions, Fields: []semreg.DefinitionID{f.Ref.ID}}}}
	return s, r
}
func m36Decimal(number string) semreg.Decimal {
	var exponent int32
	for number != "0" && strings.HasSuffix(number, "0") {
		number = strings.TrimSuffix(number, "0")
		exponent++
	}
	return semreg.Decimal{Coefficient: number, Exponent10: exponent}
}
func m36UnadmittedView(t *testing.T, s semreg.Snapshot) semanticview.View {
	t.Helper()
	m36Seal(t, &s)
	e, err := semreg.EvaluateSnapshot(s, semreg.EvaluationContext{EvaluatedAt: s.EvaluatedAt, EvaluateMonotonic: s.EvaluateMonotonic})
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.View{Assets: []semanticview.Asset{{AssetID: s.AssetID, Snapshot: &s, Evaluation: &e}}}
}
func m36Quantity(number, unit string) semreg.Value {
	return semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: m36Decimal(number), Unit: semreg.DefinitionID(unit)}}
}
func m36Render(t *testing.T, v semanticview.View, r Registry) string {
	t.Helper()
	text, err := Render(v, r)
	if err != nil {
		t.Fatal(err)
	}
	return text
}
func m36Available(text string, b Binding, field semreg.DefinitionID, available int) bool {
	return strings.Contains(text, fmt.Sprintf("%s{domain=\"%s\",field=\"%s\",slot=\"%d\"} %d\n", availabilityName, b.Domain, field, b.Slot, available))
}

func TestM36Quantities(t *testing.T) {
	tests := []struct{ field, name, unit, number, want string }{
		{"thermal.demand.level", "helianthus_thermal_demand_ratio", "unit.percent", "25", "0.25"},
		{"thermal.measurement.air_temperature", "helianthus_thermal_air_temperature_celsius", "unit.celsius", "-12", "-12"},
		{"thermal.measurement.flow_rate", "helianthus_thermal_flow_cubic_meters_per_second", "unit.litre_per_minute", "60", "0.001"},
		{"thermal.measurement.humidity_relative", "helianthus_thermal_relative_humidity_ratio", "unit.percent", "50", "0.5"},
		{"thermal.measurement.power", "helianthus_thermal_power_watts", "unit.watt", "-12", "-12"},
		{"thermal.measurement.water_temperature", "helianthus_thermal_water_temperature_celsius", "unit.celsius", "42", "42"},
		{"thermal.setpoint.dhw_temperature", "helianthus_thermal_dhw_setpoint_celsius", "unit.celsius", "55", "55"},
		{"thermal.setpoint.temperature", "helianthus_thermal_setpoint_celsius", "unit.celsius", "20", "20"},
		{"infrastructure.ac.active_power", "helianthus_infrastructure_ac_active_power_watts", "unit.watt", "-12", "-12"},
		{"infrastructure.ac.apparent_power", "helianthus_infrastructure_ac_apparent_power_volt_amperes", "unit.volt_ampere", "12", "12"},
		{"infrastructure.ac.current", "helianthus_infrastructure_ac_current_amperes", "unit.ampere", "12", "12"},
		{"infrastructure.ac.frequency", "helianthus_infrastructure_ac_frequency_hertz", "unit.hertz", "50", "50"},
		{"infrastructure.ac.power_factor", "helianthus_infrastructure_ac_power_factor_ratio", "unit.ratio", "-1", "-1"},
		{"infrastructure.ac.reactive_power", "helianthus_infrastructure_ac_reactive_power_vars", "unit.volt_ampere_reactive", "-12", "-12"},
		{"infrastructure.ac.voltage", "helianthus_infrastructure_ac_voltage_volts", "unit.volt", "230", "230"},
		{"infrastructure.energy.export", "helianthus_infrastructure_energy_export_joules", "unit.kilowatt_hour", "2", "7.2e+06"},
		{"infrastructure.energy.import", "helianthus_infrastructure_energy_import_joules", "unit.kilowatt_hour", "3", "1.08e+07"},
		{"infrastructure.power.apparent", "helianthus_infrastructure_apparent_power_volt_amperes", "unit.volt_ampere", "12", "12"},
		{"infrastructure.power.export_active", "helianthus_infrastructure_export_active_power_watts", "unit.watt", "12", "12"},
		{"infrastructure.power.export_reactive", "helianthus_infrastructure_export_reactive_power_vars", "unit.volt_ampere_reactive", "12", "12"},
		{"infrastructure.power.import_active", "helianthus_infrastructure_import_active_power_watts", "unit.watt", "12", "12"},
		{"infrastructure.power.import_reactive", "helianthus_infrastructure_import_reactive_power_vars", "unit.volt_ampere_reactive", "12", "12"},
	}
	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			s, r := m36Fixture(t, tt.field, m36Quantity(tt.number, tt.unit))
			v := m36View(t, s)
			before := m36JSON(t, v)
			text := m36Render(t, v, r)
			if !strings.Contains(text, fmt.Sprintf("%s{slot=\"0\"} %s\n", tt.name, tt.want)) || !strings.Contains(text, "# TYPE "+tt.name+" gauge\n") || !strings.Contains(text, "# HELP "+tt.name+" "+tt.field+" (") || !m36Available(text, r.Bindings[0], semreg.DefinitionID(tt.field), 1) {
				t.Fatal(text)
			}
			if text != m36Render(t, v, r) || !reflect.DeepEqual(before, m36JSON(t, v)) {
				t.Fatal("nondeterministic or mutating render")
			}
		})
	}
}
func TestM36Symbols(t *testing.T) {
	tests := []struct {
		field, name string
		symbols     []string
	}{
		{"thermal.action.state", "helianthus_thermal_action_state", []string{"idle", "heating", "cooling", "dhw", "ventilating"}},
		{"thermal.mode.system", "helianthus_thermal_system_mode", []string{"off", "auto", "heat", "cool"}},
		{"thermal.mode.zone", "helianthus_thermal_zone_mode", []string{"off", "auto", "comfort", "eco"}},
		{"thermal.status.fault", "helianthus_thermal_fault_present", []string{"clear", "present"}},
		{"thermal.status.operation", "helianthus_thermal_operation_active", []string{"idle", "active"}},
		{"infrastructure.status.breaker", "helianthus_infrastructure_breaker_open", []string{"closed", "open"}},
		{"infrastructure.status.connection", "helianthus_infrastructure_connection_connected", []string{"disconnected", "connected"}},
		{"infrastructure.status.fault", "helianthus_infrastructure_fault_present", []string{"clear", "present"}},
		{"infrastructure.status.interlock", "helianthus_infrastructure_interlock_active", []string{"clear", "active"}},
		{"infrastructure.status.readiness", "helianthus_infrastructure_ready", []string{"not_ready", "ready"}},
	}
	for _, tt := range tests {
		for i, symbol := range append(tt.symbols, "unknown") {
			t.Run(tt.field+"/"+symbol, func(t *testing.T) {
				value := semreg.Value{Kind: semreg.ValueSymbol, Symbol: &semreg.Symbol{Namespace: semreg.DefinitionID(tt.field), Token: symbol, Known: true}}
				s, r := m36Fixture(t, tt.field, value)
				var v semanticview.View
				if symbol == "unknown" {
					v = m36UnadmittedView(t, s)
				} else {
					v = m36View(t, s)
				}
				text := m36Render(t, v, r)
				if symbol == "unknown" {
					if strings.Contains(text, tt.name+"{slot=") || !m36Available(text, r.Bindings[0], semreg.DefinitionID(tt.field), 0) {
						t.Fatal(text)
					}
				} else if !strings.Contains(text, fmt.Sprintf("%s{slot=\"0\"} %d\n", tt.name, i)) {
					t.Fatal(text)
				}
			})
		}
	}
}

func TestM36Unavailable(t *testing.T) {
	changes := map[string]func(*semreg.Snapshot, *Registry){
		"ambiguous_candidates": func(s *semreg.Snapshot, _ *Registry) {
			c := s.Facts[0].Candidates[0]
			c.CandidateID = "candidate:two"
			s.Facts[0].Candidates = append(s.Facts[0].Candidates, c)
		},
		"withheld_reason": func(s *semreg.Snapshot, _ *Registry) {
			s.Facts[0].Candidates[0].Quality.Reasons = []semreg.DefinitionID{"unknown_pending_std_01"}
		},

		"unqualified": func(s *semreg.Snapshot, _ *Registry) {
			s.Facts[0].Candidates[0].Quality.Qualification = semreg.QualificationUnknown
			s.Facts[0].Candidates[0].Quality.Promotion = semreg.PromotionUnpromoted
		},
		"invalid": func(s *semreg.Snapshot, _ *Registry) {
			s.Facts[0].Candidates[0].Quality.Validity = semreg.ValidityBad
			s.Facts[0].Candidates[0].Quality.Promotion = semreg.PromotionUnpromoted
		},
		"absent": func(s *semreg.Snapshot, _ *Registry) {
			s.Facts[0].Candidates[0].Value = nil
			s.Facts[0].Candidates[0].Quality.Qualification = semreg.QualificationUnsupported
			s.Facts[0].Candidates[0].Quality.Promotion = semreg.PromotionUnpromoted
		},
		"withdrawn": func(s *semreg.Snapshot, _ *Registry) {
			s.Facts[0].Candidates[0].Value = nil
			s.Facts[0].Candidates[0].Quality.Availability = semreg.AvailabilityWithdrawn
			s.Facts[0].Candidates[0].Quality.Promotion = semreg.PromotionUnpromoted
		},
		"stale":      func(s *semreg.Snapshot, _ *Registry) { s.EvaluateMonotonic.Nanoseconds = "2" },
		"expired":    func(s *semreg.Snapshot, _ *Registry) { s.EvaluateMonotonic.Nanoseconds = "3" },
		"wrong_unit": func(s *semreg.Snapshot, _ *Registry) { s.Facts[0].Candidates[0].Value.Quantity.Unit = "unit.volt" },
		"negative_directional": func(s *semreg.Snapshot, _ *Registry) {
			s.Facts[0].Candidates[0].Value.Quantity.Number.Coefficient = "-1"
		},
		"retired_slot":         func(_ *semreg.Snapshot, r *Registry) { r.Bindings[0].Retired = true },
		"registry_unvalidated": func(_ *semreg.Snapshot, r *Registry) { r.Validated = false },
		"service_unqualified":  func(s *semreg.Snapshot, _ *Registry) { s.Services[0].Qualification = semreg.QualificationUnknown },
		"capability_withdrawn": func(s *semreg.Snapshot, _ *Registry) { s.Capabilities[0].Availability = semreg.AvailabilityWithdrawn },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			s, r := m36Fixture(t, "infrastructure.power.import_active", m36Quantity("12", "unit.watt"))
			change(&s, &r)
			var v semanticview.View
			if name == "wrong_unit" || name == "negative_directional" {
				v = m36UnadmittedView(t, s)
			} else {
				v = m36View(t, s)
			}
			text := m36Render(t, v, r)
			if strings.Contains(text, "helianthus_infrastructure_import_active_power_watts{slot=") || !m36Available(text, r.Bindings[0], r.Bindings[0].Fields[0], 0) {
				t.Fatal(text)
			}
		})
	}
	s, r := m36Fixture(t, "thermal.measurement.air_temperature", m36Quantity("0", "unit.celsius"))
	withheld := semanticview.View{Assets: []semanticview.Asset{{AssetID: s.AssetID, UnavailableReason: "semantic.read.reason.publication_withheld"}}}
	if text := m36Render(t, withheld, r); !m36Available(text, r.Bindings[0], r.Bindings[0].Fields[0], 0) {
		t.Fatal(text)
	}
	if text := m36Render(t, m36View(t, s), r); !strings.Contains(text, "helianthus_thermal_air_temperature_celsius{slot=\"0\"} 0\n") {
		t.Fatal("known zero omitted")
	}
}

func TestM36BindingIdentity(t *testing.T) {
	changes := map[string]func(*Binding){
		"asset": func(b *Binding) { b.AssetID = "asset:other" }, "source": func(b *Binding) { b.SourceID = "source:other" },
		"epoch": func(b *Binding) { b.SourceEpochID = "epoch:other" }, "native_binding": func(b *Binding) { b.BindingID = "binding:other" },
		"generation": func(b *Binding) { b.DriverGeneration = "2" }, "service": func(b *Binding) { b.ServiceInstance = "service:other" },
		"dimension_id":    func(b *Binding) { b.Dimensions[0].ID = "infrastructure.dimension.meter" },
		"dimension_value": func(b *Binding) { value := "other"; b.Dimensions[0].Value.Text = &value },
		"field":           func(b *Binding) { b.Fields[0] = "infrastructure.ac.current" },
		"domain":          func(b *Binding) { b.Domain = "thermal"; b.Fields = []semreg.DefinitionID{"thermal.measurement.power"} },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			s, r := m36Fixture(t, "infrastructure.ac.voltage", m36Quantity("230", "unit.volt"))
			v := m36View(t, s)
			change(&r.Bindings[0])
			text := m36Render(t, v, r)
			if !m36Available(text, r.Bindings[0], r.Bindings[0].Fields[0], 0) {
				t.Fatal(text)
			}
		})
	}
	s, r := m36Fixture(t, "infrastructure.ac.voltage", m36Quantity("230", "unit.volt"))
	r.Bindings[0].Slot = 15
	if text := m36Render(t, m36View(t, s), r); !strings.Contains(text, "helianthus_infrastructure_ac_voltage_volts{slot=\"15\"} 230") {
		t.Fatal(text)
	}
}

func TestM36CoherenceAndSiblings(t *testing.T) {
	s, r := m36Fixture(t, "thermal.measurement.air_temperature", m36Quantity("21", "unit.celsius"))
	sibling := s.Facts[0]
	sibling.Candidates = append([]semreg.FactCandidate(nil), sibling.Candidates...)
	sibling.Key.FactID = "thermal.measurement.water_temperature"
	sibling.Candidates[0].CandidateID = "candidate:sibling"
	sibling.Candidates[0].Key = sibling.Key
	sibling.Candidates[0].Value = nil
	sibling.Candidates[0].Quality.Qualification = semreg.QualificationUnsupported
	sibling.Candidates[0].Quality.Promotion = semreg.PromotionUnpromoted
	s.Facts = append(s.Facts, sibling)
	r.Bindings[0].Fields = append(r.Bindings[0].Fields, sibling.Key.FactID)
	v := m36View(t, s)
	text := m36Render(t, v, r)
	if !m36Available(text, r.Bindings[0], r.Bindings[0].Fields[0], 1) || !m36Available(text, r.Bindings[0], sibling.Key.FactID, 0) {
		t.Fatal(text)
	}
	for _, axis := range []string{"semantic", "identity", "facts", "services", "capabilities", "snapshot", "evaluation"} {
		t.Run(axis, func(t *testing.T) {
			var changed semanticview.View
			if err := json.Unmarshal(m36JSON(t, v), &changed); err != nil {
				t.Fatal(err)
			}
			e := changed.Assets[0].Evaluation
			switch axis {
			case "semantic":
				e.Revisions.Semantic = "7"
			case "identity":
				e.Revisions.Identity = "7"
			case "facts":
				e.Revisions.Facts = "7"
			case "services":
				e.Revisions.Services = "7"
			case "capabilities":
				e.Revisions.Capabilities = "7"
			case "snapshot":
				d := sha256.Sum256([]byte("different synthetic snapshot"))
				e.SnapshotID = semreg.SnapshotID(fmt.Sprintf("sha256:%x", d))
			case "evaluation":
				e.Context.EvaluateMonotonic.Nanoseconds = "3"
			}
			text := m36Render(t, changed, r)
			if !m36Available(text, r.Bindings[0], r.Bindings[0].Fields[0], 0) {
				t.Fatal(text)
			}
		})
	}
}

func TestM36Cardinality(t *testing.T) {
	s, r := m36Fixture(t, "infrastructure.ac.voltage", m36Quantity("230", "unit.volt"))
	v := m36View(t, s)
	for i := 0; i < 2000; i++ {
		v.Assets = append(v.Assets, semanticview.Asset{AssetID: semreg.AssetID(fmt.Sprintf("asset:opaque-%d", i)), UnavailableReason: "semantic.read.reason.asset_unknown"})
	}
	text := m36Render(t, v, r)
	if strings.Contains(text, "opaque") || strings.Count(text, "{slot=") > 1 || !m36Available(text, r.Bindings[0], r.Bindings[0].Fields[0], 0) {
		t.Fatal(text)
	}
	// A normal bounded capture also cannot dynamically allocate unconfigured assets.
	v.Assets = v.Assets[:32]
	text = m36Render(t, v, r)
	if !m36Available(text, r.Bindings[0], r.Bindings[0].Fields[0], 1) || strings.Contains(text, "opaque") {
		t.Fatal(text)
	}
	for _, slot := range []int{-1, 16} {
		r.Bindings[0].Slot = slot
		if _, err := Render(v, r); err == nil {
			t.Fatal("invalid slot accepted")
		}
	}
	r.Bindings[0].Slot = 0
	r.Bindings = append(r.Bindings, r.Bindings[0])
	if _, err := Render(v, r); err == nil {
		t.Fatal("slot collision accepted")
	}
	r.Bindings[1].Slot = 1
	if _, err := Render(v, r); err == nil {
		t.Fatal("tuple collision accepted")
	}
}

func TestM36FactKeyAndCandidateRevision(t *testing.T) {
	for _, axis := range []string{"pack_id", "pack_version", "candidate_revision"} {
		t.Run(axis, func(t *testing.T) {
			s, r := m36Fixture(t, "infrastructure.ac.voltage", m36Quantity("230", "unit.volt"))
			v := m36View(t, s)
			c := &v.Assets[0].Snapshot.Facts[0].Candidates[0]
			switch axis {
			case "pack_id":
				v.Assets[0].Snapshot.Facts[0].Key.PackID = "helianthus.pack.thermal"
				c.Key.PackID = "helianthus.pack.thermal"
			case "pack_version":
				v.Assets[0].Snapshot.Facts[0].Key.PackVersion = "1.0.1"
				c.Key.PackVersion = "1.0.1"
			case "candidate_revision":
				c.Revision = "2"
			}
			// Re-seal the publication while retaining the old evaluation: neither
			// a different key nor candidate revision may borrow its availability.
			m36Seal(t, v.Assets[0].Snapshot)
			text := m36Render(t, v, r)
			if !m36Available(text, r.Bindings[0], r.Bindings[0].Fields[0], 0) {
				t.Fatal(text)
			}
		})
	}
}
