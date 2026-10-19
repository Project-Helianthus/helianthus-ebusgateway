package main

import (
	"math"
	"reflect"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func thermalHumidityFixture() thermalHumidityObservation {
	v, binding, assignment, connected := 40.0, uint16(2), uint16(1), true
	phenomenon := time.Unix(50, 0).UTC()
	return thermalHumidityObservation{
		Identity: thermalHumidityIdentity{Asset: "synthetic-asset", ServiceInstance: "synthetic-zone", Source: "synthetic-source", SourceEpoch: "epoch-a", DriverGeneration: "generation-a", BindingRevision: "binding-a", AssociationRevision: "association-a", Sensor: "sensor-a", Profile: "profile-a", Role: "unresolved", Selector: thermalHumiditySelector{PB: 0xb5, SB: 0x24, Opcode: 2, OperationType: 0, Group: 3, Instance: 1, Register: 0x28, Direction: "target_to_initiator"}},
		Percent:  &v, RawRequest: []byte{2, 0, 3, 1, 0x28, 0}, RawReply: []byte{0, 0, 0x20, 0x42}, Binding: &binding, Assignment: &assignment, Connected: &connected,
		Codec: "synthetic-f32-le", SourceUnit: "percent", ResponseStatus: "synthetic-success", ResponseShape: "synthetic-correlated-value", EvidenceReference: "synthetic-evidence", Correlated: true,
		ReceivedAt: time.Unix(100, 0).UTC(), ReceiptMonotonic: time.Second, ClockEpoch: "clock-a", PhenomenonAt: &phenomenon, Sequence: "sequence-a", EvidenceStatus: "Unknown", Qualification: "unknown", Freshness: "unknown",
	}
}

func TestThermalHumidityMappingPercent(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		value                 float64
		validity, coefficient string
		exponent              int32
	}{
		{"zero", 0, "valid", "0", 0}, {"maximum", 100, "valid", "1", 2},
		{"percent_not_fraction", 50, "valid", "5", 1}, {"fractional_percent", 12.5, "valid", "125", -1},
		{"below_one_percent", 0.5, "valid", "5", -1},
		{"negative_zero", math.Copysign(0, -1), "valid", "0", 0},
		{"negative", -1, "invalid", "", 0}, {"over", 101, "invalid", "", 0},
		{"nan", math.NaN(), "invalid", "", 0}, {"positive_infinity", math.Inf(1), "invalid", "", 0}, {"negative_infinity", math.Inf(-1), "invalid", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := thermalHumidityFixture()
			o.Percent = &tc.value
			r := mapThermalHumidity([]thermalHumidityRequest{{Identity: o.Identity, Observation: &o}})[0]
			if r.Validity != tc.validity || r.Withheld != "humidity_binding_unresolved" {
				t.Fatalf("unexpected result: %+v", r)
			}
			if tc.validity == "valid" {
				want := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: tc.coefficient, Exponent10: tc.exponent}, Unit: "unit.percent"}}
				if !reflect.DeepEqual(r.Candidate, &want) {
					t.Fatalf("candidate = %+v, want %+v", r.Candidate, want)
				}
				if err := r.Candidate.Quantity.Number.Validate(); err != nil {
					t.Fatal(err)
				}
			} else if r.Candidate != nil || r.RetainedNative != nil || r.SourceLoss != "invalid_percent" {
				t.Fatal("invalid input became a candidate or retained value")
			}
		})
	}
	for _, absent := range []bool{false, true} {
		o := thermalHumidityFixture()
		o.Percent = nil
		request := thermalHumidityRequest{Identity: o.Identity, Observation: &o}
		if absent {
			request.Observation = nil
		}
		r := mapThermalHumidity([]thermalHumidityRequest{request})[0]
		if r.Validity != "missing" || r.Candidate != nil || r.SourceLoss != "missing" {
			t.Fatal("missing became invalid or zero")
		}
	}
}

func TestThermalHumidityMappingUnrepresentable(t *testing.T) {
	o := thermalHumidityFixture()
	value := 1e-19
	o.Percent = &value
	r := mapThermalHumidity([]thermalHumidityRequest{{Identity: o.Identity, Observation: &o}})[0]
	if r.Validity != "valid" || r.Candidate != nil || r.Withheld != "percent_unrepresentable" || !reflect.DeepEqual(r.RetainedNative, &o) {
		t.Fatal("native value was rounded or erased to fit SemReg")
	}
}

func TestThermalHumidityMappingSelectors(t *testing.T) {
	for _, tc := range []struct {
		name          string
		opcode, group byte
		register      uint16
		service       string
	}{
		{"zone", 2, 3, 0x28, "zone"}, {"circuit", 2, 2, 0x22, "circuit"},
		{"remote_regulator", 6, 9, 7, "zone"}, {"remote_thermostat", 6, 10, 7, "zone"},
		{"threshold", 2, 0, 0x0e, ""}, {"wrong_opcode", 2, 9, 7, ""}, {"wrong_group", 6, 3, 7, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := thermalHumidityFixture()
			o.Identity.Selector.Opcode, o.Identity.Selector.Group, o.Identity.Selector.Register = tc.opcode, tc.group, tc.register
			r := mapThermalHumidity([]thermalHumidityRequest{{Identity: o.Identity, Observation: &o}})[0]
			if !reflect.DeepEqual(r.Native, &o) {
				t.Fatal("native selector/evidence erased")
			}
			if tc.service == "" {
				if r.Field != nil || r.Service != nil || r.Capability != nil || r.Candidate != nil || r.Withheld != "unsupported_native_selector" {
					t.Fatal("unsupported selector projected")
				}
			} else if r.Field == nil || r.Field.ID != "thermal.measurement.humidity_relative" || r.Service == nil || r.Service.ID != semreg.DefinitionID("thermal.service."+tc.service) || r.Capability.ID != semreg.DefinitionID("thermal.capability.read."+tc.service) || r.Candidate == nil || r.Withheld != "humidity_binding_unresolved" {
				t.Fatalf("wrong mapping: %+v", r)
			}
		})
	}
	remote := thermalHumidityFixture()
	remote.Identity.Selector.Opcode, remote.Identity.Selector.Group, remote.Identity.Selector.Register = 6, 10, 7
	remote.Identity.ServiceInstance = ""
	r := mapThermalHumidity([]thermalHumidityRequest{{Identity: remote.Identity, Observation: &remote}})[0]
	if r.Service != nil || r.Capability != nil || r.Field == nil || r.Candidate == nil {
		t.Fatal("unassigned remote acquired a service, or lost its native candidate")
	}
}

// Each mutation independently removes continuity on one claimed identity axis.
// Replacing the production identity comparison with one omitting that field
// must break both stale-observation rejection and prior-evidence fencing.
func TestThermalHumidityMappingIdentityFence(t *testing.T) {
	mutations := map[string]func(*thermalHumidityIdentity){
		"asset":                func(i *thermalHumidityIdentity) { i.Asset = "other-asset" },
		"service_instance":     func(i *thermalHumidityIdentity) { i.ServiceInstance = "other-service" },
		"source":               func(i *thermalHumidityIdentity) { i.Source = "other-source" },
		"source_epoch":         func(i *thermalHumidityIdentity) { i.SourceEpoch = "other-epoch" },
		"driver_generation":    func(i *thermalHumidityIdentity) { i.DriverGeneration = "other-generation" },
		"binding_revision":     func(i *thermalHumidityIdentity) { i.BindingRevision = "other-binding" },
		"association_revision": func(i *thermalHumidityIdentity) { i.AssociationRevision = "other-association" },
		"sensor":               func(i *thermalHumidityIdentity) { i.Sensor = "other-sensor" },
		"profile":              func(i *thermalHumidityIdentity) { i.Profile = "other-profile" },
		"role":                 func(i *thermalHumidityIdentity) { i.Role = "other-role" },
		"pb":                   func(i *thermalHumidityIdentity) { i.Selector.PB = 0xb6 },
		"sb":                   func(i *thermalHumidityIdentity) { i.Selector.SB = 0x25 },
		"opcode":               func(i *thermalHumidityIdentity) { i.Selector.Opcode = 6 },
		"operation_type":       func(i *thermalHumidityIdentity) { i.Selector.OperationType = 1 },
		"group":                func(i *thermalHumidityIdentity) { i.Selector.Group = 2 },
		"instance":             func(i *thermalHumidityIdentity) { i.Selector.Instance = 2 },
		"register":             func(i *thermalHumidityIdentity) { i.Selector.Register = 0x22 },
		"direction":            func(i *thermalHumidityIdentity) { i.Selector.Direction = "initiator_to_target" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			old, sibling := thermalHumidityFixture(), thermalHumidityFixture()
			sibling.Identity.Source = "unaffected-source"
			expected := old.Identity
			mutate(&expected)
			requests := []thermalHumidityRequest{{Identity: expected, Observation: &old, Previous: &old}, {Identity: sibling.Identity, Observation: &sibling}}
			r := mapThermalHumidity(requests)
			if r[0].Validity != "identity_mismatch" || r[0].Candidate != nil || r[0].RetainedNative != nil || r[0].SourceLoss != "identity_changed" {
				t.Fatal("changed identity reused native state")
			}
			if r[1].Validity != "valid" || !reflect.DeepEqual(r[1].Native, &sibling) || r[1].Candidate.Quantity.Number != (semreg.Decimal{Coefficient: "4", Exponent10: 1}) {
				t.Fatal("sibling changed")
			}
			requests[0].Observation = nil
			if got := mapThermalHumidity(requests)[0]; got.RetainedNative != nil || got.Validity != "missing" {
				t.Fatal("missing new source inherited old source")
			}
		})
	}
}

func TestThermalHumidityMappingLifecycle(t *testing.T) {
	for _, freshness := range []string{"fresh", "stale", "unknown"} {
		for _, qualification := range []string{"synthetic-qualified", "unqualified", "unknown"} {
			for _, validity := range []string{"valid", "invalid", "missing"} {
				t.Run(freshness+"/"+qualification+"/"+validity, func(t *testing.T) {
					previous, current := thermalHumidityFixture(), thermalHumidityFixture()
					current.Freshness, current.Qualification = freshness, qualification
					current.ReceivedAt = time.Unix(200, 0).UTC()
					if validity == "invalid" {
						v := 101.0
						current.Percent = &v
					}
					if validity == "missing" {
						current.Percent = nil
					}
					r := mapThermalHumidity([]thermalHumidityRequest{{Identity: current.Identity, Observation: &current, Previous: &previous}})[0]
					if r.Validity != validity || r.Native.Freshness != freshness || r.Native.Qualification != qualification || r.Withheld != "humidity_binding_unresolved" {
						t.Fatal("lifecycle axes collapsed or unresolved key admitted")
					}
					want := &previous
					if validity == "valid" {
						want = &current
					}
					if !reflect.DeepEqual(r.RetainedNative, want) {
						t.Fatal("retention changed original receipt/context or lost prior valid member")
					}
				})
			}
		}
	}
}

func TestThermalHumidityMappingNoFallback(t *testing.T) {
	zone, remote, circuit := thermalHumidityFixture(), thermalHumidityFixture(), thermalHumidityFixture()
	remote.Identity.Source = "remote"
	remote.Identity.Selector.Opcode, remote.Identity.Selector.Group, remote.Identity.Selector.Register = 6, 9, 7
	circuit.Identity.Source = "circuit"
	circuit.Identity.Selector.Group, circuit.Identity.Selector.Register = 2, 0x22
	circuit.Identity.ServiceInstance = "synthetic-circuit"
	for _, missing := range []bool{false, true} {
		request := thermalHumidityRequest{Identity: zone.Identity, Observation: &zone}
		if missing {
			request.Observation = nil
		}
		r := mapThermalHumidity([]thermalHumidityRequest{request, {Identity: remote.Identity, Observation: &remote}, {Identity: circuit.Identity, Observation: &circuit}})
		if missing && (r[0].Candidate != nil || r[0].RetainedNative != nil) {
			t.Fatal("cross-source fallback")
		}
		if r[1].Native.Identity == r[2].Native.Identity || r[1].Service.ID == r[2].Service.ID || r[1].Candidate.Quantity.Number != (semreg.Decimal{Coefficient: "4", Exponent10: 1}) || r[2].Candidate.Quantity.Number != (semreg.Decimal{Coefficient: "4", Exponent10: 1}) {
			t.Fatal("equal scalars aliased sources/services")
		}
		baseline := mapThermalHumidity([]thermalHumidityRequest{{Identity: circuit.Identity, Observation: &circuit}})[0]
		if !reflect.DeepEqual(r[2], baseline) {
			t.Fatal("another source's loss overwrote circuit result")
		}
	}
	changed := remote
	assignment := uint16(2)
	changed.Assignment = &assignment
	changed.Identity.AssociationRevision = "assignment-changed"
	r := mapThermalHumidity([]thermalHumidityRequest{{Identity: changed.Identity, Previous: &remote}})[0]
	if r.RetainedNative != nil {
		t.Fatal("assignment change relabelled history")
	}
}

func TestThermalHumidityMappingDetached(t *testing.T) {
	o, original := thermalHumidityFixture(), thermalHumidityFixture()
	request := []thermalHumidityRequest{{Identity: o.Identity, Observation: &o, Previous: &o}}
	r := mapThermalHumidity(request)
	*o.Percent, o.RawRequest[0], o.RawReply[0], *o.Binding, *o.Assignment, *o.Connected = 90, 99, 99, 9, 9, false
	*o.PhenomenonAt = time.Unix(999, 0)
	if !reflect.DeepEqual(r[0].Native, &original) || !reflect.DeepEqual(r[0].RetainedNative, &original) {
		t.Fatal("input aliases output")
	}
	*r[0].Native.Percent, r[0].Native.RawRequest[0], r[0].Native.RawReply[0], *r[0].Native.Binding, *r[0].Native.Assignment, *r[0].Native.Connected = 80, 88, 88, 8, 8, false
	*r[0].Native.PhenomenonAt = time.Unix(888, 0)
	if !reflect.DeepEqual(r[0].RetainedNative, &original) || *o.Percent != 90 || *o.Binding != 9 || o.RawReply[0] != 99 {
		t.Fatal("output aliases input or sibling retained record")
	}
	r[0].Candidate.Quantity.Number.Coefficient = "7"
	r[0].Field.ID = "modified"
	fresh := mapThermalHumidity([]thermalHumidityRequest{{Identity: original.Identity, Observation: &original}})[0]
	if fresh.Candidate.Quantity.Number != (semreg.Decimal{Coefficient: "4", Exponent10: 1}) || fresh.Field.ID != "thermal.measurement.humidity_relative" {
		t.Fatal("output aliases future calls")
	}
	missing := mapThermalHumidity([]thermalHumidityRequest{{Identity: original.Identity, Previous: &original}})[0]
	*missing.RetainedNative.Percent = 77
	if *original.Percent != 40 {
		t.Fatal("prior evidence aliases result")
	}
}
