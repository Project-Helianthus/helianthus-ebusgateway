// Package semanticmetrics renders bounded semantic gauges without acquisition.
package semanticmetrics

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/infrastructure"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

// Binding is a configured resource/service/dimension tuple, never a label source.
// The collector must persist assignments before publication, reserve retired slots
// forever for its target, and revalidate the original registry after restart.
type Binding struct {
	Domain           string
	Slot             int
	AssetID          semreg.AssetID
	SourceID         semreg.SourceID
	SourceEpochID    semreg.SourceEpochID
	BindingID        semreg.NativeBindingID
	DriverGeneration semreg.Uint64
	ServiceInstance  semreg.ServiceInstanceID
	Dimensions       []semreg.Dimension
	Fields           []semreg.DefinitionID
	Retired          bool
}

// Registry carries an externally persisted registry's validation result. A
// missing/corrupt registry must set Validated false; the renderer never allocates
// or reuses slots. Configured availability remains zero until revalidation.
type Registry struct {
	Validated bool
	Bindings  []Binding
}

type metric struct {
	field      semreg.DefinitionID
	name, help string
	unit       semreg.DefinitionID
	factor     float64 // Divisor, except kilowatt-hours use a multiplier.
	symbols    []string
}

var metrics = []metric{
	{"thermal.action.state", "helianthus_thermal_action_state", "thermal.action.state (enum: idle=0, heating=1, cooling=2, dhw=3, ventilating=4).", "", 1, []string{"idle", "heating", "cooling", "dhw", "ventilating"}},
	{"thermal.demand.level", "helianthus_thermal_demand_ratio", "thermal.demand.level (ratio; percent / 100).", "unit.percent", 100, []string{}},
	{"thermal.measurement.air_temperature", "helianthus_thermal_air_temperature_celsius", "thermal.measurement.air_temperature (Celsius; identity).", "unit.celsius", 1, []string{}},
	{"thermal.measurement.flow_rate", "helianthus_thermal_flow_cubic_meters_per_second", "thermal.measurement.flow_rate (m^3/s; litre/minute / 60,000).", "unit.litre_per_minute", 60000, []string{}},
	{"thermal.measurement.humidity_relative", "helianthus_thermal_relative_humidity_ratio", "thermal.measurement.humidity_relative (ratio; percent / 100).", "unit.percent", 100, []string{}},
	{"thermal.measurement.power", "helianthus_thermal_power_watts", "thermal.measurement.power (watts; preserve sign).", "unit.watt", 1, []string{}},
	{"thermal.measurement.water_temperature", "helianthus_thermal_water_temperature_celsius", "thermal.measurement.water_temperature (Celsius; identity).", "unit.celsius", 1, []string{}},
	{"thermal.mode.system", "helianthus_thermal_system_mode", "thermal.mode.system (enum: off=0, auto=1, heat=2, cool=3).", "", 1, []string{"off", "auto", "heat", "cool"}},
	{"thermal.mode.zone", "helianthus_thermal_zone_mode", "thermal.mode.zone (enum: off=0, auto=1, comfort=2, eco=3).", "", 1, []string{"off", "auto", "comfort", "eco"}},
	{"thermal.setpoint.dhw_temperature", "helianthus_thermal_dhw_setpoint_celsius", "thermal.setpoint.dhw_temperature (Celsius; identity; read-only observation).", "unit.celsius", 1, []string{}},
	{"thermal.setpoint.temperature", "helianthus_thermal_setpoint_celsius", "thermal.setpoint.temperature (Celsius; identity; read-only observation).", "unit.celsius", 1, []string{}},
	{"thermal.status.fault", "helianthus_thermal_fault_present", "thermal.status.fault (boolean: clear=0, present=1).", "", 1, []string{"clear", "present"}},
	{"thermal.status.operation", "helianthus_thermal_operation_active", "thermal.status.operation (boolean: idle=0, active=1).", "", 1, []string{"idle", "active"}},
	{"infrastructure.ac.active_power", "helianthus_infrastructure_ac_active_power_watts", "infrastructure.ac.active_power (watts; preserve sign).", "unit.watt", 1, []string{}},
	{"infrastructure.ac.apparent_power", "helianthus_infrastructure_ac_apparent_power_volt_amperes", "infrastructure.ac.apparent_power (VA; identity).", "unit.volt_ampere", 1, []string{}},
	{"infrastructure.ac.current", "helianthus_infrastructure_ac_current_amperes", "infrastructure.ac.current (amperes; identity).", "unit.ampere", 1, []string{}},
	{"infrastructure.ac.frequency", "helianthus_infrastructure_ac_frequency_hertz", "infrastructure.ac.frequency (hertz; identity).", "unit.hertz", 1, []string{}},
	{"infrastructure.ac.power_factor", "helianthus_infrastructure_ac_power_factor_ratio", "infrastructure.ac.power_factor (ratio; preserve sign).", "unit.ratio", 1, []string{}},
	{"infrastructure.ac.reactive_power", "helianthus_infrastructure_ac_reactive_power_vars", "infrastructure.ac.reactive_power (var; preserve sign).", "unit.volt_ampere_reactive", 1, []string{}},
	{"infrastructure.ac.voltage", "helianthus_infrastructure_ac_voltage_volts", "infrastructure.ac.voltage (volts; identity).", "unit.volt", 1, []string{}},
	{"infrastructure.energy.export", "helianthus_infrastructure_energy_export_joules", "infrastructure.energy.export (joules; kWh x 3,600,000; gauge).", "unit.kilowatt_hour", 3600000, []string{}},
	{"infrastructure.energy.import", "helianthus_infrastructure_energy_import_joules", "infrastructure.energy.import (joules; kWh x 3,600,000; gauge).", "unit.kilowatt_hour", 3600000, []string{}},
	{"infrastructure.power.apparent", "helianthus_infrastructure_apparent_power_volt_amperes", "infrastructure.power.apparent (VA; identity).", "unit.volt_ampere", 1, []string{}},
	{"infrastructure.power.export_active", "helianthus_infrastructure_export_active_power_watts", "infrastructure.power.export_active (watts; nonnegative directional value).", "unit.watt", 1, []string{}},
	{"infrastructure.power.export_reactive", "helianthus_infrastructure_export_reactive_power_vars", "infrastructure.power.export_reactive (var; nonnegative directional value).", "unit.volt_ampere_reactive", 1, []string{}},
	{"infrastructure.power.import_active", "helianthus_infrastructure_import_active_power_watts", "infrastructure.power.import_active (watts; nonnegative directional value).", "unit.watt", 1, []string{}},
	{"infrastructure.power.import_reactive", "helianthus_infrastructure_import_reactive_power_vars", "infrastructure.power.import_reactive (var; nonnegative directional value).", "unit.volt_ampere_reactive", 1, []string{}},
	{"infrastructure.status.breaker", "helianthus_infrastructure_breaker_open", "infrastructure.status.breaker (boolean: closed=0, open=1).", "", 1, []string{"closed", "open"}},
	{"infrastructure.status.connection", "helianthus_infrastructure_connection_connected", "infrastructure.status.connection (boolean: disconnected=0, connected=1).", "", 1, []string{"disconnected", "connected"}},
	{"infrastructure.status.fault", "helianthus_infrastructure_fault_present", "infrastructure.status.fault (boolean: clear=0, present=1).", "", 1, []string{"clear", "present"}},
	{"infrastructure.status.interlock", "helianthus_infrastructure_interlock_active", "infrastructure.status.interlock (boolean: clear=0, active=1).", "", 1, []string{"clear", "active"}},
	{"infrastructure.status.readiness", "helianthus_infrastructure_ready", "infrastructure.status.readiness (boolean: not_ready=0, ready=1).", "", 1, []string{"not_ready", "ready"}},
}

const availabilityName = "helianthus_semantic_field_available"

// Render uses one detached M08 view for all samples. The caller must not mutate
// its inputs during the call. No provider, device, process clock or global
// registration is consulted. Malformed configuration fails before any text is
// returned; invalid/withheld selections omit values and emit availability zero.
func Render(view semanticview.View, registry Registry) (string, error) {
	metadata, err := packs.NewMetadataRegistry()
	if err != nil {
		return "", err
	}
	bindings := append([]Binding(nil), registry.Bindings...)
	slots, tuples := map[string]bool{}, map[string]bool{}
	for _, b := range bindings {
		if (b.Domain != "thermal" && b.Domain != "infrastructure") || b.Slot < 0 || b.Slot > 15 ||
			b.AssetID.Validate() != nil || b.SourceID.Validate() != nil || b.SourceEpochID.Validate() != nil ||
			b.BindingID.Validate() != nil || b.DriverGeneration.Validate() != nil || b.ServiceInstance.Validate() != nil || len(b.Fields) == 0 || len(b.Dimensions) != 1 {
			return "", fmt.Errorf("invalid semantic metric binding")
		}
		slot := fmt.Sprintf("%s/%d", b.Domain, b.Slot)
		tuple := b
		tuple.Slot = 0
		tuple.Fields = nil
		tuple.Retired = false
		raw, err := json.Marshal(tuple)
		if err != nil || slots[slot] || tuples[string(raw)] {
			return "", fmt.Errorf("ambiguous semantic metric binding")
		}
		slots[slot], tuples[string(raw)] = true, true
		fields := map[semreg.DefinitionID]bool{}
		for _, field := range b.Fields {
			if _, ok := findMetric(field); !ok || !strings.HasPrefix(string(field), b.Domain+".") || fields[field] {
				return "", fmt.Errorf("invalid semantic metric field")
			}
			fields[field] = true
			key := semreg.FactKey{PackID: semreg.DefinitionID("helianthus.pack." + b.Domain), PackVersion: "1.0.0", FactID: field, Dimensions: b.Dimensions}
			if key.Validate() != nil {
				return "", fmt.Errorf("invalid semantic metric dimensions")
			}
		}
	}
	sort.Slice(bindings, func(i, j int) bool {
		if bindings[i].Domain != bindings[j].Domain {
			return bindings[i].Domain < bindings[j].Domain
		}
		return bindings[i].Slot < bindings[j].Slot
	})
	// Rebuild to validate snapshot/evaluation correspondence and discard mutable
	// derived projections supplied by a caller. Failure withholds every value.
	inputs := make([]semanticview.Input, 0, len(view.Assets))
	for _, a := range view.Assets {
		inputs = append(inputs, semanticview.Input{AssetID: a.AssetID, Snapshot: a.Snapshot, Evaluation: a.Evaluation, Selections: a.Selections, UnavailableReason: a.UnavailableReason})
	}
	detached, buildErr := semanticview.Build(metadata, inputs)
	assets := map[semreg.AssetID]semanticview.Asset{}
	if buildErr == nil {
		for _, a := range detached.Assets {
			assets[a.AssetID] = a
		}
	}
	var values, availability strings.Builder
	fmt.Fprintf(&availability, "# HELP %s Whether a configured semantic field is current, admitted, qualified, valid and fresh (0 or 1).\n# TYPE %s gauge\n", availabilityName, availabilityName)
	for _, m := range metrics {
		var samples strings.Builder
		for _, b := range bindings {
			configured := false
			for _, field := range b.Fields {
				configured = configured || field == m.field
			}
			if !configured {
				continue
			}
			value, ok := sample(assets[b.AssetID], b, m, metadata)
			ok = ok && registry.Validated && !b.Retired
			available := 0
			if ok {
				available = 1
				fmt.Fprintf(&samples, "%s{slot=\"%d\"} %s\n", m.name, b.Slot, strconv.FormatFloat(value, 'g', -1, 64))
			}
			fmt.Fprintf(&availability, "%s{domain=\"%s\",field=\"%s\",slot=\"%d\"} %d\n", availabilityName, b.Domain, m.field, b.Slot, available)
		}
		if samples.Len() > 0 {
			fmt.Fprintf(&values, "# HELP %s %s\n# TYPE %s gauge\n%s", m.name, m.help, m.name, samples.String())
		}
	}
	return values.String() + availability.String(), nil
}

func findMetric(field semreg.DefinitionID) (metric, bool) {
	for _, m := range metrics {
		if m.field == field {
			return m, true
		}
	}
	return metric{}, false
}

// projectionExact consumes the shared view's disposition rather than deriving
// public exactness from an individual candidate or native capability instance.
func projectionExact(a semanticview.Asset, kind projection.ItemKind, id semreg.DefinitionID) bool {
	if a.Projection != nil {
		for _, d := range a.Projection.Dispositions {
			if d.Kind == kind && d.ItemID == id {
				return d.Outcome == projection.ProjectionExact
			}
		}
	}
	return false
}

func sample(a semanticview.Asset, b Binding, m metric, metadata *semreg.PackMetadataRegistry) (float64, bool) {
	if a.UnavailableReason != "" || a.Snapshot == nil || a.Evaluation == nil || !projectionExact(a, projection.ItemFact, m.field) {
		return 0, false
	}
	s, e := a.Snapshot, a.Evaluation
	sourceOK, bindingOK := false, false
	for _, source := range s.Sources {
		if source.SourceID == b.SourceID && source.SourceEpochID == b.SourceEpochID && source.State == semreg.SourceCurrent {
			sourceOK = true
		}
	}
	for _, binding := range s.Bindings {
		if binding.BindingID == b.BindingID && binding.AssetID == b.AssetID && binding.SourceID == b.SourceID && binding.SourceEpochID == b.SourceEpochID && binding.DriverGeneration == b.DriverGeneration && binding.State == semreg.BindingCurrent {
			bindingOK = true
		}
	}
	if !sourceOK || !bindingOK {
		return 0, false
	}
	pack := semreg.PackRef{ID: semreg.DefinitionID("helianthus.pack." + b.Domain), Version: "1.0.0"}

	serviceOK := false
	for _, service := range s.Services {
		if service.InstanceID != b.ServiceInstance || service.BindingID != b.BindingID || service.SourceEpochID != b.SourceEpochID || service.DriverGeneration != b.DriverGeneration || service.Qualification != semreg.QualificationQualified || service.Availability != semreg.AvailabilityAvailable {
			continue
		}
		for _, cap := range s.Capabilities {
			if cap.ServiceInstance == service.InstanceID && cap.BindingID == b.BindingID && cap.SourceEpochID == b.SourceEpochID && cap.DriverGeneration == b.DriverGeneration && cap.Qualification == semreg.QualificationQualified && cap.Availability == semreg.AvailabilityAvailable && service.Definition.Pack == pack && metadata.ServiceOwnsCapability(service.Definition, cap.Definition) && strings.HasPrefix(string(cap.Definition.ID), b.Domain+".capability.read.") && projectionExact(a, projection.ItemCapability, cap.Definition.ID) {
				serviceOK = true
			}
		}
	}
	if !serviceOK {
		return 0, false
	}
	key := semreg.FactKey{PackID: pack.ID, PackVersion: pack.Version, FactID: m.field, Dimensions: b.Dimensions}
	for _, fact := range s.Facts {
		if !reflect.DeepEqual(fact.Key, key) {
			continue
		}
		if len(fact.Conflicts) != 0 || len(fact.Candidates) != 1 {
			return 0, false
		}
		c := fact.Candidates[0]
		if c.BindingID == nil || *c.BindingID != b.BindingID || c.SourceEpochID == nil || *c.SourceEpochID != b.SourceEpochID || c.DriverGeneration == nil || *c.DriverGeneration != b.DriverGeneration || c.Value == nil || c.Quality.Qualification != semreg.QualificationQualified || c.Quality.Promotion != semreg.PromotionPromoted || c.Quality.Validity != semreg.ValidityGood {
			return 0, false
		}
		fresh := false
		for _, v := range e.Facts {
			if v.CandidateID == c.CandidateID && v.CandidateRevision == c.Revision && v.Freshness == semreg.FreshnessFresh && v.EffectiveAvailability == semreg.AvailabilityAvailable {
				fresh = true
			}
		}
		if !fresh {
			return 0, false
		}
		for _, reason := range c.Quality.Reasons {
			if reason == "unknown_pending_std_01" {
				return 0, false
			}
		}
		validator := thermal.New()
		if b.Domain == "infrastructure" {
			validator = infrastructure.New()
		}
		if validator.ValidateFact(key, c.Value) != nil {
			return 0, false
		}
		if m.unit == "" {
			if c.Value.Symbol == nil || !c.Value.Symbol.Known {
				return 0, false
			}
			for i, symbol := range m.symbols {
				if c.Value.Symbol.Token == symbol {
					return float64(i), true
				}
			}
			return 0, false
		}
		q := c.Value.Quantity
		if q == nil || q.Unit != m.unit {
			return 0, false
		}
		number, err := strconv.ParseFloat(q.Number.Coefficient+"e"+strconv.FormatInt(int64(q.Number.Exponent10), 10), 64)
		if m.unit == "unit.kilowatt_hour" {
			number *= m.factor
		} else {
			number /= m.factor
		}
		return number, err == nil && !math.IsNaN(number) && !math.IsInf(number, 0)
	}
	return 0, false
}
