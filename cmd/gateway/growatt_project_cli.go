package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	canbus "github.com/Project-Helianthus/helianthus-canbus"
	canbusreg "github.com/Project-Helianthus/helianthus-canbusreg"
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

// Native boundaries: PCS Profile selection / Data validity and consumer
// obligations; LINK Identify the peer before interpreting version-dependent
// fields / Transport, addressing and data validity. Context is operator input.
func bindGrowattProjectFlags(fs *flag.FlagSet, cfg *ebusgateway.Config) {
	for _, family := range []struct {
		name   string
		config *ebusgateway.GrowattProjectCANConfig
	}{
		{"pcs", &cfg.GrowattPCSCANConfig}, {"link", &cfg.GrowattLINKCANConfig},
	} {
		c, prefix := family.config, "growatt-bms-"+family.name+"-can-"
		fs.BoolVar(&c.Enabled, prefix+"enabled", c.Enabled, "enable this independent receive-only family (disabled by default)")
		fs.StringVar(&c.Interface, prefix+"interface", c.Interface, "explicit SocketCAN interface; enabled CAN families require distinct interfaces")
		fs.BoolVar(&c.ListenOnlyConfirmed, prefix+"listen-only-confirmed", c.ListenOnlyConfirmed, "record external controller listen-only confirmation; does not set or verify mode or physical acknowledgement suppression")
		fs.DurationVar(&c.StaleAfter, prefix+"stale-after", c.StaleAfter, "positive native cache maximum age; required when enabled")
		fs.StringVar(&c.ProjectCode, prefix+"project-code", c.ProjectCode, "exact-case project code; LINK default only for use_default entries")
		fs.StringVar(&c.Release, prefix+"release", c.Release, "explicit four-digit release; LINK default only for use_default entries")
		fs.StringVar(&c.NativeProfile, prefix+"native-profile", c.NativeProfile, "explicit PCS profile: standard, extended19, parallel (LINK peer profile)")
		fs.StringVar(&c.Scope, prefix+"scope", c.Scope, "explicit local or aggregate scope; LINK requires local")
		fs.StringVar(&c.Bank, prefix+"bank", c.Bank, "explicit ordinary or parallel bank matching the PCS profile")
		fs.StringVar(&c.Context, prefix+"context", c.Context, "strict JSON array of explicit route and association entries, one per selection; all keys required; see docs/growatt-can-project-runtime-v1.md Gateway flags; no traffic-derived defaults; offline classifier: go run ./cmd/growatt-can-classify -input synthetic.txt or -input - for stdin")
	}
	fs.StringVar(&cfg.GrowattPCSCANConfig.NativeContract, "growatt-bms-pcs-can-native-contract", cfg.GrowattPCSCANConfig.NativeContract, "legacy19 or release11 when the explicit PCS selection requires disambiguation")
	fs.StringVar(&cfg.GrowattLINKCANConfig.SourceSelectors, "growatt-bms-link-can-source-selectors", cfg.GrowattLINKCANConfig.SourceSelectors, `one ordered JSON array, 1..32 entries: [{"source":1,"project_code":"QAAA","release":"0011","contract":"release11"}] or [{"source":1,"use_default":true,"contract":"release11"}]; sources 1..254 except 244,245; no omitted source inherits defaults`)
}

type growattProjectGate struct {
	Configured bool   `json:"configured"`
	Valid      bool   `json:"valid"`
	Status     string `json:"status"`
	Age        string `json:"age"`
}
type growattProjectVectorContext struct {
	Count      int                   `json:"count"`
	CountValid bool                  `json:"count_valid"`
	Gates      [4]growattProjectGate `json:"gates"`
}
type growattProjectContext struct {
	Source             uint8                       `json:"source"`
	InterfaceIndex     int                         `json:"interface_index"`
	Destination        uint8                       `json:"destination"`
	Endpoint           string                      `json:"endpoint"`
	Group              string                      `json:"group"`
	Slot               string                      `json:"slot"`
	Epoch              uint64                      `json:"epoch"`
	SerialSlot         uint8                       `json:"serial_slot"`
	SerialSlotValid    bool                        `json:"serial_slot_valid"`
	NativeSlot         uint8                       `json:"native_slot"`
	NativeGroup        uint8                       `json:"native_group"`
	SlotValid          bool                        `json:"slot_valid"`
	GroupValid         bool                        `json:"group_valid"`
	PublisherResolved  bool                        `json:"publisher_resolved"`
	MeasurementValid   bool                        `json:"measurement_valid"`
	TemperatureValid   bool                        `json:"temperature_valid"`
	TopologyAssociated bool                        `json:"topology_associated"`
	FieldAge           string                      `json:"field_age"`
	Cells              growattProjectVectorContext `json:"cells"`
	Sensors            growattProjectVectorContext `json:"sensors"`
}

// Reject duplicate keys at every depth, nulls and trailing values before typed
// decoding. Required exact-case keys also keep absent assertions distinct from
// explicitly false validity and zero values.
func growattProjectJSONValue(d *json.Decoder) (any, error) {
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch t {
	case json.Delim('{'):
		m := map[string]any{}
		for d.More() {
			k, err := d.Token()
			if err != nil {
				return nil, err
			}
			key, ok := k.(string)
			if !ok {
				return nil, errors.New("context object key required")
			}
			if _, exists := m[key]; exists {
				return nil, fmt.Errorf("duplicate context key %q", key)
			}
			v, err := growattProjectJSONValue(d)
			if err != nil {
				return nil, err
			}
			m[key] = v
		}
		_, err = d.Token()
		return m, err
	case json.Delim('['):
		a := []any{}
		for d.More() {
			v, err := growattProjectJSONValue(d)
			if err != nil {
				return nil, err
			}
			a = append(a, v)
		}
		_, err = d.Token()
		return a, err
	case nil:
		return nil, errors.New("null context assertion")
	default:
		if _, delim := t.(json.Delim); delim {
			return nil, errors.New("invalid context delimiter")
		}
		return t, nil
	}
}
func growattProjectRequired(v any, typ reflect.Type, path string) error {
	switch typ.Kind() {
	case reflect.Struct:
		m, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("%s requires an object", path)
		}
		if len(m) != typ.NumField() { // Name the missing field before unknown keys.
			for i := 0; i < typ.NumField(); i++ {
				key := typ.Field(i).Tag.Get("json")
				if _, ok := m[key]; !ok {
					return fmt.Errorf("%s.%s required", path, key)
				}
			}
			return fmt.Errorf("%s has unknown keys", path)
		}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			key := f.Tag.Get("json")
			item, ok := m[key]
			if !ok {
				return fmt.Errorf("%s.%s required", path, key)
			}
			if err := growattProjectRequired(item, f.Type, path+"."+key); err != nil {
				return err
			}
		}
	case reflect.Array:
		a, ok := v.([]any)
		if !ok || len(a) != typ.Len() {
			return fmt.Errorf("%s requires exactly %d entries", path, typ.Len())
		}
		for i, item := range a {
			if err := growattProjectRequired(item, typ.Elem(), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}
func parseGrowattProjectContexts(value string) ([]growattProjectContext, error) {
	if len(value) > 131072 {
		return nil, errors.New("context exceeds input bound")
	}
	d := json.NewDecoder(strings.NewReader(value))
	d.UseNumber()
	v, err := growattProjectJSONValue(d)
	if err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, errors.New("trailing context JSON")
	}
	a, ok := v.([]any)
	if !ok || len(a) < 1 || len(a) > 32 {
		return nil, errors.New("context requires 1..32 entries")
	}
	for i, item := range a {
		if err = growattProjectRequired(item, reflect.TypeOf(growattProjectContext{}), fmt.Sprintf("context[%d]", i)); err != nil {
			return nil, err
		}
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	var result []growattProjectContext
	if err = json.Unmarshal(raw, &result); err != nil {
		return nil, errors.New("context assertion has invalid type or range")
	}
	return result, nil
}
func growattProjectAge(value string) (time.Duration, error) {
	age, err := time.ParseDuration(value)
	if err != nil || age < 0 {
		return 0, errors.New("explicit nonnegative assertion age required")
	}
	return age, nil
}
func growattProjectAged(age, elapsed time.Duration) time.Duration {
	const maxAge = time.Duration(1<<63 - 1)
	if elapsed > maxAge-age {
		return maxAge
	}
	return age + elapsed
}
func (c growattProjectContext) input(s canbusreg.Selection) (canbusreg.ReplayInput, error) {
	n := s.SelectionSpecValue()
	age, err := growattProjectAge(c.FieldAge)
	if err != nil {
		return canbusreg.ReplayInput{}, err
	}
	var in canbusreg.ReplayInput
	in.Selection = s
	in.PCS.LINKInput = canbusreg.LINKInput{Epoch: n.Epoch, Direction: n.Direction, Scope: n.Scope, Bank: n.Bank, Endpoint: n.Endpoint, Group: n.Group, Slot: n.Slot, FieldAge: age}
	in.PCS.SerialSlot, in.PCS.SerialSlotValid = c.SerialSlot, c.SerialSlotValid
	in.PCS.NativeSlot, in.PCS.NativeGroup = c.NativeSlot, c.NativeGroup
	in.PCS.SlotValid, in.PCS.GroupValid = c.SlotValid, c.GroupValid
	in.PCS.PublisherResolved, in.PCS.MeasurementValid, in.PCS.TemperatureValid = c.PublisherResolved, c.MeasurementValid, c.TemperatureValid
	in.TopologyAssociated = c.TopologyAssociated
	in.CellCount, in.CellCountValid = c.Cells.Count, c.Cells.CountValid
	in.SensorCount, in.SensorCountValid = c.Sensors.Count, c.Sensors.CountValid
	for _, v := range []struct {
		context growattProjectVectorContext
		target  *[4]canbusreg.MeasurementGate
		maximum int
	}{{c.Cells, &in.CellGates, 16}, {c.Sensors, &in.SensorGates, 8}} {
		if v.context.Count < 0 || v.context.Count > v.maximum || (v.context.CountValid && v.context.Count == 0) {
			return in, errors.New("context count outside software bounds")
		}
		for j, g := range v.context.Gates {
			age, err := growattProjectAge(g.Age)
			if err != nil {
				return in, err
			}
			switch g.Status {
			case "", "absent", "invalid", "unsupported", "stale", "conflict":
			default:
				return in, errors.New("unknown measurement gate status")
			}
			v.target[j] = canbusreg.MeasurementGate{Configured: g.Configured, Valid: g.Valid, Status: g.Status, Age: age}
		}
	}
	return in, nil
}

type growattProjectAdmission struct {
	pcs        ebusgateway.GrowattPCSConfig
	link       ebusgateway.GrowattLINKConfig
	selections []canbusreg.Selection
	associate  growattProjectAssociation
}

func admitGrowattProject(c ebusgateway.GrowattProjectCANConfig, link bool) (growattProjectAdmission, error) {
	var a growattProjectAdmission
	if !c.Enabled {
		if c != (ebusgateway.GrowattProjectCANConfig{}) {
			return a, errors.New("disabled family has active fields")
		}
		return a, nil
	}
	if !c.ListenOnlyConfirmed {
		return a, errors.New("listen-only-confirmed required (external precondition only)")
	}
	if c.StaleAfter <= 0 {
		return a, errors.New("positive stale-after required")
	}
	if c.Scope != "local" && c.Scope != "aggregate" {
		return a, errors.New("explicit scope required")
	}
	flavor := ""
	switch c.NativeProfile {
	case "standard":
		flavor = "pcs-standard"
	case "extended19":
		flavor = "pcs-extended19"
	case "parallel":
		flavor = "pcs-parallel"
	default:
		return a, errors.New("explicit native-profile standard, extended19 or parallel required")
	}
	if (flavor == "pcs-parallel" && c.Bank != "parallel") || (flavor != "pcs-parallel" && c.Bank != "ordinary") {
		return a, errors.New("explicit matching bank required")
	}
	contexts, err := parseGrowattProjectContexts(c.Context)
	if err != nil {
		return a, err
	}
	var entries []ebusgateway.GrowattLINKSourceSelector
	if link {
		if c.NativeContract != "" || c.Scope != "local" {
			return a, errors.New("LINK requires local scope and per-source contract")
		}
		entries, err = ebusgateway.ParseGrowattLINKSourceSelectors(c.SourceSelectors, c.ProjectCode, c.Release)
		if err != nil {
			return a, err
		}
	} else {
		if c.SourceSelectors != "" {
			return a, errors.New("PCS cannot contain LINK source selectors")
		}
		entries = []ebusgateway.GrowattLINKSourceSelector{{Source: contexts[0].Source, ProjectCode: c.ProjectCode, Release: c.Release, Contract: c.NativeContract}}
	}
	if len(entries) != len(contexts) {
		return a, errors.New("one explicit context per selection required")
	}
	inputs := map[canbusreg.Selection]canbusreg.ReplayInput{}
	for i, entry := range entries {
		context := contexts[i]
		if context.Source != entry.Source {
			return a, fmt.Errorf("context[%d].source must match ordered selection", i)
		}
		if context.InterfaceIndex != contexts[0].InterfaceIndex {
			return a, errors.New("one explicit interface index required")
		}
		id, err := canbus.NewInterfaceIdentity(c.Interface, context.InterfaceIndex)
		if err != nil {
			return a, errors.New("explicit interface and positive interface_index required")
		}
		if context.Endpoint == "" || context.Group == "" || context.Slot == "" || context.Epoch == 0 {
			return a, errors.New("explicit endpoint, group, slot and positive epoch required")
		}
		n := canbusreg.SelectionSpec{Interface: id, Flavor: flavor, ProjectCode: entry.ProjectCode, Release: entry.Release, Choice: entry.Contract, Direction: "bms-to-pcs", Scope: c.Scope, Bank: c.Bank, Endpoint: context.Endpoint, Group: context.Group, Slot: context.Slot, Epoch: context.Epoch, RouteSet: true, Source: context.Source, Destination: context.Destination}
		if link {
			n.Flavor = "link"
			n.PCSFlavor = flavor
			n.Direction = "module-to-coordinator"
		}
		var selection canbusreg.Selection
		if link {
			// The accepted config validates the complete source map and route before open.
			one := ebusgateway.GrowattLINKConfig{Enabled: true, ListenOnlyConfirmed: c.ListenOnlyConfirmed, StaleAfter: c.StaleAfter, Native: n}
			one.Native.Source = 0
			raw, _ := json.Marshal([]map[string]any{{"source": entry.Source, "project_code": entry.ProjectCode, "release": entry.Release, "contract": entry.Contract}})
			one.SourceSelectors = string(raw)
			selected, err := one.Selections()
			if err != nil {
				return a, fmt.Errorf("context[%d]: %w", i, err)
			}
			selection = selected[0]
			if i == 0 {
				a.link = one
			}
			// The factory has a common route/association identity. Refuse heterogeneous
			// routes rather than replacing per-source assertions with the first entry.
			base := a.link.Native
			candidate := n
			candidate.Source = 0
			candidate.ProjectCode = base.ProjectCode
			candidate.Release = base.Release
			candidate.Choice = base.Choice
			if candidate != base {
				return a, errors.New("LINK selections require one common route and association identity")
			}
		} else {
			a.pcs = ebusgateway.GrowattPCSConfig{Enabled: true, ListenOnlyConfirmed: c.ListenOnlyConfirmed, StaleAfter: c.StaleAfter, Native: n}
			selection, err = a.pcs.Selection()
			if err != nil {
				return a, err
			}
		}
		in, err := context.input(selection)
		if err != nil {
			return a, fmt.Errorf("context[%d]: %w", i, err)
		}
		inputs[selection] = in
		a.selections = append(a.selections, selection)
	}
	if link {
		a.link.SourceSelectors = c.SourceSelectors
		a.link.Native.ProjectCode = c.ProjectCode
		a.link.Native.Release = c.Release
		a.link.Native.Choice = ""
	}
	origin := time.Now()
	a.associate = func(s canbusreg.Selection, e canbusreg.Evidence) canbusreg.ReplayInput {
		in, ok := inputs[s]
		if !ok {
			return canbusreg.ReplayInput{}
		}
		n := s.SelectionSpecValue()
		in.PCS.Evidence = e
		in.PCS.Epoch = n.Epoch
		elapsed := time.Since(origin)
		in.PCS.FieldAge = growattProjectAged(in.PCS.FieldAge, elapsed)
		for i := range in.CellGates {
			in.CellGates[i].Age = growattProjectAged(in.CellGates[i].Age, elapsed)
			in.SensorGates[i].Age = growattProjectAged(in.SensorGates[i].Age, elapsed)
		}
		return in
	}
	return a, nil
}

func validateGrowattCANInterfaces(cfg ebusgateway.Config) error {
	// Preserve the standalone V1.04 path; a new combined selection must validate
	// its existing sibling before either new receiver can open.
	if cfg.GrowattPCSCANConfig.Enabled || cfg.GrowattLINKCANConfig.Enabled {
		c := cfg.GrowattBMSCANConfig
		if !c.Enabled && c != (ebusgateway.GrowattBMSCANConfig{}) {
			return errGrowattBMSCANDisabledConfig
		}
		if c.Enabled && (!validGrowattBMSCANInterfaceName(c.Interface) || c.StaleAfter <= 0) {
			return errGrowattBMSCANConfig
		}
		if c.Enabled && !c.ListenOnlyConfirmed {
			return errGrowattBMSCANListenOnly
		}
	}

	seen := map[string]bool{}
	for _, c := range []struct {
		enabled bool
		name    string
	}{{cfg.GrowattBMSCANConfig.Enabled, cfg.GrowattBMSCANConfig.Interface}, {cfg.GrowattPCSCANConfig.Enabled, cfg.GrowattPCSCANConfig.Interface}, {cfg.GrowattLINKCANConfig.Enabled, cfg.GrowattLINKCANConfig.Interface}} {
		if !c.enabled {
			continue
		}
		if seen[c.name] {
			return errors.New("enabled V1.04, PCS and LINK require distinct interfaces")
		}
		seen[c.name] = true
	}
	return nil
}
