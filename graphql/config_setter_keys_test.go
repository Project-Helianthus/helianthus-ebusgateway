package graphql

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
)

// configSetterKeysDoc mirrors docs/testing/config-setter-keys.json, the
// consumer fixture of the accepted config setter keys.
type configSetterKeysDoc struct {
	Format      string                        `json:"format"`
	Description string                        `json:"description"`
	Setters     map[string]configSetterKeySet `json:"setters"`
}

type configSetterKeySet struct {
	Service string               `json:"service"`
	Keys    []configSetterKeyRow `json:"keys"`
}

type configSetterKeyRow struct {
	Key          string            `json:"key"`
	Selector     configSetterSel   `json:"selector"`
	ValueType    string            `json:"value_type"`
	Enum         map[string]uint16 `json:"enum,omitempty"`
	Open         bool              `json:"open"`
	OpenQuestion string            `json:"open_question,omitempty"`
}

type configSetterSel struct {
	Opcode    string   `json:"opcode,omitempty"`
	Group     string   `json:"group,omitempty"`
	Instance  string   `json:"instance,omitempty"`
	Registers []string `json:"registers"`
}

const configSetterKeysPath = "../docs/testing/config-setter-keys.json"

func loadConfigSetterKeys(t *testing.T) configSetterKeysDoc {
	t.Helper()
	raw, err := os.ReadFile(configSetterKeysPath)
	if err != nil {
		t.Fatalf("read %s: %v", configSetterKeysPath, err)
	}
	var doc configSetterKeysDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode %s: %v", configSetterKeysPath, err)
	}
	return doc
}

func configValueTypeName(valueType configValueType) string {
	switch valueType {
	case configValueFloat32:
		return "float32"
	case configValueUint16:
		return "uint16"
	case configValueBoolU8:
		return "bool_u8"
	case configValueEnumU16:
		return "enum_u16"
	case configValueCString:
		return "cstring"
	case configValueDateHDA3:
		return "date_hda3"
	default:
		return fmt.Sprintf("unknown(%d)", valueType)
	}
}

// indexedSetterRows renders the circuit and zone setter specs as fixture rows:
// the instance is the mutation's index argument.
func indexedSetterRows(specs map[string]configFieldSpec) []configSetterKeyRow {
	keys := make([]string, 0, len(specs))
	for key := range specs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]configSetterKeyRow, 0, len(keys))
	for _, key := range keys {
		spec := specs[key]
		rows = append(rows, configSetterKeyRow{
			Key: key,
			Selector: configSetterSel{
				Opcode:    fmt.Sprintf("0x%02X", mutationB524OpcodeLocal),
				Group:     fmt.Sprintf("0x%02X", spec.group),
				Instance:  "index",
				Registers: []string{fmt.Sprintf("0x%04X", spec.addr)},
			},
			ValueType: configValueTypeName(spec.valueType),
			Enum:      spec.enum,
		})
	}
	return rows
}

func diffConfigSetterKeySet(name string, want, got configSetterKeySet) error {
	if want.Service != got.Service {
		return fmt.Errorf("%s service = %q; setter specs give %q", name, got.Service, want.Service)
	}
	if len(want.Keys) != len(got.Keys) {
		return fmt.Errorf("%s has %d keys; setter specs have %d", name, len(got.Keys), len(want.Keys))
	}
	for i := range want.Keys {
		if !reflect.DeepEqual(want.Keys[i], got.Keys[i]) {
			return fmt.Errorf("%s key %d = %+v; setter specs give %+v", name, i, got.Keys[i], want.Keys[i])
		}
	}
	return nil
}

func TestConfigSetterKeysFixtureMatchesCircuitAndZoneSpecs(t *testing.T) {
	doc := loadConfigSetterKeys(t)
	for name, specs := range map[string]map[string]configFieldSpec{
		"set_circuit_config": circuitConfigFieldSpecs,
		"set_zone_config":    zoneConfigFieldSpecs,
	} {
		got, ok := doc.Setters[name]
		if !ok {
			t.Fatalf("fixture lacks %s", name)
		}
		want := configSetterKeySet{Service: "B524", Keys: indexedSetterRows(specs)}
		if err := diffConfigSetterKeySet(name, want, got); err != nil {
			t.Fatal(err)
		}
	}
}

// TestConfigSetterKeysFixtureComparisonCoversEveryField changes each field of
// each circuit and zone row in turn and requires the comparison to fail, so a
// spec change in any of key, opcode, group, instance, register, value type or
// enum mapping, or a wrong open marker, cannot pass unnoticed.
func TestConfigSetterKeysFixtureComparisonCoversEveryField(t *testing.T) {
	mutations := map[string]func(*configSetterKeyRow){
		"key":       func(row *configSetterKeyRow) { row.Key += "_x" },
		"opcode":    func(row *configSetterKeyRow) { row.Selector.Opcode = "0x00" },
		"group":     func(row *configSetterKeyRow) { row.Selector.Group = "0x7F" },
		"instance":  func(row *configSetterKeyRow) { row.Selector.Instance = "0x00" },
		"register":  func(row *configSetterKeyRow) { row.Selector.Registers = []string{"0xFFFF"} },
		"registers": func(row *configSetterKeyRow) { row.Selector.Registers = append(row.Selector.Registers, "0xFFFF") },
		"value":     func(row *configSetterKeyRow) { row.ValueType = "uint8" },
		"enum":      func(row *configSetterKeyRow) { row.Enum = map[string]uint16{"other": 9} },
		"open":      func(row *configSetterKeyRow) { row.Open = !row.Open },
	}
	for name, specs := range map[string]map[string]configFieldSpec{
		"set_circuit_config": circuitConfigFieldSpecs,
		"set_zone_config":    zoneConfigFieldSpecs,
	} {
		want := configSetterKeySet{Service: "B524", Keys: indexedSetterRows(specs)}
		for i := range want.Keys {
			for field, mutate := range mutations {
				got := configSetterKeySet{Service: want.Service, Keys: append([]configSetterKeyRow(nil), want.Keys...)}
				row := got.Keys[i]
				row.Selector.Registers = append([]string(nil), row.Selector.Registers...)
				mutate(&row)
				got.Keys[i] = row
				if err := diffConfigSetterKeySet(name, want, got); err == nil {
					t.Fatalf("%s row %s: changed %s not detected", name, want.Keys[i].Key, field)
				}
			}
		}
		if err := diffConfigSetterKeySet(name, want, configSetterKeySet{Service: "B509", Keys: want.Keys}); err == nil {
			t.Fatalf("%s: changed service not detected", name)
		}
		if err := diffConfigSetterKeySet(name, want, configSetterKeySet{Service: want.Service, Keys: want.Keys[1:]}); err == nil {
			t.Fatalf("%s: missing key not detected", name)
		}
	}
}

func TestConfigSetterKeysFixtureIsSortedAndComplete(t *testing.T) {
	doc := loadConfigSetterKeys(t)
	setters := make([]string, 0, len(doc.Setters))
	for name, set := range doc.Setters {
		setters = append(setters, name)
		keys := make([]string, 0, len(set.Keys))
		for _, row := range set.Keys {
			keys = append(keys, row.Key)
			if row.Open != (row.OpenQuestion != "") {
				t.Fatalf("%s key %s: open=%v with open_question %q", name, row.Key, row.Open, row.OpenQuestion)
			}
		}
		if !sort.StringsAreSorted(keys) {
			t.Fatalf("%s keys not sorted: %v", name, keys)
		}
	}
	sort.Strings(setters)
	if want := []string{"set_boiler_config", "set_circuit_config", "set_system_config", "set_zone_config"}; !reflect.DeepEqual(setters, want) {
		t.Fatalf("fixture setters = %v; want %v", setters, want)
	}
}
