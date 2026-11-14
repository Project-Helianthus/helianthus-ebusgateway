package graphql

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
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

// configSetterKeyRow is one fixture row. Key, selector, value type and enum
// come from the setter specs; provisional and open question come from the
// documentation.
type configSetterKeyRow struct {
	Key          string            `json:"key"`
	Selector     configSetterSel   `json:"selector"`
	ValueType    string            `json:"value_type"`
	Enum         map[string]uint16 `json:"enum,omitempty"`
	Provisional  bool              `json:"provisional"`
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

// specFields drops the documentation status of a row, which the setter specs
// do not carry.
func specFields(row configSetterKeyRow) configSetterKeyRow {
	row.Provisional = false
	row.OpenQuestion = ""
	return row
}

func diffConfigSetterKeySet(name string, want, got configSetterKeySet) error {
	if want.Service != got.Service {
		return fmt.Errorf("%s service = %q; setter specs give %q", name, got.Service, want.Service)
	}
	if len(want.Keys) != len(got.Keys) {
		return fmt.Errorf("%s has %d keys; setter specs have %d", name, len(got.Keys), len(want.Keys))
	}
	for i := range want.Keys {
		if !reflect.DeepEqual(want.Keys[i], specFields(got.Keys[i])) {
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

// TestConfigSetterKeysFixtureComparisonCoversEveryField changes each spec
// field of each circuit and zone row in turn and requires the comparison to
// fail, so a spec change in any of key, opcode, group, instance, register,
// value type or enum mapping cannot pass unnoticed.
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

// TestConfigSetterKeysFixtureRowShape requires every row to carry exactly the
// published members: key, selector, value_type, the boolean provisional, an
// enum only for enum keys and a non-empty open_question only where the
// documentation records one, which it always does for a provisional name.
func TestConfigSetterKeysFixtureRowShape(t *testing.T) {
	raw, err := os.ReadFile(configSetterKeysPath)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Setters map[string]struct {
			Keys []map[string]json.RawMessage `json:"keys"`
		} `json:"setters"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Setters) == 0 {
		t.Fatal("fixture has no setters")
	}
	for name, set := range doc.Setters {
		for i, row := range set.Keys {
			if err := checkConfigSetterRowShape(row); err != nil {
				t.Errorf("%s row %d: %v", name, i, err)
			}
		}
	}
}

func checkConfigSetterRowShape(row map[string]json.RawMessage) error {
	for member := range row {
		switch member {
		case "key", "selector", "value_type", "enum", "provisional", "open_question":
		default:
			return fmt.Errorf("unexpected member %q", member)
		}
	}
	for _, member := range []string{"key", "selector", "value_type", "provisional"} {
		if _, ok := row[member]; !ok {
			return fmt.Errorf("member %q missing", member)
		}
	}
	var provisional any
	if err := json.Unmarshal(row["provisional"], &provisional); err != nil {
		return err
	}
	isProvisional, ok := provisional.(bool)
	if !ok {
		return fmt.Errorf("provisional = %s; want a boolean", row["provisional"])
	}
	question := ""
	if rawQuestion, ok := row["open_question"]; ok {
		if err := json.Unmarshal(rawQuestion, &question); err != nil || strings.TrimSpace(question) == "" {
			return fmt.Errorf("open_question = %s; want a non-empty string when present", rawQuestion)
		}
	}
	if isProvisional && question == "" {
		return fmt.Errorf("provisional row without open_question")
	}
	var valueType string
	if err := json.Unmarshal(row["value_type"], &valueType); err != nil {
		return err
	}
	if _, hasEnum := row["enum"]; hasEnum != (valueType == "enum_u16") {
		return fmt.Errorf("enum present=%v for value type %q", hasEnum, valueType)
	}
	return nil
}

func TestConfigSetterKeysFixtureRowShapeRejectsEveryDefect(t *testing.T) {
	good := map[string]json.RawMessage{
		"key":           json.RawMessage(`"k"`),
		"selector":      json.RawMessage(`{"registers":["0x0001"]}`),
		"value_type":    json.RawMessage(`"uch"`),
		"provisional":   json.RawMessage(`true`),
		"open_question": json.RawMessage(`"q"`),
	}
	if err := checkConfigSetterRowShape(good); err != nil {
		t.Fatalf("good row rejected: %v", err)
	}
	defects := map[string]func(map[string]json.RawMessage){
		"former open member":      func(row map[string]json.RawMessage) { row["open"] = json.RawMessage(`false`) },
		"provisional missing":     func(row map[string]json.RawMessage) { delete(row, "provisional") },
		"provisional not boolean": func(row map[string]json.RawMessage) { row["provisional"] = json.RawMessage(`"true"`) },
		"question missing":        func(row map[string]json.RawMessage) { delete(row, "open_question") },
		"question empty":          func(row map[string]json.RawMessage) { row["open_question"] = json.RawMessage(`" "`) },
		"enum on non-enum":        func(row map[string]json.RawMessage) { row["enum"] = json.RawMessage(`{"a":1}`) },
		"key missing":             func(row map[string]json.RawMessage) { delete(row, "key") },
	}
	for name, mutate := range defects {
		row := map[string]json.RawMessage{}
		for member, value := range good {
			row[member] = value
		}
		mutate(row)
		if err := checkConfigSetterRowShape(row); err == nil {
			t.Fatalf("%s not rejected", name)
		}
	}
}

// configSetterDocEntry is the documentation of one register selector: its
// canonical name, whether the name is provisional and its open question.
type configSetterDocEntry struct {
	name        string
	provisional bool
	question    string
}

const (
	docsQuestionZoneOperatingMode = "Representation is not established; the relation to `OP=0x02, GG=0x03, RR=0x0006` (same name) is not established."
	docsQuestionPartialLoad       = "One community configuration lists the unit as kW or %; the unit is unresolved."
	docsQuestionHeatingFlowMax    = "Separate selector with the same community name; which devices of the class use which id is unresolved."
	docsQuestionPumpMode          = "Candidate interpretation, unconfirmed on the wire: central heating pump mode (0 post-run, 1 permanent, 2 winter). Another reading is an installer access code (the legacy API path describes one); see the Open questions of the boiler map. Which devices of the class use which meaning is unresolved. The canonical name is provisional until this is resolved."
)

// configSetterDocs is the documentation of every register a config setter
// writes, keyed "B524 opcode/group/register" or "B509 register". For B524 the
// status and question are those of protocols/vaillant/ebus-vaillant-b524-
// register-map.md as decomposed in scripts/name-decomposition.json; for B509
// they are the name and open_question columns of
// protocols/vaillant/fixtures/b509-register-name-correspondence.csv, where a
// name is provisional when its open question says so.
// TestConfigSetterDocsMatchDocumentation compares this table with a checkout
// of the documentation repository.
var configSetterDocs = map[string]configSetterDocEntry{
	"B524 0x02/0x00/0x0001": {name: "dhw_outdoor_bivalence_temperature_limit"},
	"B524 0x02/0x00/0x000E": {name: "room_humidity_limit_maximum"},
	"B524 0x02/0x00/0x0014": {name: "adaptive_heating_curve_enabled"},
	"B524 0x02/0x00/0x0023": {name: "heating_outdoor_bivalence_temperature_limit"},
	"B524 0x02/0x00/0x0026": {name: "heating_flow_temperature_setpoint_maximum_at_heat_pump_fault"},
	"B524 0x02/0x00/0x002C": {name: "maintenance_date"},
	"B524 0x02/0x00/0x0046": {name: "dhw_flow_temperature_setpoint_maximum"},
	"B524 0x02/0x00/0x006C": {name: "installer_name_prefix"},
	"B524 0x02/0x00/0x006D": {name: "installer_name_suffix"},
	"B524 0x02/0x00/0x006F": {name: "installer_phone_number_prefix"},
	"B524 0x02/0x00/0x0070": {name: "installer_phone_number_suffix"},
	"B524 0x02/0x00/0x0076": {name: "installer_access_code"},
	"B524 0x02/0x02/0x0006": {name: "circuit_cooling_enabled"},
	"B524 0x02/0x02/0x000F": {name: "circuit_heating_curve_factor"},
	"B524 0x02/0x02/0x0010": {name: "heating_circuit_flow_temperature_setpoint_maximum"},
	"B524 0x02/0x02/0x0012": {name: "circuit_flow_temperature_setpoint_minimum"},
	"B524 0x02/0x02/0x0014": {name: "heating_circuit_outdoor_temperature_limit_maximum"},
	"B524 0x02/0x02/0x0015": {name: "circuit_room_temperature_influence_mode"},
	"B524 0x02/0x03/0x0005": {name: "zone_room_temperature_setpoint_at_holiday"},
	"B524 0x02/0x03/0x0006": {name: "heating_zone_operating_mode", provisional: true, question: docsQuestionZoneOperatingMode},
	"B524 0x02/0x03/0x0008": {name: "zone_room_temperature_setpoint_at_quick_veto"},
	"B524 0x02/0x03/0x0022": {name: "heating_zone_room_manual_temperature_setpoint"},
	"B524 0x02/0x03/0x0026": {name: "zone_quick_veto_time"},
	"B509 0x0704":           {name: "heating_partial_load_power", question: docsQuestionPartialLoad},
	"B509 0x0804":           {name: "dhw_partial_load_power", question: docsQuestionPartialLoad},
	"B509 0x0E04":           {name: "maximum_heating_flow_temperature_setpoint", question: docsQuestionHeatingFlowMax},
	"B509 0xA500":           {name: "maximum_heating_flow_temperature_setpoint", question: docsQuestionHeatingFlowMax},
	"B509 0x0F04":           {name: "maximum_dhw_flow_temperature_setpoint"},
	"B509 0x4904":           {name: "central_heating_pump_mode", provisional: true, question: docsQuestionPumpMode},
	"B509 0x8104":           {name: "installer_phone_number"},
}

// configSetterDocsKey is the configSetterDocs key of one register of a row.
func configSetterDocsKey(service string, sel configSetterSel, register string) string {
	if service == "B509" {
		return "B509 " + register
	}
	return fmt.Sprintf("B524 %s/%s/%s", sel.Opcode, sel.Group, register)
}

// checkConfigSetterRowDocs requires a row's key, provisional flag and open
// question to be those of the documentation of its registers. A key written to
// two registers is the documentation name shared by both, or the common stem
// of a _prefix/_suffix pair; it is provisional when one register name is, and
// carries the one open question its registers record.
func checkConfigSetterRowDocs(service string, row configSetterKeyRow) error {
	if len(row.Selector.Registers) == 0 {
		return fmt.Errorf("key %s has no register", row.Key)
	}
	provisional := false
	questions := map[string]bool{}
	names := make([]string, 0, len(row.Selector.Registers))
	for _, register := range row.Selector.Registers {
		entry, ok := configSetterDocs[configSetterDocsKey(service, row.Selector, register)]
		if !ok {
			return fmt.Errorf("key %s: register %s has no documentation entry", row.Key, configSetterDocsKey(service, row.Selector, register))
		}
		names = append(names, entry.name)
		provisional = provisional || entry.provisional
		if entry.question != "" {
			questions[entry.question] = true
		}
	}
	switch {
	case len(names) == 1 && names[0] == row.Key:
	case len(names) == 2 && names[0] == row.Key && names[1] == row.Key:
	case len(names) == 2 && names[0] == row.Key+"_prefix" && names[1] == row.Key+"_suffix":
	default:
		return fmt.Errorf("key %s: documentation names its registers %v", row.Key, names)
	}
	if len(questions) > 1 {
		return fmt.Errorf("key %s: registers record %d different open questions", row.Key, len(questions))
	}
	question := ""
	for text := range questions {
		question = text
	}
	if row.Provisional != provisional {
		return fmt.Errorf("key %s: provisional = %v; documentation gives %v", row.Key, row.Provisional, provisional)
	}
	if row.OpenQuestion != question {
		return fmt.Errorf("key %s: open_question = %q; documentation gives %q", row.Key, row.OpenQuestion, question)
	}
	return nil
}

func TestConfigSetterKeysFixtureFollowsDocumentation(t *testing.T) {
	doc := loadConfigSetterKeys(t)
	used := map[string]bool{}
	for name, set := range doc.Setters {
		for _, row := range set.Keys {
			if err := checkConfigSetterRowDocs(set.Service, row); err != nil {
				t.Errorf("%s: %v", name, err)
			}
			for _, register := range row.Selector.Registers {
				used[configSetterDocsKey(set.Service, row.Selector, register)] = true
			}
		}
	}
	for key := range configSetterDocs {
		if !used[key] {
			t.Errorf("documentation entry %s is written by no fixture row", key)
		}
	}
}

// TestConfigSetterKeysDocumentationCheckCoversEveryField changes each field a
// row's documentation status depends on and requires the check to fail: the
// key (a name that is not the documentation name, as the former boiler key
// was), the provisional flag (dropped or invented), the open question
// (dropped, invented or reworded), the service, the opcode, the group and the
// registers (a selector whose documentation differs).
func TestConfigSetterKeysDocumentationCheckCoversEveryField(t *testing.T) {
	rows := map[string]configSetterKeyRow{
		"B524 single": {Key: "heating_zone_operating_mode", Selector: configSetterSel{Opcode: "0x02", Group: "0x03", Instance: "index", Registers: []string{"0x0006"}}, Provisional: true, OpenQuestion: docsQuestionZoneOperatingMode},
		"B524 plain":  {Key: "zone_quick_veto_time", Selector: configSetterSel{Opcode: "0x02", Group: "0x03", Instance: "index", Registers: []string{"0x0026"}}},
		"B524 pair":   {Key: "installer_name", Selector: configSetterSel{Opcode: "0x02", Group: "0x00", Instance: "0x00", Registers: []string{"0x006C", "0x006D"}}},
		"B509 single": {Key: "central_heating_pump_mode", Selector: configSetterSel{Registers: []string{"0x4904"}}, Provisional: true, OpenQuestion: docsQuestionPumpMode},
		"B509 plain":  {Key: "maximum_dhw_flow_temperature_setpoint", Selector: configSetterSel{Registers: []string{"0x0F04"}}},
		"B509 pair":   {Key: "maximum_heating_flow_temperature_setpoint", Selector: configSetterSel{Registers: []string{"0x0E04", "0xA500"}}, OpenQuestion: docsQuestionHeatingFlowMax},
	}
	mutations := map[string]func(*configSetterKeyRow){
		"key":         func(row *configSetterKeyRow) { row.Key = "installerMenuCode" },
		"provisional": func(row *configSetterKeyRow) { row.Provisional = !row.Provisional },
		"question": func(row *configSetterKeyRow) {
			row.OpenQuestion = map[bool]string{true: "open?", false: ""}[row.OpenQuestion == ""]
		},
		"reworded":  func(row *configSetterKeyRow) { row.OpenQuestion += " " },
		"opcode":    func(row *configSetterKeyRow) { row.Selector.Opcode = "0x06" },
		"group":     func(row *configSetterKeyRow) { row.Selector.Group = "0x01" },
		"register":  func(row *configSetterKeyRow) { row.Selector.Registers = []string{"0x0704"} },
		"registers": func(row *configSetterKeyRow) { row.Selector.Registers = append(row.Selector.Registers, "0x0804") },
	}
	for name, row := range rows {
		service := strings.Fields(name)[0]
		if err := checkConfigSetterRowDocs(service, row); err != nil {
			t.Fatalf("%s: unchanged row rejected: %v", name, err)
		}
		other := map[string]string{"B524": "B509", "B509": "B524"}[service]
		if err := checkConfigSetterRowDocs(other, row); err == nil {
			t.Fatalf("%s: changed service not detected", name)
		}
		for field, mutate := range mutations {
			if service == "B509" && (field == "opcode" || field == "group") {
				continue // a B509 selector has no opcode or group
			}
			changed := row
			changed.Selector.Registers = append([]string(nil), row.Selector.Registers...)
			mutate(&changed)
			if err := checkConfigSetterRowDocs(service, changed); err == nil {
				t.Fatalf("%s: changed %s not detected", name, field)
			}
		}
	}
}

// configSetterDocsRootEnv names a checkout of the documentation repository.
// When it is set, the documentation table above is compared with the files it
// was copied from.
const configSetterDocsRootEnv = "HELIANTHUS_DOCS_EBUS_DIR"

func TestConfigSetterDocsMatchDocumentation(t *testing.T) {
	root := os.Getenv(configSetterDocsRootEnv)
	if root == "" {
		t.Skipf("%s is not set; the documentation table is compared with a documentation checkout only when one is supplied", configSetterDocsRootEnv)
	}
	b524 := map[string]configSetterDocEntry{}
	rawDecomposition, err := os.ReadFile(filepath.Join(root, "scripts", "name-decomposition.json"))
	if err != nil {
		t.Fatal(err)
	}
	var decomposition struct {
		Entries []struct {
			Selector     string `json:"selector"`
			Emitted      string `json:"emitted"`
			Status       string `json:"status"`
			OpenQuestion string `json:"open_question"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(rawDecomposition, &decomposition); err != nil {
		t.Fatal(err)
	}
	for _, entry := range decomposition.Entries {
		key := "B524 " + entry.Selector
		if _, seen := b524[key]; seen {
			t.Fatalf("name decomposition lists %s twice", entry.Selector)
		}
		b524[key] = configSetterDocEntry{name: entry.Emitted, provisional: entry.Status == "provisional", question: entry.OpenQuestion}
	}
	opQuestions := readDocsCSV(t, filepath.Join(root, "protocols", "vaillant", "fixtures", "b524-op02-register-names.csv"))
	for _, record := range opQuestions {
		key := fmt.Sprintf("B524 %s/%s/%s", record["opcode"], record["group"], record["register"])
		entry, ok := b524[key]
		if !ok || entry.name != record["name"] {
			continue
		}
		if entry.question == "" {
			entry.question = record["open_question"]
		}
		b524[key] = entry
	}
	b509 := map[string]configSetterDocEntry{}
	for _, record := range readDocsCSV(t, filepath.Join(root, "protocols", "vaillant", "fixtures", "b509-register-name-correspondence.csv")) {
		if record["device_class"] != "boiler" || record["operation"] != "0D" {
			continue
		}
		question := record["open_question"]
		b509["B509 "+record["selector"]] = configSetterDocEntry{name: record["name"], provisional: strings.Contains(question, "provisional"), question: question}
	}
	for key, want := range configSetterDocs {
		source := b524
		if strings.HasPrefix(key, "B509 ") {
			source = b509
		}
		got, ok := source[key]
		if !ok {
			t.Errorf("%s: no documentation entry", key)
			continue
		}
		if got != want {
			t.Errorf("%s: documentation gives %+v; table holds %+v", key, got, want)
		}
	}
}

func readDocsCSV(t *testing.T, path string) []map[string]string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(bytes.NewReader(raw)).ReadAll()
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	if len(records) == 0 {
		t.Fatalf("%s is empty", path)
	}
	out := make([]map[string]string, 0, len(records)-1)
	for _, record := range records[1:] {
		row := map[string]string{}
		for i, column := range records[0] {
			if i < len(record) {
				row[column] = record[i]
			}
		}
		out = append(out, row)
	}
	return out
}

// expectedConfigSetterFieldDescription renders the description the field
// argument of a setter must carry, from the fixture rows of that setter.
func expectedConfigSetterFieldDescription(rows []configSetterKeyRow) string {
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, row.Key)
	}
	var b strings.Builder
	b.WriteString("Key of the value to write; one of: " + strings.Join(keys, ", ") + ". Any other key is rejected with this list; there is no alias.")
	provisional := 0
	for _, row := range rows {
		switch {
		case row.Provisional:
			provisional++
			b.WriteString(" Key " + row.Key + " has a provisional name (the documentation name is not final): " + row.OpenQuestion)
		case row.OpenQuestion != "":
			b.WriteString(" Key " + row.Key + " has an open question in the documentation: " + row.OpenQuestion)
		}
	}
	if provisional == 0 {
		b.WriteString(" No key of this setter has a provisional name.")
	}
	return b.String()
}

// TestConfigSetterFieldArgumentDescriptions requires the field argument of each
// of the four setters to describe its accepted keys and to name every
// provisional key with its open question, exactly as the fixture lists them.
func TestConfigSetterFieldArgumentDescriptions(t *testing.T) {
	doc := loadConfigSetterKeys(t)
	schema, err := NewSchema(NewBuilder(nil, nil), mutationTestRegistry{}, &mutationTestInvoker{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mutation := schema.MutationType()
	if mutation == nil {
		t.Fatal("schema has no mutation type")
	}
	for _, name := range []string{"set_boiler_config", "set_circuit_config", "set_system_config", "set_zone_config"} {
		set, ok := doc.Setters[name]
		if !ok {
			t.Fatalf("fixture lacks %s", name)
		}
		field, ok := mutation.Fields()[name]
		if !ok {
			t.Fatalf("mutation lacks %s", name)
		}
		description := ""
		for _, arg := range field.Args {
			if arg.Name() == "field" {
				description = arg.Description()
			}
		}
		if want := expectedConfigSetterFieldDescription(set.Keys); description != want {
			t.Errorf("%s field description = %q; want %q", name, description, want)
		}
		for _, row := range set.Keys {
			if row.Provisional && !strings.Contains(description, " Key "+row.Key+" has a provisional name") {
				t.Errorf("%s field description does not name provisional key %s", name, row.Key)
			}
		}
	}
}

// TestConfigSetterKeyTableMatchesSpecs binds the description table to the keys
// the circuit, zone and system setters accept.
func TestConfigSetterKeyTableMatchesSpecs(t *testing.T) {
	for name, specs := range map[string]map[string]configFieldSpec{
		"set_circuit_config": circuitConfigFieldSpecs,
		"set_zone_config":    zoneConfigFieldSpecs,
		"set_system_config":  systemConfigFieldSpecs,
	} {
		want := make([]string, 0, len(specs))
		for key := range specs {
			want = append(want, key)
		}
		sort.Strings(want)
		if got := configSetterAcceptedKeys(name); !reflect.DeepEqual(got, want) {
			t.Errorf("%s description table keys = %v; setter accepts %v", name, got, want)
		}
	}
}
