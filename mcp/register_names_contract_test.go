package mcp

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The record of B524 and B509 register-derived names lives with the GraphQL
// contract test; every surface checks the same rows and unmapped fields.
const mcpRegisterNamesFixturePath = "../graphql/testdata/register_derived_names.json"

type mcpRegisterNamesRow struct {
	PublishedName         string            `json:"published_name"`
	Service               string            `json:"service"`
	Opcode                string            `json:"opcode"`
	Group                 string            `json:"group"`
	Register              string            `json:"register"`
	Selector              string            `json:"selector"`
	DocsEbusName          string            `json:"docs_ebus_name"`
	PreviousName          string            `json:"previous_name"`
	PreviousPublishedName string            `json:"previous_published_name"`
	Surfaces              map[string]string `json:"surfaces"`
	Join                  *struct {
		DocsEbusName string `json:"docs_ebus_name"`
		PreviousName string `json:"previous_name"`
	} `json:"join"`
	Bound *struct {
		PublishedName         string            `json:"published_name"`
		PreviousPublishedName string            `json:"previous_published_name"`
		Surfaces              map[string]string `json:"surfaces"`
	} `json:"bound"`
	Exemption string `json:"exemption"`
}

// fields returns object and name of every field the row publishes on this
// surface: the value and, when recorded, its bound.
func (row mcpRegisterNamesRow) fields() [][2]string {
	var out [][2]string
	if object := row.Surfaces["mcp"]; object != "" {
		out = append(out, [2]string{object, row.PublishedName})
	}
	if row.Bound != nil {
		if object := row.Bound.Surfaces["mcp"]; object != "" {
			out = append(out, [2]string{object, row.Bound.PublishedName})
		}
	}
	return out
}

// retired returns the names of the row no surface may publish: the
// docs-ebus previous_name, the gateway's previous published names of the
// value and of its bound, and the previous_name of a joined register.
func (row mcpRegisterNamesRow) retired() []string {
	var names []string
	if row.Exemption == "" && row.PreviousName != "" && row.PreviousName != row.DocsEbusName {
		names = append(names, row.PreviousName)
	}
	if row.PreviousPublishedName != "" {
		names = append(names, row.PreviousPublishedName)
	}
	if row.Join != nil {
		names = append(names, row.Join.PreviousName)
	}
	if row.Bound != nil {
		names = append(names, row.Bound.PreviousPublishedName)
	}
	return names
}

// identity is the native identity of the row: service, opcode and either
// group and register (B524) or selector (B509).
func (row mcpRegisterNamesRow) identity() string {
	if row.Service == "B509" {
		return row.Service + "/" + row.Opcode + "/" + row.Selector
	}
	return row.Service + "/" + row.Opcode + "/" + row.Group + "/" + row.Register
}

// docsNameMatches requires the published name to be the docs-ebus name of
// the register or, for a joined field, the stem of the docs-ebus names of its
// two registers.
func (row mcpRegisterNamesRow) docsNameMatches() bool {
	if row.Join == nil {
		return row.PublishedName == row.DocsEbusName
	}
	return row.DocsEbusName == row.PublishedName+"_prefix" && row.Join.DocsEbusName == row.PublishedName+"_suffix"
}

// mcpPublishedKeys marshals a value of typ with every pointer field set and
// returns the JSON object keys the MCP surface publishes for it.
func mcpPublishedKeys(t *testing.T, typ reflect.Type) map[string]bool {
	t.Helper()
	value := reflect.New(typ).Elem()
	for i := 0; i < typ.NumField(); i++ {
		if field := value.Field(i); field.Kind() == reflect.Pointer {
			field.Set(reflect.New(field.Type().Elem()))
		}
	}
	raw, err := json.Marshal(value.Interface())
	if err != nil {
		t.Fatalf("marshal %s: %v", typ.Name(), err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("unmarshal %s: %v", typ.Name(), err)
	}
	keys := map[string]bool{}
	for key := range object {
		keys[key] = true
	}
	return keys
}

func TestMCPPublishesDocsEbusRegisterNames(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(mcpRegisterNamesFixturePath)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var doc struct {
		Rows     []mcpRegisterNamesRow `json:"rows"`
		Unmapped []struct {
			PublishedName string            `json:"published_name"`
			Surfaces      map[string]string `json:"surfaces"`
		} `json:"unmapped_fields"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if len(doc.Rows) == 0 {
		t.Fatal("fixture has no rows")
	}

	types := map[string]reflect.Type{
		"SystemState":       reflect.TypeOf(SystemState{}),
		"SystemConfig":      reflect.TypeOf(SystemConfig{}),
		"SystemProperties":  reflect.TypeOf(SystemProperties{}),
		"CircuitConfig":     reflect.TypeOf(CircuitConfig{}),
		"BoilerState":       reflect.TypeOf(BoilerState{}),
		"BoilerConfig":      reflect.TypeOf(BoilerConfig{}),
		"BoilerDiagnostics": reflect.TypeOf(BoilerDiagnostics{}),
	}
	// Every listed object is scanned for previous names, including one the
	// fixture records no row for.
	published := map[string]map[string]bool{}
	for object, typ := range types {
		published[object] = mcpPublishedKeys(t, typ)
	}
	for _, row := range doc.Rows {
		for _, field := range row.fields() {
			if _, ok := types[field[0]]; !ok {
				t.Fatalf("fixture names unknown MCP object %q", field[0])
			}
		}
	}

	var errs []string
	for _, row := range doc.Rows {
		for _, field := range row.fields() {
			if !published[field[0]][field[1]] {
				errs = append(errs, fmt.Sprintf("%s does not publish %s (register %s)", field[0], field[1], row.identity()))
			}
		}
		if row.Exemption == "" && !row.docsNameMatches() {
			errs = append(errs, fmt.Sprintf("register %s: published %s, docs-ebus name %s", row.identity(), row.PublishedName, row.DocsEbusName))
		}
		if row.Exemption != "" && (!strings.Contains(row.Exemption, "helianthus-docs-ebus#1145") || row.Service != "B524" || row.Register != "0x0007") {
			errs = append(errs, "exemption outside GG00 RR0007 / helianthus-docs-ebus#1145")
		}
		for _, retired := range row.retired() {
			for object, keys := range published {
				if keys[retired] {
					errs = append(errs, fmt.Sprintf("%s publishes previous name %s (register %s)", object, retired, row.identity()))
				}
			}
		}
	}
	for _, field := range doc.Unmapped {
		object := field.Surfaces["mcp"]
		if object == "" {
			continue
		}
		if _, ok := types[object]; !ok {
			errs = append(errs, "fixture names unknown MCP object "+object)
		} else if !published[object][field.PublishedName] {
			errs = append(errs, fmt.Sprintf("%s does not publish unmapped field %s", object, field.PublishedName))
		}
	}
	sort.Strings(errs)
	if len(errs) > 0 {
		t.Fatalf("MCP register names:\n%s", strings.Join(errs, "\n"))
	}
}

// mcpRegisterNamesField is one field_registers entry of the record: a zone,
// circuit or hot water field read from one register, published under the
// docs-ebus name of that register, or a derived field.
type mcpRegisterNamesField struct {
	Object        string          `json:"object"`
	PublishedName string          `json:"published_name"`
	DocsEbusName  string          `json:"docs_ebus_name"`
	Derived       json.RawMessage `json:"derived"`
}

// mcpFieldObjectTypes maps each zone, circuit and hot water object of the
// record to the MCP type that publishes it.
var mcpFieldObjectTypes = map[string]reflect.Type{
	"zone_state":     reflect.TypeOf(ZoneState{}),
	"zone_config":    reflect.TypeOf(ZoneConfig{}),
	"circuit_state":  reflect.TypeOf(CircuitState{}),
	"circuit_config": reflect.TypeOf(CircuitConfig{}),
	"dhw_state":      reflect.TypeOf(DhwState{}),
	"dhw_config":     reflect.TypeOf(DhwConfig{}),
}

// mcpDeclaredKeys returns every JSON object key a struct type declares,
// whether or not its value is empty.
func mcpDeclaredKeys(typ reflect.Type) map[string]bool {
	keys := map[string]bool{}
	for i := 0; i < typ.NumField(); i++ {
		name, _, _ := strings.Cut(typ.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			keys[name] = true
		}
	}
	return keys
}

// mcpFieldRegisterErrors requires every key the MCP surface declares for a
// recorded zone, circuit or hot water object to be a field the record lists
// for that object, and every single-register field it publishes to carry the
// docs-ebus name of its register. published maps object to declared keys.
func mcpFieldRegisterErrors(fields []mcpRegisterNamesField, published map[string]map[string]bool) []string {
	var errs []string
	recorded := map[string]map[string]mcpRegisterNamesField{}
	for _, field := range fields {
		if recorded[field.Object] == nil {
			recorded[field.Object] = map[string]mcpRegisterNamesField{}
		}
		recorded[field.Object][field.PublishedName] = field
	}
	for object := range mcpFieldObjectTypes {
		keys, ok := published[object]
		if !ok || len(recorded[object]) == 0 {
			errs = append(errs, "object "+object+" not published or not recorded")
			continue
		}
		for key := range keys {
			field, ok := recorded[object][key]
			switch {
			case !ok:
				errs = append(errs, fmt.Sprintf("%s publishes %s, which the record does not list for it", object, key))
			case len(field.Derived) == 0 && field.PublishedName != field.DocsEbusName:
				errs = append(errs, fmt.Sprintf("%s publishes %s, docs-ebus name %s", object, key, field.DocsEbusName))
			}
		}
	}
	sort.Strings(errs)
	return errs
}

func loadMCPRegisterNamesFields(t *testing.T) []mcpRegisterNamesField {
	t.Helper()
	raw, err := os.ReadFile(mcpRegisterNamesFixturePath)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var doc struct {
		Fields []mcpRegisterNamesField `json:"field_registers"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if len(doc.Fields) == 0 {
		t.Fatal("fixture has no field_registers")
	}
	return doc.Fields
}

func mcpFieldObjectKeys() map[string]map[string]bool {
	published := map[string]map[string]bool{}
	for object, typ := range mcpFieldObjectTypes {
		published[object] = mcpDeclaredKeys(typ)
	}
	return published
}

func TestMCPPublishesZoneCircuitAndHotWaterFieldsUnderRecordedNames(t *testing.T) {
	t.Parallel()
	fields := loadMCPRegisterNamesFields(t)
	if errs := mcpFieldRegisterErrors(fields, mcpFieldObjectKeys()); len(errs) > 0 {
		t.Fatalf("MCP zone, circuit and hot water names:\n%s", strings.Join(errs, "\n"))
	}
}

// TestMCPFieldRegisterCheckerCatchesEachMutation varies both parts of a
// published key (object and name) and the docs-ebus name of a recorded
// single-register field, and requires the checker to report each change.
func TestMCPFieldRegisterCheckerCatchesEachMutation(t *testing.T) {
	t.Parallel()
	base := loadMCPRegisterNamesFields(t)
	if errs := mcpFieldRegisterErrors(base, mcpFieldObjectKeys()); len(errs) != 0 {
		t.Fatalf("clean surface reported: %v", errs)
	}
	single := -1
	for i, field := range base {
		if len(field.Derived) == 0 && field.Object == "circuit_state" && mcpFieldObjectKeys()[field.Object][field.PublishedName] {
			single = i
			break
		}
	}
	if single < 0 {
		t.Fatal("fixture needs a single-register circuit_state field the MCP surface publishes")
	}
	s := base[single]
	keyMutations := map[string]func(map[string]map[string]bool){
		// Name: the key reverts to a spelling the record does not list.
		"name reverted": func(m map[string]map[string]bool) {
			delete(m[s.Object], s.PublishedName)
			m[s.Object]["pump_active"] = true
		},
		"name gains a unit suffix": func(m map[string]map[string]bool) {
			delete(m[s.Object], s.PublishedName)
			m[s.Object][s.PublishedName+"_c"] = true
		},
		// Object: the key is published on an object that does not record it.
		"object moved": func(m map[string]map[string]bool) {
			delete(m[s.Object], s.PublishedName)
			m["dhw_state"][s.PublishedName] = true
		},
		"object missing": func(m map[string]map[string]bool) { delete(m, "zone_config") },
	}
	for name, mutate := range keyMutations {
		published := mcpFieldObjectKeys()
		mutate(published)
		if len(mcpFieldRegisterErrors(base, published)) == 0 {
			t.Errorf("key mutation %q not caught", name)
		}
	}
	// Record: a single-register field whose published name differs from the
	// docs-ebus name of its register.
	changed := append([]mcpRegisterNamesField(nil), base...)
	changed[single].DocsEbusName = s.PublishedName + "_value"
	if len(mcpFieldRegisterErrors(changed, mcpFieldObjectKeys())) == 0 {
		t.Error("record mutation \"docs-ebus name differs\" not caught")
	}
}
