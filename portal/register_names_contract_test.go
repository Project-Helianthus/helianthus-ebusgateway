package portal

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
const portalRegisterNamesFixturePath = "../graphql/testdata/register_derived_names.json"

type portalRegisterNamesRow struct {
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
func (row portalRegisterNamesRow) fields() [][2]string {
	var out [][2]string
	if object := row.Surfaces["portal"]; object != "" {
		out = append(out, [2]string{object, row.PublishedName})
	}
	if row.Bound != nil {
		if object := row.Bound.Surfaces["portal"]; object != "" {
			out = append(out, [2]string{object, row.Bound.PublishedName})
		}
	}
	return out
}

// retired returns the names of the row no surface may publish: the
// docs-ebus previous_name, the gateway's previous published names of the
// value and of its bound, and the previous_name of a joined register.
func (row portalRegisterNamesRow) retired() []string {
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
func (row portalRegisterNamesRow) identity() string {
	if row.Service == "B509" {
		return row.Service + "/" + row.Opcode + "/" + row.Selector
	}
	return row.Service + "/" + row.Opcode + "/" + row.Group + "/" + row.Register
}

// docsNameMatches requires the published name to be the docs-ebus name of
// the register or, for a joined field, the stem of the docs-ebus names of its
// two registers.
func (row portalRegisterNamesRow) docsNameMatches() bool {
	if row.Join == nil {
		return row.PublishedName == row.DocsEbusName
	}
	return row.DocsEbusName == row.PublishedName+"_prefix" && row.Join.DocsEbusName == row.PublishedName+"_suffix"
}

// portalPublishedKeys marshals a value of typ with every pointer field set
// and returns the JSON object keys the Portal publishes for it.
func portalPublishedKeys(t *testing.T, typ reflect.Type) map[string]bool {
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

func TestPortalPublishesDocsEbusRegisterNames(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(portalRegisterNamesFixturePath)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var doc struct {
		Rows     []portalRegisterNamesRow `json:"rows"`
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
		"SemanticSystemState":       reflect.TypeOf(SemanticSystemState{}),
		"SemanticSystemConfig":      reflect.TypeOf(SemanticSystemConfig{}),
		"SemanticSystemProperties":  reflect.TypeOf(SemanticSystemProperties{}),
		"SemanticCircuitConfig":     reflect.TypeOf(SemanticCircuitConfig{}),
		"SemanticBoilerState":       reflect.TypeOf(SemanticBoilerState{}),
		"SemanticBoilerConfig":      reflect.TypeOf(SemanticBoilerConfig{}),
		"SemanticBoilerDiagnostics": reflect.TypeOf(SemanticBoilerDiagnostics{}),
	}
	// Every listed object is scanned for previous names, including one the
	// fixture records no Portal row for.
	published := map[string]map[string]bool{}
	for object, typ := range types {
		published[object] = portalPublishedKeys(t, typ)
	}

	var errs []string
	for _, row := range doc.Rows {
		for _, field := range row.fields() {
			if _, ok := types[field[0]]; !ok {
				errs = append(errs, "fixture names unknown Portal object "+field[0])
			} else if !published[field[0]][field[1]] {
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
			for candidate, keys := range published {
				if keys[retired] {
					errs = append(errs, fmt.Sprintf("%s publishes previous name %s (register %s)", candidate, retired, row.identity()))
				}
			}
		}
	}
	for _, field := range doc.Unmapped {
		object := field.Surfaces["portal"]
		if object == "" {
			continue
		}
		if _, ok := types[object]; !ok {
			errs = append(errs, "fixture names unknown Portal object "+object)
		} else if !published[object][field.PublishedName] {
			errs = append(errs, fmt.Sprintf("%s does not publish unmapped field %s", object, field.PublishedName))
		}
	}
	sort.Strings(errs)
	if len(errs) > 0 {
		t.Fatalf("Portal register names:\n%s", strings.Join(errs, "\n"))
	}
}

// portalRegisterNamesField is one field_registers entry of the record: a
// zone, circuit or hot water field read from one register, published under
// the docs-ebus name of that register, or a derived field.
type portalRegisterNamesField struct {
	Object        string          `json:"object"`
	PublishedName string          `json:"published_name"`
	DocsEbusName  string          `json:"docs_ebus_name"`
	Derived       json.RawMessage `json:"derived"`
}

// portalFieldObjectTypes maps each zone, circuit and hot water object of the
// record to the Portal type that publishes it.
var portalFieldObjectTypes = map[string]reflect.Type{
	"zone_state":     reflect.TypeOf(SemanticZoneState{}),
	"zone_config":    reflect.TypeOf(SemanticZoneConfig{}),
	"circuit_state":  reflect.TypeOf(SemanticCircuitState{}),
	"circuit_config": reflect.TypeOf(SemanticCircuitConfig{}),
	"dhw_state":      reflect.TypeOf(SemanticDhwState{}),
	"dhw_config":     reflect.TypeOf(SemanticDhwConfig{}),
}

// portalDerivedSpellings are the Portal keys of derived zone fields that the
// Portal publishes under its own spelling, each with the recorded derived
// field it carries. Only a derived field may appear here.
var portalDerivedSpellings = map[string]map[string]string{
	"zone_state": {
		"current_temp_c":       "zone_room_temperature",
		"current_humidity_pct": "zone_room_humidity",
	},
}

// portalDeclaredKeys returns every JSON object key a struct type declares,
// whether or not its value is empty.
func portalDeclaredKeys(typ reflect.Type) map[string]bool {
	keys := map[string]bool{}
	for i := 0; i < typ.NumField(); i++ {
		name, _, _ := strings.Cut(typ.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			keys[name] = true
		}
	}
	return keys
}

// portalFieldRegisterErrors requires every key the Portal declares for a
// recorded zone, circuit or hot water object to be a field the record lists
// for that object, or a listed spelling of a derived field of that object,
// and every single-register field it publishes to carry the docs-ebus name of
// its register. published maps object to declared keys.
func portalFieldRegisterErrors(fields []portalRegisterNamesField, spellings map[string]map[string]string, published map[string]map[string]bool) []string {
	var errs []string
	recorded := map[string]map[string]portalRegisterNamesField{}
	for _, field := range fields {
		if recorded[field.Object] == nil {
			recorded[field.Object] = map[string]portalRegisterNamesField{}
		}
		recorded[field.Object][field.PublishedName] = field
	}
	for object, keys := range spellings {
		for key, name := range keys {
			if field, ok := recorded[object][name]; !ok || len(field.Derived) == 0 {
				errs = append(errs, fmt.Sprintf("%s spelling %s names %s, which is not a recorded derived field", object, key, name))
			}
		}
	}
	for object := range portalFieldObjectTypes {
		keys, ok := published[object]
		if !ok || len(recorded[object]) == 0 {
			errs = append(errs, "object "+object+" not published or not recorded")
			continue
		}
		for key := range keys {
			if _, derived := spellings[object][key]; derived {
				continue
			}
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

func loadPortalRegisterNamesFields(t *testing.T) []portalRegisterNamesField {
	t.Helper()
	raw, err := os.ReadFile(portalRegisterNamesFixturePath)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var doc struct {
		Fields []portalRegisterNamesField `json:"field_registers"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if len(doc.Fields) == 0 {
		t.Fatal("fixture has no field_registers")
	}
	return doc.Fields
}

func portalFieldObjectKeys() map[string]map[string]bool {
	published := map[string]map[string]bool{}
	for object, typ := range portalFieldObjectTypes {
		published[object] = portalDeclaredKeys(typ)
	}
	return published
}

func TestPortalPublishesZoneCircuitAndHotWaterFieldsUnderRecordedNames(t *testing.T) {
	t.Parallel()
	fields := loadPortalRegisterNamesFields(t)
	if errs := portalFieldRegisterErrors(fields, portalDerivedSpellings, portalFieldObjectKeys()); len(errs) > 0 {
		t.Fatalf("Portal zone, circuit and hot water names:\n%s", strings.Join(errs, "\n"))
	}
}

// TestPortalFieldRegisterCheckerCatchesEachMutation varies both parts of a
// published key (object and name), both parts of a derived spelling (object
// and the derived field it names) and the docs-ebus name of a recorded
// single-register field, and requires the checker to report each change.
func TestPortalFieldRegisterCheckerCatchesEachMutation(t *testing.T) {
	t.Parallel()
	base := loadPortalRegisterNamesFields(t)
	if errs := portalFieldRegisterErrors(base, portalDerivedSpellings, portalFieldObjectKeys()); len(errs) != 0 {
		t.Fatalf("clean surface reported: %v", errs)
	}
	single := -1
	for i, field := range base {
		if len(field.Derived) == 0 && field.Object == "dhw_state" && portalFieldObjectKeys()[field.Object][field.PublishedName] {
			single = i
			break
		}
	}
	if single < 0 {
		t.Fatal("fixture needs a single-register dhw_state field the Portal publishes")
	}
	s := base[single]
	keyMutations := map[string]func(map[string]map[string]bool){
		// Name: the key reverts to a spelling the record does not list.
		"name reverted": func(m map[string]map[string]bool) {
			delete(m[s.Object], s.PublishedName)
			m[s.Object]["current_temp_c"] = true
		},
		"name gains a unit suffix": func(m map[string]map[string]bool) {
			delete(m[s.Object], s.PublishedName)
			m[s.Object][s.PublishedName+"_c"] = true
		},
		// Object: the key is published on an object that does not record it.
		"object moved": func(m map[string]map[string]bool) {
			delete(m[s.Object], s.PublishedName)
			m["circuit_state"][s.PublishedName] = true
		},
		"object missing": func(m map[string]map[string]bool) { delete(m, "circuit_config") },
	}
	for name, mutate := range keyMutations {
		published := portalFieldObjectKeys()
		mutate(published)
		if len(portalFieldRegisterErrors(base, portalDerivedSpellings, published)) == 0 {
			t.Errorf("key mutation %q not caught", name)
		}
	}
	spellings := func() map[string]map[string]string {
		out := map[string]map[string]string{}
		for object, keys := range portalDerivedSpellings {
			out[object] = map[string]string{}
			for key, name := range keys {
				out[object][key] = name
			}
		}
		return out
	}
	spellingMutations := map[string]func(map[string]map[string]string){
		// Derived field: the spelling names a single-register field.
		"spelling names a single-register field": func(m map[string]map[string]string) {
			m[s.Object] = map[string]string{"current_temp_c": s.PublishedName}
		},
		// Derived field: the spelling names no recorded field.
		"spelling names an unrecorded field": func(m map[string]map[string]string) {
			m["zone_state"]["current_temp_c"] = "zone_room_temperature_value"
		},
		// Object: the spelling sits on an object that does not record the
		// derived field it names.
		"spelling on another object": func(m map[string]map[string]string) {
			m["circuit_state"] = map[string]string{"current_temp_c": "zone_room_temperature"}
		},
	}
	for name, mutate := range spellingMutations {
		m := spellings()
		mutate(m)
		if len(portalFieldRegisterErrors(base, m, portalFieldObjectKeys())) == 0 {
			t.Errorf("spelling mutation %q not caught", name)
		}
	}
	// Without its listed spelling, a derived zone key is reported.
	unlisted := spellings()
	delete(unlisted["zone_state"], "current_temp_c")
	if len(portalFieldRegisterErrors(base, unlisted, portalFieldObjectKeys())) == 0 {
		t.Error("spelling mutation \"listed spelling removed\" not caught")
	}
	// Record: a single-register field whose published name differs from the
	// docs-ebus name of its register.
	changed := append([]portalRegisterNamesField(nil), base...)
	changed[single].DocsEbusName = s.PublishedName + "_value"
	if len(portalFieldRegisterErrors(changed, portalDerivedSpellings, portalFieldObjectKeys())) == 0 {
		t.Error("record mutation \"docs-ebus name differs\" not caught")
	}
}
