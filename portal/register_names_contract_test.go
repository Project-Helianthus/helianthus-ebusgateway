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
