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

// The record of B524 register-derived names lives with the GraphQL contract
// test; every surface checks the same rows.
const portalRegisterNamesFixturePath = "../graphql/testdata/register_derived_names.json"

type portalRegisterNamesRow struct {
	PublishedName string            `json:"published_name"`
	Group         string            `json:"group"`
	Register      string            `json:"register"`
	DocsEbusName  string            `json:"docs_ebus_name"`
	PreviousName  string            `json:"previous_name"`
	Surfaces      map[string]string `json:"surfaces"`
	Exemption     string            `json:"exemption"`
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
		Rows []portalRegisterNamesRow `json:"rows"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if len(doc.Rows) == 0 {
		t.Fatal("fixture has no rows")
	}

	types := map[string]reflect.Type{
		"SemanticSystemState":      reflect.TypeOf(SemanticSystemState{}),
		"SemanticSystemConfig":     reflect.TypeOf(SemanticSystemConfig{}),
		"SemanticSystemProperties": reflect.TypeOf(SemanticSystemProperties{}),
		"SemanticCircuitConfig":    reflect.TypeOf(SemanticCircuitConfig{}),
	}
	// Every listed object is scanned for previous names, including one the
	// fixture records no Portal row for.
	published := map[string]map[string]bool{}
	for object, typ := range types {
		published[object] = portalPublishedKeys(t, typ)
	}

	var errs []string
	for _, row := range doc.Rows {
		object := row.Surfaces["portal"]
		if object != "" {
			if _, ok := types[object]; !ok {
				errs = append(errs, "fixture names unknown Portal object "+object)
			} else if !published[object][row.PublishedName] {
				errs = append(errs, fmt.Sprintf("%s does not publish %s (register %s/%s)", object, row.PublishedName, row.Group, row.Register))
			}
		}
		if row.Exemption == "" && row.PublishedName != row.DocsEbusName {
			errs = append(errs, fmt.Sprintf("register %s/%s: published %s, docs-ebus name %s", row.Group, row.Register, row.PublishedName, row.DocsEbusName))
		}
		if row.Exemption != "" {
			if !strings.Contains(row.Exemption, "helianthus-docs-ebus#1145") || row.Register != "0x0007" {
				errs = append(errs, "exemption outside GG00 RR0007 / helianthus-docs-ebus#1145")
			}
			continue
		}
		if row.PreviousName == row.DocsEbusName {
			continue
		}
		for candidate, keys := range published {
			if keys[row.PreviousName] {
				errs = append(errs, fmt.Sprintf("%s publishes previous name %s (register %s/%s)", candidate, row.PreviousName, row.Group, row.Register))
			}
		}
	}
	sort.Strings(errs)
	if len(errs) > 0 {
		t.Fatalf("Portal register names:\n%s", strings.Join(errs, "\n"))
	}
}
