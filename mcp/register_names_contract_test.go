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

// The record of B524 register-derived names lives with the GraphQL contract
// test; every surface checks the same rows.
const mcpRegisterNamesFixturePath = "../graphql/testdata/register_derived_names.json"

type mcpRegisterNamesRow struct {
	PublishedName string            `json:"published_name"`
	Group         string            `json:"group"`
	Register      string            `json:"register"`
	DocsEbusName  string            `json:"docs_ebus_name"`
	PreviousName  string            `json:"previous_name"`
	Surfaces      map[string]string `json:"surfaces"`
	Exemption     string            `json:"exemption"`
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
		Rows []mcpRegisterNamesRow `json:"rows"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if len(doc.Rows) == 0 {
		t.Fatal("fixture has no rows")
	}

	types := map[string]reflect.Type{
		"SystemState":      reflect.TypeOf(SystemState{}),
		"SystemConfig":     reflect.TypeOf(SystemConfig{}),
		"SystemProperties": reflect.TypeOf(SystemProperties{}),
	}
	published := map[string]map[string]bool{}
	for _, row := range doc.Rows {
		object := row.Surfaces["mcp"]
		if object == "" {
			continue
		}
		typ, ok := types[object]
		if !ok {
			t.Fatalf("fixture names unknown MCP object %q", object)
		}
		if published[object] == nil {
			published[object] = mcpPublishedKeys(t, typ)
		}
	}

	var errs []string
	for _, row := range doc.Rows {
		if object := row.Surfaces["mcp"]; object != "" && !published[object][row.PublishedName] {
			errs = append(errs, fmt.Sprintf("%s does not publish %s (register %s/%s)", object, row.PublishedName, row.Group, row.Register))
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
		for object, keys := range published {
			if keys[row.PreviousName] {
				errs = append(errs, fmt.Sprintf("%s publishes previous name %s (register %s/%s)", object, row.PreviousName, row.Group, row.Register))
			}
		}
	}
	sort.Strings(errs)
	if len(errs) > 0 {
		t.Fatalf("MCP register names:\n%s", strings.Join(errs, "\n"))
	}
}
