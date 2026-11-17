package graphql

import (
	"os"
	"regexp"
	"strings"
	"testing"

	graphqlgo "github.com/graphql-go/graphql"
)

// boilerPumpProvenanceDocPath is the page that tells consumers where the
// circuit's own pump and circuit state are published.
const boilerPumpProvenanceDocPath = "../docs/boiler-pump-provenance.md"

var (
	// boilerPumpDocCircuitPath is a GraphQL path to a circuit state field.
	boilerPumpDocCircuitPath = regexp.MustCompile("`circuits\\[\\]\\.state\\.([A-Za-z0-9_]+)`")
	// boilerPumpDocSurfaceName is a field name the page gives for the MCP
	// and Portal surfaces.
	boilerPumpDocSurfaceName = regexp.MustCompile("`([A-Za-z0-9_]+)` in (?:GraphQL, )?MCP and Portal")
)

// boilerPumpProvenanceDocErrors reports every circuit state field the page
// names that is not a circuit_state field both recorded in the register
// fixture and published by the schema.
func boilerPumpProvenanceDocErrors(page string, recorded, published map[string]bool) []string {
	var names []string
	for _, match := range boilerPumpDocCircuitPath.FindAllStringSubmatch(page, -1) {
		names = append(names, match[1])
	}
	for _, match := range boilerPumpDocSurfaceName.FindAllStringSubmatch(page, -1) {
		names = append(names, match[1])
	}
	if len(names) == 0 {
		return []string{"page names no circuit state field"}
	}
	var errs []string
	for _, name := range names {
		if !recorded[name] {
			errs = append(errs, "page names "+name+", which is not a recorded circuit_state field")
		}
		if !published[name] {
			errs = append(errs, "page names "+name+", which the circuit_state type does not publish")
		}
	}
	return errs
}

func TestBoilerPumpProvenanceDocNamesPublishedCircuitFields(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(boilerPumpProvenanceDocPath)
	if err != nil {
		t.Fatalf("read %s: %v", boilerPumpProvenanceDocPath, err)
	}
	doc := loadRegisterNamesDoc(t)
	recorded := map[string]bool{}
	for _, field := range doc.Fields {
		if field.Object == "circuit_state" {
			recorded[field.PublishedName] = true
		}
	}
	schema, err := NewQuerySchema(NewBuilder(nil, nil))
	if err != nil {
		t.Fatalf("query schema: %v", err)
	}
	object, ok := schema.Type("circuit_state").(*graphqlgo.Object)
	if !ok {
		t.Fatal("schema has no circuit_state object")
	}
	published := map[string]bool{}
	for name := range object.Fields() {
		published[name] = true
	}
	if errs := boilerPumpProvenanceDocErrors(string(raw), recorded, published); len(errs) > 0 {
		t.Fatalf("%s:\n%s", boilerPumpProvenanceDocPath, strings.Join(errs, "\n"))
	}
	for _, page := range []string{
		"(`circuits[].state.pumpActive` in GraphQL)",
		"(`pump_active` in MCP and Portal)",
		"(`circuits[].state.circuitState`)",
		"no circuit field named here",
	} {
		if errs := boilerPumpProvenanceDocErrors(page, recorded, published); len(errs) == 0 {
			t.Errorf("checker accepts %q", page)
		}
	}
}
