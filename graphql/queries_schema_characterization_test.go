package graphql

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	graphqlgo "github.com/graphql-go/graphql"
)

var updateQuerySchemaIntrospectionGolden = flag.Bool("update", false, "regenerate graphql/testdata/query_schema_introspection.golden.json")

const querySchemaIntrospectionGoldenPath = "testdata/query_schema_introspection.golden.json"

// querySchemaIntrospectionDocument is the one fixed introspection document the
// golden records. The type reference chain is seven levels deep, enough for
// the deepest wrapping a field type can carry.
const querySchemaIntrospectionDocument = `query QuerySchemaIntrospection {
  __schema {
    queryType { name }
    types {
      kind
      name
      fields(includeDeprecated: true) {
        name
        args { name type { ...TypeRef } }
        type { ...TypeRef }
      }
      inputFields { name type { ...TypeRef } }
      enumValues(includeDeprecated: true) { name }
    }
  }
}

fragment TypeRef on __Type {
  kind
  name
  ofType {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType { kind name }
            }
          }
        }
      }
    }
  }
}
`

func TestQuerySchemaMatchesIntrospectionGolden(t *testing.T) {
	schema, err := NewQuerySchema(NewBuilder(nil, nil))
	if err != nil {
		t.Fatalf("NewQuerySchema() error = %v", err)
	}

	got, err := canonicalSchemaIntrospection(schema)
	if err != nil {
		t.Fatalf("canonicalSchemaIntrospection() error = %v", err)
	}

	if *updateQuerySchemaIntrospectionGolden {
		if err := os.MkdirAll(filepath.Dir(querySchemaIntrospectionGoldenPath), 0o755); err != nil {
			t.Fatalf("create golden directory: %v", err)
		}
		if err := os.WriteFile(querySchemaIntrospectionGoldenPath, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
	}

	want, err := os.ReadFile(querySchemaIntrospectionGoldenPath)
	if err != nil {
		t.Fatalf("read golden %s: %v (run go test ./graphql -run TestQuerySchema -update to regenerate it)", querySchemaIntrospectionGoldenPath, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("query schema introspection differs from %s (run go test ./graphql -run TestQuerySchema -update to regenerate it and review the diff)\n%s",
			querySchemaIntrospectionGoldenPath, firstGoldenLineDifference(want, got))
	}
}

func TestQuerySchemaIntrospectionGoldenRecordsTheDocument(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(querySchemaIntrospectionGoldenPath)
	if err != nil {
		t.Fatalf("read golden %s: %v", querySchemaIntrospectionGoldenPath, err)
	}
	var golden struct {
		Document string `json:"document"`
		Response struct {
			Schema struct {
				QueryType struct {
					Name string `json:"name"`
				} `json:"queryType"`
				Types []map[string]any `json:"types"`
			} `json:"__schema"`
		} `json:"response"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if golden.Document != querySchemaIntrospectionDocument {
		t.Fatalf("golden document differs from the executed introspection document")
	}
	schema, err := NewQuerySchema(NewBuilder(nil, nil))
	if err != nil {
		t.Fatalf("NewQuerySchema() error = %v", err)
	}
	if want := schema.QueryType().Name(); golden.Response.Schema.QueryType.Name != want {
		t.Fatalf("golden query type = %q; want %q", golden.Response.Schema.QueryType.Name, want)
	}
	if len(golden.Response.Schema.Types) == 0 {
		t.Fatalf("golden records no types")
	}
}

// TestQuerySchemaIntrospectionDetectsEveryRecordedChange varies each recorded
// element of a small schema and requires the canonical introspection to change.
func TestQuerySchemaIntrospectionDetectsEveryRecordedChange(t *testing.T) {
	t.Parallel()

	type shape struct {
		typeName   string
		fieldName  string
		fieldType  graphqlgo.Output
		argName    string
		argType    graphqlgo.Input
		enumName   string
		enumValue  string
		inputName  string
		inputField string
		inputType  graphqlgo.Input
	}
	base := shape{
		typeName:   "Probe",
		fieldName:  "reading",
		fieldType:  graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.Int))),
		argName:    "limit",
		argType:    graphqlgo.Int,
		enumName:   "ProbeMode",
		enumValue:  "AUTO",
		inputName:  "ProbeFilter",
		inputField: "min",
		inputType:  graphqlgo.Float,
	}
	build := func(s shape) []byte {
		t.Helper()
		enum := graphqlgo.NewEnum(graphqlgo.EnumConfig{
			Name:   s.enumName,
			Values: graphqlgo.EnumValueConfigMap{s.enumValue: &graphqlgo.EnumValueConfig{Value: s.enumValue}},
		})
		input := graphqlgo.NewInputObject(graphqlgo.InputObjectConfig{
			Name:   s.inputName,
			Fields: graphqlgo.InputObjectConfigFieldMap{s.inputField: &graphqlgo.InputObjectFieldConfig{Type: s.inputType}},
		})
		args := graphqlgo.FieldConfigArgument{
			s.argName: &graphqlgo.ArgumentConfig{Type: s.argType},
			"mode":    &graphqlgo.ArgumentConfig{Type: enum},
			"filter":  &graphqlgo.ArgumentConfig{Type: input},
		}
		probe := graphqlgo.NewObject(graphqlgo.ObjectConfig{
			Name: s.typeName,
			Fields: graphqlgo.Fields{
				s.fieldName: &graphqlgo.Field{Type: s.fieldType, Args: args},
			},
		})
		schema, err := graphqlgo.NewSchema(graphqlgo.SchemaConfig{
			Query: graphqlgo.NewObject(graphqlgo.ObjectConfig{
				Name:   "Query",
				Fields: graphqlgo.Fields{"probe": &graphqlgo.Field{Type: probe}},
			}),
		})
		if err != nil {
			t.Fatalf("NewSchema() error = %v", err)
		}
		out, err := canonicalSchemaIntrospection(schema)
		if err != nil {
			t.Fatalf("canonicalSchemaIntrospection() error = %v", err)
		}
		return out
	}

	reference := build(base)
	if !bytes.Equal(reference, build(base)) {
		t.Fatalf("canonical introspection is not deterministic")
	}
	variations := map[string]func(*shape){
		"object type name": func(s *shape) { s.typeName = "Sensor" },
		"field name":       func(s *shape) { s.fieldName = "value" },
		"field named type": func(s *shape) {
			s.fieldType = graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.String)))
		},
		"field wrapper kind": func(s *shape) { s.fieldType = graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.Int)) },
		"argument name":      func(s *shape) { s.argName = "first" },
		"argument type":      func(s *shape) { s.argType = graphqlgo.NewNonNull(graphqlgo.Int) },
		"enum type name":     func(s *shape) { s.enumName = "ProbeSetting" },
		"enum value":         func(s *shape) { s.enumValue = "MANUAL" },
		"input type name":    func(s *shape) { s.inputName = "ProbeRange" },
		"input field name":   func(s *shape) { s.inputField = "max" },
		"input field type":   func(s *shape) { s.inputType = graphqlgo.String },
	}
	for name, vary := range variations {
		varied := base
		vary(&varied)
		if bytes.Equal(reference, build(varied)) {
			t.Errorf("changing the %s left the canonical introspection unchanged", name)
		}
	}
}

func TestQuerySchemaKeepsExactRootFieldSet(t *testing.T) {
	t.Parallel()

	schema, err := NewQuerySchema(NewBuilder(nil, nil))
	if err != nil {
		t.Fatalf("NewQuerySchema() error = %v", err)
	}

	got := make([]string, 0, len(schema.QueryType().Fields()))
	for name := range schema.QueryType().Fields() {
		got = append(got, name)
	}
	sort.Strings(got)
	want := []string{
		"adapter_hardware_info",
		"adapter_status",
		"boiler_status",
		"bus_identification_evidence",
		"bus_messages",
		"bus_periodicity",
		"bus_summary",
		"circuits",
		"cylinders",
		"daemon_status",
		"device",
		"devices",
		"dhw",
		"energy_totals",
		"fm5_interpretation",
		"fm5_semantic_mode",
		"gateway_identity",
		"methods",
		"planes",
		"radio_devices",
		"schedules",
		"semantic_snapshot",
		"solar",
		"system",
		"vaillant_capabilities",
		"vaillant_error_history",
		"vaillant_errors",
		"vaillant_errors_history",
		"vaillant_live_monitor",
		"vaillant_live_monitor_session",
		"vaillant_regulator_capability",
		"vaillant_service_current",
		"vaillant_service_history",
		"watch_summary",
		"zones",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("query root fields = %v; want %v", got, want)
	}
}

// canonicalSchemaIntrospection executes the fixed introspection document and
// returns the golden bytes: the document and the response with types, fields,
// arguments, input fields and enum values sorted by name, indented JSON with a
// trailing newline.
func canonicalSchemaIntrospection(schema graphqlgo.Schema) ([]byte, error) {
	result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: querySchemaIntrospectionDocument})
	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("introspection errors: %v", result.Errors)
	}
	// Round-trip through JSON so the response is plain maps and slices.
	raw, err := json.Marshal(result.Data)
	if err != nil {
		return nil, fmt.Errorf("encode introspection response: %w", err)
	}
	var response map[string]any
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("decode introspection response: %w", err)
	}
	schemaNode, ok := response["__schema"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("introspection response has no __schema object")
	}
	types, ok := schemaNode["types"].([]any)
	if !ok {
		return nil, fmt.Errorf("introspection response has no types list")
	}
	sortIntrospectionByName(types)
	for _, entry := range types {
		typeNode, ok := entry.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("introspection type entry is not an object")
		}
		for _, key := range []string{"fields", "inputFields", "enumValues"} {
			list, _ := typeNode[key].([]any)
			sortIntrospectionByName(list)
		}
		fields, _ := typeNode["fields"].([]any)
		for _, field := range fields {
			fieldNode, ok := field.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("introspection field entry is not an object")
			}
			if err := requireCompleteTypeChain(fieldNode["type"]); err != nil {
				return nil, fmt.Errorf("type %v field %v: %w", typeNode["name"], fieldNode["name"], err)
			}
			args, _ := fieldNode["args"].([]any)
			sortIntrospectionByName(args)
		}
	}

	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(map[string]any{
		"document": querySchemaIntrospectionDocument,
		"response": response,
	}); err != nil {
		return nil, fmt.Errorf("encode golden: %w", err)
	}
	return out.Bytes(), nil
}

// requireCompleteTypeChain fails when a type reference ends in a wrapper the
// document's chain depth did not reach, so the golden never records a
// truncated type.
func requireCompleteTypeChain(node any) error {
	for node != nil {
		typeRef, ok := node.(map[string]any)
		if !ok {
			return fmt.Errorf("type reference is not an object")
		}
		kind, _ := typeRef["kind"].(string)
		if kind != "LIST" && kind != "NON_NULL" {
			return nil
		}
		next, present := typeRef["ofType"]
		if !present {
			return fmt.Errorf("type chain deeper than the introspection document records")
		}
		node = next
	}
	return fmt.Errorf("type chain ends in a wrapper without a named type")
}

func sortIntrospectionByName(list []any) {
	sort.SliceStable(list, func(i, j int) bool {
		return introspectionEntryName(list[i]) < introspectionEntryName(list[j])
	})
}

func introspectionEntryName(entry any) string {
	node, _ := entry.(map[string]any)
	name, _ := node["name"].(string)
	return name
}

func firstGoldenLineDifference(want, got []byte) string {
	wantLines := strings.Split(string(want), "\n")
	gotLines := strings.Split(string(got), "\n")
	for i := 0; i < len(wantLines) || i < len(gotLines); i++ {
		var w, g string
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if w != g {
			return fmt.Sprintf("first difference at line %d:\n  golden: %s\n  schema: %s", i+1, w, g)
		}
	}
	return "no line difference (trailing bytes differ)"
}
