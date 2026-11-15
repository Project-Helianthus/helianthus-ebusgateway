package graphql

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"testing"

	graphqlgo "github.com/graphql-go/graphql"
)

// Computed by querySchemaShapeDigest after every system-plane register field
// except system_off took its docs-ebus name (issue #1483), the circuit
// configuration published RR001D as the integer circuit_special_function_mode
// in place of the removed float field (issue #1474), and the semantic snapshot
// gained the additive optional resource and relation fields of the re-pinned
// semreg snapshot records (issue #1554).
var querySchemaShapeSHA256 = fmt.Sprintf("%x", [sha256.Size]byte{54, 214, 31, 12, 92, 209, 237, 224, 160, 69, 226, 65, 201, 163, 171, 12, 10, 96, 41, 124, 179, 50, 221, 238, 94, 154, 11, 44, 241, 45, 78, 148})

func TestQuerySchemaKeepsIntrospectionShape(t *testing.T) {
	t.Parallel()

	schema, err := NewQuerySchema(NewBuilder(nil, nil))
	if err != nil {
		t.Fatalf("NewQuerySchema() error = %v", err)
	}

	got := querySchemaShapeDigest(schema)
	if got != querySchemaShapeSHA256 {
		t.Fatalf("query schema shape SHA-256 = %s; want %s", got, querySchemaShapeSHA256)
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

func querySchemaShapeDigest(schema graphqlgo.Schema) string {
	var lines []string
	for name, schemaType := range schema.TypeMap() {
		if strings.HasPrefix(name, "__") {
			continue
		}
		switch typed := schemaType.(type) {
		case *graphqlgo.Object:
			fieldNames := make([]string, 0, len(typed.Fields()))
			for fieldName := range typed.Fields() {
				fieldNames = append(fieldNames, fieldName)
			}
			sort.Strings(fieldNames)
			for _, fieldName := range fieldNames {
				field := typed.Fields()[fieldName]
				arguments := make([]string, 0, len(field.Args))
				for _, argument := range field.Args {
					arguments = append(arguments, fmt.Sprintf("%s:%s", argument.Name(), argument.Type.String()))
				}
				sort.Strings(arguments)
				lines = append(lines, fmt.Sprintf("object:%s:%s:%s:(%s)", name, fieldName, field.Type.String(), strings.Join(arguments, ",")))
			}
		case *graphqlgo.Enum:
			values := make([]string, 0, len(typed.Values()))
			for _, value := range typed.Values() {
				values = append(values, value.Name)
			}
			sort.Strings(values)
			lines = append(lines, fmt.Sprintf("enum:%s:%s", name, strings.Join(values, ",")))
		case *graphqlgo.InputObject:
			fieldNames := make([]string, 0, len(typed.Fields()))
			for fieldName := range typed.Fields() {
				fieldNames = append(fieldNames, fieldName)
			}
			sort.Strings(fieldNames)
			for _, fieldName := range fieldNames {
				field := typed.Fields()[fieldName]
				lines = append(lines, fmt.Sprintf("input:%s:%s:%s", name, fieldName, field.Type.String()))
			}
		}
	}
	sort.Strings(lines)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(lines, "\n"))))
}
