package graphql

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"testing"

	graphqlgo "github.com/graphql-go/graphql"
)

const querySchemaShapeSHA256 = "92f2b7e6700bdb2181a108096f39295e5e16b87870fa7e9b3f5d81eadfa50cb1"

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
		"adapterHardwareInfo", "adapterStatus", "adapter_hardware_info", "adapter_status",
		"boilerStatus", "boiler_status", "busMessages", "busPeriodicity", "busSummary",
		"circuits", "cylinders", "daemonStatus", "daemon_status", "device", "devices", "dhw",
		"energyTotals", "energy_totals", "fm5Interpretation", "fm5SemanticMode", "fm5_semantic_mode",
		"gatewayIdentity", "gateway_identity", "methods", "planes", "radioDevices", "radio_devices",
		"schedules", "solar", "system", "vaillantCapabilities", "vaillantErrorHistory", "vaillantErrors",
		"vaillantLiveMonitor", "vaillantServiceCurrent", "vaillantServiceHistory", "watchSummary", "zones",
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
