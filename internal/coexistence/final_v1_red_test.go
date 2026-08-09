package coexistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestMSP08FinalV1ExposesSyntheticAndCapturedRuntimeProfiles(t *testing.T) {
	registryPath := filepath.Join(packageDir(t), "contracts/multi-runtime-coexistence-registry-v1.json")
	assertFileSHA256(t, registryPath, "8fab50c488cf99a5f6c29cb8cddc41df9728b5c5edde99e3c1e58d13c9f8407b")

	registry := loadObject(t, registryPath)
	profiles := objectValue(t, registry, "scenario_profiles")
	assertStringSlice(t, profiles["SYNTHETIC_OFFLINE_FIXTURE"], []string{
		"EEBUS_DISABLED_BASELINE",
		"EEBUS_DISABLED_CONFIRMED",
		"EEBUS_ENABLED_NO_SERVICES",
		"EEBUS_CONNECTED_CANDIDATE_ONLY",
		"EEBUS_CONFLICTED_WITHHELD",
		"EEBUS_DISABLED_ROLLBACK",
	})
	assertStringSlice(t, profiles["CAPTURED_RUNTIME_EVIDENCE"], []string{
		"EEBUS_CONNECTED_BASELINE",
		"EEBUS_CONNECTED_RAW_WITHHELD",
		"EEBUS_RESTART_PERSISTED",
		"EEBUS_CONNECTED_ROLLBACK",
	})
	assertStringSlice(t, registry["protected_views"], []string{
		"mcp.ebus.v1.responses",
		"mcp.tool.inventory",
		"graphql.schema",
		"graphql.ebus.values",
		"ha.graphql.values",
		"ha.identity",
		"debug.ebus",
		"portal.ebus.bootstrap",
		"command.routing",
		"semantic.registry",
		"mcp.eebus.v1.contract",
	})
	precedence := stringSlice(t, registry["validation_precedence"])
	if !containsString(precedence, "redaction.public") {
		t.Fatal("final V1 validation precedence omits public redaction")
	}
}

func TestMSP08FinalV1InputsCarryLiveAndTerminalM7Evidence(t *testing.T) {
	typeOfInputs := reflect.TypeOf(InputsV1{})
	for _, field := range []string{
		"M7LiveStatus",
		"M7TerminalGraph",
		"M7TerminalReplay",
		"M7TerminalSourceBundle",
		"M7TerminalSourceReplay",
	} {
		if _, ok := typeOfInputs.FieldByName(field); !ok {
			t.Fatalf("InputsV1 omits final live-profile field %s", field)
		}
	}
}

func TestMSP08FinalV1RemovesAuthorizationMetadataSurface(t *testing.T) {
	exported := exportedProductionNames(t)
	for _, forbidden := range []string{"Binding", "BindingV1"} {
		if exported[forbidden] {
			t.Fatalf("obsolete authorization-era API %s remains exported", forbidden)
		}
	}

	harness := readFile(t, filepath.Join(packageDir(t), "cmd/msp08harness/main.go"))
	if strings.Contains(string(harness), `case "binding":`) {
		t.Fatal("obsolete binding command remains in the private harness")
	}
}

func exportedProductionNames(t *testing.T) map[string]bool {
	t.Helper()
	root := packageDir(t)
	entries, err := filepath.Glob(filepath.Join(root, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]bool)
	files := token.NewFileSet()
	for _, path := range entries {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(files, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range parsed.Decls {
			switch typed := declaration.(type) {
			case *ast.FuncDecl:
				if ast.IsExported(typed.Name.Name) {
					result[typed.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, specification := range typed.Specs {
					switch item := specification.(type) {
					case *ast.TypeSpec:
						if ast.IsExported(item.Name.Name) {
							result[item.Name.Name] = true
						}
					case *ast.ValueSpec:
						for _, name := range item.Names {
							if ast.IsExported(name.Name) {
								result[name.Name] = true
							}
						}
					}
				}
			}
		}
	}
	return result
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
