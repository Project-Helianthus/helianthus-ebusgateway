package main

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

type msp06GatewayRuntime struct {
	snapshot      eebusruntime.SnapshotV1
	snapshotErr   error
	snapshotCalls int
}

var _ mcp.EEBusV1Provider = (*eebusRuntimeAdapter)(nil)

func (*msp06GatewayRuntime) Start(context.Context) error { return nil }
func (*msp06GatewayRuntime) Shutdown() error             { return nil }
func (*msp06GatewayRuntime) PairingState() ([]eebusruntime.PairingObservationV1, error) {
	return nil, nil
}

func (runtime *msp06GatewayRuntime) Snapshot() (eebusruntime.SnapshotV1, error) {
	runtime.snapshotCalls++
	return runtime.snapshot, runtime.snapshotErr
}

func TestMSP06RuntimeAdapterForwardsOnlySnapshotReads(t *testing.T) {
	wantSnapshot := eebusruntime.SnapshotV1{
		Meta: eebusruntime.SnapshotMetaV1{Contract: eebusruntime.SnapshotContractV1},
	}
	wantErr := errors.New("fixture snapshot failure")
	runtime := &msp06GatewayRuntime{snapshot: wantSnapshot, snapshotErr: wantErr}
	adapter := &eebusRuntimeAdapter{runtime: runtime}

	gotSnapshot, gotErr := adapter.Snapshot()
	if gotSnapshot.Meta.Contract != wantSnapshot.Meta.Contract || !errors.Is(gotErr, wantErr) {
		t.Fatalf("adapter.Snapshot() = (%+v, %v), want forwarded (%+v, %v)", gotSnapshot, gotErr, wantSnapshot, wantErr)
	}
	if runtime.snapshotCalls != 1 {
		t.Fatalf("runtime Snapshot calls = %d, want one", runtime.snapshotCalls)
	}

	var nilAdapter *eebusRuntimeAdapter
	if _, err := nilAdapter.Snapshot(); err == nil {
		t.Fatal("nil adapter Snapshot succeeded, want unavailable error")
	}
	if _, err := (&eebusRuntimeAdapter{}).Snapshot(); err == nil {
		t.Fatal("adapter without runtime Snapshot succeeded, want unavailable error")
	}
}

func TestMSP06GatewayRegistersProviderConditionallyBeforeMCPMount(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}

	var registerPosition token.Pos
	var mountPosition token.Pos
	registrationGuarded := false
	ast.Inspect(file, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.CallExpr:
			selector, ok := value.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if selector.Sel.Name == "RegisterEEBusV1Provider" {
				if registerPosition.IsValid() {
					t.Fatal("RegisterEEBusV1Provider appears more than once in gateway bootstrap")
				}
				registerPosition = value.Pos()
			}
			if selector.Sel.Name == "Handle" && len(value.Args) > 0 {
				pathSelector, ok := value.Args[0].(*ast.SelectorExpr)
				if ok && pathSelector.Sel.Name == "MCPPath" {
					mountPosition = value.Pos()
				}
			}
		case *ast.IfStmt:
			containsRegistration := false
			ast.Inspect(value.Body, func(child ast.Node) bool {
				call, ok := child.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if ok && selector.Sel.Name == "RegisterEEBusV1Provider" {
					containsRegistration = true
				}
				return true
			})
			if containsRegistration {
				var condition strings.Builder
				if err := printer.Fprint(&condition, fset, value.Cond); err != nil {
					t.Fatal(err)
				}
				registrationGuarded = strings.Contains(condition.String(), "!= nil")
			}
		}
		return true
	})

	if !registerPosition.IsValid() {
		t.Fatal("gateway bootstrap does not register the enabled eeBUS runtime with MCP")
	}
	if !mountPosition.IsValid() {
		t.Fatal("gateway MCP mount was not found")
	}
	if registerPosition >= mountPosition {
		t.Fatalf("eeBUS provider registration at %s must complete before MCP mount at %s", fset.Position(registerPosition), fset.Position(mountPosition))
	}
	if !registrationGuarded {
		t.Fatal("eeBUS MCP provider registration is not guarded by a non-nil enabled runtime")
	}
}

func TestMSP06GatewayPassesTypedProviderDirectlyWithoutContextTransport(t *testing.T) {
	fset := token.NewFileSet()
	mainFile, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	adapterFile, err := parser.ParseFile(fset, "eebus_runtime_adapter.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	typedParameter := false
	explicitArgument := false
	for _, declaration := range mainFile.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "startHTTPServer" {
			continue
		}
		for _, field := range function.Type.Params.List {
			var rendered strings.Builder
			if err := printer.Fprint(&rendered, fset, field.Type); err != nil {
				t.Fatal(err)
			}
			if rendered.String() == "mcp.EEBusV1Provider" {
				typedParameter = true
			}
		}
	}
	ast.Inspect(mainFile, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		function, ok := call.Fun.(*ast.Ident)
		if !ok || function.Name != "startHTTPServerFn" {
			return true
		}
		for _, argument := range call.Args {
			identifier, ok := argument.(*ast.Ident)
			if ok && identifier.Name == "eebusAdapter" {
				explicitArgument = true
			}
		}
		return true
	})
	if !typedParameter {
		t.Error("startHTTPServer has no explicit mcp.EEBusV1Provider parameter")
	}
	if !explicitArgument {
		t.Error("gateway bootstrap does not pass eebusAdapter directly to startHTTPServerFn")
	}

	for filename, file := range map[string]*ast.File{
		"main.go":                  mainFile,
		"eebus_runtime_adapter.go": adapterFile,
	} {
		ast.Inspect(file, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			switch identifier.Name {
			case "eebusMCPProviderContextKey", "withEEBusMCPProvider", "eebusMCPProviderFromContext":
				t.Errorf("%s retains hidden provider context transport %q at %s", filename, identifier.Name, fset.Position(identifier.Pos()))
			}
			return true
		})
	}
}

func TestMSP06NoEEBusConsumerOrSemanticProjectionDrift(t *testing.T) {
	for _, directory := range []string{"../../graphql", "../../portal"} {
		err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			text := string(content)
			if strings.Contains(text, "eebus.v1") || strings.Contains(text, "helianthus-eebusreg") || strings.Contains(text, "EEBusV1Provider") {
				t.Errorf("%s contains an MSP-06 eeBUS consumer/projection dependency", path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMSP06ProviderRegistrationDoesNotEscapeMCPBootstrap(t *testing.T) {
	root := filepath.Clean("../..")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !strings.Contains(string(content), "EEBusV1Provider") && !strings.Contains(string(content), "RegisterEEBusV1Provider") {
			return nil
		}
		relative, err := filepath.Rel(".", path)
		if err != nil {
			return err
		}
		clean := filepath.Clean(relative)
		allowedMCP := strings.HasPrefix(clean, filepath.Clean("../../mcp")+string(filepath.Separator))
		allowedGateway := clean == filepath.Clean("../../cmd/gateway/main.go") || clean == filepath.Clean("../../cmd/gateway/eebus_runtime_adapter.go")
		if !allowedMCP && !allowedGateway {
			t.Errorf("MSP-06 provider registration escaped MCP/bootstrap boundary into %s", relative)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMSP06PublicMCPPackageDeclaresNoToolDropCapability(t *testing.T) {
	entries, err := os.ReadDir("../../mcp")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join("../../mcp", name)
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if ok && identifier.Name == "ToolDrop" && identifier.Obj != nil {
				t.Errorf("%s declares forbidden public ToolDrop capability at %s", path, fset.Position(identifier.Pos()))
			}
			return true
		})
	}
}
