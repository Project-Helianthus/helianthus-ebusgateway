package ebusgateway

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"golang.org/x/mod/modfile"
)

const (
	eebusregModule = "github.com/Project-Helianthus/helianthus-eebusreg"
	eebusgoModule  = "github.com/Project-Helianthus/helianthus-eebus-go"
	shipgoModule   = "github.com/Project-Helianthus/helianthus-ship-go"
	spinegoModule  = "github.com/Project-Helianthus/helianthus-spine-go"
)

func TestIssue762ReleasedEEBusDependencyClosure(t *testing.T) {
	contents, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	parsed, err := modfile.Parse("go.mod", contents, nil)
	if err != nil {
		t.Fatalf("parse go.mod: %v", err)
	}
	if len(parsed.Replace) != 0 {
		t.Fatalf("go.mod has replace directives: %#v", parsed.Replace)
	}

	want := map[string]string{
		eebusregModule: "v0.1.27",
		eebusgoModule:  "v0.7.1-helianthus.12",
		shipgoModule:   "v0.6.1-helianthus.10",
		spinegoModule:  "v0.7.1-helianthus.8",
	}
	declared := make(map[string]string, len(want))
	for _, requirement := range parsed.Require {
		if _, required := want[requirement.Mod.Path]; required {
			declared[requirement.Mod.Path] = requirement.Mod.Version
			if strings.Contains(requirement.Mod.Version, "-0.") {
				t.Errorf("%s is declared at pseudo-version %q", requirement.Mod.Path, requirement.Mod.Version)
			}
		}
	}
	if !reflect.DeepEqual(declared, want) {
		t.Fatalf("eeBUS declared requirements = %#v; want %#v", declared, want)
	}

	selected := selectedModuleVersions(t, want)
	if !reflect.DeepEqual(selected, want) {
		t.Fatalf("eeBUS selected module graph = %#v; want %#v", selected, want)
	}
}

func selectedModuleVersions(t *testing.T, want map[string]string) map[string]string {
	t.Helper()

	command := exec.Command("go", "list", "-m", "all")
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -m all: %v\n%s", err, output)
	}

	selected := make(map[string]string, len(want))
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if _, required := want[fields[0]]; !required {
			continue
		}
		if len(fields) != 2 {
			t.Fatalf("parse selected module line %q: want path and version", line)
		}
		selected[fields[0]] = fields[1]
		if strings.Contains(fields[1], "-0.") {
			t.Errorf("%s is selected at pseudo-version %q", fields[0], fields[1])
		}
	}
	return selected
}

func TestEEBusregV0118RuntimeAndGatewayBoundary(t *testing.T) {
	remoteType := reflect.TypeOf(eebusruntime.Remote{})
	if remoteType.NumField() != 1 || remoteType.Field(0).Name != "SKI" {
		t.Fatalf("eebusruntime.Remote fields = %d/%v; want exactly SKI", remoteType.NumField(), remoteFieldNames(remoteType))
	}

	for _, path := range []string{"config.go", "cmd/gateway/eebus_config_flags.go", "cmd/gateway/eebus_runtime_config.go"} {
		file := parseGoFile(t, path)
		forbidden := forbiddenEEBusLegacySymbols(file)
		if len(forbidden) != 0 {
			t.Errorf("%s retains forbidden eeBUS legacy symbols: %s", path, strings.Join(forbidden, ", "))
		}
	}

	for _, path := range productionGoFiles(t, ".") {
		file := parseGoFile(t, path)
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok || general.Tok != token.IMPORT {
				continue
			}
			for _, spec := range general.Specs {
				importSpec := spec.(*ast.ImportSpec)
				importPath := strings.Trim(importSpec.Path.Value, "\"")
				if moduleImportMatches(importPath, eebusgoModule) || moduleImportMatches(importPath, shipgoModule) {
					t.Errorf("%s imports protocol implementation module %q directly", path, importPath)
				}
			}
		}
	}
}

func TestEEBusregV0118CandidateFlowStaysPrivate(t *testing.T) {
	forbidden := []string{
		"candidate_ref",
		"CandidateRef",
		"PairingCandidateQueuer",
		"QueuePairingCandidate",
		"VisiblePairingCandidatesUpdated",
		"select_candidate",
	}
	for _, path := range productionGoFiles(t, ".") {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, token := range forbidden {
			if strings.Contains(string(contents), token) {
				t.Errorf("%s leaks private eeBUS candidate token %q", path, token)
			}
		}
	}
}

func moduleImportMatches(importPath, modulePath string) bool {
	return importPath == modulePath || strings.HasPrefix(importPath, modulePath+"/")
}

func TestModuleImportMatches(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		importPath string
		want       bool
	}{
		{name: "module root", importPath: eebusgoModule, want: true},
		{name: "module subpackage", importPath: eebusgoModule + "/service", want: true},
		{name: "similar module", importPath: eebusgoModule + "-other/service", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := moduleImportMatches(test.importPath, eebusgoModule); got != test.want {
				t.Fatalf("moduleImportMatches(%q, %q) = %t; want %t", test.importPath, eebusgoModule, got, test.want)
			}
		})
	}
}

func remoteFieldNames(remoteType reflect.Type) []string {
	fields := make([]string, 0, remoteType.NumField())
	for index := 0; index < remoteType.NumField(); index++ {
		fields = append(fields, remoteType.Field(index).Name)
	}
	return fields
}

func forbiddenEEBusLegacySymbols(file *ast.File) []string {
	forbidden := map[string]struct{}{
		"RemoteEndpoints":                    {},
		"mapEEBusRemoteEndpoints":            {},
		"parseEEBusRemoteEndpoint":           {},
		"validateEEBusRemoteEndpointAddress": {},
		"validEEBusRemoteEndpointPath":       {},
		"Endpoint":                           {},
		"EndpointPath":                       {},
	}
	found := make(map[string]struct{})
	ast.Inspect(file, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if ok {
			if _, banned := forbidden[identifier.Name]; banned {
				found[identifier.Name] = struct{}{}
			}
		}
		return true
	})
	result := make([]string, 0, len(found))
	for symbol := range found {
		result = append(result, symbol)
	}
	return result
}

func parseGoFile(t *testing.T, path string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return file
}

func productionGoFiles(t *testing.T, root string) []string {
	t.Helper()
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) == ".go" && !strings.HasSuffix(path, "_test.go") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk production Go files: %v", err)
	}
	return paths
}
