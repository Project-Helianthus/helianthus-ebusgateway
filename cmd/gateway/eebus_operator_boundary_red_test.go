package main

import (
	"bufio"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestIssue819ReleasedDependencyClosurePinsEEBusregV0132(t *testing.T) {
	goMod, err := os.Open("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = goMod.Close() }()

	const module = "github.com/Project-Helianthus/helianthus-eebusreg"
	var versions []string
	scanner := bufio.NewScanner(goMod)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == module {
			versions = append(versions, fields[1])
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 || versions[0] != "v0.1.33" {
		t.Fatalf("%s versions = %v, want exactly [v0.1.33]", module, versions)
	}
}

func TestIssue743ProductionImportsNoUpstreamForkTypesAcrossGatewayBoundary(t *testing.T) {
	for _, directory := range []string{"../../mcp", "."} {
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(directory, entry.Name())
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			for _, spec := range file.Imports {
				importPath := strings.Trim(spec.Path.Value, `"`)
				if strings.Contains(importPath, "github.com/enbility/") ||
					strings.Contains(importPath, "helianthus-eebus-go") ||
					strings.Contains(importPath, "helianthus-ship-go") ||
					strings.Contains(importPath, "helianthus-spine-go") {
					t.Errorf("%s directly imports forbidden upstream/fork type package %q", path, importPath)
				}
			}
		}
	}
}

func TestIssue743StablePublicSurfaceHasNoCandidateRawAliasOrTierSelector(t *testing.T) {
	root := filepath.Clean("../..")
	forbidden := regexp.MustCompile(`candidate_ref|RawSnapshotV[0-9]+|eebus\.v2\.|eebus\.legacy\.`)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor", "testdata":
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
		if match := forbidden.Find(content); match != nil {
			t.Errorf("%s exposes forbidden stable surface %q", path, match)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestIssue743RawEEBusFieldsDoNotEnterGraphQLPortalOrSemanticPublicTypes(t *testing.T) {
	for _, directory := range []string{"../../graphql", "../../portal"} {
		err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, rawField := range []string{
				`json:"remote_ski`,
				`json:"ship_id`,
				`json:"entity_address`,
				`json:"feature_address`,
				`json:"context_address`,
				`json:"document_subrevision`,
			} {
				if strings.Contains(string(content), rawField) {
					t.Errorf("%s exposes raw eeBUS field %q", path, rawField)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestIssue743EEBusProviderPublicSurfaceRemainsFirstPartySnapshotOnly(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir("../../mcp")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join("../../mcp", entry.Name())
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok || general.Tok != token.TYPE {
				continue
			}
			for _, rawSpec := range general.Specs {
				spec, ok := rawSpec.(*ast.TypeSpec)
				if !ok || !spec.Name.IsExported() {
					continue
				}
				if strings.Contains(spec.Name.Name, "RawSnapshot") ||
					strings.Contains(spec.Name.Name, "RedactedSnapshot") ||
					strings.Contains(spec.Name.Name, "TierSelector") {
					t.Errorf("%s exports forbidden gateway-owned compatibility/selector type %s", path, spec.Name.Name)
				}
			}
		}
	}
}

func TestIssue743ExportedSignaturesContainNoUpstreamForkTypes(t *testing.T) {
	for _, path := range issue743ProductionGoFiles(t, "../../mcp") {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		imports := make(map[string]string)
		for _, spec := range file.Imports {
			importPath := strings.Trim(spec.Path.Value, `"`)
			name := filepath.Base(importPath)
			if spec.Name != nil {
				name = spec.Name.Name
			}
			imports[name] = importPath
		}
		for _, declaration := range file.Decls {
			if !issue743ExportedDeclaration(declaration) {
				continue
			}
			ast.Inspect(declaration, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				identifier, ok := selector.X.(*ast.Ident)
				if !ok {
					return true
				}
				importPath := imports[identifier.Name]
				if issue743ForbiddenForkImport(importPath) {
					t.Errorf("%s exported declaration references forbidden upstream type %s.%s from %q",
						path, identifier.Name, selector.Sel.Name, importPath)
				}
				return true
			})
		}
	}
}

func TestIssue743ConsumerDependencyClosureDoesNotDirectlyImportRawEEBusRuntime(t *testing.T) {
	command := exec.Command("go", "list", "-json", "../../graphql", "../../portal", "../../ui")
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	for decoder.More() {
		var pkg struct {
			ImportPath string
			Imports    []string
		}
		if err := decoder.Decode(&pkg); err != nil {
			t.Fatal(err)
		}
		for _, importPath := range pkg.Imports {
			if importPath == "github.com/Project-Helianthus/helianthus-eebusreg" ||
				importPath == "github.com/Project-Helianthus/helianthus-eebusreg/eebusraw" ||
				issue743ForbiddenForkImport(importPath) {
				t.Errorf("%s directly imports raw eeBUS runtime dependency %q", pkg.ImportPath, importPath)
			}
		}
	}
}

func TestIssue809GraphQLSemanticAndUnprotectedPortalTypesContainNoRawEEBusFields(t *testing.T) {
	root := filepath.Clean("../..")
	paths := []string{
		filepath.Join(root, "graphql"),
		filepath.Join(root, "ui"),
	}
	for _, surface := range paths {
		err := filepath.WalkDir(surface, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if entry.Name() == "node_modules" {
					return filepath.SkipDir
				}
				return nil
			}
			switch filepath.Ext(path) {
			case ".go", ".js", ".json", ".html":
			default:
				return nil
			}
			if strings.HasSuffix(path, "_test.go") || strings.Contains(path, string(filepath.Separator)+"test"+string(filepath.Separator)) {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, forbidden := range []string{
				"candidate_ref",
				"remote_ski",
				"ship_id",
				"entity_address",
				"feature_address",
				"context_address",
				"document_subrevision",
				"secondary_digest",
			} {
				if strings.Contains(string(content), forbidden) {
					t.Errorf("%s exposes raw eeBUS surface token %q", path, forbidden)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// The post-M9 Portal owner workbench is the one authorized raw consumer.
	// Its JavaScript calls only the authenticated admin boundary; raw eeBUS
	// fields still may not become Portal Go DTOs, GraphQL, UI, or semantic
	// registry types. Candidate non-persistence is exercised by the Portal
	// node tests rather than by banning the field names needed to render it.
	for _, portalRoot := range []string{filepath.Join(root, "portal")} {
		err := filepath.WalkDir(portalRoot, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			content, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			for _, forbidden := range []string{`json:"remote_ski`, `json:"ship_id`, `json:"entity_address`, `json:"feature_address`, `json:"context_address`} {
				if strings.Contains(string(content), forbidden) {
					t.Errorf("%s exposes raw eeBUS Portal Go type %q", path, forbidden)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestIssue817EEBusSpecificAuthenticationAndSplitConsumerPolicyAreAbsent(t *testing.T) {
	checks := map[string][]string{
		"../../internal/eebusadmin/auth.go": {
			"type authentication struct", "authenticate(", "validateCSRF", "ownerSessionCookie",
			"OwnerSecret", "HASecret", "Authorization", "SetCookie", "X-CSRF-Token",
		},
		"../../internal/eebusadmin/types.go": {
			"type AuthConfig struct", "Auth  AuthConfig", "PrincipalPortalOwner", "PrincipalHAIntegration",
			"projection_revision", "haEnvelope", "haStatus",
		},
		"../../internal/eebusadmin/server.go": {
			"newAuthentication", "server.auth", "validateCSRF", "PrincipalPortalOwner", "PrincipalHAIntegration",
			"sanitizeHAPartner", "acceptHAProjection", "haPseudonym", "projectionRevision",
		},
		"../../portal/web/src/app.js": {
			"loginEEBusAdmin", "_eebusCSRFToken", "X-CSRF-Token", "Owner username", "Owner credential", ">Authenticate<",
		},
	}
	for path, forbiddenTokens := range checks {
		content, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) && strings.HasSuffix(path, "/auth.go") {
				continue
			}
			t.Fatal(err)
		}
		for _, forbidden := range forbiddenTokens {
			if strings.Contains(string(content), forbidden) {
				t.Errorf("%s retains eeBUS-specific auth or split-consumer token %q", path, forbidden)
			}
		}
	}
}

func TestIssue817OpaqueOperatorStateRemainsBoundedAndPrivate(t *testing.T) {
	serverContent, err := os.ReadFile("../../internal/eebusadmin/server.go")
	if err != nil {
		t.Fatal(err)
	}
	serverSource := string(serverContent)
	for _, required := range []string{
		"maxHTTPMutationReplays = 128",
		"maxCapabilities",
		"= 512",
		"kindCount >= 128",
		"2 * time.Minute",
	} {
		if !strings.Contains(serverSource, required) {
			t.Errorf("server.go no longer proves bounded replay/capability lifetime %q", required)
		}
	}
	spineContent, err := os.ReadFile("../../internal/eebusadmin/spine.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(spineContent), "spineSnapshotMaxLife = 2 * time.Minute") {
		t.Error("SPINE snapshot lifetime is not bounded to two minutes")
	}
	for _, path := range []string{"../../internal/eebusadmin/server.go", "../../internal/eebusadmin/spine.go", "../../portal/handler.go"} {
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		for _, forbidden := range []string{"operator-mcp.sock", "StateRoot", "trust-store", "private_key", "private-key", "PEM"} {
			if strings.Contains(string(content), forbidden) {
				t.Errorf("%s reaches forbidden store/socket/secret token %q", path, forbidden)
			}
		}
	}
}

func issue743ProductionGoFiles(t *testing.T, root string) []string {
	t.Helper()
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".go" && !strings.HasSuffix(path, "_test.go") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

func issue743ExportedDeclaration(declaration ast.Decl) bool {
	switch typed := declaration.(type) {
	case *ast.FuncDecl:
		return typed.Name.IsExported()
	case *ast.GenDecl:
		for _, rawSpec := range typed.Specs {
			if spec, ok := rawSpec.(*ast.TypeSpec); ok && spec.Name.IsExported() {
				return true
			}
		}
	}
	return false
}

func issue743ForbiddenForkImport(importPath string) bool {
	return strings.HasPrefix(importPath, "github.com/enbility/") ||
		strings.Contains(importPath, "helianthus-eebus-go") ||
		strings.Contains(importPath, "helianthus-ship-go") ||
		strings.Contains(importPath, "helianthus-spine-go")
}
