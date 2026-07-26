package main

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestIssue743DependencyClosurePinsEEBusregV0115(t *testing.T) {
	goMod, err := os.Open("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = goMod.Close() }()

	const module = "github.com/Project-Helianthus/helianthus-ebusreg"
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
	if len(versions) != 1 || versions[0] != "v0.1.15" {
		t.Fatalf("%s versions = %v, want exactly [v0.1.15]", module, versions)
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
