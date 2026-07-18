package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type processExitViolation struct {
	position token.Position
	call     string
}

func processExitViolations(filename string, source any) ([]processExitViolation, error) {
	files := token.NewFileSet()
	parsed, err := parser.ParseFile(files, filename, source, 0)
	if err != nil {
		return nil, err
	}

	aliases := make(map[string]string)
	for _, imported := range parsed.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			return nil, fmt.Errorf("unquote import %s: %w", imported.Path.Value, err)
		}
		if path != "log" && path != "os" {
			continue
		}
		alias := filepath.Base(path)
		if imported.Name != nil {
			alias = imported.Name.Name
		}
		if alias == "_" {
			continue
		}
		if alias == "." {
			return nil, fmt.Errorf("dot import of process-termination package %q is forbidden", path)
		}
		aliases[alias] = path
	}

	var mainBody *ast.BlockStmt
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Recv == nil && function.Name.Name == "main" {
			mainBody = function.Body
			break
		}
	}

	var violations []processExitViolation
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		qualifier, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}
		path := aliases[qualifier.Name]
		terminates := path == "os" && selector.Sel.Name == "Exit"
		terminates = terminates || path == "log" && (selector.Sel.Name == "Fatal" || selector.Sel.Name == "Fatalf" || selector.Sel.Name == "Fatalln")
		if !terminates {
			return true
		}
		if mainBody != nil && call.Pos() >= mainBody.Pos() && call.End() <= mainBody.End() {
			return true
		}
		violations = append(violations, processExitViolation{
			position: files.Position(call.Pos()),
			call:     qualifier.Name + "." + selector.Sel.Name,
		})
		return true
	})
	return violations, nil
}

func TestGatewayWorkerCodeCannotTerminateProcess(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read gateway package: %v", err)
	}

	var findings []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		violations, err := processExitViolations(name, nil)
		if err != nil {
			t.Fatalf("inspect %s: %v", name, err)
		}
		for _, violation := range violations {
			findings = append(findings, fmt.Sprintf("%s: %s", violation.position, violation.call))
		}
	}
	if len(findings) != 0 {
		t.Fatalf("gateway process termination is allowed only inside main():\n%s", strings.Join(findings, "\n"))
	}
}

func TestProcessExitGuardResolvesStdlibAliases(t *testing.T) {
	const source = `package fixture
import (
  logging "log"
  system "os"
)
func worker() {
  logging.Fatalf("stop")
  system.Exit(1)
}
`
	violations, err := processExitViolations("fixture.go", source)
	if err != nil {
		t.Fatalf("inspect aliased fixture: %v", err)
	}
	if len(violations) != 2 {
		t.Fatalf("aliased process-exit violations = %v; want 2", violations)
	}
}
