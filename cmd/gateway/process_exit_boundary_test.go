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
	var nestedMainFunctions []*ast.FuncLit
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Recv == nil && function.Name.Name == "main" {
			mainBody = function.Body
			ast.Inspect(mainBody, func(node ast.Node) bool {
				literal, ok := node.(*ast.FuncLit)
				if !ok {
					return true
				}
				nestedMainFunctions = append(nestedMainFunctions, literal)
				return false
			})
			break
		}
	}

	var violations []processExitViolation
	ast.Inspect(parsed, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		qualifier, qualified := selector.X.(*ast.Ident)
		path := ""
		call := selector.Sel.Name
		if qualified {
			path = aliases[qualifier.Name]
			call = qualifier.Name + "." + selector.Sel.Name
		}
		terminates := path == "os" && selector.Sel.Name == "Exit"
		terminates = terminates || selector.Sel.Name == "Fatal" || selector.Sel.Name == "Fatalf" || selector.Sel.Name == "Fatalln"
		if !terminates {
			return true
		}
		insideMain := mainBody != nil && selector.Pos() >= mainBody.Pos() && selector.End() <= mainBody.End()
		insideNestedFunction := false
		for _, literal := range nestedMainFunctions {
			if selector.Pos() >= literal.Pos() && selector.End() <= literal.End() {
				insideNestedFunction = true
				break
			}
		}
		if insideMain && !insideNestedFunction {
			return true
		}
		violations = append(violations, processExitViolation{
			position: files.Position(selector.Pos()),
			call:     call,
		})
		return true
	})
	return violations, nil
}

func TestGatewayWorkerCodeCannotTerminateProcess(t *testing.T) {
	var findings []string
	for _, directory := range []string{".", "../.."} {
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatalf("read gateway package %s: %v", directory, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			filename := filepath.Join(directory, name)
			violations, err := processExitViolations(filename, nil)
			if err != nil {
				t.Fatalf("inspect %s: %v", filename, err)
			}
			for _, violation := range violations {
				findings = append(findings, fmt.Sprintf("%s: %s", violation.position, violation.call))
			}
		}
	}
	if len(findings) != 0 {
		t.Fatalf("gateway process termination is allowed only inside main():\n%s", strings.Join(findings, "\n"))
	}
}

func TestProcessExitGuardRejectsLoggerReceiversAndNestedMainWorkers(t *testing.T) {
	const source = `package fixture
import "log"
func main() {
  log.Fatalf("top-level termination")
  worker := func() {
    logger := log.Default()
    logger.Fatal("nested termination")
  }
  _ = worker
}
`
	violations, err := processExitViolations("fixture.go", source)
	if err != nil {
		t.Fatalf("inspect receiver fixture: %v", err)
	}
	if len(violations) != 1 {
		t.Fatalf("receiver/nested process-exit violations = %v; want 1", violations)
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
  terminate := system.Exit
  terminate(1)
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
