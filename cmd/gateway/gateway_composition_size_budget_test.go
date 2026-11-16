package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"
)

// gatewayCompositionBudgetFiles lists the two composition entry files and
// every owner file that the decomposition of the lifecycle and of the HTTP
// server created. Each one stays within the production size budget.
var gatewayCompositionBudgetFiles = []string{
	"gateway_run_lifecycle.go",
	"gateway_http_server.go",
	"gateway_process_shell.go",
	"gateway_api_composition.go",
	"gateway_lifecycle_observability.go",
	"gateway_retained_consumers.go",
	"gateway_ebus_generation_owners.go",
	"gateway_http_mcp_surface.go",
	"gateway_http_portal_surface.go",
}

const (
	gatewayCompositionMaxFunctionLines = 150
	gatewayCompositionMaxFileLines     = 1000
	gatewayCompositionMaxFunctions     = 60
)

// gatewayCompositionSize is the measured size of one production file: its line
// count, its function and method declarations, and the line count of each one
// from the func keyword to the closing brace.
type gatewayCompositionSize struct {
	lines     int
	functions map[string]int
}

func measureGatewayCompositionSize(name string, source []byte) (gatewayCompositionSize, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, source, 0)
	if err != nil {
		return gatewayCompositionSize{}, err
	}
	lines := bytes.Count(source, []byte("\n"))
	if len(source) > 0 && source[len(source)-1] != '\n' {
		lines++
	}
	size := gatewayCompositionSize{lines: lines, functions: map[string]int{}}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		key := function.Name.Name
		if function.Recv != nil && len(function.Recv.List) == 1 {
			receiver := function.Recv.List[0].Type
			if star, ok := receiver.(*ast.StarExpr); ok {
				receiver = star.X
			}
			if ident, ok := receiver.(*ast.Ident); ok {
				key = ident.Name + "." + key
			}
		}
		if _, duplicate := size.functions[key]; duplicate {
			// Repeated init functions keep distinct keys.
			key = fmt.Sprintf("%s#%d", key, len(size.functions))
		}
		size.functions[key] = fset.Position(function.End()).Line - fset.Position(function.Pos()).Line + 1
	}
	return size, nil
}

// gatewayCompositionBudgetViolations returns one message per exceeded budget.
func gatewayCompositionBudgetViolations(name string, size gatewayCompositionSize) []string {
	var violations []string
	if size.lines > gatewayCompositionMaxFileLines {
		violations = append(violations, fmt.Sprintf("%s has %d lines; the budget is %d", name, size.lines, gatewayCompositionMaxFileLines))
	}
	if len(size.functions) > gatewayCompositionMaxFunctions {
		violations = append(violations, fmt.Sprintf("%s declares %d functions; the budget is %d", name, len(size.functions), gatewayCompositionMaxFunctions))
	}
	for function, lines := range size.functions {
		if lines > gatewayCompositionMaxFunctionLines {
			violations = append(violations, fmt.Sprintf("%s: %s has %d lines; the budget is %d", name, function, lines, gatewayCompositionMaxFunctionLines))
		}
	}
	return violations
}

// The composition entry files and their owner files stay within the hard
// production budget: no function over 150 lines, no file over 1,000 lines or
// 60 functions.
func TestGatewayCompositionSizeBudget(t *testing.T) {
	for _, name := range gatewayCompositionBudgetFiles {
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		size, err := measureGatewayCompositionSize(name, source)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		if len(size.functions) == 0 {
			t.Fatalf("%s declares no function; the budget list is stale", name)
		}
		for _, violation := range gatewayCompositionBudgetViolations(name, size) {
			t.Error(violation)
		}
	}
}

// The two entry functions are measured where they are declared.
func TestGatewayCompositionSizeBudgetMeasuresTheEntryFunctions(t *testing.T) {
	for _, entry := range []struct{ file, function string }{
		{"gateway_run_lifecycle.go", "runGatewayLifecycle"},
		{"gateway_http_server.go", "startHTTPServer"},
	} {
		source, err := os.ReadFile(entry.file)
		if err != nil {
			t.Fatal(err)
		}
		size, err := measureGatewayCompositionSize(entry.file, source)
		if err != nil {
			t.Fatal(err)
		}
		lines, ok := size.functions[entry.function]
		if !ok || lines < 10 {
			t.Fatalf("%s: %s measured %d lines (declared=%v)", entry.file, entry.function, lines, ok)
		}
		t.Logf("%s: %s has %d lines", entry.file, entry.function, lines)
	}
}

// Each budget fails on a synthetic file one unit over it and accepts the
// boundary value.
func TestGatewayCompositionSizeBudgetRejectsEachExcess(t *testing.T) {
	function := func(name string, bodyLines int) string {
		var body bytes.Buffer
		fmt.Fprintf(&body, "func %s() {\n", name)
		for line := 0; line < bodyLines; line++ {
			body.WriteString("\t_ = 0\n")
		}
		body.WriteString("}\n")
		return body.String()
	}
	padding := func(lines int) string {
		var text bytes.Buffer
		for line := 0; line < lines; line++ {
			text.WriteString("// padding\n")
		}
		return text.String()
	}
	manyFunctions := func(count int) string {
		var text bytes.Buffer
		for index := 0; index < count; index++ {
			text.WriteString(function(fmt.Sprintf("f%d", index), 0))
		}
		return text.String()
	}
	const header = "package main\n"
	for _, tc := range []struct {
		name       string
		source     string
		violations int
	}{
		// A function of n lines is the func line, n-2 body lines and the brace.
		{"function at the budget", header + function("f", gatewayCompositionMaxFunctionLines-2), 0},
		{"function over the budget", header + function("f", gatewayCompositionMaxFunctionLines-1), 1},
		{"file at the budget", header + function("f", 0) + padding(gatewayCompositionMaxFileLines-3), 0},
		{"file over the budget", header + function("f", 0) + padding(gatewayCompositionMaxFileLines-2), 1},
		{"functions at the budget", header + manyFunctions(gatewayCompositionMaxFunctions), 0},
		{"functions over the budget", header + manyFunctions(gatewayCompositionMaxFunctions+1), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			size, err := measureGatewayCompositionSize("synthetic.go", []byte(tc.source))
			if err != nil {
				t.Fatal(err)
			}
			if got := gatewayCompositionBudgetViolations("synthetic.go", size); len(got) != tc.violations {
				t.Fatalf("violations = %v; want %d", got, tc.violations)
			}
		})
	}
}
