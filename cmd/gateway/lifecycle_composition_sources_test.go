package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

// lifecycleCompositionFileNames lists the production files that hold the
// runGatewayLifecycle composition. Source pins on the lifecycle read every
// file in this list.
var lifecycleCompositionFileNames = []string{
	"gateway_run_lifecycle.go",
	"gateway_lifecycle_observability.go",
}

type lifecycleCompositionFile struct {
	name   string
	syntax *ast.File
}

// lifecycleCompositionSources parses every lifecycle composition file with
// fset and returns the parsed files in list order and their concatenated text.
func lifecycleCompositionSources(t testing.TB, fset *token.FileSet) ([]lifecycleCompositionFile, string) {
	t.Helper()
	files := make([]lifecycleCompositionFile, 0, len(lifecycleCompositionFileNames))
	var text strings.Builder
	for _, name := range lifecycleCompositionFileNames {
		content, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		syntax, err := parser.ParseFile(fset, name, content, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, lifecycleCompositionFile{name: name, syntax: syntax})
		text.Write(content)
		text.WriteString("\n")
	}
	return files, text.String()
}

func lifecycleFunctionDecl(t *testing.T, files []lifecycleCompositionFile, name string) *ast.FuncDecl {
	t.Helper()
	var found *ast.FuncDecl
	for _, file := range files {
		for _, declaration := range file.syntax.Decls {
			if function, ok := declaration.(*ast.FuncDecl); ok && function.Recv == nil && function.Name.Name == name {
				if found != nil {
					t.Fatalf("%s is declared more than once in the lifecycle composition", name)
				}
				found = function
			}
		}
	}
	if found == nil {
		t.Fatalf("%s is not declared in the lifecycle composition files", name)
	}
	return found
}

// The lifecycle calls each extracted observability step exactly once, in the
// order in which the statements ran before they were extracted, and registers
// the final artifact emission as a deferred call.
func TestRunLifecycleCallsObservabilityStepsOnceInOrder(t *testing.T) {
	fset := token.NewFileSet()
	files, _ := lifecycleCompositionSources(t, fset)
	lifecycle := lifecycleFunctionDecl(t, files, "runGatewayLifecycle")

	steps := []string{
		"newStartupSourceSelection",
		"startArtifactEmission",
		"emitFinalArtifact",
		"wireObserveFirstObserversFn",
		"wireSemanticMetricsProvider",
		"wireAdaptermuxDiagnosticProviders",
		"wireV8RolloutObservability",
		"wireSourceSelectionBaselineEvidence",
		"wireSemanticStartupObservability",
	}
	positions := make(map[string][]token.Pos, len(steps))
	deferred := map[string]bool{}
	ast.Inspect(lifecycle.Body, func(node ast.Node) bool {
		if statement, ok := node.(*ast.DeferStmt); ok {
			if selector, ok := statement.Call.Fun.(*ast.SelectorExpr); ok {
				deferred[selector.Sel.Name] = true
			}
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch function := call.Fun.(type) {
		case *ast.Ident:
			positions[function.Name] = append(positions[function.Name], call.Pos())
		case *ast.SelectorExpr:
			positions[function.Sel.Name] = append(positions[function.Sel.Name], call.Pos())
		}
		return true
	})
	last := token.NoPos
	for _, step := range steps {
		if len(positions[step]) != 1 {
			t.Fatalf("runGatewayLifecycle calls %s %d times; want 1", step, len(positions[step]))
		}
		if positions[step][0] <= last {
			t.Fatalf("runGatewayLifecycle calls %s out of order", step)
		}
		last = positions[step][0]
	}
	if !deferred["emitFinalArtifact"] {
		t.Fatal("the final source-selection artifact emission is not deferred")
	}
	if deferred["startArtifactEmission"] {
		t.Fatal("the delayed source-selection artifact emission must start during setup")
	}
}

// The two composition statements that a transport-gate fixture reverse-applies
// a patch to stay at the top level of runGatewayLifecycle.
func TestRunLifecycleKeepsPortalCatalogAndM2MStatementsAtTopLevel(t *testing.T) {
	fset := token.NewFileSet()
	files, _ := lifecycleCompositionSources(t, fset)
	lifecycle := lifecycleFunctionDecl(t, files, "runGatewayLifecycle")
	topLevel := map[string]int{}
	for _, statement := range lifecycle.Body.List {
		assign, ok := statement.(*ast.AssignStmt)
		if !ok || len(assign.Rhs) != 1 {
			continue
		}
		if call, ok := assign.Rhs[0].(*ast.CallExpr); ok {
			if function, ok := call.Fun.(*ast.Ident); ok {
				topLevel[function.Name]++
			}
		}
	}
	for _, name := range []string{"newGatewayPortalCatalogSource", "newProfileM2MRuntime"} {
		if topLevel[name] != 1 {
			t.Fatalf("%s is assigned %d times at the top level of runGatewayLifecycle; want 1", name, topLevel[name])
		}
	}
}

func TestNewStartupSourceSelectionResolvesExplicitSourceOverride(t *testing.T) {
	source := byte(0x31)
	cases := []struct {
		name           string
		protocol       ebusgateway.TransportProtocol
		source         *byte
		admissionPath  ebusgateway.TransportAdmissionPath
		overrideSet    bool
		overrideSource byte
		mode           string
		state          string
		artifactSource uint8
	}{
		{
			name:           "explicit source on a source-selection-capable transport",
			protocol:       ebusgateway.TransportENH,
			source:         &source,
			admissionPath:  ebusgateway.TransportAdmissionSourceSelectionCapable,
			overrideSet:    true,
			overrideSource: source,
			mode:           "explicit_validate_only",
			state:          "active",
			artifactSource: source,
		},
		{
			name:          "no explicit source",
			protocol:      ebusgateway.TransportENH,
			admissionPath: ebusgateway.TransportAdmissionSourceSelectionCapable,
			mode:          "degraded_no_events",
			state:         "pending",
		},
		{
			name:          "explicit source on a static-fallback transport",
			protocol:      ebusgateway.TransportProtocol("unrecognised"),
			source:        &source,
			admissionPath: ebusgateway.TransportAdmissionStaticFallback,
			mode:          "degraded_no_events",
			state:         "pending",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ebusgateway.DefaultConfig()
			cfg.TransportConfig.Protocol = tc.protocol
			cfg.StartupSource.Source = tc.source
			selection, err := newStartupSourceSelection(cfg)
			if err != nil {
				t.Fatalf("newStartupSourceSelection() error = %v", err)
			}
			if selection.admissionPath != tc.admissionPath {
				t.Fatalf("admission path = %v; want %v", selection.admissionPath, tc.admissionPath)
			}
			if selection.overrideSet != tc.overrideSet || selection.overrideSource != tc.overrideSource {
				t.Fatalf("override = %v/%#x; want %v/%#x", selection.overrideSet, selection.overrideSource, tc.overrideSet, tc.overrideSource)
			}
			if selection.metrics != ebusgateway.GetOrInitStartupSourceSelectionMetrics() {
				t.Fatal("startup source selection does not use the process metrics")
			}
			artifact, _, err := selection.artifactBuilder.Emit()
			if err != nil {
				t.Fatalf("Emit() error = %v", err)
			}
			admission := artifact.Admission
			if admission.SourceSelection.Mode != tc.mode || admission.TransportKind != string(tc.protocol) {
				t.Fatalf("artifact mode/transport = %q/%q; want %q/%q", admission.SourceSelection.Mode, admission.TransportKind, tc.mode, tc.protocol)
			}
			if admission.State != tc.state {
				t.Fatalf("artifact state = %q; want %q", admission.State, tc.state)
			}
			if admission.Source != tc.artifactSource {
				t.Fatalf("artifact source = %#x; want %#x", admission.Source, tc.artifactSource)
			}
		})
	}
}

func TestWireSourceSelectionBaselineEvidenceWithoutStoreLeavesProviderUnset(t *testing.T) {
	builder := ebusgateway.NewSourceSelectionArtifactBuilder(string(ebusgateway.TransportENH))
	builder.RecordBaselineEvidence(0x08, 3)
	wireSourceSelectionBaselineEvidence(builder, nil)
	artifact, _, err := builder.Emit()
	if err != nil {
		t.Fatal(err)
	}
	if got := artifact.Discovery.PerBaselineAddressEvidenceCounts["08"]; got != 3 {
		t.Fatalf("recorded baseline evidence = %d; want 3 without an observability store", got)
	}
}
