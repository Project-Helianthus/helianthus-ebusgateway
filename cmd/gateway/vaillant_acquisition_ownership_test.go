package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The Vaillant acquisition owner (vaillantSemanticPoller) reads, refreshes,
// discovers and writes over the bus; the publication owner
// (vaillantSemanticPublication) maps native values to the GraphQL provider
// and hub and holds no bus access. The checks below pin that boundary on the
// package sources.

const (
	ownershipGatewayModule = "github.com/Project-Helianthus/helianthus-ebusgateway"
	ownershipEbusgoModule  = "github.com/Project-Helianthus/helianthus-ebusgo"
	ownershipPollerType    = "vaillantSemanticPoller"
	ownershipPublisherType = "vaillantSemanticPublisher"
)

// ownershipPublishedPackages are the packages the acquisition owner must not
// import.
var ownershipPublishedPackages = []string{
	ownershipGatewayModule + "/graphql",
	ownershipGatewayModule + "/mcp",
	ownershipGatewayModule + "/portal",
}

// ownershipBusPackages are the packages the publication files must not
// import.
var ownershipBusPackages = []string{
	ownershipEbusgoModule + "/protocol",
	ownershipEbusgoModule + "/transport",
}

type ownershipFile struct {
	name    string
	ast     *ast.File
	imports map[string]string // local name -> import path
}

type ownershipPackage struct {
	files []ownershipFile
}

func ownershipImportMatches(path string, packages []string) bool {
	for _, pkg := range packages {
		if path == pkg || strings.HasPrefix(path, pkg+"/") {
			return true
		}
	}
	return false
}

// parseOwnershipPackage parses the given non-test sources by file name.
func parseOwnershipPackage(t *testing.T, sources map[string]string) ownershipPackage {
	t.Helper()
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	fset := token.NewFileSet()
	var pkg ownershipPackage
	for _, name := range names {
		file, err := parser.ParseFile(fset, name, sources[name], parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		imports := make(map[string]string)
		for _, spec := range file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("%s: import %s: %v", name, spec.Path.Value, err)
			}
			local := path[strings.LastIndex(path, "/")+1:]
			if spec.Name != nil {
				local = spec.Name.Name
			}
			imports[local] = path
		}
		pkg.files = append(pkg.files, ownershipFile{name: name, ast: file, imports: imports})
	}
	return pkg
}

// loadOwnershipPackage reads every non-test Go file of the package
// directory.
func loadOwnershipPackage(t *testing.T) ownershipPackage {
	t.Helper()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	sources := make(map[string]string)
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = string(data)
	}
	return parseOwnershipPackage(t, sources)
}

func isOwnershipPublicationFile(name string) bool {
	return strings.HasPrefix(name, "vaillant_semantic_publication") && strings.HasSuffix(name, ".go")
}

func isOwnershipAcquisitionFileName(name string) bool {
	return strings.HasPrefix(name, "semantic_vaillant") && strings.HasSuffix(name, ".go")
}

func ownershipReceiverType(decl *ast.FuncDecl) string {
	if decl.Recv == nil || len(decl.Recv.List) == 0 {
		return ""
	}
	expr := decl.Recv.List[0].Type
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}

// pollerMethodFiles returns, per file, the poller methods it declares.
func (pkg ownershipPackage) pollerMethodFiles() map[string][]string {
	out := make(map[string][]string)
	for _, file := range pkg.files {
		for _, decl := range file.ast.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && ownershipReceiverType(fn) == ownershipPollerType {
				out[file.name] = append(out[file.name], fn.Name.Name)
			}
		}
	}
	return out
}

// publicationTypes returns the types declared in the publication files.
func (pkg ownershipPackage) publicationTypes() map[string]string {
	out := make(map[string]string)
	for _, file := range pkg.files {
		if !isOwnershipPublicationFile(file.name) {
			continue
		}
		for _, decl := range file.ast.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				out[spec.(*ast.TypeSpec).Name.Name] = file.name
			}
		}
	}
	return out
}

// findType returns the declaration of a named type and its file.
func (pkg ownershipPackage) findType(name string) (*ast.TypeSpec, ownershipFile, bool) {
	for _, file := range pkg.files {
		for _, decl := range file.ast.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				if typeSpec := spec.(*ast.TypeSpec); typeSpec.Name.Name == name {
					return typeSpec, file, true
				}
			}
		}
	}
	return nil, ownershipFile{}, false
}

// acquisitionImportViolations reports every acquisition file that imports a
// published package: a file that declares a poller method, and every
// semantic_vaillant*.go file whether or not it declares one yet.
func (pkg ownershipPackage) acquisitionImportViolations() []string {
	methods := pkg.pollerMethodFiles()
	var out []string
	for _, file := range pkg.files {
		declared, hasMethods := methods[file.name]
		if !hasMethods && !isOwnershipAcquisitionFileName(file.name) {
			continue
		}
		for _, path := range file.imports {
			if !ownershipImportMatches(path, ownershipPublishedPackages) {
				continue
			}
			if hasMethods {
				out = append(out, file.name+" imports "+path+" and declares poller methods "+strings.Join(declared, ", "))
			} else {
				out = append(out, file.name+" imports "+path)
			}
		}
	}
	sort.Strings(out)
	return out
}

// typeReferenceViolations reports every reference of expr to a published
// package, a bus package when bus is set, or a publication type.
func typeReferenceViolations(file ownershipFile, expr ast.Node, publication map[string]string, what string) []string {
	var out []string
	ast.Inspect(expr, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.SelectorExpr:
			if pkgIdent, ok := n.X.(*ast.Ident); ok {
				if path, imported := file.imports[pkgIdent.Name]; imported && ownershipImportMatches(path, ownershipPublishedPackages) {
					out = append(out, what+" uses "+pkgIdent.Name+"."+n.Sel.Name)
				}
			}
			return false
		case *ast.Ident:
			if owner, ok := publication[n.Name]; ok {
				out = append(out, what+" uses publication type "+n.Name+" of "+owner)
			}
		}
		return true
	})
	return out
}

// pollerFieldViolations reports every poller field whose type refers to a
// published package or to a type of the publication files.
func (pkg ownershipPackage) pollerFieldViolations() ([]string, bool) {
	spec, file, ok := pkg.findType(ownershipPollerType)
	if !ok {
		return nil, false
	}
	structType, ok := spec.Type.(*ast.StructType)
	if !ok {
		return []string{ownershipPollerType + " is not a struct"}, true
	}
	publication := pkg.publicationTypes()
	var out []string
	for _, field := range structType.Fields.List {
		name := "embedded field"
		if len(field.Names) > 0 {
			name = "field " + field.Names[0].Name
		}
		out = append(out, typeReferenceViolations(file, field.Type, publication, ownershipPollerType+" "+name)...)
	}
	return out, true
}

// publisherInterfaceViolations reports every reference of the publisher
// interface to a published package or to a type of the publication files.
func (pkg ownershipPackage) publisherInterfaceViolations() ([]string, bool) {
	spec, file, ok := pkg.findType(ownershipPublisherType)
	if !ok {
		return nil, false
	}
	var out []string
	if isOwnershipPublicationFile(file.name) {
		out = append(out, ownershipPublisherType+" is declared in publication file "+file.name)
	}
	return append(out, typeReferenceViolations(file, spec.Type, pkg.publicationTypes(), ownershipPublisherType)...), true
}

// publicationBusViolations reports every publication file that imports a
// bus package.
func (pkg ownershipPackage) publicationBusViolations() ([]string, int) {
	var out []string
	count := 0
	for _, file := range pkg.files {
		if !isOwnershipPublicationFile(file.name) {
			continue
		}
		count++
		for _, path := range file.imports {
			if ownershipImportMatches(path, ownershipBusPackages) {
				out = append(out, file.name+" imports "+path)
			}
		}
	}
	sort.Strings(out)
	return out, count
}

func reportOwnershipViolations(t *testing.T, rule string, violations []string) {
	t.Helper()
	for _, violation := range violations {
		t.Errorf("%s: %s", rule, violation)
	}
}

// TestVaillantAcquisitionOwnershipImportsNoPublishedPackage pins that every
// file declaring a poller method, and every semantic_vaillant*.go file,
// imports neither the GraphQL, the MCP nor the Portal package.
func TestVaillantAcquisitionOwnershipImportsNoPublishedPackage(t *testing.T) {
	pkg := loadOwnershipPackage(t)
	if len(pkg.pollerMethodFiles()) == 0 {
		t.Fatal("no poller method found; the guard no longer sees the acquisition owner")
	}
	reportOwnershipViolations(t, "acquisition import", pkg.acquisitionImportViolations())
}

// TestVaillantAcquisitionOwnershipPollerFieldsAreNative pins that no poller
// field has a type of the GraphQL, MCP or Portal package or of the
// publication files.
func TestVaillantAcquisitionOwnershipPollerFieldsAreNative(t *testing.T) {
	violations, found := loadOwnershipPackage(t).pollerFieldViolations()
	if !found {
		t.Fatalf("type %s not found", ownershipPollerType)
	}
	reportOwnershipViolations(t, "poller field", violations)
}

// TestVaillantAcquisitionOwnershipPublisherIsNative pins that the interface
// through which the poller reaches its publication owner is declared on the
// acquisition side and speaks native types only.
func TestVaillantAcquisitionOwnershipPublisherIsNative(t *testing.T) {
	violations, found := loadOwnershipPackage(t).publisherInterfaceViolations()
	if !found {
		t.Fatalf("type %s not found", ownershipPublisherType)
	}
	reportOwnershipViolations(t, "publisher interface", violations)
}

// TestVaillantAcquisitionOwnershipPublicationHoldsNoBusAccess pins that the
// publication files import no ebusgo protocol or transport package.
func TestVaillantAcquisitionOwnershipPublicationHoldsNoBusAccess(t *testing.T) {
	violations, count := loadOwnershipPackage(t).publicationBusViolations()
	if count == 0 {
		t.Fatal("no publication file found; the guard no longer sees the publication owner")
	}
	reportOwnershipViolations(t, "publication bus access", violations)
}

// TestVaillantAcquisitionOwnershipGuardDetectsEachViolation runs the checks
// on synthetic sources that break one rule each, so a check that stops
// looking at its input fails here:
//   - a poller method in a file importing graphql, mcp or portal (one case
//     per package);
//   - a new semantic_vaillant*.go file that imports graphql without
//     declaring a poller method;
//   - a poller field typed with a GraphQL type, and one typed with a
//     publication type;
//   - a publisher interface method using a GraphQL type, and one using a
//     publication type;
//   - a publication file importing the ebusgo protocol, and one importing
//     the ebusgo transport package.
func TestVaillantAcquisitionOwnershipGuardDetectsEachViolation(t *testing.T) {
	const pollerFile = `package main
type vaillantSemanticPoller struct{ mu int }
type vaillantSemanticPublisher interface{ live() bool }
`
	const publicationFile = `package main
type vaillantSemanticPublication struct{}
type vaillantFM5View struct{}
`
	clean := map[string]string{
		"semantic_vaillant.go":             pollerFile,
		"vaillant_semantic_publication.go": publicationFile,
	}
	with := func(name, source string) map[string]string {
		out := map[string]string{name: source}
		for key, value := range clean {
			if _, replaced := out[key]; !replaced {
				out[key] = value
			}
		}
		return out
	}

	cleanPkg := parseOwnershipPackage(t, clean)
	if got := cleanPkg.acquisitionImportViolations(); len(got) != 0 {
		t.Fatalf("clean sources report import violations %v", got)
	}
	if got, _ := cleanPkg.pollerFieldViolations(); len(got) != 0 {
		t.Fatalf("clean sources report field violations %v", got)
	}
	if got, _ := cleanPkg.publisherInterfaceViolations(); len(got) != 0 {
		t.Fatalf("clean sources report publisher violations %v", got)
	}
	if got, _ := cleanPkg.publicationBusViolations(); len(got) != 0 {
		t.Fatalf("clean sources report bus violations %v", got)
	}

	for _, pkgName := range []string{"graphql", "mcp", "portal"} {
		source := "package main\nimport \"" + ownershipGatewayModule + "/" + pkgName + "\"\nvar _ " + pkgName + ".X\nfunc (p *vaillantSemanticPoller) publishSomething() {}\n"
		if got := parseOwnershipPackage(t, with("other_owner.go", source)).acquisitionImportViolations(); len(got) != 1 {
			t.Errorf("poller method in a file importing %s: violations %v; want one", pkgName, got)
		}
	}
	newFile := "package main\nimport \"" + ownershipGatewayModule + "/graphql\"\nvar _ graphql.Zone\n"
	if got := parseOwnershipPackage(t, with("semantic_vaillant_new_plane.go", newFile)).acquisitionImportViolations(); len(got) != 1 {
		t.Errorf("new semantic_vaillant file importing graphql: violations %v; want one", got)
	}

	graphqlField := "package main\nimport \"" + ownershipGatewayModule + "/graphql\"\ntype vaillantSemanticPoller struct{ provider *graphql.LiveSemanticProvider }\ntype vaillantSemanticPublisher interface{ live() bool }\n"
	if got, _ := parseOwnershipPackage(t, with("semantic_vaillant.go", graphqlField)).pollerFieldViolations(); len(got) != 1 {
		t.Errorf("poller field of a GraphQL type: violations %v; want one", got)
	}
	ownerField := "package main\ntype vaillantSemanticPoller struct{ publication *vaillantSemanticPublication }\ntype vaillantSemanticPublisher interface{ live() bool }\n"
	if got, _ := parseOwnershipPackage(t, with("semantic_vaillant.go", ownerField)).pollerFieldViolations(); len(got) != 1 {
		t.Errorf("poller field of a publication type: violations %v; want one", got)
	}

	graphqlMethod := "package main\nimport \"" + ownershipGatewayModule + "/graphql\"\ntype vaillantSemanticPoller struct{}\ntype vaillantSemanticPublisher interface{ publishZones([]graphql.Zone) }\n"
	if got, _ := parseOwnershipPackage(t, with("semantic_vaillant.go", graphqlMethod)).publisherInterfaceViolations(); len(got) != 1 {
		t.Errorf("publisher method with a GraphQL type: violations %v; want one", got)
	}
	viewMethod := "package main\ntype vaillantSemanticPoller struct{}\ntype vaillantSemanticPublisher interface{ publishFM5View(vaillantFM5View) }\n"
	if got, _ := parseOwnershipPackage(t, with("semantic_vaillant.go", viewMethod)).publisherInterfaceViolations(); len(got) != 1 {
		t.Errorf("publisher method with a publication type: violations %v; want one", got)
	}

	for _, busPkg := range []string{"protocol", "transport"} {
		source := "package main\nimport \"" + ownershipEbusgoModule + "/" + busPkg + "\"\nvar _ " + busPkg + ".X\n"
		if got, _ := parseOwnershipPackage(t, with("vaillant_semantic_publication_bus.go", source)).publicationBusViolations(); len(got) != 1 {
			t.Errorf("publication file importing %s: violations %v; want one", busPkg, got)
		}
	}
}
