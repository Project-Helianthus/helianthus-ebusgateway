package graphql

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	graphqlgo "github.com/graphql-go/graphql"
)

// registerNamesFixturePath is the record of the B524 register-derived fields
// the system plane publishes, with the docs-ebus name and previous_name of
// each register. The MCP and Portal contract tests read the same file.
const registerNamesFixturePath = "testdata/register_derived_names.json"

// registerNamesExemptionIssue is the only docs-ebus issue that may hold a
// published name back from its docs-ebus name.
const registerNamesExemptionIssue = "helianthus-docs-ebus#1145"

type registerNamesDoc struct {
	Format      string             `json:"format"`
	Description string             `json:"description"`
	Source      string             `json:"source"`
	Rows        []registerNamesRow `json:"rows"`
}

type registerNamesRow struct {
	PublishedName string            `json:"published_name"`
	Opcode        string            `json:"opcode"`
	Group         string            `json:"group"`
	Register      string            `json:"register"`
	DocsEbusName  string            `json:"docs_ebus_name"`
	PreviousName  string            `json:"previous_name"`
	Surfaces      map[string]string `json:"surfaces"`
	Exemption     string            `json:"exemption,omitempty"`
}

func loadRegisterNamesDoc(t *testing.T) registerNamesDoc {
	t.Helper()
	raw, err := os.ReadFile(registerNamesFixturePath)
	if err != nil {
		t.Fatalf("read %s: %v", registerNamesFixturePath, err)
	}
	var doc registerNamesDoc
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil {
		t.Fatalf("decode %s: %v", registerNamesFixturePath, err)
	}
	return doc
}

var (
	registerNamesSnake    = regexp.MustCompile(`^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$`)
	registerNamesHexByte  = regexp.MustCompile(`^0x[0-9A-F]{2}$`)
	registerNamesHexWord  = regexp.MustCompile(`^0x[0-9A-F]{4}$`)
	registerNamesSurfaces = []string{"graphql", "mcp", "portal"}
)

// registerNamesFixtureErrors checks the record itself: complete native
// identity, one row per register, docs-ebus names as published names, and
// at most the single listed exemption.
func registerNamesFixtureErrors(doc registerNamesDoc) []string {
	var errs []string
	if doc.Format != "helianthus-register-derived-names/v1" {
		errs = append(errs, "format "+doc.Format)
	}
	if !strings.Contains(doc.Source, "b524-op02-register-names.csv") || !strings.Contains(doc.Description, "b524-op02-register-names.csv") {
		errs = append(errs, "source page not stated")
	}
	if len(doc.Rows) == 0 {
		errs = append(errs, "no rows")
	}
	identities := map[string]bool{}
	published := map[string]bool{}
	exemptions := 0
	for _, row := range doc.Rows {
		id := row.Opcode + "/" + row.Group + "/" + row.Register
		if row.Opcode != "0x02" || !registerNamesHexByte.MatchString(row.Group) || !registerNamesHexWord.MatchString(row.Register) {
			errs = append(errs, "identity "+id)
		}
		if identities[id] {
			errs = append(errs, "duplicate register "+id)
		}
		identities[id] = true
		for _, name := range []string{row.PublishedName, row.DocsEbusName, row.PreviousName} {
			if !registerNamesSnake.MatchString(name) {
				errs = append(errs, fmt.Sprintf("%s: name %q", id, name))
			}
		}
		if len(row.Surfaces) != len(registerNamesSurfaces) {
			errs = append(errs, id+": surfaces")
		}
		for _, surface := range registerNamesSurfaces {
			object, ok := row.Surfaces[surface]
			if !ok {
				errs = append(errs, id+": surface "+surface+" missing")
				continue
			}
			if object == "" {
				continue
			}
			key := surface + "/" + object + "/" + row.PublishedName
			if published[key] {
				errs = append(errs, "duplicate published name "+key)
			}
			published[key] = true
		}
		if row.Exemption == "" {
			if row.PublishedName != row.DocsEbusName {
				errs = append(errs, fmt.Sprintf("%s: published %q, docs-ebus name %q", id, row.PublishedName, row.DocsEbusName))
			}
			continue
		}
		exemptions++
		if !strings.Contains(row.Exemption, registerNamesExemptionIssue) || row.PublishedName != row.PreviousName || row.Register != "0x0007" || row.Group != "0x00" {
			errs = append(errs, id+": exemption is limited to GG00 RR0007 under "+registerNamesExemptionIssue)
		}
	}
	if exemptions > 1 {
		errs = append(errs, "more than one exemption")
	}
	sort.Strings(errs)
	return errs
}

// registerNamesSurfaceErrors checks one surface: every recorded row is
// published under its recorded name in its recorded object, and no recorded
// previous_name is published in any recorded object of that surface.
// published maps object name to the field names that object publishes.
func registerNamesSurfaceErrors(doc registerNamesDoc, surface string, published map[string]map[string]bool) []string {
	var errs []string
	objects := map[string]bool{}
	for _, row := range doc.Rows {
		if object := row.Surfaces[surface]; object != "" {
			objects[object] = true
		}
	}
	for object := range objects {
		if published[object] == nil {
			errs = append(errs, surface+": object "+object+" not found")
		}
	}
	for _, row := range doc.Rows {
		object := row.Surfaces[surface]
		if object != "" && !published[object][row.PublishedName] {
			errs = append(errs, fmt.Sprintf("%s: %s does not publish %s (register %s/%s)", surface, object, row.PublishedName, row.Group, row.Register))
		}
		if row.Exemption != "" || row.PreviousName == row.DocsEbusName {
			continue
		}
		for candidate := range objects {
			if published[candidate][row.PreviousName] {
				errs = append(errs, fmt.Sprintf("%s: %s publishes previous name %s (register %s/%s)", surface, candidate, row.PreviousName, row.Group, row.Register))
			}
		}
	}
	sort.Strings(errs)
	return errs
}

func graphqlPublishedFields(t *testing.T, schema graphqlgo.Schema, doc registerNamesDoc) map[string]map[string]bool {
	t.Helper()
	published := map[string]map[string]bool{}
	for _, row := range doc.Rows {
		name := row.Surfaces["graphql"]
		if name == "" || published[name] != nil {
			continue
		}
		object, ok := schema.Type(name).(*graphqlgo.Object)
		if !ok {
			continue
		}
		fields := map[string]bool{}
		for field := range object.Fields() {
			fields[field] = true
		}
		published[name] = fields
	}
	return published
}

func TestRegisterNamesFixtureIsConsistent(t *testing.T) {
	t.Parallel()
	doc := loadRegisterNamesDoc(t)
	if errs := registerNamesFixtureErrors(doc); len(errs) > 0 {
		t.Fatalf("fixture errors:\n%s", strings.Join(errs, "\n"))
	}
}

func TestGraphQLPublishesDocsEbusRegisterNames(t *testing.T) {
	t.Parallel()
	doc := loadRegisterNamesDoc(t)
	for _, build := range []struct {
		name   string
		schema func() (graphqlgo.Schema, error)
	}{
		{"query", func() (graphqlgo.Schema, error) { return NewQuerySchema(NewBuilder(nil, nil)) }},
		{"full", func() (graphqlgo.Schema, error) {
			return NewSchema(NewBuilder(nil, nil), mutationTestRegistry{}, &mutationTestInvoker{}, nil)
		}},
	} {
		schema, err := build.schema()
		if err != nil {
			t.Fatalf("%s schema: %v", build.name, err)
		}
		if errs := registerNamesSurfaceErrors(doc, "graphql", graphqlPublishedFields(t, schema, doc)); len(errs) > 0 {
			t.Fatalf("%s schema:\n%s", build.name, strings.Join(errs, "\n"))
		}
		for _, row := range doc.Rows {
			if row.Exemption != "" || row.PreviousName == row.DocsEbusName {
				continue
			}
			query := "{ system { " + map[string]string{
				"system_state":      "state",
				"system_config":     "config",
				"system_properties": "properties",
			}[row.Surfaces["graphql"]] + " { " + row.PreviousName + " } } }"
			if result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: query}); len(result.Errors) == 0 {
				t.Errorf("%s schema accepts previous name: %s", build.name, query)
			}
		}
	}
}

// TestRegisterNamesCheckersCatchEachMutation varies every field of the
// recorded identity and of the published name set and requires the checkers
// to report each change.
func TestRegisterNamesCheckersCatchEachMutation(t *testing.T) {
	t.Parallel()
	base := loadRegisterNamesDoc(t)
	if len(registerNamesFixtureErrors(base)) != 0 {
		t.Fatal("base fixture must be clean")
	}
	clone := func() registerNamesDoc {
		doc := base
		doc.Rows = make([]registerNamesRow, len(base.Rows))
		for i, row := range base.Rows {
			surfaces := map[string]string{}
			for k, v := range row.Surfaces {
				surfaces[k] = v
			}
			row.Surfaces = surfaces
			doc.Rows[i] = row
		}
		return doc
	}
	renamed, exempt := -1, -1
	for i, row := range base.Rows {
		if row.Exemption != "" {
			exempt = i
		} else if renamed < 0 && row.PreviousName != row.DocsEbusName {
			renamed = i
		}
	}
	if renamed < 0 || exempt < 0 {
		t.Fatal("fixture needs a renamed row and the exempt row")
	}

	fixtureMutations := map[string]func(*registerNamesDoc){
		"format":                    func(d *registerNamesDoc) { d.Format = "other" },
		"source":                    func(d *registerNamesDoc) { d.Source = "elsewhere" },
		"opcode":                    func(d *registerNamesDoc) { d.Rows[renamed].Opcode = "0x01" },
		"group":                     func(d *registerNamesDoc) { d.Rows[renamed].Group = "0x0" },
		"register":                  func(d *registerNamesDoc) { d.Rows[renamed].Register = "0x39" },
		"duplicate register":        func(d *registerNamesDoc) { d.Rows[renamed].Register = d.Rows[exempt].Register },
		"published differs":         func(d *registerNamesDoc) { d.Rows[renamed].PublishedName = d.Rows[renamed].PreviousName },
		"docs-ebus name differs":    func(d *registerNamesDoc) { d.Rows[renamed].DocsEbusName = "other_name" },
		"previous name not snake":   func(d *registerNamesDoc) { d.Rows[renamed].PreviousName = "previousName" },
		"surface missing":           func(d *registerNamesDoc) { delete(d.Rows[renamed].Surfaces, "mcp") },
		"second exemption":          func(d *registerNamesDoc) { d.Rows[renamed].Exemption = d.Rows[exempt].Exemption },
		"exemption without issue":   func(d *registerNamesDoc) { d.Rows[exempt].Exemption = "open" },
		"exemption on new register": func(d *registerNamesDoc) { d.Rows[exempt].Register = "0x00FF" },
	}
	for name, mutate := range fixtureMutations {
		doc := clone()
		mutate(&doc)
		if len(registerNamesFixtureErrors(doc)) == 0 {
			t.Errorf("fixture mutation %q not caught", name)
		}
	}

	published := func() map[string]map[string]bool {
		out := map[string]map[string]bool{}
		for _, row := range base.Rows {
			object := row.Surfaces["graphql"]
			if out[object] == nil {
				out[object] = map[string]bool{}
			}
			out[object][row.PublishedName] = true
		}
		return out
	}
	if errs := registerNamesSurfaceErrors(base, "graphql", published()); len(errs) != 0 {
		t.Fatalf("clean published set reported: %v", errs)
	}
	row := base.Rows[renamed]
	var sibling string
	for _, other := range base.Rows {
		if other.Surfaces["graphql"] != row.Surfaces["graphql"] {
			sibling = other.Surfaces["graphql"]
			break
		}
	}
	surfaceMutations := map[string]func(map[string]map[string]bool){
		"field back to previous name": func(p map[string]map[string]bool) {
			delete(p[row.Surfaces["graphql"]], row.PublishedName)
			p[row.Surfaces["graphql"]][row.PreviousName] = true
		},
		"previous name as alias":   func(p map[string]map[string]bool) { p[row.Surfaces["graphql"]][row.PreviousName] = true },
		"previous name in sibling": func(p map[string]map[string]bool) { p[sibling][row.PreviousName] = true },
		"field moved to sibling": func(p map[string]map[string]bool) {
			delete(p[row.Surfaces["graphql"]], row.PublishedName)
			p[sibling][row.PublishedName] = true
		},
		"field dropped": func(p map[string]map[string]bool) { delete(p[row.Surfaces["graphql"]], row.PublishedName) },
		"exempt field renamed": func(p map[string]map[string]bool) {
			delete(p[base.Rows[exempt].Surfaces["graphql"]], base.Rows[exempt].PublishedName)
		},
		"recorded object missing": func(p map[string]map[string]bool) { delete(p, sibling) },
	}
	for name, mutate := range surfaceMutations {
		p := published()
		mutate(p)
		if len(registerNamesSurfaceErrors(base, "graphql", p)) == 0 {
			t.Errorf("surface mutation %q not caught", name)
		}
	}
}
