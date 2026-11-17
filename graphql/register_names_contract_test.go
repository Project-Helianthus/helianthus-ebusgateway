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

// registerNamesFixturePath is the record of the B524 and B509
// register-derived fields the system, circuit and boiler planes publish, with
// the docs-ebus name of each register. The MCP and Portal contract tests read
// the same file.
const registerNamesFixturePath = "testdata/register_derived_names.json"

// registerNamesExemptionIssue is the only docs-ebus issue that may hold a
// published name back from its docs-ebus name.
const registerNamesExemptionIssue = "helianthus-docs-ebus#1145"

type registerNamesDoc struct {
	Format      string                  `json:"format"`
	Description string                  `json:"description"`
	Source      string                  `json:"source"`
	Rows        []registerNamesRow      `json:"rows"`
	Unmapped    []registerNamesUnmapped `json:"unmapped_fields"`
	Fields      []registerNamesField    `json:"field_registers"`
	Pending     []registerNamesPending  `json:"pending"`
}

// registerNamesField is one published field of a zone, circuit or hot water
// object: either the single B524 register it is read from, with the
// docs-ebus name of that register, or derived.
type registerNamesField struct {
	Object        string                `json:"object"`
	PublishedName string                `json:"published_name"`
	Opcode        string                `json:"opcode,omitempty"`
	Group         string                `json:"group,omitempty"`
	Register      string                `json:"register,omitempty"`
	DocsEbusName  string                `json:"docs_ebus_name,omitempty"`
	Derived       *registerNamesDerived `json:"derived,omitempty"`
}

// registerNamesDerived lists the B524 registers a derived field combines,
// each written opcode/group/register, and why the field has no single
// register name. A derived field without registers is fed by no register.
type registerNamesDerived struct {
	Registers []string `json:"registers"`
	Reason    string   `json:"reason"`
}

// registerNamesPending is a single-register field whose published name still
// differs from the docs-ebus name of its register.
type registerNamesPending struct {
	Object        string `json:"object"`
	PublishedName string `json:"published_name"`
	DocsEbusName  string `json:"docs_ebus_name"`
}

// registerNamesUnmapped is a field of a recorded object that no single
// register feeds; it keeps its published name.
type registerNamesUnmapped struct {
	PublishedName string            `json:"published_name"`
	Surfaces      map[string]string `json:"surfaces"`
	Reason        string            `json:"reason"`
}

type registerNamesRow struct {
	PublishedName         string              `json:"published_name"`
	Service               string              `json:"service"`
	Opcode                string              `json:"opcode"`
	Group                 string              `json:"group,omitempty"`
	Register              string              `json:"register,omitempty"`
	Selector              string              `json:"selector,omitempty"`
	FallbackSelector      string              `json:"fallback_selector,omitempty"`
	DocsEbusName          string              `json:"docs_ebus_name"`
	PreviousName          string              `json:"previous_name,omitempty"`
	FormerAPIPath         string              `json:"legacy_api_path,omitempty"`
	Unit                  string              `json:"unit,omitempty"`
	Provisional           bool                `json:"provisional,omitempty"`
	PreviousPublishedName string              `json:"previous_published_name,omitempty"`
	Surfaces              map[string]string   `json:"surfaces"`
	Join                  *registerNamesJoin  `json:"join,omitempty"`
	Bound                 *registerNamesBound `json:"bound,omitempty"`
	Exemption             string              `json:"exemption,omitempty"`
}

// registerNamesJoin is the second register of a field published as the join
// of two registers; the first register is the row's own.
type registerNamesJoin struct {
	Register     string `json:"register"`
	DocsEbusName string `json:"docs_ebus_name"`
	PreviousName string `json:"previous_name"`
}

// registerNamesBound is the field that publishes the active write bound of
// the row's value.
type registerNamesBound struct {
	PublishedName         string            `json:"published_name"`
	PreviousPublishedName string            `json:"previous_published_name"`
	Surfaces              map[string]string `json:"surfaces"`
}

// registerNamesIdentity is the native identity of a row: service, opcode and
// either group and register (B524) or selector (B509).
func registerNamesIdentity(row registerNamesRow) string {
	if row.Service == "B509" {
		return row.Service + "/" + row.Opcode + "/" + row.Selector
	}
	return row.Service + "/" + row.Opcode + "/" + row.Group + "/" + row.Register
}

// registerNamesRetired returns the names of a row that no surface may
// publish: the docs-ebus previous_name, the gateway's previous published
// names of the value and of its bound, and the previous_name of a joined
// register. The exempt row still publishes its previous_name.
func registerNamesRetired(row registerNamesRow) []string {
	var names []string
	if row.Exemption == "" && row.PreviousName != "" && row.PreviousName != row.DocsEbusName {
		names = append(names, row.PreviousName)
	}
	if row.PreviousPublishedName != "" {
		names = append(names, row.PreviousPublishedName)
	}
	if row.Join != nil {
		names = append(names, row.Join.PreviousName)
	}
	if row.Bound != nil {
		names = append(names, row.Bound.PreviousPublishedName)
	}
	return names
}

// registerNamesPublished returns, for one surface, the object and name of
// every field a row publishes there: the value and, when recorded, its bound.
func registerNamesPublished(row registerNamesRow, surface string) [][2]string {
	var out [][2]string
	if object := row.Surfaces[surface]; object != "" {
		out = append(out, [2]string{object, row.PublishedName})
	}
	if row.Bound != nil {
		if object := row.Bound.Surfaces[surface]; object != "" {
			out = append(out, [2]string{object, row.Bound.PublishedName})
		}
	}
	return out
}

// registerNamesObjects returns every object of one surface the record names,
// for its rows and for its unmapped fields.
func registerNamesObjects(doc registerNamesDoc, surface string) map[string]bool {
	objects := map[string]bool{}
	for _, row := range doc.Rows {
		for _, field := range registerNamesPublished(row, surface) {
			objects[field[0]] = true
		}
	}
	for _, field := range doc.Unmapped {
		if object := field.Surfaces[surface]; object != "" {
			objects[object] = true
		}
	}
	return objects
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
	// registerNamesFormerPath is a documentary B509 API path: the boiler
	// object and the field the gateway once published.
	registerNamesFormerPath = regexp.MustCompile(`^(state|config|diagnostics)\.[a-z][A-Za-z]*$`)
	// registerNamesUnitSuffix is a unit carried in a name; units belong in
	// the field description.
	registerNamesUnitSuffix = regexp.MustCompile(`_(c|k|bar|mbar|pct|rpm|ua|lpm|kw|w|s)$`)
)

// registerNamesFixtureErrors checks the record itself: complete native
// identity, one row per register, docs-ebus names as published names, and
// at most the single listed exemption.
func registerNamesFixtureErrors(doc registerNamesDoc) []string {
	var errs []string
	if doc.Format != "helianthus-register-derived-names/v1" {
		errs = append(errs, "format "+doc.Format)
	}
	for _, page := range []string{"b524-op02-register-names.csv", "b509-register-name-correspondence.csv"} {
		if !strings.Contains(doc.Source, page) || !strings.Contains(doc.Description, page) {
			errs = append(errs, "source page "+page+" not stated")
		}
	}
	if len(doc.Rows) == 0 {
		errs = append(errs, "no rows")
	}
	identities := map[string]bool{}
	published := map[string]bool{}
	formerPaths := map[string]bool{}
	exemptions := 0
	for _, row := range doc.Rows {
		id := registerNamesIdentity(row)
		var names []string
		switch row.Service {
		case "B524":
			if row.Opcode != "0x02" || !registerNamesHexByte.MatchString(row.Group) || !registerNamesHexWord.MatchString(row.Register) {
				errs = append(errs, "identity "+id)
			}
			if row.Selector != "" || row.FallbackSelector != "" || row.FormerAPIPath != "" || row.Unit != "" || row.Provisional {
				errs = append(errs, id+": B509 column on a B524 row")
			}
			names = []string{row.PublishedName, row.DocsEbusName, row.PreviousName}
		case "B509":
			errs = append(errs, registerNamesB509Errors(id, row, identities, formerPaths)...)
			names = []string{row.PublishedName, row.DocsEbusName}
		default:
			errs = append(errs, "service "+id)
		}
		if identities[id] {
			errs = append(errs, "duplicate register "+id)
		}
		identities[id] = true
		if row.PreviousPublishedName != "" {
			names = append(names, row.PreviousPublishedName)
			if row.PreviousPublishedName == row.PublishedName {
				errs = append(errs, id+": previous published name equals the published name")
			}
		}
		if row.Join != nil {
			names = append(names, row.Join.DocsEbusName, row.Join.PreviousName)
			joinID := row.Service + "/" + row.Opcode + "/" + row.Group + "/" + row.Join.Register
			if !registerNamesHexWord.MatchString(row.Join.Register) {
				errs = append(errs, "identity "+joinID)
			}
			if identities[joinID] {
				errs = append(errs, "duplicate register "+joinID)
			}
			identities[joinID] = true
		}
		if row.Bound != nil {
			names = append(names, row.Bound.PublishedName, row.Bound.PreviousPublishedName)
			if row.Bound.PublishedName != row.PublishedName+"_bound" {
				errs = append(errs, fmt.Sprintf("%s: bound %q, want %q", id, row.Bound.PublishedName, row.PublishedName+"_bound"))
			}
			if row.Bound.PreviousPublishedName == row.Bound.PublishedName {
				errs = append(errs, id+": bound previous published name equals the bound name")
			}
			if len(row.Bound.Surfaces) != len(registerNamesSurfaces) {
				errs = append(errs, id+": bound surfaces")
			}
		}
		for _, name := range names {
			if !registerNamesSnake.MatchString(name) {
				errs = append(errs, fmt.Sprintf("%s: name %q", id, name))
			}
		}
		if len(row.Surfaces) != len(registerNamesSurfaces) {
			errs = append(errs, id+": surfaces")
		}
		for _, surface := range registerNamesSurfaces {
			if _, ok := row.Surfaces[surface]; !ok {
				errs = append(errs, id+": surface "+surface+" missing")
			}
			if row.Bound != nil {
				if _, ok := row.Bound.Surfaces[surface]; !ok {
					errs = append(errs, id+": bound surface "+surface+" missing")
				}
			}
			for _, field := range registerNamesPublished(row, surface) {
				key := surface + "/" + field[0] + "/" + field[1]
				if published[key] {
					errs = append(errs, "duplicate published name "+key)
				}
				published[key] = true
			}
		}
		if row.Exemption == "" {
			errs = append(errs, registerNamesDocsNameErrors(id, row)...)
			continue
		}
		exemptions++
		if !strings.Contains(row.Exemption, registerNamesExemptionIssue) || row.PublishedName != row.PreviousName || row.Register != "0x0007" || row.Group != "0x00" || row.Join != nil || row.Bound != nil || row.PreviousPublishedName != "" {
			errs = append(errs, id+": exemption is limited to GG00 RR0007 under "+registerNamesExemptionIssue)
		}
	}
	if exemptions > 1 {
		errs = append(errs, "more than one exemption")
	}
	errs = append(errs, registerNamesUnmappedErrors(doc, published)...)
	sort.Strings(errs)
	return errs
}

// registerNamesB509Errors checks the columns of one B509 row: operation
// opcode, selector and optional fallback selector, no B524 column, a
// documentary former API path whose object matches the GraphQL object, a published
// name without a unit suffix and a recorded unit where the previous published
// name carried one.
func registerNamesB509Errors(id string, row registerNamesRow, identities, formerPaths map[string]bool) []string {
	var errs []string
	if row.Opcode != "0x0D" || !registerNamesHexWord.MatchString(row.Selector) {
		errs = append(errs, "identity "+id)
	}
	if row.FallbackSelector != "" {
		fallbackID := row.Service + "/" + row.Opcode + "/" + row.FallbackSelector
		if !registerNamesHexWord.MatchString(row.FallbackSelector) || row.FallbackSelector == row.Selector {
			errs = append(errs, "fallback identity "+fallbackID)
		}
		if identities[fallbackID] {
			errs = append(errs, "duplicate register "+fallbackID)
		}
		identities[fallbackID] = true
	}
	if row.Group != "" || row.Register != "" || row.PreviousName != "" || row.Join != nil || row.Bound != nil || row.Exemption != "" {
		errs = append(errs, id+": B524 column on a B509 row")
	}
	if !registerNamesFormerPath.MatchString(row.FormerAPIPath) {
		errs = append(errs, fmt.Sprintf("%s: former API path %q", id, row.FormerAPIPath))
	} else if object := "boiler_" + strings.SplitN(row.FormerAPIPath, ".", 2)[0]; row.Surfaces["graphql"] != object {
		errs = append(errs, fmt.Sprintf("%s: former API path %q outside GraphQL object %q", id, row.FormerAPIPath, row.Surfaces["graphql"]))
	}
	if formerPaths[row.FormerAPIPath] {
		errs = append(errs, "duplicate former API path "+row.FormerAPIPath)
	}
	formerPaths[row.FormerAPIPath] = true
	if registerNamesUnitSuffix.MatchString(row.PublishedName) {
		errs = append(errs, fmt.Sprintf("%s: unit suffix in %q", id, row.PublishedName))
	}
	if registerNamesUnitSuffix.MatchString(row.PreviousPublishedName) && row.Unit == "" {
		errs = append(errs, fmt.Sprintf("%s: unit of %q not recorded", id, row.PreviousPublishedName))
	}
	return errs
}

// registerNamesUnmappedErrors checks the unmapped fields: snake_case, a
// reason, all surfaces named, and a name that is neither recorded for a
// register nor retired.
func registerNamesUnmappedErrors(doc registerNamesDoc, published map[string]bool) []string {
	var errs []string
	retired := map[string]bool{}
	for _, row := range doc.Rows {
		for _, name := range registerNamesRetired(row) {
			retired[name] = true
		}
	}
	seen := map[string]bool{}
	for _, field := range doc.Unmapped {
		name := field.PublishedName
		if !registerNamesSnake.MatchString(name) {
			errs = append(errs, fmt.Sprintf("unmapped name %q", name))
		}
		if strings.TrimSpace(field.Reason) == "" {
			errs = append(errs, "unmapped "+name+": no reason")
		}
		if retired[name] {
			errs = append(errs, "unmapped "+name+": retired name")
		}
		if len(field.Surfaces) != len(registerNamesSurfaces) {
			errs = append(errs, "unmapped "+name+": surfaces")
		}
		for _, surface := range registerNamesSurfaces {
			object, ok := field.Surfaces[surface]
			if !ok {
				errs = append(errs, "unmapped "+name+": surface "+surface+" missing")
				continue
			}
			key := surface + "/" + object + "/" + name
			if object != "" && (published[key] || seen[key]) {
				errs = append(errs, "unmapped "+name+": duplicate published name "+key)
			}
			seen[key] = true
		}
	}
	return errs
}

// registerNamesDescriptionErrors checks the GraphQL description of every
// B509 field: it states the recorded unit and says provisional exactly when
// the docs-ebus row carries an open question. descriptions maps object name
// to field name to description.
func registerNamesDescriptionErrors(doc registerNamesDoc, descriptions map[string]map[string]string) []string {
	var errs []string
	for _, row := range doc.Rows {
		object := row.Surfaces["graphql"]
		if row.Service != "B509" || object == "" {
			continue
		}
		description := descriptions[object][row.PublishedName]
		if row.Unit != "" && !strings.Contains(description, " in "+row.Unit+".") {
			errs = append(errs, fmt.Sprintf("%s.%s: description %q does not state unit %s", object, row.PublishedName, description, row.Unit))
		}
		if strings.Contains(description, "Provisional") != row.Provisional {
			errs = append(errs, fmt.Sprintf("%s.%s: description %q, provisional %v", object, row.PublishedName, description, row.Provisional))
		}
	}
	sort.Strings(errs)
	return errs
}

// registerNamesDocsNameErrors requires the published name to be the docs-ebus
// name of the register or, for a joined field, the common stem of the
// docs-ebus names of its two registers.
func registerNamesDocsNameErrors(id string, row registerNamesRow) []string {
	if row.Join == nil {
		if row.PublishedName != row.DocsEbusName {
			return []string{fmt.Sprintf("%s: published %q, docs-ebus name %q", id, row.PublishedName, row.DocsEbusName)}
		}
		return nil
	}
	var errs []string
	if row.DocsEbusName != row.PublishedName+"_prefix" {
		errs = append(errs, fmt.Sprintf("%s: joined field %q, docs-ebus name %q", id, row.PublishedName, row.DocsEbusName))
	}
	if row.Join.DocsEbusName != row.PublishedName+"_suffix" {
		errs = append(errs, fmt.Sprintf("%s: joined field %q, joined docs-ebus name %q", id, row.PublishedName, row.Join.DocsEbusName))
	}
	return errs
}

// registerNamesSurfaceErrors checks one surface: every recorded row is
// published under its recorded name in its recorded object, and no recorded
// previous_name is published in any recorded object of that surface.
// published maps object name to the field names that object publishes.
func registerNamesSurfaceErrors(doc registerNamesDoc, surface string, published map[string]map[string]bool) []string {
	var errs []string
	objects := registerNamesObjects(doc, surface)
	for object := range objects {
		if published[object] == nil {
			errs = append(errs, surface+": object "+object+" not found")
		}
	}
	for _, row := range doc.Rows {
		for _, field := range registerNamesPublished(row, surface) {
			if !published[field[0]][field[1]] {
				errs = append(errs, fmt.Sprintf("%s: %s does not publish %s (register %s)", surface, field[0], field[1], registerNamesIdentity(row)))
			}
		}
		for _, retired := range registerNamesRetired(row) {
			for candidate := range objects {
				if published[candidate][retired] {
					errs = append(errs, fmt.Sprintf("%s: %s publishes previous name %s (register %s)", surface, candidate, retired, registerNamesIdentity(row)))
				}
			}
		}
	}
	for _, field := range doc.Unmapped {
		if object := field.Surfaces[surface]; object != "" && !published[object][field.PublishedName] {
			errs = append(errs, fmt.Sprintf("%s: %s does not publish unmapped field %s", surface, object, field.PublishedName))
		}
	}
	sort.Strings(errs)
	return errs
}

func graphqlPublishedFields(t *testing.T, schema graphqlgo.Schema, doc registerNamesDoc) map[string]map[string]bool {
	t.Helper()
	published := map[string]map[string]bool{}
	for name := range registerNamesObjects(doc, "graphql") {
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

func graphqlFieldDescriptions(schema graphqlgo.Schema, doc registerNamesDoc) map[string]map[string]string {
	descriptions := map[string]map[string]string{}
	for name := range registerNamesObjects(doc, "graphql") {
		object, ok := schema.Type(name).(*graphqlgo.Object)
		if !ok {
			continue
		}
		fields := map[string]string{}
		for field, definition := range object.Fields() {
			fields[field] = definition.Description
		}
		descriptions[name] = fields
	}
	return descriptions
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
		if errs := registerNamesDescriptionErrors(doc, graphqlFieldDescriptions(schema, doc)); len(errs) > 0 {
			t.Fatalf("%s schema descriptions:\n%s", build.name, strings.Join(errs, "\n"))
		}
		for _, row := range doc.Rows {
			path, ok := map[string]string{
				"system_state":       "system { state",
				"system_config":      "system { config",
				"system_properties":  "system { properties",
				"circuit_config":     "circuits { config",
				"boiler_state":       "boiler_status { state",
				"boiler_config":      "boiler_status { config",
				"boiler_diagnostics": "boiler_status { diagnostics",
			}[row.Surfaces["graphql"]]
			if !ok {
				t.Fatalf("no query path for GraphQL object %q", row.Surfaces["graphql"])
			}
			for _, retired := range registerNamesRetired(row) {
				query := "{ " + path + " { " + retired + " } } }"
				if result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: query}); len(result.Errors) == 0 {
					t.Errorf("%s schema accepts previous name: %s", build.name, query)
				}
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
			if row.Join != nil {
				join := *row.Join
				row.Join = &join
			}
			if row.Bound != nil {
				bound := *row.Bound
				bound.Surfaces = map[string]string{}
				for k, v := range row.Bound.Surfaces {
					bound.Surfaces[k] = v
				}
				row.Bound = &bound
			}
			doc.Rows[i] = row
		}
		doc.Unmapped = make([]registerNamesUnmapped, len(base.Unmapped))
		for i, field := range base.Unmapped {
			surfaces := map[string]string{}
			for k, v := range field.Surfaces {
				surfaces[k] = v
			}
			field.Surfaces = surfaces
			doc.Unmapped[i] = field
		}
		return doc
	}
	renamed, exempt, gateway, joined, bounded := -1, -1, -1, -1, -1
	b509, b509Other, fallback, provisional, plain := -1, -1, -1, -1, -1
	for i, row := range base.Rows {
		if row.Service == "B509" {
			switch {
			case row.FallbackSelector != "":
				fallback = i
			case b509 < 0 && row.Unit != "" && row.PreviousPublishedName != "" && !row.Provisional:
				b509 = i
			case b509Other < 0 && row.Unit != "":
				b509Other = i
			}
			if provisional < 0 && row.Provisional && row.Unit != "" {
				provisional = i
			}
			if plain < 0 && !row.Provisional && row.Unit == "" {
				plain = i
			}
			continue
		}
		if row.Exemption != "" {
			exempt = i
		} else if renamed < 0 && row.PreviousName != row.DocsEbusName {
			renamed = i
		}
		if gateway < 0 && row.PreviousPublishedName != "" && row.Join == nil && row.Bound == nil {
			gateway = i
		}
		if row.Join != nil {
			joined = i
		}
		if row.Bound != nil {
			bounded = i
		}
	}
	if renamed < 0 || exempt < 0 || gateway < 0 || joined < 0 || bounded < 0 {
		t.Fatal("fixture needs a renamed row, the exempt row, a row with a previous published name, a joined row and a row with a bound")
	}
	if b509 < 0 || b509Other < 0 || fallback < 0 || provisional < 0 || plain < 0 || len(base.Unmapped) < 2 {
		t.Fatal("fixture needs two B509 rows with a unit, a B509 row with a fallback selector, a provisional B509 row, a B509 row without unit and two unmapped fields")
	}
	otherObject := func(object string) string {
		for _, row := range base.Rows {
			if row.Service == "B509" && row.Surfaces["graphql"] != object {
				return strings.TrimPrefix(row.Surfaces["graphql"], "boiler_")
			}
		}
		return ""
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
		"exemption with previous published name": func(d *registerNamesDoc) {
			d.Rows[exempt].PreviousPublishedName = d.Rows[gateway].PreviousPublishedName
		},
		"previous published name not snake":  func(d *registerNamesDoc) { d.Rows[gateway].PreviousPublishedName = "previousName" },
		"previous published name is current": func(d *registerNamesDoc) { d.Rows[gateway].PreviousPublishedName = d.Rows[gateway].PublishedName },
		"joined published name differs":      func(d *registerNamesDoc) { d.Rows[joined].PublishedName = d.Rows[joined].PreviousPublishedName },
		"joined docs-ebus name differs":      func(d *registerNamesDoc) { d.Rows[joined].DocsEbusName = d.Rows[joined].PublishedName },
		"join docs-ebus name differs":        func(d *registerNamesDoc) { d.Rows[joined].Join.DocsEbusName = "other_name" },
		"join register":                      func(d *registerNamesDoc) { d.Rows[joined].Join.Register = "0x70" },
		"join register duplicates row":       func(d *registerNamesDoc) { d.Rows[joined].Join.Register = d.Rows[joined].Register },
		"join register duplicates other row": func(d *registerNamesDoc) { d.Rows[joined].Join.Register = d.Rows[renamed].Register },
		"join previous name not snake":       func(d *registerNamesDoc) { d.Rows[joined].Join.PreviousName = "installerPhone2" },
		"bound name differs":                 func(d *registerNamesDoc) { d.Rows[bounded].Bound.PublishedName = "other_bound" },
		"bound previous name is current": func(d *registerNamesDoc) {
			d.Rows[bounded].Bound.PreviousPublishedName = d.Rows[bounded].Bound.PublishedName
		},
		"bound previous name not snake":        func(d *registerNamesDoc) { d.Rows[bounded].Bound.PreviousPublishedName = "previousBound" },
		"bound surface missing":                func(d *registerNamesDoc) { delete(d.Rows[bounded].Bound.Surfaces, "mcp") },
		"value published under the bound name": func(d *registerNamesDoc) { d.Rows[bounded].PublishedName = d.Rows[bounded].Bound.PublishedName },
		// B509 identity: service, opcode, selector, fallback selector; every
		// one caught when changed or duplicated.
		"B509 service":                    func(d *registerNamesDoc) { d.Rows[b509].Service = "B510" },
		"B509 row as B524":                func(d *registerNamesDoc) { d.Rows[b509].Service = "B524" },
		"B524 row as B509":                func(d *registerNamesDoc) { d.Rows[renamed].Service = "B509" },
		"B509 opcode":                     func(d *registerNamesDoc) { d.Rows[b509].Opcode = "0x02" },
		"B509 selector":                   func(d *registerNamesDoc) { d.Rows[b509].Selector = "0x18" },
		"B509 selector duplicated":        func(d *registerNamesDoc) { d.Rows[b509].Selector = d.Rows[b509Other].Selector },
		"B509 selector is a fallback":     func(d *registerNamesDoc) { d.Rows[b509].Selector = d.Rows[fallback].FallbackSelector },
		"fallback selector":               func(d *registerNamesDoc) { d.Rows[fallback].FallbackSelector = "0xA5" },
		"fallback selector is own":        func(d *registerNamesDoc) { d.Rows[fallback].FallbackSelector = d.Rows[fallback].Selector },
		"fallback selector duplicated":    func(d *registerNamesDoc) { d.Rows[fallback].FallbackSelector = d.Rows[b509].Selector },
		"B509 row with group":             func(d *registerNamesDoc) { d.Rows[b509].Group = "0x00" },
		"B509 row with register":          func(d *registerNamesDoc) { d.Rows[b509].Register = "0x0039" },
		"B509 row with previous_name":     func(d *registerNamesDoc) { d.Rows[b509].PreviousName = d.Rows[b509].PreviousPublishedName },
		"B509 row with exemption":         func(d *registerNamesDoc) { d.Rows[b509].Exemption = "open" },
		"B524 row with selector":          func(d *registerNamesDoc) { d.Rows[renamed].Selector = d.Rows[b509].Selector },
		"B524 row with fallback selector": func(d *registerNamesDoc) { d.Rows[renamed].FallbackSelector = "0xA500" },
		"B524 row with former API path":   func(d *registerNamesDoc) { d.Rows[renamed].FormerAPIPath = d.Rows[b509].FormerAPIPath },
		"B524 row with unit":              func(d *registerNamesDoc) { d.Rows[renamed].Unit = "bar" },
		"B524 row provisional":            func(d *registerNamesDoc) { d.Rows[renamed].Provisional = true },
		// B509 names: published, docs-ebus, previous published, former API path.
		"B509 published differs":       func(d *registerNamesDoc) { d.Rows[b509].PublishedName = d.Rows[b509].PreviousPublishedName },
		"B509 docs-ebus name differs":  func(d *registerNamesDoc) { d.Rows[b509].DocsEbusName = "other_name" },
		"B509 previous published name": func(d *registerNamesDoc) { d.Rows[b509].PreviousPublishedName = d.Rows[b509].PublishedName },
		"B509 unit suffix": func(d *registerNamesDoc) {
			d.Rows[b509].PublishedName += "_c"
			d.Rows[b509].DocsEbusName = d.Rows[b509].PublishedName
		},
		"B509 unit not recorded":        func(d *registerNamesDoc) { d.Rows[b509].Unit = "" },
		"former API path missing":       func(d *registerNamesDoc) { d.Rows[b509].FormerAPIPath = "" },
		"former API path without plane": func(d *registerNamesDoc) { d.Rows[b509].FormerAPIPath = "flowTemperatureC" },
		"former API path other plane": func(d *registerNamesDoc) {
			path := d.Rows[b509].FormerAPIPath
			d.Rows[b509].FormerAPIPath = otherObject(d.Rows[b509].Surfaces["graphql"]) + path[strings.Index(path, "."):]
		},
		"former API path duplicated": func(d *registerNamesDoc) { d.Rows[b509].FormerAPIPath = d.Rows[b509Other].FormerAPIPath },
		"B509 published name duplicated": func(d *registerNamesDoc) {
			d.Rows[b509].Surfaces = d.Rows[b509Other].Surfaces
			d.Rows[b509].PublishedName = d.Rows[b509Other].PublishedName
			d.Rows[b509].DocsEbusName = d.Rows[b509Other].DocsEbusName
		},
		// Unmapped fields: name, reason, each surface.
		"unmapped name not snake":  func(d *registerNamesDoc) { d.Unmapped[0].PublishedName = "returnTemperature" },
		"unmapped name is retired": func(d *registerNamesDoc) { d.Unmapped[0].PublishedName = d.Rows[b509].PreviousPublishedName },
		"unmapped name is published": func(d *registerNamesDoc) {
			d.Unmapped[0].Surfaces = d.Rows[b509].Surfaces
			d.Unmapped[0].PublishedName = d.Rows[b509].PublishedName
		},
		"unmapped name duplicated": func(d *registerNamesDoc) { d.Unmapped[1] = d.Unmapped[0] },
		"unmapped reason missing":  func(d *registerNamesDoc) { d.Unmapped[0].Reason = " " },
		"unmapped graphql missing": func(d *registerNamesDoc) { delete(d.Unmapped[0].Surfaces, "graphql") },
		"unmapped mcp missing":     func(d *registerNamesDoc) { delete(d.Unmapped[0].Surfaces, "mcp") },
		"unmapped portal missing":  func(d *registerNamesDoc) { delete(d.Unmapped[0].Surfaces, "portal") },
		"source B509 page": func(d *registerNamesDoc) {
			d.Source = strings.ReplaceAll(d.Source, "b509-register-name-correspondence.csv", "b509.csv")
		},
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
			for _, field := range registerNamesPublished(row, "graphql") {
				if out[field[0]] == nil {
					out[field[0]] = map[string]bool{}
				}
				out[field[0]][field[1]] = true
			}
		}
		for _, field := range base.Unmapped {
			if out[field.Surfaces["graphql"]] == nil {
				out[field.Surfaces["graphql"]] = map[string]bool{}
			}
			out[field.Surfaces["graphql"]][field.PublishedName] = true
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
		"field back to previous published name": func(p map[string]map[string]bool) {
			g := base.Rows[gateway]
			delete(p[g.Surfaces["graphql"]], g.PublishedName)
			p[g.Surfaces["graphql"]][g.PreviousPublishedName] = true
		},
		"previous published name as alias": func(p map[string]map[string]bool) {
			g := base.Rows[gateway]
			p[g.Surfaces["graphql"]][g.PreviousPublishedName] = true
		},
		"previous published name in sibling": func(p map[string]map[string]bool) {
			p[sibling][base.Rows[gateway].PreviousPublishedName] = true
		},
		"joined register previous name published": func(p map[string]map[string]bool) {
			j := base.Rows[joined]
			p[j.Surfaces["graphql"]][j.Join.PreviousName] = true
		},
		"bound dropped": func(p map[string]map[string]bool) {
			b := base.Rows[bounded]
			delete(p[b.Bound.Surfaces["graphql"]], b.Bound.PublishedName)
		},
		"bound back to previous published name": func(p map[string]map[string]bool) {
			b := base.Rows[bounded]
			delete(p[b.Bound.Surfaces["graphql"]], b.Bound.PublishedName)
			p[b.Bound.Surfaces["graphql"]][b.Bound.PreviousPublishedName] = true
		},
		"bound previous published name as alias": func(p map[string]map[string]bool) {
			b := base.Rows[bounded]
			p[b.Bound.Surfaces["graphql"]][b.Bound.PreviousPublishedName] = true
		},
	}
	for name, mutate := range surfaceMutations {
		p := published()
		mutate(p)
		if len(registerNamesSurfaceErrors(base, "graphql", p)) == 0 {
			t.Errorf("surface mutation %q not caught", name)
		}
	}

	// B509 rows and unmapped fields on a surface.
	b := base.Rows[b509]
	u := base.Unmapped[0]
	b509Mutations := map[string]func(map[string]map[string]bool){
		"B509 field back to previous published name": func(p map[string]map[string]bool) {
			delete(p[b.Surfaces["graphql"]], b.PublishedName)
			p[b.Surfaces["graphql"]][b.PreviousPublishedName] = true
		},
		"B509 previous published name as alias": func(p map[string]map[string]bool) { p[b.Surfaces["graphql"]][b.PreviousPublishedName] = true },
		"B509 previous published name in sibling": func(p map[string]map[string]bool) {
			p[base.Rows[renamed].Surfaces["graphql"]][b.PreviousPublishedName] = true
		},
		"B509 field dropped":     func(p map[string]map[string]bool) { delete(p[b.Surfaces["graphql"]], b.PublishedName) },
		"unmapped field dropped": func(p map[string]map[string]bool) { delete(p[u.Surfaces["graphql"]], u.PublishedName) },
		"unmapped field renamed": func(p map[string]map[string]bool) {
			delete(p[u.Surfaces["graphql"]], u.PublishedName)
			p[u.Surfaces["graphql"]][strings.TrimSuffix(u.PublishedName, "_c")] = true
		},
		"unmapped object missing": func(p map[string]map[string]bool) { delete(p, u.Surfaces["graphql"]) },
	}
	for name, mutate := range b509Mutations {
		p := published()
		mutate(p)
		if len(registerNamesSurfaceErrors(base, "graphql", p)) == 0 {
			t.Errorf("B509 surface mutation %q not caught", name)
		}
	}

	// GraphQL descriptions: unit and provisional marker.
	descriptions := func() map[string]map[string]string {
		out := map[string]map[string]string{}
		for _, row := range base.Rows {
			object := row.Surfaces["graphql"]
			if row.Service != "B509" || object == "" {
				continue
			}
			if out[object] == nil {
				out[object] = map[string]string{}
			}
			text := "Value"
			if row.Unit != "" {
				text += " in " + row.Unit + "."
			}
			if row.Provisional {
				text += " Provisional name."
			}
			out[object][row.PublishedName] = text
		}
		return out
	}
	if errs := registerNamesDescriptionErrors(base, descriptions()); len(errs) != 0 {
		t.Fatalf("clean descriptions reported: %v", errs)
	}
	pr, pl := base.Rows[provisional], base.Rows[plain]
	descriptionMutations := map[string]func(*registerNamesDoc, map[string]map[string]string){
		"unit dropped from description": func(_ *registerNamesDoc, d map[string]map[string]string) {
			d[b.Surfaces["graphql"]][b.PublishedName] = "Value."
		},
		"recorded unit changed": func(doc *registerNamesDoc, _ map[string]map[string]string) { doc.Rows[b509].Unit = "m" + b.Unit },
		"provisional marker dropped": func(_ *registerNamesDoc, d map[string]map[string]string) {
			d[pr.Surfaces["graphql"]][pr.PublishedName] = "Value in " + pr.Unit + "."
		},
		"provisional marker on a settled name": func(_ *registerNamesDoc, d map[string]map[string]string) {
			d[pl.Surfaces["graphql"]][pl.PublishedName] = "Value. Provisional name."
		},
		"provisional flag dropped": func(doc *registerNamesDoc, _ map[string]map[string]string) { doc.Rows[provisional].Provisional = false },
		"provisional flag added":   func(doc *registerNamesDoc, _ map[string]map[string]string) { doc.Rows[plain].Provisional = true },
		"description missing": func(_ *registerNamesDoc, d map[string]map[string]string) {
			delete(d[pr.Surfaces["graphql"]], pr.PublishedName)
		},
	}
	for name, mutate := range descriptionMutations {
		doc := clone()
		d := descriptions()
		mutate(&doc, d)
		if len(registerNamesDescriptionErrors(doc, d)) == 0 {
			t.Errorf("description mutation %q not caught", name)
		}
	}
}

// registerNamesFieldGroups maps each GraphQL object whose fields
// field_registers records to the B524 group of its plane.
var registerNamesFieldGroups = map[string]string{
	"zone_state":     "0x03",
	"zone_config":    "0x03",
	"circuit_state":  "0x02",
	"circuit_config": "0x02",
	"dhw_state":      "0x01",
	"dhw_config":     "0x01",
}

var registerNamesDerivedRegister = regexp.MustCompile(`^0x0[26]/0x[0-9A-F]{2}/0x[0-9A-F]{4}$`)

// registerNamesUnfedReason opens the reason of a derived field that no B524
// register feeds.
const registerNamesUnfedReason = "No B524 register feeds it"

// registerNamesFieldErrors checks field_registers and pending: one entry per
// object and field, a complete identity in the group of the object's plane
// for a single-register field, registers and a reason for a derived field,
// agreement with the rows that record the same register, and a pending list
// equal to the single-register fields whose published name differs from the
// docs-ebus name.
func registerNamesFieldErrors(doc registerNamesDoc) []string {
	var errs []string
	rows := map[string]registerNamesRow{}
	for _, row := range doc.Rows {
		if row.Service == "B524" {
			rows[row.Group+"/"+row.Register] = row
		}
	}
	seen := map[string]bool{}
	registers := map[string]string{}
	objects := map[string]bool{}
	differences := map[string]bool{}
	for _, field := range doc.Fields {
		key := field.Object + "." + field.PublishedName
		group, known := registerNamesFieldGroups[field.Object]
		if !known {
			errs = append(errs, key+": object not recorded")
		}
		objects[field.Object] = true
		if seen[key] {
			errs = append(errs, "duplicate field "+key)
		}
		seen[key] = true
		if !registerNamesSnake.MatchString(field.PublishedName) {
			errs = append(errs, fmt.Sprintf("%s: name %q", key, field.PublishedName))
		}
		if field.Derived != nil {
			errs = append(errs, registerNamesDerivedErrors(key, field)...)
			continue
		}
		if field.Opcode != "0x02" || field.Group != group || !registerNamesHexWord.MatchString(field.Register) {
			errs = append(errs, fmt.Sprintf("%s: identity %s/%s/%s", key, field.Opcode, field.Group, field.Register))
		}
		if !registerNamesSnake.MatchString(field.DocsEbusName) {
			errs = append(errs, fmt.Sprintf("%s: docs-ebus name %q", key, field.DocsEbusName))
		}
		id := field.Group + "/" + field.Register
		if other, ok := registers[id]; ok {
			errs = append(errs, fmt.Sprintf("%s: register %s already recorded for %s", key, id, other))
		}
		registers[id] = key
		if row, ok := rows[id]; ok && (row.Surfaces["graphql"] != field.Object || row.PublishedName != field.PublishedName || row.DocsEbusName != field.DocsEbusName) {
			errs = append(errs, fmt.Sprintf("%s: disagrees with the row of register %s", key, id))
		}
		if field.PublishedName != field.DocsEbusName {
			differences[field.Object+"."+field.PublishedName+" -> "+field.DocsEbusName] = true
		}
	}
	for object := range registerNamesFieldGroups {
		if !objects[object] {
			errs = append(errs, "object "+object+" has no fields")
		}
	}
	for _, row := range doc.Rows {
		if _, ok := registerNamesFieldGroups[row.Surfaces["graphql"]]; ok && row.Service == "B524" {
			if _, recorded := registers[row.Group+"/"+row.Register]; !recorded {
				errs = append(errs, "row "+registerNamesIdentity(row)+" missing from field_registers")
			}
		}
	}
	errs = append(errs, registerNamesPendingErrors(doc.Pending, differences)...)
	sort.Strings(errs)
	return errs
}

// registerNamesDerivedErrors checks one derived field: no single-register
// column, distinct well-formed registers, and a reason that says no register
// feeds the field exactly when it lists none.
func registerNamesDerivedErrors(key string, field registerNamesField) []string {
	var errs []string
	if field.Opcode != "" || field.Group != "" || field.Register != "" || field.DocsEbusName != "" {
		errs = append(errs, key+": derived field with a single-register column")
	}
	reason := strings.TrimSpace(field.Derived.Reason)
	if reason == "" || !strings.HasSuffix(reason, ".") {
		errs = append(errs, fmt.Sprintf("%s: reason %q", key, field.Derived.Reason))
	}
	if unfed := strings.HasPrefix(reason, registerNamesUnfedReason); unfed != (len(field.Derived.Registers) == 0) {
		errs = append(errs, fmt.Sprintf("%s: %d registers, reason %q", key, len(field.Derived.Registers), reason))
	}
	if len(field.Derived.Registers) == 1 {
		errs = append(errs, key+": a derived field combines more than one register")
	}
	listed := map[string]bool{}
	for _, register := range field.Derived.Registers {
		if !registerNamesDerivedRegister.MatchString(register) {
			errs = append(errs, fmt.Sprintf("%s: derived register %q", key, register))
		}
		if listed[register] {
			errs = append(errs, key+": derived register "+register+" listed twice")
		}
		listed[register] = true
	}
	return errs
}

// registerNamesPendingErrors requires the pending list to equal the computed
// differences, each written object.published -> docs-ebus name: a field
// renamed to its docs-ebus name leaves the list, a new difference enters it.
func registerNamesPendingErrors(pending []registerNamesPending, differences map[string]bool) []string {
	var errs []string
	listed := map[string]bool{}
	for _, entry := range pending {
		key := entry.Object + "." + entry.PublishedName + " -> " + entry.DocsEbusName
		if listed[key] {
			errs = append(errs, "pending lists "+key+" twice")
		}
		listed[key] = true
		if !differences[key] {
			errs = append(errs, "pending lists "+key+", which is not a difference")
		}
	}
	for key := range differences {
		if !listed[key] {
			errs = append(errs, "pending lacks "+key)
		}
	}
	return errs
}

// registerNamesFieldCoverageErrors requires field_registers to record exactly
// the fields each recorded object publishes. published maps object name to
// the field names that object publishes.
func registerNamesFieldCoverageErrors(doc registerNamesDoc, published map[string]map[string]bool) []string {
	var errs []string
	recorded := map[string]map[string]bool{}
	for _, field := range doc.Fields {
		if recorded[field.Object] == nil {
			recorded[field.Object] = map[string]bool{}
		}
		recorded[field.Object][field.PublishedName] = true
	}
	for object := range registerNamesFieldGroups {
		fields, ok := published[object]
		if !ok {
			errs = append(errs, "object "+object+" not found")
			continue
		}
		for name := range fields {
			if !recorded[object][name] {
				errs = append(errs, object+"."+name+" published but not recorded")
			}
		}
		for name := range recorded[object] {
			if !fields[name] {
				errs = append(errs, object+"."+name+" recorded but not published")
			}
		}
	}
	sort.Strings(errs)
	return errs
}

func TestRegisterNamesFieldRegistersAreConsistent(t *testing.T) {
	t.Parallel()
	doc := loadRegisterNamesDoc(t)
	if errs := registerNamesFieldErrors(doc); len(errs) > 0 {
		t.Fatalf("field_registers errors:\n%s", strings.Join(errs, "\n"))
	}
}

func TestGraphQLFieldRegistersRecordEveryPublishedField(t *testing.T) {
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
		published := map[string]map[string]bool{}
		for name := range registerNamesFieldGroups {
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
		if errs := registerNamesFieldCoverageErrors(doc, published); len(errs) > 0 {
			t.Fatalf("%s schema:\n%s", build.name, strings.Join(errs, "\n"))
		}
	}
}

// TestRegisterNamesFieldCheckersCatchEachMutation varies every column of a
// field_registers entry (object, published name, opcode, group, register,
// docs-ebus name, derived registers and reason), every column of a pending
// entry (object, published name, docs-ebus name) and the published field set,
// and requires the checkers to report each change.
func TestRegisterNamesFieldCheckersCatchEachMutation(t *testing.T) {
	t.Parallel()
	base := registerNamesWithPendingField(t, loadRegisterNamesDoc(t))
	if len(registerNamesFieldErrors(base)) != 0 {
		t.Fatal("base fixture must be clean")
	}
	clone := func() registerNamesDoc {
		doc := base
		doc.Fields = make([]registerNamesField, len(base.Fields))
		for i, field := range base.Fields {
			if field.Derived != nil {
				derived := *field.Derived
				derived.Registers = append([]string(nil), field.Derived.Registers...)
				field.Derived = &derived
			}
			doc.Fields[i] = field
		}
		doc.Pending = append([]registerNamesPending(nil), base.Pending...)
		return doc
	}
	pending, settled, combined, unfed, shared := -1, -1, -1, -1, -1
	for i, field := range base.Fields {
		switch {
		case field.Derived == nil && field.PublishedName != field.DocsEbusName && pending < 0:
			pending = i
		case field.Derived == nil && field.PublishedName == field.DocsEbusName:
			for _, row := range base.Rows {
				if row.Service == "B524" && row.Group == field.Group && row.Register == field.Register {
					shared = i
				}
			}
			if shared != i && settled < 0 {
				settled = i
			}
		case field.Derived != nil && len(field.Derived.Registers) > 1 && combined < 0:
			combined = i
		case field.Derived != nil && len(field.Derived.Registers) == 0 && unfed < 0:
			unfed = i
		}
	}
	if pending < 0 || settled < 0 || combined < 0 || unfed < 0 || shared < 0 || len(base.Pending) == 0 {
		t.Fatal("fixture needs a pending field, a settled field, a field shared with a row, a derived field with registers and one without")
	}
	other := func(object string) string {
		for _, field := range base.Fields {
			if registerNamesFieldGroups[field.Object] != registerNamesFieldGroups[object] {
				return field.Object
			}
		}
		return ""
	}
	p, s := base.Fields[pending], base.Fields[settled]
	mutations := map[string]func(*registerNamesDoc){
		// Single-register identity and names.
		"object unknown":            func(d *registerNamesDoc) { d.Fields[pending].Object = "other_object" },
		"object of another plane":   func(d *registerNamesDoc) { d.Fields[pending].Object = other(p.Object) },
		"published name not snake":  func(d *registerNamesDoc) { d.Fields[pending].PublishedName = "flowTemp" },
		"settled field diverges":    func(d *registerNamesDoc) { d.Fields[settled].PublishedName = s.PublishedName + "_value" },
		"opcode":                    func(d *registerNamesDoc) { d.Fields[pending].Opcode = "0x06" },
		"group":                     func(d *registerNamesDoc) { d.Fields[pending].Group = "0x00" },
		"register malformed":        func(d *registerNamesDoc) { d.Fields[pending].Register = "0x21" },
		"register of another field": func(d *registerNamesDoc) { d.Fields[pending].Register = s.Register; d.Fields[pending].Group = s.Group },
		"docs-ebus name changed":    func(d *registerNamesDoc) { d.Fields[pending].DocsEbusName = "other_name" },
		"settled docs-ebus name":    func(d *registerNamesDoc) { d.Fields[settled].DocsEbusName = "other_name" },
		"docs-ebus name missing":    func(d *registerNamesDoc) { d.Fields[pending].DocsEbusName = "" },
		"field duplicated":          func(d *registerNamesDoc) { d.Fields = append(d.Fields, d.Fields[settled]) },
		"single field also derived": func(d *registerNamesDoc) { d.Fields[pending].Derived = d.Fields[combined].Derived },
		"row and field disagree":    func(d *registerNamesDoc) { d.Fields[shared].DocsEbusName = "other_name" },
		"row register not recorded": func(d *registerNamesDoc) { d.Fields[shared].Register = "0x00FE" },
		"row object differs":        func(d *registerNamesDoc) { d.Fields[shared].Object = other(base.Fields[shared].Object) },
		"object without fields":     func(d *registerNamesDoc) { d.Fields = filterFields(d.Fields, p.Object) },
		"derived with a register":   func(d *registerNamesDoc) { d.Fields[combined].Register = p.Register },
		"derived with a group":      func(d *registerNamesDoc) { d.Fields[combined].Group = p.Group },
		"derived with an opcode":    func(d *registerNamesDoc) { d.Fields[combined].Opcode = "0x02" },
		"derived with a docs name":  func(d *registerNamesDoc) { d.Fields[combined].DocsEbusName = p.DocsEbusName },
		"derived reason missing":    func(d *registerNamesDoc) { d.Fields[combined].Derived.Reason = " " },
		"derived registers dropped": func(d *registerNamesDoc) { d.Fields[combined].Derived.Registers = nil },
		"derived single register": func(d *registerNamesDoc) {
			d.Fields[combined].Derived.Registers = d.Fields[combined].Derived.Registers[:1]
		},
		"derived register opcode": func(d *registerNamesDoc) {
			d.Fields[combined].Derived.Registers[0] = "0x01" + d.Fields[combined].Derived.Registers[0][4:]
		},
		"derived register group":     func(d *registerNamesDoc) { d.Fields[combined].Derived.Registers[0] = "0x02/0x3/0x0012" },
		"derived register malformed": func(d *registerNamesDoc) { d.Fields[combined].Derived.Registers[0] = "0x02/0x03/0x12" },
		"derived register twice": func(d *registerNamesDoc) {
			d.Fields[combined].Derived.Registers[1] = d.Fields[combined].Derived.Registers[0]
		},
		"unfed field gains a register": func(d *registerNamesDoc) {
			d.Fields[unfed].Derived.Registers = []string{"0x02/0x03/0x000F", "0x02/0x03/0x0013"}
		},
		"unfed reason replaced": func(d *registerNamesDoc) { d.Fields[unfed].Derived.Reason = "Computed elsewhere." },
		// Pending entries: each column, removal, addition and duplication.
		"pending object":         func(d *registerNamesDoc) { d.Pending[0].Object = other(d.Pending[0].Object) },
		"pending published name": func(d *registerNamesDoc) { d.Pending[0].PublishedName += "_value" },
		"pending docs-ebus name": func(d *registerNamesDoc) { d.Pending[0].DocsEbusName = "other_name" },
		"pending entry removed":  func(d *registerNamesDoc) { d.Pending = d.Pending[1:] },
		"pending entry added": func(d *registerNamesDoc) {
			d.Pending = append(d.Pending, registerNamesPending{Object: s.Object, PublishedName: s.PublishedName, DocsEbusName: s.DocsEbusName})
		},
		"pending entry duplicated": func(d *registerNamesDoc) { d.Pending = append(d.Pending, d.Pending[0]) },
		"renamed field kept in pending": func(d *registerNamesDoc) {
			d.Fields[pending].PublishedName = p.DocsEbusName
		},
	}
	for name, mutate := range mutations {
		doc := clone()
		mutate(&doc)
		if len(registerNamesFieldErrors(doc)) == 0 {
			t.Errorf("field mutation %q not caught", name)
		}
	}

	// The renamed field and its pending entry leave together: the record
	// stays clean.
	renamed := clone()
	renamed.Fields[pending].PublishedName = p.DocsEbusName
	kept := renamed.Pending[:0]
	for _, entry := range renamed.Pending {
		if entry.Object != p.Object || entry.PublishedName != p.PublishedName {
			kept = append(kept, entry)
		}
	}
	renamed.Pending = kept
	if errs := registerNamesFieldErrors(renamed); len(errs) != 0 {
		t.Errorf("renamed field with its pending entry removed reported: %v", errs)
	}

	published := func() map[string]map[string]bool {
		out := map[string]map[string]bool{}
		for _, field := range base.Fields {
			if out[field.Object] == nil {
				out[field.Object] = map[string]bool{}
			}
			out[field.Object][field.PublishedName] = true
		}
		return out
	}
	if errs := registerNamesFieldCoverageErrors(base, published()); len(errs) != 0 {
		t.Fatalf("clean published set reported: %v", errs)
	}
	coverageMutations := map[string]func(map[string]map[string]bool){
		"field added":   func(m map[string]map[string]bool) { m[p.Object]["new_field"] = true },
		"field dropped": func(m map[string]map[string]bool) { delete(m[p.Object], p.PublishedName) },
		"field renamed": func(m map[string]map[string]bool) {
			delete(m[p.Object], p.PublishedName)
			m[p.Object][p.DocsEbusName] = true
		},
		"field moved": func(m map[string]map[string]bool) {
			delete(m[p.Object], p.PublishedName)
			m[other(p.Object)][p.PublishedName] = true
		},
		"object missing": func(m map[string]map[string]bool) { delete(m, s.Object) },
	}
	for name, mutate := range coverageMutations {
		m := published()
		mutate(m)
		if len(registerNamesFieldCoverageErrors(base, m)) == 0 {
			t.Errorf("coverage mutation %q not caught", name)
		}
	}
}

// TestRegisterNamesNoFieldPending requires every single-register zone,
// circuit and hot water field to be published under the docs-ebus name of its
// register, with nothing left pending.
func TestRegisterNamesNoFieldPending(t *testing.T) {
	t.Parallel()
	doc := loadRegisterNamesDoc(t)
	if len(doc.Pending) != 0 {
		t.Errorf("pending lists %d fields; want none", len(doc.Pending))
	}
	for _, field := range doc.Fields {
		if field.Derived == nil && field.PublishedName != field.DocsEbusName {
			t.Errorf("%s.%s: docs-ebus name %s", field.Object, field.PublishedName, field.DocsEbusName)
		}
	}
}

// registerNamesWithPendingField returns a copy of doc in which one
// single-register field that no row records is published under a name other
// than its docs-ebus name and is listed in pending, so the checkers can be
// exercised on a record with a pending field.
func registerNamesWithPendingField(t *testing.T, doc registerNamesDoc) registerNamesDoc {
	t.Helper()
	rows := map[string]bool{}
	for _, row := range doc.Rows {
		if row.Service == "B524" {
			rows[row.Group+"/"+row.Register] = true
		}
	}
	out := doc
	out.Fields = append([]registerNamesField(nil), doc.Fields...)
	out.Pending = append([]registerNamesPending(nil), doc.Pending...)
	for i, field := range out.Fields {
		if field.Derived != nil || field.PublishedName != field.DocsEbusName || rows[field.Group+"/"+field.Register] {
			continue
		}
		out.Fields[i].PublishedName = field.DocsEbusName + "_value"
		out.Pending = append(out.Pending, registerNamesPending{Object: field.Object, PublishedName: field.DocsEbusName + "_value", DocsEbusName: field.DocsEbusName})
		return out
	}
	t.Fatal("fixture needs a single-register field that no row records")
	return out
}

// filterFields returns the fields outside one object.
func filterFields(fields []registerNamesField, object string) []registerNamesField {
	var out []registerNamesField
	for _, field := range fields {
		if field.Object != object {
			out = append(out, field)
		}
	}
	return out
}
