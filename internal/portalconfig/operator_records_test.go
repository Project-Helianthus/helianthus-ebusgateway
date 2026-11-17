package portalconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// The operator-record fixtures are synthetic: a PV installation with one
// inverter homed in a driver asset and configuration-only resources homed in
// assets of the configuration provider.
const (
	oprHome     = "asset:portal-opr-home"
	oprOther    = "asset:portal-opr-other"
	oprInverter = "asset:portal-opr-inverter"
)

func oprDefinition(id string) *semreg.DefinitionRef {
	return &semreg.DefinitionRef{Pack: semreg.PackRef{ID: "helianthus.pack.pv", Version: "1.0.0"}, ID: semreg.DefinitionID(id), Version: "1.0.0"}
}

func oprAnchor(home, id, kind, slot string) OperatorRecord {
	return OperatorRecord{Form: OperatorFormAnchor, Home: home, ResourceID: id, Kind: kind, Slot: slot}
}

func oprRelation(home, definition, from, to string) OperatorRecord {
	return OperatorRecord{Form: OperatorFormRelation, Home: home, Definition: oprDefinition(definition), From: from, To: to, BasisClass: string(semreg.RelationConfiguration)}
}

// oprOneOfEach is a record set with one record of each form: an operator-slot
// anchor, a configured relation from the driver's inverter to that anchored
// array, and a pairing of two views of one PV system.
func oprOneOfEach() OperatorRecordSet {
	return OperatorRecordSet{Format: OperatorRecordsFormat, Revision: "0", Records: []OperatorRecord{
		oprAnchor(oprHome, "resource:portal-opr:array-1", "resource.pv_array", "array-south"),
		oprRelation(oprInverter, "pv.relation.inverter_array", "resource:portal-opr:inverter-1", "resource:portal-opr:array-1"),
		{Form: OperatorFormPairing, Kind: "resource.pv_system", Slot: "system-main", Views: []OperatorView{
			{Home: oprHome, ResourceID: "resource:portal-opr:system-a"},
			{Home: oprOther, ResourceID: "resource:portal-opr:system-b"},
		}},
	}}
}

func requireField(t *testing.T, e error, code, field string) {
	t.Helper()
	var typed *Error
	if !errors.As(e, &typed) || typed.Code != code || typed.Field != field {
		t.Fatalf("error %#v, want %s at %s", e, code, field)
	}
	if typed.NextStep == "" {
		t.Fatalf("error %s at %s has no next step", code, field)
	}
}

func requireNoSection(t *testing.T, s *Store, root string) {
	t.Helper()
	set, e := s.OperatorRecords()
	if e != nil || set.Revision != "0" || len(set.Records) != 0 || set.Format != OperatorRecordsFormat {
		t.Fatalf("section %+v %v, want the empty section at revision 0", set, e)
	}
	if _, e := os.Lstat(filepath.Join(root, OperatorRecordsFile)); !os.IsNotExist(e) {
		t.Fatalf("section file present: %v", e)
	}
}

func TestOperatorRecordSetAcceptsOneOfEachForm(t *testing.T) {
	s, root := openTest(t)
	before, e := os.ReadFile(filepath.Join(root, "portal-config.json"))
	if e != nil {
		t.Fatal(e)
	}
	saved, e := s.ReplaceOperatorRecords(oprOneOfEach())
	if e != nil {
		t.Fatal(e)
	}
	want := oprOneOfEach()
	want.Revision = "1"
	if !reflect.DeepEqual(saved, want) {
		t.Fatalf("saved %+v, want %+v", saved, want)
	}
	read, e := s.OperatorRecords()
	if e != nil || !reflect.DeepEqual(read, want) {
		t.Fatalf("read %+v %v", read, e)
	}
	for i, homes := range [][]string{{oprHome}, {oprInverter}, {oprHome, oprOther}} {
		if got := read.Records[i].Homes(); !reflect.DeepEqual(got, homes) {
			t.Fatalf("record %d homes %v, want %v", i, got, homes)
		}
	}
	after, e := os.ReadFile(filepath.Join(root, "portal-config.json"))
	if e != nil || !bytes.Equal(before, after) {
		t.Fatalf("the configuration record changed: %v", e)
	}
	// An unchanged set at the current revision is accepted without a new revision.
	again, e := s.ReplaceOperatorRecords(read)
	if e != nil || again.Revision != "1" {
		t.Fatalf("unchanged replace %+v %v", again, e)
	}
	// A stale revision is a conflict and changes nothing.
	stale := oprOneOfEach()
	stale.Records = stale.Records[:1]
	_, e = s.ReplaceOperatorRecords(stale)
	requireCode(t, e, "revision_conflict")
	// A withdrawal of every record keeps the section and advances its revision.
	empty := OperatorRecordSet{Format: OperatorRecordsFormat, Revision: "1", Records: []OperatorRecord{}}
	cleared, e := s.ReplaceOperatorRecords(empty)
	if e != nil || cleared.Revision != "2" || len(cleared.Records) != 0 {
		t.Fatalf("cleared %+v %v", cleared, e)
	}
	// The section survives a reopen.
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	reopened, e := Open(root, initial(), constraints())
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = reopened.Close() }()
	if read, e = reopened.OperatorRecords(); e != nil || read.Revision != "2" || len(read.Records) != 0 {
		t.Fatalf("reopened %+v %v", read, e)
	}
}

// Every refusal names the field that caused it and leaves the stored section
// as it was.
func TestOperatorRecordRefusalsCarryAFieldError(t *testing.T) {
	for _, c := range []struct {
		name, code, field string
		edit              func(*OperatorRecordSet)
	}{
		{"starting end homed in another asset", "starting_end_invalid", "records[3].from", func(s *OperatorRecordSet) {
			s.Records = append(s.Records, oprRelation(oprInverter, "pv.relation.array_string", "resource:portal-opr:array-1", "resource:portal-opr:string-1"))
		}},
		{"starting end of a kind the definition does not admit", "starting_end_invalid", "records[3].from", func(s *OperatorRecordSet) {
			s.Records = append(s.Records, oprRelation(oprHome, "pv.relation.inverter_array", "resource:portal-opr:array-1", "resource:portal-opr:array-9"))
		}},
		{"starting end that is not a resource identifier", "starting_end_invalid", "records[1].from", func(s *OperatorRecordSet) { s.Records[1].From = " inverter-1" }},
		{"far end of a kind the definition does not admit", "far_end_invalid", "records[3].to", func(s *OperatorRecordSet) {
			s.Records = append(s.Records, oprRelation(oprInverter, "pv.relation.inverter_array", "resource:portal-opr:inverter-1", "resource:portal-opr:system-a"))
		}},
		{"unknown relation definition", "relation_definition_unknown", "records[1].definition", func(s *OperatorRecordSet) {
			s.Records[1].Definition = oprDefinition("pv.relation.inverter_meter")
		}},
		{"relation definition of another version", "relation_definition_unknown", "records[1].definition", func(s *OperatorRecordSet) {
			s.Records[1].Definition.Version = "2.0.0"
		}},
		{"relation definition of another pack", "relation_definition_unknown", "records[1].definition", func(s *OperatorRecordSet) {
			s.Records[1].Definition.Pack = semreg.PackRef{ID: "helianthus.pack.storage", Version: "1.1.0"}
		}},
		{"basis class the definition does not admit", "basis_class_not_admitted", "records[1].basis_class", func(s *OperatorRecordSet) {
			s.Records[1].Definition = oprDefinition("pv.relation.inverter_input")
			s.Records[1].To = "resource:portal-opr:input-1"
		}},
		{"native basis class on a definition that admits only configuration", "basis_class_not_admitted", "records[1].basis_class", func(s *OperatorRecordSet) {
			s.Records[1].BasisClass = string(semreg.RelationGateRule)
		}},
		{"native basis class on a definition that admits it", "basis_class_not_admitted", "records[1].basis_class", func(s *OperatorRecordSet) {
			s.Records[1].Definition = oprDefinition("pv.relation.system_inverter")
			s.Records[1].From, s.Records[1].To = "resource:portal-opr:system-9", "resource:portal-opr:inverter-1"
			s.Records[1].BasisClass = string(semreg.RelationGateRule)
		}},
		{"duplicate relation", "duplicate_key", "records[3]", func(s *OperatorRecordSet) { s.Records = append(s.Records, s.Records[1]) }},
		{"duplicate resource of an anchor and a view", "duplicate_key", "records[3].resource_id", func(s *OperatorRecordSet) {
			s.Records = append(s.Records, oprAnchor(oprOther, "resource:portal-opr:system-a", "resource.pv_array", "array-north"))
		}},
		{"duplicate anchor key", "duplicate_key", "records[3].slot", func(s *OperatorRecordSet) {
			s.Records = append(s.Records, oprAnchor(oprHome, "resource:portal-opr:array-2", "resource.pv_array", "array-south"))
		}},
		{"duplicate view of a pairing", "duplicate_key", "records[2].views[1].resource_id", func(s *OperatorRecordSet) {
			s.Records[2].Views[1].ResourceID = s.Records[2].Views[0].ResourceID
		}},
		{"unknown resource kind", "resource_kind_unknown", "records[0].kind", func(s *OperatorRecordSet) { s.Records[0].Kind = "resource.solar_canopy" }},
		{"component without a parent", "anchor_invalid", "records[0].parent", func(s *OperatorRecordSet) { s.Records[0].Kind = "resource.pv_string" }},
		{"episode on an operator slot", "anchor_invalid", "records[0].kind", func(s *OperatorRecordSet) {
			s.Records[0].Kind, s.Records[0].Parent = "resource.charging_session", "resource:portal-opr:connector-1"
		}},
		{"pairing of two views in one home", "pairing_invalid", "records[2].views[1].home", func(s *OperatorRecordSet) { s.Records[2].Views[1].Home = oprHome }},
		{"slot over its bound", "invalid_record", "records[0].slot", func(s *OperatorRecordSet) { s.Records[0].Slot = strings.Repeat("s", 129) }},
		{"slot with a control character", "invalid_record", "records[0].slot", func(s *OperatorRecordSet) { s.Records[0].Slot = "array\u0007south" }},
		{"home that is not an asset identifier", "invalid_record", "records[0].home", func(s *OperatorRecordSet) { s.Records[0].Home = "asset home" }},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, root := openTest(t)
			set := oprOneOfEach()
			c.edit(&set)
			current, e := s.ReplaceOperatorRecords(set)
			requireField(t, e, c.code, c.field)
			if current.Revision != "0" || len(current.Records) != 0 {
				t.Fatalf("refusal returned %+v, want the unchanged section", current)
			}
			requireNoSection(t, s, root)
		})
	}
}

// dispositionCatalog is the embedded element catalog with the disposition of
// one resource kind replaced. The pinned catalog lists every resource kind as
// a member, so a listed kind that is not a member exists only through it.
type dispositionCatalog struct {
	semreg.GraphResourceCatalog
	kind        semreg.DefinitionID
	disposition semreg.GraphDisposition
}

func (c dispositionCatalog) ResourceKindMembership(kind semreg.DefinitionID) (semreg.GraphMembership, bool) {
	membership, ok := c.GraphResourceCatalog.ResourceKindMembership(kind)
	if ok && kind == c.kind {
		membership.Disposition = c.disposition
	}
	return membership, ok
}

// A resource kind the catalog lists without the member disposition is
// refused like an unknown kind. The disposition varies over every non-member
// value; the member disposition of the same kind is the control, so the test
// fails when the membership check is dropped from the anchor rule.
func TestOperatorAnchorRefusesAListedResourceKindThatIsNotAMember(t *testing.T) {
	catalog, err := operatorCatalog()
	if err != nil {
		t.Fatal(err)
	}
	record := oprOneOfEach().Records[0]
	kind := semreg.DefinitionID(record.Kind)
	membership, ok := catalog.ResourceKindMembership(kind)
	if !ok || membership.Disposition != semreg.GraphMember {
		t.Fatalf("fixture kind %s is not a listed member: %+v, %v", kind, membership, ok)
	}
	if err := validateOperatorAnchor(dispositionCatalog{catalog, kind, semreg.GraphMember}, record, "records[0]"); err != nil {
		t.Fatalf("member kind refused: %v", err)
	}
	for _, disposition := range []semreg.GraphDisposition{semreg.GraphUnsupported, semreg.GraphUnknown} {
		t.Run(string(disposition), func(t *testing.T) {
			wrapped := dispositionCatalog{catalog, kind, disposition}
			if _, listed := wrapped.ResourceKindMembership(kind); !listed {
				t.Fatalf("kind %s is no longer listed", kind)
			}
			requireField(t, validateOperatorAnchor(wrapped, record, "records[0]"), "resource_kind_unknown", "records[0].kind")
		})
	}
}

// Each duplicate rule is keyed by exactly its fields: a copy that differs in
// any one of them is accepted, so dropping a field from a key fails here.
func TestOperatorRecordDuplicateKeysVaryEveryField(t *testing.T) {
	relation := oprRelation(oprInverter, "pv.relation.inverter_array", "resource:portal-opr:inverter-1", "resource:portal-opr:array-9")
	anchor := oprAnchor(oprHome, "resource:portal-opr:array-1", "resource.pv_array", "array-south")
	for _, c := range []struct {
		name       string
		base, copy OperatorRecord
		duplicate  string
	}{
		// Relation key (definition, from, to).
		{"relation unchanged", relation, relation, "records[1]"},
		{"relation definition", relation, func() OperatorRecord {
			r := relation
			r.Definition = oprDefinition("pv.relation.system_inverter")
			return r
		}(), ""},
		{"relation from", relation, func() OperatorRecord { r := relation; r.From = "resource:portal-opr:inverter-2"; return r }(), ""},
		{"relation to", relation, func() OperatorRecord { r := relation; r.To = "resource:portal-opr:array-8"; return r }(), ""},
		// Resource key: one identifier is declared once, whatever its anchor.
		{"resource identifier under another anchor", anchor, oprAnchor(oprOther, anchor.ResourceID, "resource.pv_system", "system-east"), "records[1].resource_id"},
		{"resource identifier", anchor, oprAnchor(oprOther, "resource:portal-opr:array-2", "resource.pv_system", "system-east"), ""},
		// Anchor key (home, kind, parent, slot) of two resources.
		{"anchor key unchanged", anchor, oprAnchor(oprHome, "resource:portal-opr:array-2", "resource.pv_array", "array-south"), "records[1].slot"},
		{"anchor home", anchor, oprAnchor(oprOther, "resource:portal-opr:array-2", "resource.pv_array", "array-south"), ""},
		{"anchor kind", anchor, oprAnchor(oprHome, "resource:portal-opr:array-2", "resource.pv_system", "array-south"), ""},
		{"anchor parent", anchor, func() OperatorRecord {
			r := oprAnchor(oprHome, "resource:portal-opr:array-2", "resource.pv_array", "array-south")
			r.Parent = "resource:portal-opr:site-1"
			return r
		}(), ""},
		{"anchor slot", anchor, oprAnchor(oprHome, "resource:portal-opr:array-2", "resource.pv_array", "array-north"), ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, _ := openTest(t)
			set := OperatorRecordSet{Format: OperatorRecordsFormat, Revision: "0", Records: []OperatorRecord{c.base, c.copy}}
			_, e := s.ReplaceOperatorRecords(set)
			if c.duplicate == "" {
				if e != nil {
					t.Fatalf("distinct records refused: %v", e)
				}
				return
			}
			requireField(t, e, "duplicate_key", c.duplicate)
		})
	}
}

// The section is optional: a stored record written without it loads
// unchanged, an empty section is never written, and the record and the
// section keep independent revisions.
func TestOperatorRecordSectionLeavesStoredDocumentsUnchanged(t *testing.T) {
	root := t.TempDir()
	legacy := []byte(`{"format":"helianthus.portal-config/v1","revision":"4","drivers":[{"driver_id":"family.primary","enabled":true}],"sharing":{"graphql":true,"mcp":false},"adapter_proxy":false,"output_bindings":{"matter":false}}`)
	path := filepath.Join(root, "portal-config.json")
	if e := os.WriteFile(path, legacy, 0o600); e != nil {
		t.Fatal(e)
	}
	s, e := Open(root, initial(), constraints())
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = s.Close() }()
	record, e := s.Read()
	if e != nil || record.Revision != "4" || !record.Sharing.GraphQL {
		t.Fatalf("legacy record %+v %v", record, e)
	}
	requireNoSection(t, s, root)
	if _, e = s.ReplaceOperatorRecords(OperatorRecordSet{Format: OperatorRecordsFormat, Revision: "0", Records: []OperatorRecord{}}); e != nil {
		t.Fatal(e)
	}
	requireNoSection(t, s, root)
	if _, e = s.ReplaceOperatorRecords(oprOneOfEach()); e != nil {
		t.Fatal(e)
	}
	if stored, e := os.ReadFile(path); e != nil || !bytes.Equal(stored, legacy) {
		t.Fatalf("the stored record changed: %s %v", stored, e)
	}
	record.Sharing.GraphQL = false
	if record, e = s.Update(record); e != nil || record.Revision != "5" {
		t.Fatalf("record update after a section replace %+v %v", record, e)
	}
	if set, e := s.OperatorRecords(); e != nil || set.Revision != "1" || len(set.Records) != 3 {
		t.Fatalf("section after a record update %+v %v", set, e)
	}
}

func TestOperatorRecordDecodeIsClosedAndBounded(t *testing.T) {
	valid, e := json.Marshal(oprOneOfEach())
	if e != nil {
		t.Fatal(e)
	}
	set, e := DecodeOperatorRecords(valid)
	if e != nil || !reflect.DeepEqual(set, oprOneOfEach()) {
		t.Fatalf("decode %+v %v", set, e)
	}
	many := oprOneOfEach()
	for len(many.Records) <= MaxOperatorRecords {
		many.Records = append(many.Records, many.Records[0])
	}
	manyBytes, _ := json.Marshal(many)
	for _, c := range []struct {
		name, body, code, field string
	}{
		{"unknown section member", strings.Replace(string(valid), `"records":`, `"private_marker":1,"records":`, 1), "invalid_record", "operator_records"},
		{"duplicate section member", strings.Replace(string(valid), `"revision":"0"`, `"revision":"0","revision":"0"`, 1), "invalid_record", "operator_records"},
		{"null member", strings.Replace(string(valid), `"revision":"0"`, `"revision":null`, 1), "invalid_record", "operator_records"},
		{"member of another form", strings.Replace(string(valid), `"slot":"array-south"`, `"slot":"array-south","from":"resource:portal-opr:inverter-1"`, 1), "invalid_record", "records[0]"},
		{"missing member", strings.Replace(string(valid), `,"slot":"array-south"`, ``, 1), "invalid_record", "records[0]"},
		{"unknown form", strings.Replace(string(valid), `"form":"anchor"`, `"form":"alias"`, 1), "invalid_record", "records[0].form"},
		{"pairing of one view", strings.Replace(string(valid), `,{"home":"asset:portal-opr-other","resource_id":"resource:portal-opr:system-b"}`, ``, 1), "invalid_record", "records[2].views"},
		{"other format", strings.Replace(string(valid), OperatorRecordsFormat, "helianthus.portal-config.operator-records/v0", 1), "unsupported_format", "format"},
		{"non-canonical revision", strings.Replace(string(valid), `"revision":"0"`, `"revision":"00"`, 1), "invalid_record", "revision"},
		{"too many records", string(manyBytes), "bounds_exceeded", "records"},
		{"oversized document", `{"format":"` + strings.Repeat("x", MaxBytes) + `"}`, "invalid_record", "operator_records"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, e := DecodeOperatorRecords([]byte(c.body))
			requireField(t, e, c.code, c.field)
		})
	}
}

// A failed write leaves the accepted section readable and unchanged.
func TestOperatorRecordReplaceFailureKeepsTheAcceptedSection(t *testing.T) {
	s, _ := openTest(t)
	saved, e := s.ReplaceOperatorRecords(oprOneOfEach())
	if e != nil {
		t.Fatal(e)
	}
	next := saved
	next.Records = next.Records[:1]
	for _, stage := range []string{"create", "write", "file_sync", "close", "rename"} {
		s.fault = func(name string) error {
			if name == stage {
				return errors.New("injected")
			}
			return nil
		}
		current, e := s.ReplaceOperatorRecords(next)
		requireCode(t, e, "persistence_failed")
		if !reflect.DeepEqual(current, saved) {
			t.Fatalf("%s: returned %+v, want the accepted section", stage, current)
		}
		s.fault = nil
		if read, e := s.OperatorRecords(); e != nil || !reflect.DeepEqual(read, saved) {
			t.Fatalf("%s: read %+v %v", stage, read, e)
		}
	}
	// A failed directory sync makes durability uncertain: the store refuses
	// further reads and writes instead of presenting an unconfirmed revision.
	s.fault = func(name string) error {
		if name == "directory_sync" {
			return errors.New("injected")
		}
		return nil
	}
	_, e = s.ReplaceOperatorRecords(next)
	requireCode(t, e, "persistence_failed")
	s.fault = nil
	_, e = s.OperatorRecords()
	requireCode(t, e, "persistence_failed")
}
