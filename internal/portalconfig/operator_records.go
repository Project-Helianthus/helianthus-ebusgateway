package portalconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"syscall"
	"unicode"
	"unicode/utf8"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/graph"
	"golang.org/x/text/unicode/norm"
)

// OperatorRecordsFormat is the format of the operator-record section.
const OperatorRecordsFormat = "helianthus.portal-config.operator-records/v1"

// OperatorRecordsFile is the section's file inside the store root. It is
// absent until the first non-empty section is accepted, so a stored
// configuration record without operator records loads unchanged.
const OperatorRecordsFile = "portal-config-operator-records.json"

// MaxOperatorRecords bounds the records of one section.
const MaxOperatorRecords = 128

// The three forms of an operator declaration.
const (
	// OperatorFormAnchor declares a resource that only configuration knows,
	// anchored by an operator slot in its home asset.
	OperatorFormAnchor = "anchor"
	// OperatorFormRelation declares a relation instance whose starting end is
	// homed in the record's home asset.
	OperatorFormRelation = "relation"
	// OperatorFormPairing declares two views of one unit, homed in two
	// assets, by giving both one operator-slot anchor key.
	OperatorFormPairing = "pairing"
)

// Statuses of one operator record.
const (
	OperatorRecordPublished = "published"
	OperatorRecordWaiting   = "waiting"
	OperatorRecordRefused   = "refused"
)

// OperatorRecordSet is the operator-record section: every operator
// declaration of the installation, in the operator's order. Its revision is
// the section's own expected revision, independent of the record's.
type OperatorRecordSet struct {
	Format   string           `json:"format"`
	Revision string           `json:"revision"`
	Records  []OperatorRecord `json:"records"`
}

// OperatorRecord is one operator declaration. Form selects the members it
// carries:
//   - anchor: home, resource_id, kind, slot and an optional parent;
//   - relation: home, definition, from, to and basis_class;
//   - pairing: kind, slot, an optional parent and exactly two views.
type OperatorRecord struct {
	Form       string                `json:"form"`
	Home       string                `json:"home,omitempty"`
	ResourceID string                `json:"resource_id,omitempty"`
	Kind       string                `json:"kind,omitempty"`
	Parent     string                `json:"parent,omitempty"`
	Slot       string                `json:"slot,omitempty"`
	Definition *semreg.DefinitionRef `json:"definition,omitempty"`
	From       string                `json:"from,omitempty"`
	To         string                `json:"to,omitempty"`
	BasisClass string                `json:"basis_class,omitempty"`
	Views      []OperatorView        `json:"views,omitempty"`
}

// OperatorView is one view of a pairing: a resource and its home asset.
type OperatorView struct {
	Home       string `json:"home"`
	ResourceID string `json:"resource_id"`
}

// OperatorRecordState is the publication status of one record of a section,
// by its index: published, waiting (with the reason it waits) or refused
// (with the stable error of the refusal).
type OperatorRecordState struct {
	Index  int    `json:"index"`
	Form   string `json:"form"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// Homes returns the home assets the record is published in, in view order.
func (r OperatorRecord) Homes() []string {
	if r.Form != OperatorFormPairing {
		return []string{r.Home}
	}
	homes := make([]string, 0, len(r.Views))
	for _, view := range r.Views {
		homes = append(homes, view.Home)
	}
	return homes
}

// operatorSteps is the closed next-step vocabulary of operator-record
// errors.
var operatorSteps = map[string]string{
	"invalid_record":              "repair structure",
	"unsupported_format":          "use supported format",
	"bounds_exceeded":             "declare fewer records",
	"duplicate_key":               "remove the repeated declaration",
	"relation_definition_unknown": "choose a relation definition of the element catalog",
	"basis_class_not_admitted":    "declare the configuration basis on a definition that admits it",
	"starting_end_invalid":        "choose a starting end of an admitted kind homed in the record's home asset",
	"far_end_invalid":             "choose a far end of an admitted kind",
	"resource_kind_unknown":       "choose a resource kind of the element catalog",
	"anchor_invalid":              "choose a kind and parent an operator slot can anchor",
	"pairing_invalid":             "home the two views in two different assets",
	"catalog_unavailable":         "check the gateway build",
	"revision_conflict":           "reload",
	"revision_exhausted":          "use a future supported format migration",
	"persistence_failed":          "check local storage",
}

func operatorFailure(code, field string) *Error {
	return &Error{Code: code, Field: field, NextStep: operatorSteps[code]}
}

// SafeOperatorRecordError maps any error to the closed operator-record error
// vocabulary. An error outside it becomes persistence_failed on the section.
func SafeOperatorRecordError(err error) *Error {
	var typed *Error
	if errors.As(err, &typed) {
		if step, known := operatorSteps[typed.Code]; known {
			return &Error{Code: typed.Code, Field: typed.Field, NextStep: step}
		}
	}
	return operatorFailure("persistence_failed", "operator_records")
}

// operatorShapes lists the exact members of each form; optional members are
// marked false.
var operatorShapes = map[string]map[string]bool{
	OperatorFormAnchor:   {"form": true, "home": true, "resource_id": true, "kind": true, "slot": true, "parent": false},
	OperatorFormRelation: {"form": true, "home": true, "definition": true, "from": true, "to": true, "basis_class": true},
	OperatorFormPairing:  {"form": true, "kind": true, "slot": true, "views": true, "parent": false},
}

// DecodeOperatorRecords accepts only the bounded, closed section shape,
// including duplicate-member rejection. It checks structure, format and
// revision; ValidateOperatorRecords checks the records against the catalog.
func DecodeOperatorRecords(data []byte) (OperatorRecordSet, error) {
	whole := func() (OperatorRecordSet, error) {
		return OperatorRecordSet{}, operatorFailure("invalid_record", "operator_records")
	}
	if len(data) > MaxBytes || !utf8.Valid(data) {
		return whole()
	}
	v, err := strictJSON(data)
	if err != nil {
		return whole()
	}
	top, ok := v.(map[string]any)
	if !ok || !hasExactly(top, map[string]bool{"format": true, "revision": true, "records": true}) {
		return whole()
	}
	records, ok := top["records"].([]any)
	if !ok {
		return whole()
	}
	if len(records) > MaxOperatorRecords {
		return OperatorRecordSet{}, operatorFailure("bounds_exceeded", "records")
	}
	for i, item := range records {
		field := fmt.Sprintf("records[%d]", i)
		record, ok := item.(map[string]any)
		if !ok {
			return OperatorRecordSet{}, operatorFailure("invalid_record", field)
		}
		form, _ := record["form"].(string)
		shape, known := operatorShapes[form]
		if !known {
			return OperatorRecordSet{}, operatorFailure("invalid_record", field+".form")
		}
		if !hasExactly(record, shape) {
			return OperatorRecordSet{}, operatorFailure("invalid_record", field)
		}
		if definition, present := record["definition"]; present {
			ref, ok := definition.(map[string]any)
			pack, packOK := ref["pack"].(map[string]any)
			if !ok || !packOK || !hasExactly(ref, map[string]bool{"pack": true, "id": true, "version": true}) || !hasExactly(pack, map[string]bool{"id": true, "version": true}) {
				return OperatorRecordSet{}, operatorFailure("invalid_record", field+".definition")
			}
		}
		if views, present := record["views"]; present {
			list, ok := views.([]any)
			if !ok || len(list) != 2 {
				return OperatorRecordSet{}, operatorFailure("invalid_record", field+".views")
			}
			for j, view := range list {
				object, ok := view.(map[string]any)
				if !ok || !hasExactly(object, map[string]bool{"home": true, "resource_id": true}) {
					return OperatorRecordSet{}, operatorFailure("invalid_record", fmt.Sprintf("%s.views[%d]", field, j))
				}
			}
		}
	}
	var set OperatorRecordSet
	if json.Unmarshal(data, &set) != nil {
		return whole()
	}
	if set.Format != OperatorRecordsFormat {
		return OperatorRecordSet{}, operatorFailure("unsupported_format", "format")
	}
	n, err := strconv.ParseUint(set.Revision, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != set.Revision {
		return OperatorRecordSet{}, operatorFailure("invalid_record", "revision")
	}
	if set.Records == nil {
		set.Records = []OperatorRecord{}
	}
	return set, nil
}

// strictJSON decodes one JSON value of bounded depth, refusing null values,
// duplicate members and trailing data.
func strictJSON(data []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	invalid := operatorFailure("invalid_record", "operator_records")
	var value func(int) (any, error)
	value = func(depth int) (any, error) {
		if depth > 6 {
			return nil, invalid
		}
		t, err := d.Token()
		if err != nil || t == nil {
			return nil, invalid
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return t, nil
		}
		switch delim {
		case '{':
			m := map[string]any{}
			for d.More() {
				k, err := d.Token()
				key, ok := k.(string)
				if err != nil || !ok {
					return nil, invalid
				}
				if _, seen := m[key]; seen {
					return nil, invalid
				}
				v, err := value(depth + 1)
				if err != nil {
					return nil, err
				}
				m[key] = v
			}
			_, err = d.Token()
			return m, err
		case '[':
			a := []any{}
			for d.More() {
				v, err := value(depth + 1)
				if err != nil {
					return nil, err
				}
				a = append(a, v)
			}
			_, err = d.Token()
			return a, err
		}
		return nil, invalid
	}
	v, err := value(0)
	if err != nil {
		return nil, invalid
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, invalid
	}
	return v, nil
}

// hasExactly reports whether m has every required member of shape and no
// member outside it.
func hasExactly(m map[string]any, shape map[string]bool) bool {
	for key, required := range shape {
		if _, ok := m[key]; required && !ok {
			return false
		}
	}
	for key := range m {
		if _, ok := shape[key]; !ok {
			return false
		}
	}
	return true
}

// operatorCatalog is the element catalog the records are validated against.
// It is the catalog compiled into the SemReg graph package.
func operatorCatalog() (semreg.GraphResourceCatalog, error) {
	catalog, err := graph.Embedded()
	if err != nil || catalog == nil {
		return nil, operatorFailure("catalog_unavailable", "operator_records")
	}
	return catalog, nil
}

// operatorResourceClass returns the anchor class of a member resource kind.
func operatorResourceClass(catalog semreg.GraphResourceCatalog, kind string) (semreg.AnchorClass, bool) {
	id := semreg.DefinitionID(kind)
	if id.Validate() != nil {
		return "", false
	}
	membership, ok := catalog.ResourceKindMembership(id)
	if !ok || membership.Disposition != semreg.GraphMember {
		return "", false
	}
	return catalog.ResourceKindClass(id)
}

// OperatorResourceClass returns the anchor class the element catalog
// declares for a member resource kind.
func OperatorResourceClass(kind string) (semreg.AnchorClass, bool) {
	catalog, err := operatorCatalog()
	if err != nil {
		return "", false
	}
	return operatorResourceClass(catalog, kind)
}

func validSlot(slot string) bool {
	if !utf8.ValidString(slot) || !norm.NFC.IsNormalString(slot) || len(slot) < 1 || len(slot) > 128 {
		return false
	}
	for _, r := range slot {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// declaredResource is one resource the section declares, with its home.
type declaredResource struct {
	home string
	kind string
}

// ValidateOperatorRecords checks a decoded section against the element
// catalog: identifiers, member resource kinds and their anchor classes,
// member relation definitions and the basis class each admits, the kinds and
// homes of declared relation ends, and duplicate declarations. The first
// failure in record order is returned with the field that caused it.
func ValidateOperatorRecords(set OperatorRecordSet) error {
	catalog, err := operatorCatalog()
	if err != nil {
		return err
	}
	if len(set.Records) > MaxOperatorRecords {
		return operatorFailure("bounds_exceeded", "records")
	}
	definitions := make([]semreg.GraphRelationDefinition, len(set.Records))
	for i, record := range set.Records {
		field := fmt.Sprintf("records[%d]", i)
		switch record.Form {
		case OperatorFormAnchor:
			if semreg.AssetID(record.Home).Validate() != nil {
				return operatorFailure("invalid_record", field+".home")
			}
			if semreg.ResourceID(record.ResourceID).Validate() != nil {
				return operatorFailure("invalid_record", field+".resource_id")
			}
			if err := validateOperatorAnchor(catalog, record, field); err != nil {
				return err
			}
		case OperatorFormPairing:
			if err := validateOperatorAnchor(catalog, record, field); err != nil {
				return err
			}
			if len(record.Views) != 2 {
				return operatorFailure("invalid_record", field+".views")
			}
			for j, view := range record.Views {
				viewField := fmt.Sprintf("%s.views[%d]", field, j)
				if semreg.AssetID(view.Home).Validate() != nil {
					return operatorFailure("invalid_record", viewField+".home")
				}
				if semreg.ResourceID(view.ResourceID).Validate() != nil {
					return operatorFailure("invalid_record", viewField+".resource_id")
				}
			}
			if record.Views[0].Home == record.Views[1].Home {
				return operatorFailure("pairing_invalid", field+".views[1].home")
			}
			if record.Views[0].ResourceID == record.Views[1].ResourceID {
				return operatorFailure("duplicate_key", field+".views[1].resource_id")
			}
		case OperatorFormRelation:
			if semreg.AssetID(record.Home).Validate() != nil {
				return operatorFailure("invalid_record", field+".home")
			}
			if record.Definition == nil || record.Definition.Validate() != nil {
				return operatorFailure("relation_definition_unknown", field+".definition")
			}
			definition, ok := catalog.RelationDefinition(*record.Definition)
			if !ok || definition.Membership.Disposition != semreg.GraphMember {
				return operatorFailure("relation_definition_unknown", field+".definition")
			}
			// An operator origin agrees only with the configuration basis,
			// and only where the definition admits it.
			if record.BasisClass != string(semreg.RelationConfiguration) || !slices.Contains(definition.BasisClasses, semreg.RelationConfiguration) {
				return operatorFailure("basis_class_not_admitted", field+".basis_class")
			}
			if semreg.ResourceID(record.From).Validate() != nil {
				return operatorFailure("starting_end_invalid", field+".from")
			}
			if semreg.ResourceID(record.To).Validate() != nil {
				return operatorFailure("far_end_invalid", field+".to")
			}
			definitions[i] = definition
		default:
			return operatorFailure("invalid_record", field+".form")
		}
	}

	declared := map[string]declaredResource{}
	anchors := map[string]bool{}
	declare := func(field, home, id string, record OperatorRecord) error {
		if _, seen := declared[id]; seen {
			return operatorFailure("duplicate_key", field+".resource_id")
		}
		declared[id] = declaredResource{home: home, kind: record.Kind}
		key := home + "\x00" + record.Kind + "\x00" + record.Parent + "\x00" + record.Slot
		if anchors[key] {
			return operatorFailure("duplicate_key", field+".slot")
		}
		anchors[key] = true
		return nil
	}
	for i, record := range set.Records {
		field := fmt.Sprintf("records[%d]", i)
		switch record.Form {
		case OperatorFormAnchor:
			if err := declare(field, record.Home, record.ResourceID, record); err != nil {
				return err
			}
		case OperatorFormPairing:
			for j, view := range record.Views {
				if err := declare(fmt.Sprintf("%s.views[%d]", field, j), view.Home, view.ResourceID, record); err != nil {
					return err
				}
			}
		}
	}
	relations := map[string]bool{}
	for i, record := range set.Records {
		if record.Form != OperatorFormRelation {
			continue
		}
		field := fmt.Sprintf("records[%d]", i)
		canonical, err := semreg.CanonicalJSON(*record.Definition)
		if err != nil {
			return operatorFailure("relation_definition_unknown", field+".definition")
		}
		key := string(canonical) + "\x00" + record.From + "\x00" + record.To
		if relations[key] {
			return operatorFailure("duplicate_key", field)
		}
		relations[key] = true
		definition := definitions[i]
		if from, ok := declared[record.From]; ok && (from.home != record.Home || !slices.Contains(definition.FromKinds, semreg.DefinitionID(from.kind))) {
			return operatorFailure("starting_end_invalid", field+".from")
		}
		if to, ok := declared[record.To]; ok && !slices.Contains(definition.ToKinds, semreg.DefinitionID(to.kind)) {
			return operatorFailure("far_end_invalid", field+".to")
		}
	}
	return nil
}

// validateOperatorAnchor checks the kind, parent and slot of an operator-slot
// anchor. A component needs its parent; an episode is never anchored by an
// operator slot.
func validateOperatorAnchor(catalog semreg.GraphResourceCatalog, record OperatorRecord, field string) error {
	class, ok := operatorResourceClass(catalog, record.Kind)
	if !ok {
		return operatorFailure("resource_kind_unknown", field+".kind")
	}
	if class == semreg.AnchorEpisode {
		return operatorFailure("anchor_invalid", field+".kind")
	}
	if record.Parent != "" && semreg.ResourceID(record.Parent).Validate() != nil {
		return operatorFailure("invalid_record", field+".parent")
	}
	if class == semreg.AnchorComponent && record.Parent == "" {
		return operatorFailure("anchor_invalid", field+".parent")
	}
	if !validSlot(record.Slot) {
		return operatorFailure("invalid_record", field+".slot")
	}
	return nil
}

// normalizeOperatorRecords round-trips a section through its closed shape and
// validates it against the catalog.
func normalizeOperatorRecords(set OperatorRecordSet) (OperatorRecordSet, error) {
	if set.Records == nil {
		set.Records = []OperatorRecord{}
	}
	b, err := json.Marshal(set)
	if err != nil {
		return OperatorRecordSet{}, operatorFailure("invalid_record", "operator_records")
	}
	set, err = DecodeOperatorRecords(b)
	if err != nil {
		return OperatorRecordSet{}, err
	}
	if err = ValidateOperatorRecords(set); err != nil {
		return OperatorRecordSet{}, err
	}
	return set, nil
}

func emptyOperatorRecords() OperatorRecordSet {
	return OperatorRecordSet{Format: OperatorRecordsFormat, Revision: "0", Records: []OperatorRecord{}}
}

func cloneOperatorRecords(set OperatorRecordSet) OperatorRecordSet {
	b, err := json.Marshal(set)
	if err != nil {
		return emptyOperatorRecords()
	}
	var out OperatorRecordSet
	if json.Unmarshal(b, &out) != nil {
		return emptyOperatorRecords()
	}
	if out.Records == nil {
		out.Records = []OperatorRecord{}
	}
	return out
}

// OperatorRecords returns the accepted operator-record section, or the empty
// section at revision 0 when none was ever accepted. A stored section that
// does not decode or validate fails closed.
func (s *Store) OperatorRecords() (OperatorRecordSet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return emptyOperatorRecords(), err
	}
	return s.readOperatorRecordsLocked()
}

// ReplaceOperatorRecords replaces the whole section. The set's revision is
// the expected revision of the accepted section, never its successor. The
// set is validated against the element catalog before anything is written;
// an unchanged set keeps its revision. Every write uses the store's
// exclusive writer and its atomic replacement, and leaves the configuration
// record untouched.
func (s *Store) ReplaceOperatorRecords(set OperatorRecordSet) (OperatorRecordSet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return emptyOperatorRecords(), err
	}
	current, err := s.readOperatorRecordsLocked()
	if err != nil {
		return emptyOperatorRecords(), err
	}
	set, err = normalizeOperatorRecords(set)
	if err != nil {
		return current, err
	}
	if set.Revision != current.Revision {
		return current, operatorFailure("revision_conflict", "revision")
	}
	if reflect.DeepEqual(set.Records, current.Records) {
		return current, nil
	}
	n, _ := strconv.ParseUint(current.Revision, 10, 64)
	if n == math.MaxUint64 {
		return current, operatorFailure("revision_exhausted", "revision")
	}
	set.Revision = strconv.FormatUint(n+1, 10)
	b, err := json.Marshal(set)
	if err != nil {
		return current, operatorFailure("invalid_record", "operator_records")
	}
	if err = s.persistOperatorRecordsLocked(b); err != nil {
		return current, err
	}
	return cloneOperatorRecords(set), nil
}

func (s *Store) readOperatorRecordsLocked() (OperatorRecordSet, error) {
	if s.lock == nil {
		return emptyOperatorRecords(), operatorFailure("persistence_failed", "operator_records")
	}
	f, err := os.OpenFile(filepath.Join(s.root, OperatorRecordsFile), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if os.IsNotExist(err) {
		return emptyOperatorRecords(), nil
	}
	if err != nil {
		return emptyOperatorRecords(), operatorFailure("persistence_failed", "operator_records")
	}
	// Closing a read-only descriptor cannot change the bytes or read error.
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return emptyOperatorRecords(), operatorFailure("persistence_failed", "operator_records")
	}
	data, err := io.ReadAll(io.LimitReader(f, MaxBytes+1))
	if err != nil {
		return emptyOperatorRecords(), operatorFailure("persistence_failed", "operator_records")
	}
	set, err := DecodeOperatorRecords(data)
	if err != nil {
		return emptyOperatorRecords(), err
	}
	if err = ValidateOperatorRecords(set); err != nil {
		return emptyOperatorRecords(), err
	}
	return set, nil
}

// persistOperatorRecordsLocked replaces the section file atomically. A
// directory sync that fails after the rename leaves durability uncertain, so
// the store refuses every later read and write instead of presenting a
// revision that may not survive.
func (s *Store) persistOperatorRecordsLocked(b []byte) error {
	fail := func() error { return operatorFailure("persistence_failed", "operator_records") }
	if s.stage("create") != nil {
		return fail()
	}
	f, err := os.CreateTemp(s.root, ".portal-config-operator-records-*")
	if err != nil {
		return fail()
	}
	// Cleanup is best effort: incomplete temporary files are ignored on load,
	// and a successful rename has already removed this temporary path.
	defer func() { _ = os.Remove(f.Name()) }()
	if s.stage("write") != nil {
		_ = f.Close()
		return fail()
	}
	if _, err = f.Write(b); err != nil {
		_ = f.Close()
		return fail()
	}
	if s.stage("file_sync") != nil {
		_ = f.Close()
		return fail()
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return fail()
	}
	if s.stage("close") != nil {
		_ = f.Close()
		return fail()
	}
	if err = f.Close(); err != nil {
		return fail()
	}
	if s.stage("rename") != nil {
		return fail()
	}
	if err = os.Rename(f.Name(), filepath.Join(s.root, OperatorRecordsFile)); err != nil {
		return fail()
	}
	if s.stage("directory_sync") != nil || s.syncDir() != nil {
		s.blocked = failure("persistence_failed")
		return fail()
	}
	return nil
}
