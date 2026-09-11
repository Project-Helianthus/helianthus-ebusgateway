package contributionv1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

var renderers = map[string]bool{"summary": true, "field_table": true, "relationship_graph": true, "state_strip": true, "timeline": true, "evidence_table": true, "session_controls": true}
var slots = map[string]bool{"lens": true, "native_diagnostics": true}
var diagnosticKinds = map[string]bool{"field": true, "action": true}
var forbiddenMembers = map[string]bool{"script": true, "javascript": true, "html": true, "css": true, "web_component": true, "url": true, "graphql": true, "query": true, "jsonpath": true, "formula": true, "register": true, "opcode": true, "decode": true, "decoder": true, "transport": true, "endpoint": true, "route": true, "retry": true, "authority": true, "precondition": true}

// Decode enforces a closed JSON object before returning the typed manifest.
func Decode(data []byte) (Manifest, error) {
	if len(data) > MaxManifestBytes {
		return Manifest{}, fmt.Errorf("manifest exceeds %d bytes", MaxManifestBytes)
	}
	if !utf8.Valid(data) {
		return Manifest{}, fmt.Errorf("manifest contains invalid UTF-8")
	}
	var raw any
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return Manifest{}, err
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return Manifest{}, fmt.Errorf("invalid JSON: %w", err)
	}
	if _, ok := raw.(map[string]any); !ok {
		return Manifest{}, fmt.Errorf("closed manifest: top-level value must be an object")
	}
	if err := requireManifestMembers(raw.(map[string]any)); err != nil {
		return Manifest{}, err
	}
	if err := rejectForbidden(raw); err != nil {
		return Manifest{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("closed manifest: %w", err)
	}
	if err := ensureEOF(dec); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func requiredObject(value any, path string, names ...string) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("closed manifest: required object %s is absent or not an object", path)
	}
	for _, name := range names {
		if _, ok := object[name]; !ok {
			return nil, fmt.Errorf("closed manifest: missing required member %s.%s", path, name)
		}
	}
	return object, nil
}
func requiredArray(value any, path string) ([]any, error) {
	array, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("closed manifest: required array %s is absent or not an array", path)
	}
	return array, nil
}
func requireRef(value any, path string) error {
	object, err := requiredObject(value, path, "pack", "id", "version")
	if err != nil {
		return err
	}
	_, err = requiredObject(object["pack"], path+".pack", "id", "version")
	return err
}
func requireLabel(value any, path string) error {
	_, err := requiredObject(value, path, "key", "default")
	return err
}
func requireManifestMembers(root map[string]any) error {
	if _, err := requiredObject(root, "manifest", "contract", "manifest_id", "manifest_version", "contributor", "requires", "groups", "fields", "views", "actions", "diagnostics"); err != nil {
		return err
	}
	contributor, err := requiredObject(root["contributor"], "contributor", "driver_id", "native_contract")
	if err != nil {
		return err
	}
	if _, err = requiredObject(contributor["native_contract"], "contributor.native_contract", "owner", "contract", "version"); err != nil {
		return err
	}
	requires, err := requiredObject(root["requires"], "requires", "semantic_kernel", "packs")
	if err != nil {
		return err
	}
	packs, err := requiredArray(requires["packs"], "requires.packs")
	if err != nil {
		return err
	}
	for i, item := range packs {
		if _, err := requiredObject(item, fmt.Sprintf("requires.packs[%d]", i), "id", "version"); err != nil {
			return err
		}
	}
	groups, err := requiredArray(root["groups"], "groups")
	if err != nil {
		return err
	}
	for i, item := range groups {
		object, err := requiredObject(item, fmt.Sprintf("groups[%d]", i), "id", "label", "resource_context", "order")
		if err != nil {
			return err
		}
		if err := requireLabel(object["label"], fmt.Sprintf("groups[%d].label", i)); err != nil {
			return err
		}
	}
	fields, err := requiredArray(root["fields"], "fields")
	if err != nil {
		return err
	}
	for i, item := range fields {
		object, err := requiredObject(item, fmt.Sprintf("fields[%d]", i), "id", "group", "label", "ref", "service_ref", "capability_ref", "unit_ref", "order")
		if err != nil {
			return err
		}
		if err := requireLabel(object["label"], fmt.Sprintf("fields[%d].label", i)); err != nil {
			return err
		}
		for _, name := range []string{"ref", "service_ref", "capability_ref", "unit_ref"} {
			if err := requireRef(object[name], fmt.Sprintf("fields[%d].%s", i, name)); err != nil {
				return err
			}
		}
	}
	views, err := requiredArray(root["views"], "views")
	if err != nil {
		return err
	}
	for i, item := range views {
		object, err := requiredObject(item, fmt.Sprintf("views[%d]", i), "id", "group", "label", "renderer", "slot", "field_ids", "diagnostic_ids", "order")
		if err != nil {
			return err
		}
		if err := requireLabel(object["label"], fmt.Sprintf("views[%d].label", i)); err != nil {
			return err
		}
		for _, name := range []string{"field_ids", "diagnostic_ids"} {
			if _, err := requiredArray(object[name], fmt.Sprintf("views[%d].%s", i, name)); err != nil {
				return err
			}
		}
	}
	actions, err := requiredArray(root["actions"], "actions")
	if err != nil {
		return err
	}
	for i, item := range actions {
		object, err := requiredObject(item, fmt.Sprintf("actions[%d]", i), "id", "group", "label", "operation_ref", "capability_ref", "service_ref", "argument_ref", "effect_ref", "order")
		if err != nil {
			return err
		}
		if err := requireLabel(object["label"], fmt.Sprintf("actions[%d].label", i)); err != nil {
			return err
		}
		for _, name := range []string{"operation_ref", "capability_ref", "service_ref", "argument_ref", "effect_ref"} {
			if err := requireRef(object[name], fmt.Sprintf("actions[%d].%s", i, name)); err != nil {
				return err
			}
		}
	}
	diagnostics, err := requiredArray(root["diagnostics"], "diagnostics")
	if err != nil {
		return err
	}
	for i, item := range diagnostics {
		object, err := requiredObject(item, fmt.Sprintf("diagnostics[%d]", i), "id", "group", "label", "member_id", "kind", "order")
		if err != nil {
			return err
		}
		if err := requireLabel(object["label"], fmt.Sprintf("diagnostics[%d].label", i)); err != nil {
			return err
		}
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := inspectJSONValue(dec); err != nil {
		return fmt.Errorf("closed manifest: %w", err)
	}
	return ensureEOF(dec)
}

func inspectJSONValue(dec *json.Decoder) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		keys := map[string]bool{}
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if keys[key] {
				return fmt.Errorf("duplicate JSON object key %q", key)
			}
			keys[key] = true
			if err := inspectJSONValue(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
		return err
	case '[':
		for dec.More() {
			if err := inspectJSONValue(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
		return err
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
}

func ensureEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing JSON value")
	}
	return nil
}

func rejectForbidden(v any) error {
	switch x := v.(type) {
	case map[string]any:
		for k, child := range x {
			if forbiddenMembers[strings.ToLower(k)] {
				return fmt.Errorf("forbidden executable or decoding member %q", k)
			}
			if err := rejectForbidden(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range x {
			if err := rejectForbidden(child); err != nil {
				return err
			}
		}
	}
	return nil
}

// Validate fails a whole contribution closed. A host can isolate that rejected
// contribution from other drivers by retaining only independently valid entries.
func Validate(m Manifest, index SemanticIndex) error {
	if nilIndex(index) {
		return fmt.Errorf("semantic index is required")
	}
	if m.Contract != Contract {
		return fmt.Errorf("unsupported contract %q", m.Contract)
	}
	if err := id("manifest_id", m.ManifestID); err != nil {
		return err
	}
	if err := version("manifest_version", m.ManifestVersion); err != nil {
		return err
	}
	if err := id("contributor.driver_id", m.Contributor.DriverID); err != nil {
		return err
	}
	if err := native(m.Contributor.NativeContract); err != nil {
		return err
	}
	if m.Requires.SemanticKernel != SemanticKernel {
		return fmt.Errorf("wrong semantic kernel %q", m.Requires.SemanticKernel)
	}
	if len(m.Requires.Packs) == 0 {
		return fmt.Errorf("requires.packs is required")
	}
	if err := uniquePacks(m.Requires.Packs, index); err != nil {
		return err
	}
	if len(m.Groups) > MaxGroups || len(m.Views) > MaxViews || len(m.Fields) > MaxFields || len(m.Actions) > MaxActions || len(m.Diagnostics) > MaxDiagnostics {
		return fmt.Errorf("manifest collection bound exceeded")
	}
	groups, err := validateGroups(m.Groups)
	if err != nil {
		return err
	}
	fields, err := validateFields(m.Fields, groups, m.Requires.Packs, index)
	if err != nil {
		return err
	}
	diagnostics, err := validateDiagnostics(m.Diagnostics, groups, m.Contributor.NativeContract, index)
	if err != nil {
		return err
	}
	if err := validateViews(m.Views, groups, fields, diagnostics); err != nil {
		return err
	}
	if err := validateActions(m.Actions, groups, m.Requires.Packs, index); err != nil {
		return err
	}
	return nil
}

func id(name, value string) error {
	if value == "" || utf8.RuneCountInString(value) > 128 || !utf8.ValidString(value) {
		return fmt.Errorf("invalid %s", name)
	}
	return nil
}
func version(name, value string) error {
	if err := id(name, value); err != nil {
		return err
	}
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return fmt.Errorf("invalid %s", name)
	}
	for _, p := range parts {
		if p == "" {
			return fmt.Errorf("invalid %s", name)
		}
		for _, r := range p {
			if r < '0' || r > '9' {
				return fmt.Errorf("invalid %s", name)
			}
		}
	}
	return nil
}
func label(l Label) error {
	if err := id("label.key", l.Key); err != nil {
		return err
	}
	if l.Default == "" || !utf8.ValidString(l.Default) {
		return fmt.Errorf("invalid label.default")
	}
	return nil
}
func native(r NativeContractRef) error {
	if err := id("native contract owner", r.Owner); err != nil {
		return err
	}
	if err := id("native contract", r.Contract); err != nil {
		return err
	}
	return version("native contract version", r.Version)
}
func ref(r DefinitionRef, packs []PackRef, index SemanticIndex) error {
	if err := id("reference id", r.ID); err != nil {
		return err
	}
	if err := version("reference version", r.Version); err != nil {
		return err
	}
	if r.Pack.ID == "" || r.Pack.Version == "" || !containsPack(packs, r.Pack) {
		return fmt.Errorf("reference has undeclared pack %q@%q", r.Pack.ID, r.Pack.Version)
	}
	if !index.HasDefinition(r) {
		return fmt.Errorf("dangling reference %q@%q/%q@%q", r.Pack.ID, r.Pack.Version, r.ID, r.Version)
	}
	return nil
}
func containsPack(packs []PackRef, want PackRef) bool {
	for _, p := range packs {
		if p == want {
			return true
		}
	}
	return false
}
func uniquePacks(packs []PackRef, index SemanticIndex) error {
	seen := map[packKey]bool{}
	for _, p := range packs {
		if err := id("pack id", p.ID); err != nil {
			return err
		}
		if err := version("pack version", p.Version); err != nil {
			return err
		}
		if seen[asPackKey(p)] {
			return fmt.Errorf("duplicate pack %q@%q", p.ID, p.Version)
		}
		seen[asPackKey(p)] = true
		if !index.HasPack(p) {
			return fmt.Errorf("wrong or unresolved pack %q@%q", p.ID, p.Version)
		}
	}
	return nil
}

func validateGroups(groups []Group) (map[string]bool, error) {
	ids, orders := map[string]bool{}, map[int]bool{}
	for _, g := range groups {
		if err := id("group id", g.ID); err != nil {
			return nil, err
		}
		if err := label(g.Label); err != nil {
			return nil, err
		}
		if err := id("resource_context", g.ResourceContext); err != nil {
			return nil, err
		}
		if ids[g.ID] {
			return nil, fmt.Errorf("duplicate group id %q", g.ID)
		}
		if orders[g.Order] {
			return nil, fmt.Errorf("duplicate group order %d", g.Order)
		}
		ids[g.ID], orders[g.Order] = true, true
	}
	return ids, nil
}
func validateFields(fields []Field, groups map[string]bool, packs []PackRef, index SemanticIndex) (map[string]bool, error) {
	ids, orders := map[string]bool{}, map[int]bool{}
	for _, f := range fields {
		if err := id("field id", f.ID); err != nil {
			return nil, err
		}
		if !groups[f.Group] {
			return nil, fmt.Errorf("field %q has dangling group", f.ID)
		}
		if err := label(f.Label); err != nil {
			return nil, err
		}
		for _, r := range []DefinitionRef{f.Ref, f.ServiceRef, f.CapabilityRef, f.UnitRef} {
			if err := ref(r, packs, index); err != nil {
				return nil, err
			}
		}
		expected, ok := index.CanonicalUnit(f.Ref)
		if !ok || expected != f.UnitRef {
			return nil, fmt.Errorf("field %q unit mismatch", f.ID)
		}
		if !index.ServiceOwnsCapability(f.ServiceRef, f.CapabilityRef) {
			return nil, fmt.Errorf("field %q service/capability mismatch", f.ID)
		}
		if !index.FieldMatches(f.Ref, f.ServiceRef, f.CapabilityRef) {
			return nil, fmt.Errorf("field %q field/service/capability mismatch", f.ID)
		}
		if ids[f.ID] {
			return nil, fmt.Errorf("duplicate field id %q", f.ID)
		}
		if orders[f.Order] {
			return nil, fmt.Errorf("duplicate field order %d", f.Order)
		}
		ids[f.ID], orders[f.Order] = true, true
	}
	return ids, nil
}
func validateDiagnostics(items []Diagnostic, groups map[string]bool, contract NativeContractRef, index SemanticIndex) (map[string]bool, error) {
	ids, orders := map[string]bool{}, map[int]bool{}
	for _, d := range items {
		if err := id("diagnostic id", d.ID); err != nil {
			return nil, err
		}
		if !groups[d.Group] {
			return nil, fmt.Errorf("diagnostic %q has dangling group", d.ID)
		}
		if err := label(d.Label); err != nil {
			return nil, err
		}
		if !diagnosticKinds[d.Kind] {
			return nil, fmt.Errorf("unsupported diagnostic kind %q", d.Kind)
		}
		if !index.HasNativeMember(contract, d.Kind, d.MemberID) {
			return nil, fmt.Errorf("dangling native diagnostic %q", d.MemberID)
		}
		if ids[d.ID] {
			return nil, fmt.Errorf("duplicate diagnostic id %q", d.ID)
		}
		if orders[d.Order] {
			return nil, fmt.Errorf("duplicate diagnostic order %d", d.Order)
		}
		ids[d.ID], orders[d.Order] = true, true
	}
	return ids, nil
}
func validateViews(views []View, groups, fields, diagnostics map[string]bool) error {
	ids, orders := map[string]bool{}, map[int]bool{}
	for _, v := range views {
		if err := id("view id", v.ID); err != nil {
			return err
		}
		if !groups[v.Group] {
			return fmt.Errorf("view %q has dangling group", v.ID)
		}
		if err := label(v.Label); err != nil {
			return err
		}
		if !renderers[v.Renderer] {
			return fmt.Errorf("unsupported renderer %q", v.Renderer)
		}
		if !slots[v.Slot] {
			return fmt.Errorf("unsupported host slot %q", v.Slot)
		}
		for _, f := range v.FieldIDs {
			if !fields[f] {
				return fmt.Errorf("view %q has dangling field %q", v.ID, f)
			}
		}
		for _, d := range v.DiagnosticIDs {
			if !diagnostics[d] {
				return fmt.Errorf("view %q has dangling diagnostic %q", v.ID, d)
			}
		}
		if ids[v.ID] {
			return fmt.Errorf("duplicate view id %q", v.ID)
		}
		if orders[v.Order] {
			return fmt.Errorf("duplicate view order %d", v.Order)
		}
		ids[v.ID], orders[v.Order] = true, true
	}
	return nil
}
func validateActions(actions []Action, groups map[string]bool, packs []PackRef, index SemanticIndex) error {
	ids, orders := map[string]bool{}, map[int]bool{}
	for _, a := range actions {
		if err := id("action id", a.ID); err != nil {
			return err
		}
		if !groups[a.Group] {
			return fmt.Errorf("action %q has dangling group", a.ID)
		}
		if err := label(a.Label); err != nil {
			return err
		}
		for _, r := range []DefinitionRef{a.OperationRef, a.CapabilityRef, a.ServiceRef, a.ArgumentRef, a.EffectRef} {
			if err := ref(r, packs, index); err != nil {
				return err
			}
		}
		if !index.ServiceOwnsCapability(a.ServiceRef, a.CapabilityRef) {
			return fmt.Errorf("action %q service/capability mismatch", a.ID)
		}
		if !index.OperationMatches(a.OperationRef, a.CapabilityRef, a.ServiceRef, a.ArgumentRef, a.EffectRef) {
			return fmt.Errorf("action %q operation mismatch", a.ID)
		}
		if ids[a.ID] {
			return fmt.Errorf("duplicate action id %q", a.ID)
		}
		if orders[a.Order] {
			return fmt.Errorf("duplicate action order %d", a.Order)
		}
		ids[a.ID], orders[a.Order] = true, true
	}
	return nil
}

// Registry detects non-deterministic publication of the same driver manifest.
type Registry struct {
	mu        sync.Mutex
	index     SemanticIndex
	digests   map[registryKey]string
	conflicts map[registryKey]bool
}

type registryKey struct{ DriverID, ManifestID, ManifestVersion string }

func NewRegistry(index SemanticIndex) *Registry {
	return &Registry{index: index, digests: map[registryKey]string{}, conflicts: map[registryKey]bool{}}
}

// Accept derives the authoritative digest from the validated canonical
// descriptor. suppliedDigest is deliberately untrusted publisher metadata and
// cannot affect admission, replacement, or quarantine.
func (r *Registry) Accept(m Manifest, suppliedDigest string) error {
	if r == nil || nilIndex(r.index) {
		return fmt.Errorf("semantic index is required")
	}
	digest, err := CanonicalDigest(m, r.index)
	if err != nil {
		return err
	}
	key := registryKey{m.Contributor.DriverID, m.ManifestID, m.ManifestVersion}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conflicts[key] {
		return fmt.Errorf("manifest digest conflict for %q/%q@%q", key.DriverID, key.ManifestID, key.ManifestVersion)
	}
	if prior, ok := r.digests[key]; ok && prior != digest {
		r.conflicts[key] = true
		delete(r.digests, key)
		return fmt.Errorf("manifest digest conflict for %q/%q@%q", key.DriverID, key.ManifestID, key.ManifestVersion)
	}
	r.digests[key] = digest
	return nil
}

// CanonicalDigest validates the contribution, orders all descriptor
// collections, then hashes the deterministic JSON wire encoding. It is the
// only digest used by Registry admission.
func CanonicalDigest(m Manifest, index SemanticIndex) (string, error) {
	canonical, err := Canonicalize(m, index)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("canonical descriptor encoding: %w", err)
	}
	if len(encoded) > MaxManifestBytes {
		return "", fmt.Errorf("canonical manifest exceeds %d bytes", MaxManifestBytes)
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func nilIndex(index SemanticIndex) bool {
	if index == nil {
		return true
	}
	v := reflect.ValueOf(index)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// Canonicalize first validates then returns a copy whose ordered descriptor
// collections are sorted by (order,id). It never mutates caller-owned slices.
func Canonicalize(m Manifest, index SemanticIndex) (Manifest, error) {
	if err := Validate(m, index); err != nil {
		return Manifest{}, err
	}
	out := m
	out.Requires.Packs = append([]PackRef(nil), m.Requires.Packs...)
	out.Groups = append([]Group(nil), m.Groups...)
	out.Fields = append([]Field(nil), m.Fields...)
	out.Views = append([]View(nil), m.Views...)
	out.Actions = append([]Action(nil), m.Actions...)
	out.Diagnostics = append([]Diagnostic(nil), m.Diagnostics...)
	for i := range out.Views {
		out.Views[i].FieldIDs = append([]string(nil), out.Views[i].FieldIDs...)
		out.Views[i].DiagnosticIDs = append([]string(nil), out.Views[i].DiagnosticIDs...)
		sort.Strings(out.Views[i].FieldIDs)
		sort.Strings(out.Views[i].DiagnosticIDs)
	}
	sort.Slice(out.Requires.Packs, func(i, j int) bool {
		if out.Requires.Packs[i].ID != out.Requires.Packs[j].ID {
			return out.Requires.Packs[i].ID < out.Requires.Packs[j].ID
		}
		return out.Requires.Packs[i].Version < out.Requires.Packs[j].Version
	})
	sort.Slice(out.Groups, func(i, j int) bool {
		return ordered(out.Groups[i].Order, out.Groups[i].ID, out.Groups[j].Order, out.Groups[j].ID)
	})
	sort.Slice(out.Fields, func(i, j int) bool {
		return ordered(out.Fields[i].Order, out.Fields[i].ID, out.Fields[j].Order, out.Fields[j].ID)
	})
	sort.Slice(out.Views, func(i, j int) bool {
		return ordered(out.Views[i].Order, out.Views[i].ID, out.Views[j].Order, out.Views[j].ID)
	})
	sort.Slice(out.Actions, func(i, j int) bool {
		return ordered(out.Actions[i].Order, out.Actions[i].ID, out.Actions[j].Order, out.Actions[j].ID)
	})
	sort.Slice(out.Diagnostics, func(i, j int) bool {
		return ordered(out.Diagnostics[i].Order, out.Diagnostics[i].ID, out.Diagnostics[j].Order, out.Diagnostics[j].ID)
	})
	return out, nil
}

func ordered(leftOrder int, leftID string, rightOrder int, rightID string) bool {
	if leftOrder != rightOrder {
		return leftOrder < rightOrder
	}
	return leftID < rightID
}
