package contributionv1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fixtureCatalog struct {
	Index struct {
		Packs       []PackRef       `json:"packs"`
		Definitions []DefinitionRef `json:"definitions"`
		Fields      []struct {
			Ref           DefinitionRef `json:"ref"`
			UnitRef       DefinitionRef `json:"unit_ref"`
			ServiceRef    DefinitionRef `json:"service_ref"`
			CapabilityRef DefinitionRef `json:"capability_ref"`
		} `json:"fields"`
		ServiceCapabilities []struct {
			Service    DefinitionRef `json:"service"`
			Capability DefinitionRef `json:"capability"`
		} `json:"service_capabilities"`
		Operations []struct {
			Operation  DefinitionRef `json:"operation"`
			Capability DefinitionRef `json:"capability"`
			Service    DefinitionRef `json:"service"`
			Argument   DefinitionRef `json:"argument"`
			Effect     DefinitionRef `json:"effect"`
		} `json:"operations"`
		NativeMembers []struct {
			Contract NativeContractRef `json:"contract"`
			Kind     string            `json:"kind"`
			ID       string            `json:"id"`
		} `json:"native_members"`
	} `json:"index"`
	Manifests []Manifest `json:"manifests"`
	Resources []struct {
		ID             string `json:"id"`
		State          string `json:"state"`
		ContributionID string `json:"contribution_id"`
		Capabilities   []struct {
			ID    string `json:"id"`
			State string `json:"state"`
		} `json:"capabilities"`
	} `json:"resources"`
	Navigation struct {
		Perspectives []struct {
			ID string `json:"id"`
		} `json:"perspectives"`
		DefaultPerspective string `json:"default_perspective"`
		DefaultResource    string `json:"default_resource"`
	} `json:"navigation"`
	ContributionStates []struct {
		ManifestID   string `json:"manifest_id"`
		ResourceID   string `json:"resource_id"`
		CapabilityID string `json:"capability_id"`
		State        string `json:"state"`
	} `json:"contribution_states"`
}

func readFixture(t *testing.T, name string, into any) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, into); err != nil {
		t.Fatal(err)
	}
}

func indexFromCatalog(c fixtureCatalog) StaticIndex {
	s := NewStaticIndex()
	for _, p := range c.Index.Packs {
		s.AddPack(p)
	}
	for _, r := range c.Index.Definitions {
		s.AddDefinition(r)
	}
	for _, f := range c.Index.Fields {
		s.AddDefinition(f.Ref)
		s.AddDefinition(f.UnitRef)
		s.AddDefinition(f.ServiceRef)
		s.AddDefinition(f.CapabilityRef)
		s.AddCanonicalUnit(f.Ref, f.UnitRef)
		s.AddFieldRelation(f.Ref, f.ServiceRef, f.CapabilityRef)
	}
	for _, v := range c.Index.ServiceCapabilities {
		s.AddDefinition(v.Service)
		s.AddDefinition(v.Capability)
		s.AddServiceCapability(v.Service, v.Capability)
	}
	for _, v := range c.Index.Operations {
		s.AddDefinition(v.Operation)
		s.AddDefinition(v.Capability)
		s.AddDefinition(v.Service)
		s.AddDefinition(v.Argument)
		s.AddDefinition(v.Effect)
		s.AddOperation(v.Operation, v.Capability, v.Service, v.Argument, v.Effect)
	}
	for _, v := range c.Index.NativeMembers {
		s.AddNativeMember(v.Contract, v.Kind, v.ID)
	}
	return *s
}

func TestFiveDomainCatalogIsAccepted(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	idx := indexFromCatalog(c)
	if len(c.Manifests) != 5 {
		t.Fatalf("five-domain fixture has %d manifests", len(c.Manifests))
	}
	for _, m := range c.Manifests {
		if err := Validate(m, idx); err != nil {
			t.Fatalf("%s: %v", m.ManifestID, err)
		}
	}
	if err := validateFixturePresentation(c); err != nil {
		t.Fatal(err)
	}
}

func validateFixturePresentation(c fixtureCatalog) error {
	states := map[string]bool{"available": true, "unavailable": true, "stale": true, "conflict": true, "partial": true, "unknown": true, "unsupported": true, "withdrawn": true, "withheld": true}
	manifests, resources, capabilities, perspectives := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, m := range c.Manifests {
		manifests[m.ManifestID] = true
	}
	for _, resource := range c.Resources {
		if resource.ID == "" || resources[resource.ID] || !states[resource.State] || !manifests[resource.ContributionID] {
			return fmt.Errorf("invalid fixture resource %q", resource.ID)
		}
		resources[resource.ID] = true
		for _, capability := range resource.Capabilities {
			if capability.ID == "" || !states[capability.State] {
				return fmt.Errorf("invalid fixture capability %q", capability.ID)
			}
			capabilities[resource.ID+"|"+capability.ID] = true
		}
	}
	for _, perspective := range c.Navigation.Perspectives {
		if perspective.ID == "" || perspectives[perspective.ID] {
			return fmt.Errorf("invalid fixture perspective %q", perspective.ID)
		}
		perspectives[perspective.ID] = true
	}
	if !perspectives[c.Navigation.DefaultPerspective] || !resources[c.Navigation.DefaultResource] {
		return fmt.Errorf("invalid fixture defaults")
	}
	for _, item := range c.ContributionStates {
		if !manifests[item.ManifestID] || !resources[item.ResourceID] || !capabilities[item.ResourceID+"|"+item.CapabilityID] || !states[item.State] {
			return fmt.Errorf("invalid contribution state %q", item.ManifestID)
		}
	}
	return nil
}

func TestInvalidManifestsFailClosed(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	idx := indexFromCatalog(c)
	var cases struct {
		Cases []struct {
			ID       string `json:"id"`
			Mutation string `json:"mutation"`
			Want     string `json:"want"`
		} `json:"cases"`
	}
	readFixture(t, "invalid-manifests.json", &cases)
	for _, tc := range cases.Cases {
		t.Run(tc.ID, func(t *testing.T) {
			m := cloneManifest(t, c.Manifests[0])
			applyInvalidMutation(&m, tc.Mutation)
			err := Validate(m, idx)
			if err == nil || !strings.Contains(err.Error(), tc.Want) {
				t.Fatalf("error=%v, want %q", err, tc.Want)
			}
		})
	}
}

func cloneManifest(t *testing.T, source Manifest) Manifest {
	t.Helper()
	b, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var out Manifest
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func applyInvalidMutation(m *Manifest, mutation string) {
	switch mutation {
	case "duplicate-field-id":
		m.Fields = append(m.Fields, m.Fields[0])
		m.Fields[len(m.Fields)-1].Order = 99
	case "duplicate-field-order":
		m.Fields = append(m.Fields, m.Fields[0])
		m.Fields[len(m.Fields)-1].ID = "other"
	case "wrong-pack-version":
		m.Requires.Packs[0].Version = "9.9.9"
	case "dangling-service":
		m.Fields[0].ServiceRef.ID = "missing.service"
	case "unit-mismatch":
		m.Fields[0].UnitRef = m.Fields[0].Ref
	case "unsupported-renderer":
		m.Views[0].Renderer = "arbitrary_component"
	case "operation-mismatch":
		m.Actions = append(m.Actions, m.Actions[0])
		m.Actions[len(m.Actions)-1].ID = "mismatch"
		m.Actions[len(m.Actions)-1].Order = 99
		m.Actions[len(m.Actions)-1].OperationRef.ID = "missing.operation"
	case "action-without-operation":
		m.Actions = append(m.Actions, m.Actions[0])
		m.Actions[len(m.Actions)-1].ID = "no-operation"
		m.Actions[len(m.Actions)-1].Order = 99
		m.Actions[len(m.Actions)-1].OperationRef = DefinitionRef{}
	case "wrong-effect":
		m.Actions[0].EffectRef = m.Fields[0].Ref
	case "cross-service-field":
		m.Fields[0].Ref.ID = "thermal.temperature.system"
	}
}

func TestDecodeRejectsUnknownAndExecutableMembers(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	b, err := json.Marshal(c.Manifests[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range []string{"script", "html", "css", "url", "graphql", "jsonpath", "formula", "register", "opcode", "decode"} {
		bad := append(append([]byte{}, b[:len(b)-1]...), []byte(`,"`+member+`":"x"}`)...)
		if _, err := Decode(bad); err == nil {
			t.Fatalf("%s accepted", member)
		}
	}
	bad := append(append([]byte{}, b[:len(b)-1]...), []byte(`,"unknown_member":true}`)...)
	if _, err := Decode(bad); err == nil {
		t.Fatal("unknown member accepted")
	}
}

func TestDecodeRejectsCaseFoldedRawMemberAliases(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	base := rawManifest(t, c.Manifests[0])
	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"top-level", func(v map[string]any) { v["Contract"] = v["contract"] }},
		{"contributor", func(v map[string]any) { v["contributor"].(map[string]any)["Driver_ID"] = "thermal.primary" }},
		{"native-contract", func(v map[string]any) {
			v["contributor"].(map[string]any)["native_contract"].(map[string]any)["Owner"] = "helianthus"
		}},
		{"requirements", func(v map[string]any) { v["requires"].(map[string]any)["Semantic_Kernel"] = SemanticKernel }},
		{"requirements-pack", func(v map[string]any) {
			v["requires"].(map[string]any)["packs"].([]any)[0].(map[string]any)["ID"] = "helianthus.pack.thermal"
		}},
		{"group", func(v map[string]any) { v["groups"].([]any)[0].(map[string]any)["ID"] = "zone" }},
		{"label-duplicate-semantic-target", func(v map[string]any) {
			v["groups"].([]any)[0].(map[string]any)["label"].(map[string]any)["Key"] = "thermal.zone.overwrite"
		}},
		{"field", func(v map[string]any) { v["fields"].([]any)[0].(map[string]any)["Group"] = "zone" }},
		{"definition", func(v map[string]any) {
			v["fields"].([]any)[0].(map[string]any)["ref"].(map[string]any)["ID"] = "thermal.temperature.zone"
		}},
		{"definition-pack", func(v map[string]any) {
			v["fields"].([]any)[0].(map[string]any)["ref"].(map[string]any)["pack"].(map[string]any)["ID"] = "helianthus.pack.thermal"
		}},
		{"view-unicode-folded-slot", func(v map[string]any) { v["views"].([]any)[0].(map[string]any)["ſlot"] = "native_diagnostics" }},
		{"action", func(v map[string]any) {
			v["actions"].([]any)[0].(map[string]any)["Operation_Ref"] = v["actions"].([]any)[0].(map[string]any)["operation_ref"]
		}},
		{"diagnostic", func(v map[string]any) {
			v["diagnostics"] = []any{map[string]any{"id": "diagnostic", "group": "zone", "label": map[string]any{"key": "diagnostic", "default": "Diagnostic"}, "member_id": "native-field", "Member_ID": "overwrite", "kind": "field", "order": 1}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value := cloneRaw(t, base)
			tc.mutate(value)
			if _, err := Decode(rawBytes(t, value)); err == nil || !strings.Contains(err.Error(), "unknown member") {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestDecodeRejectsNestedDuplicateObjectKeys(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	b, err := json.Marshal(c.Manifests[0])
	if err != nil {
		t.Fatal(err)
	}
	needle := []byte(`"label":{"key":"thermal.zone","default":"Zone"}`)
	replacement := []byte(`"label":{"key":"thermal.zone","key":"thermal.zone.duplicate","default":"Zone"}`)
	bad := bytes.Replace(b, needle, replacement, 1)
	if _, err := Decode(bad); err == nil || !strings.Contains(err.Error(), "duplicate JSON object key") {
		t.Fatalf("error=%v", err)
	}
}

func TestDecodeRejectsRedundantActionReference(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	b, err := json.Marshal(c.Manifests[0])
	if err != nil {
		t.Fatal(err)
	}
	bad := bytes.Replace(b, []byte(`"actions":[{`), []byte(`"actions":[{"ref":null,`), 1)
	if _, err := Decode(bad); err == nil || !strings.Contains(err.Error(), "unknown member") {
		t.Fatalf("error=%v", err)
	}
}

func TestDecodeRejectsTopLevelNull(t *testing.T) {
	if _, err := Decode([]byte("null")); err == nil || !strings.Contains(err.Error(), "top-level value must be an object") {
		t.Fatalf("error=%v", err)
	}
}

func TestDecodeRequiresSchemaMembers(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	base := rawManifest(t, c.Manifests[0])
	cases := []struct {
		name   string
		remove func(map[string]any)
	}{
		{"top-level-contract", func(v map[string]any) { delete(v, "contract") }},
		{"top-level-manifest-id", func(v map[string]any) { delete(v, "manifest_id") }},
		{"top-level-manifest-version", func(v map[string]any) { delete(v, "manifest_version") }},
		{"top-level-contributor", func(v map[string]any) { delete(v, "contributor") }},
		{"top-level-requires", func(v map[string]any) { delete(v, "requires") }},
		{"top-level-empty-groups", func(v map[string]any) { delete(v, "groups") }},
		{"top-level-empty-fields", func(v map[string]any) { delete(v, "fields") }},
		{"top-level-empty-views", func(v map[string]any) { delete(v, "views") }},
		{"top-level-empty-actions", func(v map[string]any) { delete(v, "actions") }},
		{"top-level-empty-diagnostics", func(v map[string]any) { delete(v, "diagnostics") }},
		{"contributor-driver", func(v map[string]any) { delete(v["contributor"].(map[string]any), "driver_id") }},
		{"native-contract-version", func(v map[string]any) {
			delete(v["contributor"].(map[string]any)["native_contract"].(map[string]any), "version")
		}},
		{"requires-kernel", func(v map[string]any) { delete(v["requires"].(map[string]any), "semantic_kernel") }},
		{"required-pack-version", func(v map[string]any) {
			delete(v["requires"].(map[string]any)["packs"].([]any)[0].(map[string]any), "version")
		}},
		{"group-zero-order", func(v map[string]any) { delete(v["groups"].([]any)[0].(map[string]any), "order") }},
		{"group-label-default", func(v map[string]any) {
			delete(v["groups"].([]any)[0].(map[string]any)["label"].(map[string]any), "default")
		}},
		{"field-ref", func(v map[string]any) { delete(v["fields"].([]any)[0].(map[string]any), "ref") }},
		{"field-ref-pack-version", func(v map[string]any) {
			delete(v["fields"].([]any)[0].(map[string]any)["ref"].(map[string]any)["pack"].(map[string]any), "version")
		}},
		{"view-field-ids", func(v map[string]any) { delete(v["views"].([]any)[0].(map[string]any), "field_ids") }},
		{"view-diagnostic-ids", func(v map[string]any) { delete(v["views"].([]any)[0].(map[string]any), "diagnostic_ids") }},
		{"action-zero-order", func(v map[string]any) { delete(v["actions"].([]any)[0].(map[string]any), "order") }},
		{"diagnostic-zero-order", func(v map[string]any) {
			v["diagnostics"] = []any{map[string]any{"id": "d", "group": "zone", "label": map[string]any{"key": "d", "default": "D"}, "member_id": "d", "kind": "field"}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value := cloneRaw(t, base)
			tc.remove(value)
			if _, err := Decode(rawBytes(t, value)); err == nil || !strings.Contains(err.Error(), "missing required member") {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestDecodeRejectsInvalidUTF8BeforeNormalization(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	b, err := json.Marshal(c.Manifests[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range [][]byte{[]byte(`"manifest_id":"portal.thermal"`), []byte(`"id":"thermal.temperature.zone"`)} {
		bad := bytes.Replace(b, needle, append(append([]byte{}, needle[:len(needle)-2]...), []byte{0xff, '"'}...), 1)
		if _, err := Decode(bad); err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
			t.Fatalf("error=%v", err)
		}
	}
}

func TestDecodeDistinguishesPresentZeroOrderFromMissingOrder(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	raw := rawManifest(t, c.Manifests[0])
	raw["groups"].([]any)[0].(map[string]any)["order"] = 0
	if _, err := Decode(rawBytes(t, raw)); err != nil {
		t.Fatalf("present zero order rejected: %v", err)
	}
}

func TestDecodeAcceptsSchemaValidIntegralOrderSpellings(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	base := rawBytes(t, c.Manifests[0])
	for _, tc := range []struct {
		name    string
		literal string
		want    int32
		valid   bool
	}{
		{"decimal", "1.0", 1, true},
		{"exponent", "1e0", 1, true},
		{"minimum", "-2147483648", MinOrder, true},
		{"maximum", "2147483647", MaxOrder, true},
		{"below-minimum", "-2147483649", 0, false},
		{"above-maximum", "2147483648", 0, false},
		{"fraction", "1.5", 0, false},
		{"fractional-exponent", "1e-1", 0, false},
		{"non-finite-nan", "NaN", 0, false},
		{"non-finite-infinity", "Infinity", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := bytes.Replace(base, []byte(`"order":10`), []byte(`"order":`+tc.literal), 1)
			manifest, err := Decode(data)
			if (err == nil) != tc.valid {
				t.Fatalf("literal=%s err=%v valid=%t", tc.literal, err, tc.valid)
			}
			if tc.valid && manifest.Groups[0].Order != tc.want {
				t.Fatalf("literal=%s normalized order=%d want=%d", tc.literal, manifest.Groups[0].Order, tc.want)
			}
		})
	}
}

func TestDecodeRejectsNullAndWrongRequiredScalarTypes(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	base := rawManifest(t, c.Manifests[0])
	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"contract-null", func(v map[string]any) { v["contract"] = nil }},
		{"manifest-id-number", func(v map[string]any) { v["manifest_id"] = 7 }},
		{"driver-null", func(v map[string]any) { v["contributor"].(map[string]any)["driver_id"] = nil }},
		{"native-owner-null", func(v map[string]any) {
			v["contributor"].(map[string]any)["native_contract"].(map[string]any)["owner"] = nil
		}},
		{"kernel-null", func(v map[string]any) { v["requires"].(map[string]any)["semantic_kernel"] = nil }},
		{"pack-id-null", func(v map[string]any) {
			v["requires"].(map[string]any)["packs"].([]any)[0].(map[string]any)["id"] = nil
		}},
		{"group-label-null", func(v map[string]any) { v["groups"].([]any)[0].(map[string]any)["label"].(map[string]any)["key"] = nil }},
		{"group-order-null", func(v map[string]any) { v["groups"].([]any)[0].(map[string]any)["order"] = nil }},
		{"field-ref-null", func(v map[string]any) { v["fields"].([]any)[0].(map[string]any)["ref"] = nil }},
		{"view-renderer-null", func(v map[string]any) { v["views"].([]any)[0].(map[string]any)["renderer"] = nil }},
		{"view-reference-null", func(v map[string]any) { v["views"].([]any)[0].(map[string]any)["field_ids"] = nil }},
		{"action-operation-null", func(v map[string]any) { v["actions"].([]any)[0].(map[string]any)["operation_ref"] = nil }},
		{"action-order-null", func(v map[string]any) { v["actions"].([]any)[0].(map[string]any)["order"] = nil }},
		{"diagnostic-member-null", func(v map[string]any) {
			v["diagnostics"] = []any{map[string]any{"id": "d", "group": "zone", "label": map[string]any{"key": "d", "default": "D"}, "member_id": nil, "kind": "field", "order": 0}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := cloneRaw(t, base)
			tc.mutate(raw)
			if _, err := Decode(rawBytes(t, raw)); err == nil || !strings.Contains(err.Error(), "closed manifest") {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestValidateRequiresNonNilSchemaArraysAndDiagnosticMemberID(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	idx := indexFromCatalog(c)
	for _, tc := range []struct {
		name   string
		mutate func(*Manifest)
	}{
		{"packs", func(m *Manifest) { m.Requires.Packs = nil }}, {"groups", func(m *Manifest) { m.Groups = nil }}, {"fields", func(m *Manifest) { m.Fields = nil }}, {"views", func(m *Manifest) { m.Views = nil }}, {"actions", func(m *Manifest) { m.Actions = nil }}, {"diagnostics", func(m *Manifest) { m.Diagnostics = nil }}, {"view-fields", func(m *Manifest) { m.Views[0].FieldIDs = nil }}, {"view-diagnostics", func(m *Manifest) { m.Views[0].DiagnosticIDs = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := cloneManifest(t, c.Manifests[0])
			tc.mutate(&m)
			if err := Validate(m, idx); err == nil || !strings.Contains(err.Error(), "nil") {
				t.Fatalf("error=%v", err)
			}
		})
	}
	allowed := cloneManifest(t, c.Manifests[0])
	allowed.Groups, allowed.Fields, allowed.Views, allowed.Actions, allowed.Diagnostics = []Group{}, []Field{}, []View{}, []Action{}, []Diagnostic{}
	if err := Validate(allowed, idx); err != nil {
		t.Fatalf("explicit empty arrays rejected: %v", err)
	}
	for _, memberID := range []string{"", strings.Repeat("é", 129)} {
		m := cloneManifest(t, c.Manifests[0])
		m.Diagnostics = []Diagnostic{{ID: "diagnostic", Group: "zone", Label: Label{Key: "diagnostic", Default: "Diagnostic"}, MemberID: memberID, Kind: "field", Order: 1}}
		idx.AddNativeMember(m.Contributor.NativeContract, "field", memberID)
		if err := Validate(m, idx); err == nil || !strings.Contains(err.Error(), "diagnostic member_id") {
			t.Fatalf("member_id=%q error=%v", memberID, err)
		}
	}
}

func rawManifest(t *testing.T, m Manifest) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rawBytes(t, m), &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func rawBytes(t *testing.T, value any) []byte {
	t.Helper()
	out, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func cloneRaw(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rawBytes(t, value), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestIDLimitUsesUnicodeCodePoints(t *testing.T) {
	if err := id("id", strings.Repeat("é", 128)); err != nil {
		t.Fatalf("128 code points rejected: %v", err)
	}
	if err := id("id", strings.Repeat("é", 129)); err == nil {
		t.Fatal("129 code points accepted")
	}
}

func TestDecodeRejectsUnpairedSurrogateEscapes(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	b := rawBytes(t, c.Manifests[0])
	for _, needle := range [][]byte{[]byte(`"manifest_id":"portal.thermal"`), []byte(`"id":"thermal.temperature.zone"`)} {
		for _, escaped := range [][]byte{[]byte(`"\ud800"`), []byte(`"\udc00"`)} {
			bad := bytes.Replace(b, needle, append([]byte(`"manifest_id":`), escaped...), 1)
			if bytes.Equal(needle, []byte(`"id":"thermal.temperature.zone"`)) {
				bad = bytes.Replace(b, needle, append([]byte(`"id":`), escaped...), 1)
			}
			if _, err := Decode(bad); err == nil || !strings.Contains(err.Error(), "unpaired UTF-16") {
				t.Fatalf("needle=%s escape=%s error=%v", needle, escaped, err)
			}
		}
	}
	paired := bytes.Replace(b, []byte(`"manifest_id":"portal.thermal"`), []byte(`"manifest_id":"\ud83d\ude00"`), 1)
	if _, err := Decode(paired); err != nil {
		t.Fatalf("paired surrogate rejected: %v", err)
	}
}

func TestOrderUsesPortableSigned32BitRange(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	for _, value := range []int64{int64(MinOrder), int64(MaxOrder)} {
		raw := rawManifest(t, c.Manifests[0])
		raw["groups"].([]any)[0].(map[string]any)["order"] = value
		if _, err := Decode(rawBytes(t, raw)); err != nil {
			t.Fatalf("raw boundary %d rejected: %v", value, err)
		}
		m := cloneManifest(t, c.Manifests[0])
		m.Groups[0].Order, m.Fields[0].Order, m.Views[0].Order, m.Actions[0].Order = int32(value), int32(value), int32(value), int32(value)
		m.Diagnostics = []Diagnostic{{ID: "boundary-diagnostic", Group: "zone", Label: Label{Key: "boundary-diagnostic", Default: "Boundary diagnostic"}, MemberID: "boundary-member", Kind: "field", Order: int32(value)}}
		idx := indexFromCatalog(c)
		idx.AddNativeMember(m.Contributor.NativeContract, "field", "boundary-member")
		if err := Validate(m, idx); err != nil {
			t.Fatalf("typed boundary %d rejected: %v", value, err)
		}
	}
	for _, value := range []int64{int64(MinOrder) - 1, int64(MaxOrder) + 1} {
		raw := rawManifest(t, c.Manifests[0])
		raw["groups"].([]any)[0].(map[string]any)["order"] = value
		if _, err := Decode(rawBytes(t, raw)); err == nil || !strings.Contains(err.Error(), "required integer") {
			t.Fatalf("raw out-of-range %d error=%v", value, err)
		}
		if orderInRange(value) {
			t.Fatalf("typed range accepted %d", value)
		}
	}
}

func TestNilIndexFailsClosed(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	if err := Validate(c.Manifests[0], nil); err == nil {
		t.Fatal("nil index accepted")
	}
	var typedNil *StaticIndex
	if err := Validate(c.Manifests[0], typedNil); err == nil {
		t.Fatal("typed nil index accepted")
	}
	if err := NewRegistry(nil).Accept(c.Manifests[0], "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err == nil {
		t.Fatal("nil registry index accepted")
	}
}

func TestRegistryRejectsDigestConflict(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	idx := indexFromCatalog(c)
	r := NewRegistry(idx)
	if err := r.Accept(c.Manifests[0], "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err != nil {
		t.Fatal(err)
	}
	different := cloneManifest(t, c.Manifests[0])
	different.Groups[0].Label.Default = "Different"
	if err := r.Accept(different, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err == nil {
		t.Fatal("digest conflict accepted")
	}
	if err := r.Accept(c.Manifests[0], "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err == nil {
		t.Fatal("quarantined manifest was re-accepted")
	}
}

func TestRegistryQuarantinesDivergentCanonicalDescriptorsDespiteSpoofedDigest(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	idx := indexFromCatalog(c)
	r := NewRegistry(idx)
	first := cloneManifest(t, c.Manifests[0])
	different := cloneManifest(t, first)
	different.Groups[0].Label.Default = "Changed display label"
	spoof := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := r.Accept(first, spoof); err != nil {
		t.Fatal(err)
	}
	if err := r.Accept(different, spoof); err == nil {
		t.Fatal("different descriptor with spoofed digest accepted")
	}
	if err := r.Accept(first, spoof); err == nil {
		t.Fatal("descriptor identity was not quarantined")
	}
}

func TestRegistryTupleKeysDoNotAliasDelimitedIdentities(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	idx := indexFromCatalog(c)
	first, second := cloneManifest(t, c.Manifests[0]), cloneManifest(t, c.Manifests[0])
	first.Contributor.DriverID, first.ManifestID = "a", "b|c"
	second.Contributor.DriverID, second.ManifestID = "a|b", "c"
	r := NewRegistry(idx)
	if err := r.Accept(first, "spoof"); err != nil {
		t.Fatal(err)
	}
	if err := r.Accept(second, "spoof"); err != nil {
		t.Fatalf("distinct tuple collided: %v", err)
	}
}

func TestCanonicalizePreservesExplicitEmptyRequiredArrays(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	raw := rawManifest(t, c.Manifests[0])
	raw["requires"].(map[string]any)["packs"] = []any{}
	for _, name := range []string{"groups", "fields", "views", "actions", "diagnostics"} {
		raw[name] = []any{}
	}
	decoded, err := Decode(rawBytes(t, raw))
	if err != nil {
		t.Fatalf("explicit schema-valid empty arrays rejected at decode: %v", err)
	}
	if decoded.Requires.Packs == nil || decoded.Groups == nil || decoded.Fields == nil || decoded.Views == nil || decoded.Actions == nil || decoded.Diagnostics == nil {
		t.Fatal("Decode normalized an explicit empty top-level array to nil")
	}

	idx := indexFromCatalog(c)
	topLevelEmpty := cloneManifest(t, c.Manifests[0])
	topLevelEmpty.Groups, topLevelEmpty.Fields, topLevelEmpty.Views, topLevelEmpty.Actions, topLevelEmpty.Diagnostics = []Group{}, []Field{}, []View{}, []Action{}, []Diagnostic{}
	canonical, err := Canonicalize(topLevelEmpty, idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(canonical, idx); err != nil {
		t.Fatalf("canonical top-level empty arrays became invalid: %v", err)
	}
	if canonical.Requires.Packs == nil || canonical.Groups == nil || canonical.Fields == nil || canonical.Views == nil || canonical.Actions == nil || canonical.Diagnostics == nil {
		t.Fatal("Canonicalize normalized a required top-level array to nil")
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"groups":[]`, `"fields":[]`, `"views":[]`, `"actions":[]`, `"diagnostics":[]`} {
		if !bytes.Contains(encoded, []byte(want)) {
			t.Fatalf("canonical JSON does not retain %s: %s", want, encoded)
		}
	}
	if _, err := CanonicalDigest(canonical, idx); err != nil {
		t.Fatalf("canonical digest rejected explicit empty arrays: %v", err)
	}

	withEmptyViewRefs := cloneManifest(t, c.Manifests[0])
	withEmptyViewRefs.Actions, withEmptyViewRefs.Diagnostics = []Action{}, []Diagnostic{}
	withEmptyViewRefs.Views[0].FieldIDs, withEmptyViewRefs.Views[0].DiagnosticIDs = []string{}, []string{}
	canonicalView, err := Canonicalize(withEmptyViewRefs, idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(canonicalView, idx); err != nil {
		t.Fatalf("canonical view empty arrays became invalid: %v", err)
	}
	if canonicalView.Views[0].FieldIDs == nil || canonicalView.Views[0].DiagnosticIDs == nil {
		t.Fatal("Canonicalize normalized a required view array to nil")
	}
	encoded, err = json.Marshal(canonicalView)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"field_ids":[]`, `"diagnostic_ids":[]`, `"actions":[]`, `"diagnostics":[]`} {
		if !bytes.Contains(encoded, []byte(want)) {
			t.Fatalf("canonical JSON does not retain %s: %s", want, encoded)
		}
	}
	firstDigest, err := CanonicalDigest(withEmptyViewRefs, idx)
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := CanonicalDigest(cloneManifest(t, withEmptyViewRefs), idx)
	if err != nil || firstDigest != secondDigest {
		t.Fatalf("equivalent empty-array descriptors differ: %s %s %v", firstDigest, secondDigest, err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*Manifest)
	}{
		{"requires-packs", func(m *Manifest) { m.Requires.Packs = nil }},
		{"top-level-groups", func(m *Manifest) { m.Groups = nil }},
		{"top-level-fields", func(m *Manifest) { m.Fields = nil }},
		{"top-level-views", func(m *Manifest) { m.Views = nil }},
		{"top-level-actions", func(m *Manifest) { m.Actions = nil }},
		{"top-level-diagnostics", func(m *Manifest) { m.Diagnostics = nil }},
		{"view-field-ids", func(m *Manifest) { m.Views[0].FieldIDs = nil }},
		{"view-diagnostic-ids", func(m *Manifest) { m.Views[0].DiagnosticIDs = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := cloneManifest(t, c.Manifests[0])
			tc.mutate(&m)
			if _, err := Canonicalize(m, idx); err == nil || !strings.Contains(err.Error(), "nil") {
				t.Fatalf("typed nil input was canonicalized: %v", err)
			}
		})
	}
}

func TestCanonicalDigestBoundsDirectManifestAndCanonicalizesUnorderedArrays(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	idx := indexFromCatalog(c)
	oversized := cloneManifest(t, c.Manifests[0])
	oversized.Groups[0].Label.Default = strings.Repeat("x", MaxManifestBytes)
	if _, err := CanonicalDigest(oversized, idx); err == nil || !strings.Contains(err.Error(), "canonical manifest exceeds") {
		t.Fatalf("error=%v", err)
	}
	if err := NewRegistry(idx).Accept(oversized, "spoof"); err == nil {
		t.Fatal("registry accepted oversized direct manifest")
	}

	first := cloneManifest(t, c.Manifests[0])
	first.Requires.Packs = append(first.Requires.Packs, PackRef{ID: "helianthus.pack.pv", Version: "1.0.0"})
	first.Fields = append(first.Fields, Field{ID: "temperature-copy", Group: "zone", Label: Label{Key: "thermal.temperature.copy", Default: "Temperature copy"}, Ref: first.Fields[0].Ref, ServiceRef: first.Fields[0].ServiceRef, CapabilityRef: first.Fields[0].CapabilityRef, UnitRef: first.Fields[0].UnitRef, Order: 20})
	first.Views[0].FieldIDs = []string{"temperature", "temperature-copy"}
	first.Diagnostics = []Diagnostic{{ID: "d-a", Group: "zone", Label: Label{Key: "d.a", Default: "A"}, MemberID: "a", Kind: "field", Order: 10}, {ID: "d-b", Group: "zone", Label: Label{Key: "d.b", Default: "B"}, MemberID: "b", Kind: "field", Order: 20}}
	first.Views[0].DiagnosticIDs = []string{"d-a", "d-b"}
	idx.AddNativeMember(first.Contributor.NativeContract, "field", "a")
	idx.AddNativeMember(first.Contributor.NativeContract, "field", "b")
	second := cloneManifest(t, first)
	second.Requires.Packs[0], second.Requires.Packs[1] = second.Requires.Packs[1], second.Requires.Packs[0]
	second.Views[0].FieldIDs[0], second.Views[0].FieldIDs[1] = second.Views[0].FieldIDs[1], second.Views[0].FieldIDs[0]
	second.Views[0].DiagnosticIDs[0], second.Views[0].DiagnosticIDs[1] = second.Views[0].DiagnosticIDs[1], second.Views[0].DiagnosticIDs[0]
	firstDigest, err := CanonicalDigest(first, idx)
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := CanonicalDigest(second, idx)
	if err != nil {
		t.Fatal(err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("reordered equivalent descriptors differ: %s != %s", firstDigest, secondDigest)
	}
	r := NewRegistry(idx)
	if err := r.Accept(first, "spoof"); err != nil {
		t.Fatal(err)
	}
	if err := r.Accept(second, "spoof"); err != nil {
		t.Fatalf("reordered descriptor quarantined: %v", err)
	}
}

func TestStaticIndexTypedKeysDoNotAliasDelimitedValues(t *testing.T) {
	packA, packB := PackRef{ID: "a@b", Version: "c"}, PackRef{ID: "a", Version: "b@c"}
	definitionA, definitionB := DefinitionRef{Pack: packA, ID: "d/e", Version: "f"}, DefinitionRef{Pack: PackRef{ID: "a@b", Version: "c/d"}, ID: "e", Version: "f"}
	nativeA, nativeB := NativeContractRef{Owner: "a", Contract: "b/c", Version: "d"}, NativeContractRef{Owner: "a/b", Contract: "c", Version: "d"}
	index := NewStaticIndex()
	index.AddPack(packA)
	index.AddDefinition(definitionA)
	index.AddCanonicalUnit(definitionA, definitionA)
	index.AddServiceCapability(definitionA, definitionA)
	index.AddFieldRelation(definitionA, definitionA, definitionA)
	index.AddOperation(definitionA, definitionA, definitionA, definitionA, definitionA)
	index.AddNativeMember(nativeA, "field", "x")
	if index.HasPack(packB) || index.HasDefinition(definitionB) {
		t.Fatal("delimiter aliases matched static keys")
	}
	if _, ok := index.CanonicalUnit(definitionB); ok {
		t.Fatal("unit alias matched")
	}
	if index.ServiceOwnsCapability(definitionB, definitionB) || index.FieldMatches(definitionB, definitionB, definitionB) || index.OperationMatches(definitionB, definitionB, definitionB, definitionB, definitionB) || index.HasNativeMember(nativeB, "field", "x") {
		t.Fatal("composite static relation alias matched")
	}
}

func TestRegistryConcurrentSameAndDifferentDescriptorsAreDeterministic(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	idx := indexFromCatalog(c)
	for _, tc := range []struct {
		name        string
		manifests   []Manifest
		wantSuccess int
	}{
		{"same", repeatManifest(t, c.Manifests[0], 32), 32},
		{"different", differentlyLabeledManifests(t, c.Manifests[0], 32), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRegistry(idx)
			start := make(chan struct{})
			results := make(chan error, 32)
			for i := 0; i < 32; i++ {
				m := tc.manifests[i]
				go func() { <-start; results <- r.Accept(m, "sha256:spoofed") }()
			}
			close(start)
			success := 0
			for i := 0; i < 32; i++ {
				if <-results == nil {
					success++
				}
			}
			if success != tc.wantSuccess {
				t.Fatalf("success=%d, want %d", success, tc.wantSuccess)
			}
		})
	}
}

func repeatManifest(t *testing.T, source Manifest, count int) []Manifest {
	t.Helper()
	items := make([]Manifest, count)
	for i := range items {
		items[i] = cloneManifest(t, source)
	}
	return items
}

func differentlyLabeledManifests(t *testing.T, source Manifest, count int) []Manifest {
	t.Helper()
	items := repeatManifest(t, source, count)
	for i := range items {
		items[i].Groups[0].Label.Default += string(rune('a' + i))
	}
	return items
}

func TestCanonicalizeSortsDefensivelyWithoutMutatingInput(t *testing.T) {
	var c fixtureCatalog
	readFixture(t, "five-domain-catalog.json", &c)
	m := cloneManifest(t, c.Manifests[0])
	m.Groups = append(m.Groups, Group{ID: "system", Label: Label{Key: "thermal.system", Default: "System"}, ResourceContext: "system", Order: 5})
	copyOfInput := cloneManifest(t, m)
	canonical, err := Canonicalize(m, indexFromCatalog(c))
	if err != nil {
		t.Fatal(err)
	}
	if canonical.Groups[0].ID != "system" || canonical.Groups[1].ID != "zone" {
		t.Fatalf("groups not sorted: %#v", canonical.Groups)
	}
	if m.Groups[0].ID != copyOfInput.Groups[0].ID || m.Groups[1].ID != copyOfInput.Groups[1].ID {
		t.Fatal("canonicalization mutated input")
	}
}
