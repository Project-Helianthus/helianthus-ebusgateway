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
	s := StaticIndex{Packs: map[string]bool{}, Definitions: map[string]bool{}, Units: map[string]DefinitionRef{}, ServiceCapabilities: map[string]bool{}, FieldRelations: map[string]bool{}, Operations: map[string]bool{}, NativeMembers: map[string]bool{}}
	for _, p := range c.Index.Packs {
		s.Packs[p.Key()] = true
	}
	for _, r := range c.Index.Definitions {
		s.Definitions[r.Key()] = true
	}
	for _, f := range c.Index.Fields {
		s.Definitions[f.Ref.Key()] = true
		s.Definitions[f.UnitRef.Key()] = true
		s.Definitions[f.ServiceRef.Key()] = true
		s.Definitions[f.CapabilityRef.Key()] = true
		s.Units[f.Ref.Key()] = f.UnitRef
		s.FieldRelations[f.Ref.Key()+"|"+f.ServiceRef.Key()+"|"+f.CapabilityRef.Key()] = true
	}
	for _, v := range c.Index.ServiceCapabilities {
		s.Definitions[v.Service.Key()] = true
		s.Definitions[v.Capability.Key()] = true
		s.ServiceCapabilities[v.Service.Key()+"|"+v.Capability.Key()] = true
	}
	for _, v := range c.Index.Operations {
		s.Definitions[v.Operation.Key()] = true
		s.Definitions[v.Capability.Key()] = true
		s.Definitions[v.Service.Key()] = true
		s.Definitions[v.Argument.Key()] = true
		s.Definitions[v.Effect.Key()] = true
		s.Operations[v.Operation.Key()+"|"+v.Capability.Key()+"|"+v.Service.Key()+"|"+v.Argument.Key()+"|"+v.Effect.Key()] = true
	}
	for _, v := range c.Index.NativeMembers {
		s.NativeMembers[v.Contract.Key()+"|"+v.Kind+"|"+v.ID] = true
	}
	return s
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
	if _, err := Decode(bad); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("error=%v", err)
	}
}

func TestDecodeRejectsTopLevelNull(t *testing.T) {
	if _, err := Decode([]byte("null")); err == nil || !strings.Contains(err.Error(), "top-level value must be an object") {
		t.Fatalf("error=%v", err)
	}
}

func TestIDLimitUsesUnicodeCodePoints(t *testing.T) {
	if err := id("id", strings.Repeat("é", 128)); err != nil {
		t.Fatalf("128 code points rejected: %v", err)
	}
	if err := id("id", strings.Repeat("é", 129)); err == nil {
		t.Fatal("129 code points accepted")
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
