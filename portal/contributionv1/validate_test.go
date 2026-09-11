package contributionv1

import (
	"bytes"
	"encoding/json"
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
			Ref     DefinitionRef `json:"ref"`
			UnitRef DefinitionRef `json:"unit_ref"`
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
	s := StaticIndex{Packs: map[string]bool{}, Definitions: map[string]bool{}, Units: map[string]DefinitionRef{}, ServiceCapabilities: map[string]bool{}, Operations: map[string]bool{}, NativeMembers: map[string]bool{}}
	for _, p := range c.Index.Packs {
		s.Packs[p.Key()] = true
	}
	for _, r := range c.Index.Definitions {
		s.Definitions[r.Key()] = true
	}
	for _, f := range c.Index.Fields {
		s.Definitions[f.Ref.Key()] = true
		s.Definitions[f.UnitRef.Key()] = true
		s.Units[f.Ref.Key()] = f.UnitRef
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
	if err := r.Accept(c.Manifests[0], "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"); err == nil {
		t.Fatal("digest conflict accepted")
	}
	if err := r.Accept(c.Manifests[0], "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err == nil {
		t.Fatal("quarantined manifest was re-accepted")
	}
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
