package adversarial

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestManifestRejectsBindingMutations(t *testing.T) {
	type mutation struct {
		name  string
		apply func(*manifestV1, map[string][]byte)
	}
	mutations := []mutation{
		{"path_escape", func(m *manifestV1, _ map[string][]byte) { m.Artifacts[0].Path = "../escape" }},
		{"artifact_sort", func(m *manifestV1, _ map[string][]byte) {
			m.Artifacts[0], m.Artifacts[1] = m.Artifacts[1], m.Artifacts[0]
		}},
		{"size_mismatch", func(m *manifestV1, _ map[string][]byte) { m.Artifacts[0].SizeBytes++ }},
		{"digest_mismatch", func(m *manifestV1, _ map[string][]byte) { m.Artifacts[0].SHA256 = strings.Repeat("0", 64) }},
		{"unreferenced_resource", func(m *manifestV1, _ map[string][]byte) { m.Cases[0].ResourceArtifactIDs = []string{} }},
		{"duplicate_resource", func(m *manifestV1, _ map[string][]byte) {
			id := m.Cases[0].ResourceArtifactIDs[0]
			m.Cases[0].ResourceArtifactIDs = append(m.Cases[0].ResourceArtifactIDs, id)
		}},
		{"wrong_adv04_resource", func(m *manifestV1, _ map[string][]byte) {
			m.Cases[0].ResourceArtifactIDs = []string{m.Cases[1].ResourceArtifactIDs[0]}
		}},
		{"duplicate_case", func(m *manifestV1, _ map[string][]byte) { m.Cases = append(m.Cases, m.Cases[len(m.Cases)-1]) }},
		{"per_file_limit", func(m *manifestV1, _ map[string][]byte) { m.Artifacts[0].SizeBytes = maximumJSONBytes + 1 }},
		{"aggregate_limit", func(m *manifestV1, files map[string][]byte) {
			for i := range m.Artifacts {
				if m.Artifacts[i].Role == "cache-image" {
					data := make([]byte, maximumJSONBytes)
					files[m.Artifacts[i].Path] = data
					m.Artifacts[i].SizeBytes = int64(len(data))
					m.Artifacts[i].SHA256 = digest(data)
				}
			}
		}},
		{"duplicate_run", func(m *manifestV1, files map[string][]byte) {
			var first, second *manifestArtifact
			for i := range m.Artifacts {
				if m.Artifacts[i].ArtifactID == "driver-evaluated-fail" {
					first = &m.Artifacts[i]
				}
				if m.Artifacts[i].ArtifactID == "driver-execution-error" {
					second = &m.Artifacts[i]
				}
			}
			var a, b map[string]any
			_ = json.Unmarshal(files[first.Path], &a)
			_ = json.Unmarshal(files[second.Path], &b)
			b["run_id"] = a["run_id"]
			raw, _ := json.MarshalIndent(b, "", "  ")
			raw = append(raw, '\n')
			files[second.Path] = raw
			second.SizeBytes = int64(len(raw))
			second.SHA256 = digest(raw)
		}},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			m, files := freshManifest(t)
			tc.apply(&m, files)
			raw, _ := json.MarshalIndent(m, "", "  ")
			raw = append(raw, '\n')
			_, err := buildFixtureSetData(raw, func(p string) ([]byte, error) {
				b, ok := files[p]
				if !ok {
					return nil, errors.New("missing")
				}
				return b, nil
			}, false)
			if err == nil {
				t.Fatal("mutated manifest accepted")
			}
		})
	}
}

func TestFixtureResourceIdentifiersAndScenarioBinding(t *testing.T) {
	raw := fixtureBytes(t, "inputs", "offline-all-pass")
	base, err := ParseFixtureV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*FixtureV1)
	}{
		{"two_ids", func(f *FixtureV1) {
			f.Scenarios[3].ResourceArtifactIDs = []string{"cache-offline-all-pass-adv04", "cache-evaluated-fail-adv04"}
		}},
		{"malformed", func(f *FixtureV1) { f.Scenarios[3].ResourceArtifactIDs = []string{"Bad_ID"} }},
		{"overlong", func(f *FixtureV1) { f.Scenarios[3].ResourceArtifactIDs = []string{strings.Repeat("a", 65)} }},
		{"wrong_scenario", func(f *FixtureV1) { f.Scenarios[0].ResourceArtifactIDs = []string{"cache-offline-all-pass-adv04"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := cloneFixtureV1(base)
			tc.mutate(&f)
			changed, _ := json.Marshal(f)
			if _, err := ParseFixtureV1(changed); !errors.Is(err, ErrInvalidFixture) {
				t.Fatalf("ParseFixtureV1 accepted mutation: %v", err)
			}
			if _, err := BindFixtureDriverV1(changed); !errors.Is(err, ErrInvalidFixture) {
				t.Fatalf("BindFixtureDriverV1 accepted mutation: %v", err)
			}
		})
	}

	t.Run("wrong_bound_case_resource", func(t *testing.T) {
		f := cloneFixtureV1(base)
		f.Scenarios[3].ResourceArtifactIDs = []string{"cache-evaluated-fail-adv04"}
		changed, _ := json.Marshal(f)
		if _, err := ParseFixtureV1(changed); err != nil {
			t.Fatalf("structurally valid resource rejected: %v", err)
		}
		if _, err := BindFixtureDriverV1(changed); !errors.Is(err, ErrInvalidFixture) {
			t.Fatalf("manifest binding accepted wrong case resource: %v", err)
		}
	})
	for _, name := range fixtureCases {
		if _, err := ParseFixtureV1(fixtureBytes(t, "inputs", name)); err != nil {
			t.Fatalf("positive %s: %v", name, err)
		}
	}
}

func freshManifest(t *testing.T) (manifestV1, map[string][]byte) {
	t.Helper()
	raw, e := fixtureFiles.ReadFile("fixtures/v1/fixture-input-manifest.json")
	if e != nil {
		t.Fatal(e)
	}
	var m manifestV1
	if e = json.Unmarshal(raw, &m); e != nil {
		t.Fatal(e)
	}
	files := map[string][]byte{}
	for _, a := range m.Artifacts {
		b, e := fixtureFiles.ReadFile("fixtures/v1/" + a.Path)
		if e != nil {
			t.Fatal(e)
		}
		files[a.Path] = append([]byte(nil), b...)
	}
	return m, files
}
