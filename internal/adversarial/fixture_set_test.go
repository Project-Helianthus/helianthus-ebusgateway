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
