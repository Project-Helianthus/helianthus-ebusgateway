package candidatefacts

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestMSP07VisibilityIsCandidateDebugOnlyAndNeverCommandCapable(t *testing.T) {
	artifacts := pinnedTestArtifactsV1()
	graph := decodeObject(t, artifacts.PositiveGraph)
	visibility := objectAt(t, graph, "visibility")
	want := map[string]any{
		"channel":              "CANDIDATE_DEBUG_REPLAY",
		"promotion_state":      "NOT_PROMOTED",
		"stable_exposure":      false,
		"command_capable":      false,
		"protocol_translation": false,
	}
	if len(visibility) != len(want) {
		t.Fatalf("visibility keys = %#v; want %#v", visibility, want)
	}
	for key, expected := range want {
		if visibility[key] != expected {
			t.Errorf("visibility.%s = %v; want %v", key, visibility[key], expected)
		}
	}
	for _, raw := range arrayAt(t, graph, "facts") {
		fact := raw.(map[string]any)
		if fact["debug_only"] != true {
			t.Errorf("fact %v debug_only = %v; want true", fact["candidate_id"], fact["debug_only"])
		}
	}
	assertCategory(t, Verify(artifacts.NegativeGraphs["anti-leak-stable-surface.json"], artifacts.SourceBundle, artifacts.SourceReplay), "anti_leak.consumer")
}

func TestMSP07ExistingStableConsumerSourcesContainNoCandidateContract(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	surfaces := map[string][]string{
		"ebus.v1 and eebus.v1 MCP":         {"mcp"},
		"GraphQL and HA consumer contract": {"graphql"},
		"Portal":                           {"portal", "ui"},
		"command routing":                  {"cmd"},
		"promoted semantic outputs":        {"matter"},
	}
	markers := []string{
		"helianthus.platform.draft-candidate-fact-graph.v1",
		"helianthus.platform.draft-candidate-fact-replay.v1",
		"helianthus.platform.draft-candidate-fact-registry.v1",
		"CANDIDATE_DEBUG_REPLAY",
		"draft-candidate-fact",
		"dcfgv1:sha256:",
		"dcfrv1:sha256:",
		"m7-candidate-",
	}

	for surface, roots := range surfaces {
		for _, relativeRoot := range roots {
			root := filepath.Join(repoRoot, relativeRoot)
			if _, err := os.Stat(root); err != nil {
				if os.IsNotExist(err) {
					continue
				}
				t.Fatal(err)
			}
			err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.IsDir() || !stableSurfaceTextFile(path) {
					return nil
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				for _, marker := range markers {
					if strings.Contains(string(raw), marker) {
						t.Errorf("%s leaked candidate marker %q in %s", surface, marker, filepath.ToSlash(path))
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func stableSurfaceTextFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".go", ".graphql", ".json", ".html", ".js", ".css", ".py", ".yaml", ".yml":
		return true
	default:
		return false
	}
}
