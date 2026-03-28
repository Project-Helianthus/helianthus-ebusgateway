package ebusgateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type replayCorpus struct {
	Name            string             `json:"name"`
	ValidationScope string             `json:"validation_scope"`
	Cases           []replayCorpusCase `json:"cases"`
}

type replayCorpusCase struct {
	Name           string                   `json:"name"`
	Transport      string                   `json:"transport"`
	Family         string                   `json:"family"`
	ResponseClass  string                   `json:"response_class"`
	ScenarioTags   []string                 `json:"scenario_tags"`
	Expected       *replayCorpusExpectation `json:"expected,omitempty"`
	ReplayExpected *replayCorpusExpectation `json:"replay_expected,omitempty"`
}

type replayCorpusExpectation struct {
	DirectApply bool   `json:"direct_apply"`
	Disposition string `json:"disposition"`
	Reason      string `json:"reason"`
}

type mirroredGatewayCorpus struct {
	Name                  string `json:"name"`
	ValidationScope       string `json:"validation_scope"`
	MirrorSourceWorkspace string `json:"mirror_source_workspace_path"`
	Cases                 []struct {
		Name string `json:"name"`
		File string `json:"file"`
	} `json:"cases"`
}

func TestObserveFirstReplayCorpusManifest(t *testing.T) {
	t.Parallel()

	path := filepath.Join("testdata", "observe_first_replay_cases.json")
	var corpus replayCorpus
	readReplayJSON(t, path, &corpus)

	if corpus.Name != "observe_first_gateway_replay_cases_v1" {
		t.Fatalf("manifest name = %q; want observe_first_gateway_replay_cases_v1", corpus.Name)
	}
	if corpus.ValidationScope != "fixture_presence_and_locked_falsification_contract" {
		t.Fatalf("validation_scope = %q; want fixture_presence_and_locked_falsification_contract", corpus.ValidationScope)
	}

	required := map[string]bool{
		"b509_value_bearing_enh":                 false,
		"b516_broadcast_ens":                     false,
		"b524_value_bearing_enh":                 false,
		"b555_record_invalidate_ens":             false,
		"short_reply_header_only":                false,
		"ack_only_master_master":                 false,
		"collision_episode":                      false,
		"timeout_no_progress":                    false,
		"delayed_reply_value_bearing":            false,
		"ebusd_tcp_passive_unavailable_negative": false,
	}

	for _, tc := range corpus.Cases {
		if tc.Name == "" {
			t.Fatalf("replay case missing name in %s", path)
		}
		if _, ok := required[tc.Name]; ok {
			required[tc.Name] = true
		}
	}

	for name, seen := range required {
		if !seen {
			t.Fatalf("required replay corpus case %q missing from %s", name, path)
		}
	}
}

func TestObserveFirstReplayCorpusLockedFalsificationExpectations(t *testing.T) {
	t.Parallel()

	path := filepath.Join("testdata", "observe_first_replay_cases.json")
	var corpus replayCorpus
	readReplayJSON(t, path, &corpus)

	lookup := func(name string) replayCorpusCase {
		t.Helper()
		for _, tc := range corpus.Cases {
			if tc.Name == name {
				return tc
			}
		}
		t.Fatalf("replay case %q missing from %s", name, path)
		return replayCorpusCase{}
	}

	b524 := lookup("b524_value_bearing_enh")
	if b524.ReplayExpected == nil {
		t.Fatalf("%s replay_expected contract missing", b524.Name)
	}
	if b524.ReplayExpected.DirectApply {
		t.Fatalf("%s direct_apply = true; want false", b524.Name)
	}
	if b524.ReplayExpected.Disposition != "ambiguity" {
		t.Fatalf("%s disposition = %q; want ambiguity", b524.Name, b524.ReplayExpected.Disposition)
	}
	if b524.ReplayExpected.Reason == "" {
		t.Fatalf("%s expected reason missing", b524.Name)
	}

	for _, name := range []string{"collision_episode", "timeout_no_progress"} {
		tc := lookup(name)
		if tc.ReplayExpected == nil {
			t.Fatalf("%s replay_expected contract missing", tc.Name)
		}
		if tc.ReplayExpected.DirectApply {
			t.Fatalf("%s direct_apply = true; want false", tc.Name)
		}
		if tc.ReplayExpected.Disposition != "falsification" {
			t.Fatalf("%s disposition = %q; want falsification", tc.Name, tc.ReplayExpected.Disposition)
		}
		if tc.ReplayExpected.Reason == "" {
			t.Fatalf("%s expected reason missing", tc.Name)
		}
	}
}

func TestB524ProofMirrorManifest(t *testing.T) {
	t.Parallel()

	dir := filepath.Join("testdata", "b524_proof_2026_03_05")
	manifestPath := filepath.Join(dir, "manifest.json")
	var manifest mirroredGatewayCorpus
	readReplayJSON(t, manifestPath, &manifest)

	if manifest.Name != "b524_proof_2026_03_05" {
		t.Fatalf("manifest name = %q; want b524_proof_2026_03_05", manifest.Name)
	}
	if manifest.ValidationScope != "corpus_presence_only" {
		t.Fatalf("validation_scope = %q; want corpus_presence_only", manifest.ValidationScope)
	}
	if manifest.MirrorSourceWorkspace != "_work_register_mapping/B524/proof_2026-03-05/" {
		t.Fatalf("mirror_source_workspace_path = %q; want _work_register_mapping/B524/proof_2026-03-05/", manifest.MirrorSourceWorkspace)
	}
	if len(manifest.Cases) < 17 {
		t.Fatalf("cases = %d; want at least 17 mirrored proof entries", len(manifest.Cases))
	}

	for _, tc := range manifest.Cases {
		if tc.Name == "" || tc.File == "" {
			t.Fatalf("mirrored proof case %+v is missing required metadata", tc)
		}
		if _, err := os.Stat(filepath.Join(dir, tc.File)); err != nil {
			t.Fatalf("mirrored proof file %s missing: %v", tc.File, err)
		}
	}
}

func TestB524WireLogsManifest(t *testing.T) {
	t.Parallel()

	dir := filepath.Join("testdata", "b524_wire_logs")
	manifestPath := filepath.Join(dir, "manifest.json")
	var manifest mirroredGatewayCorpus
	readReplayJSON(t, manifestPath, &manifest)

	if manifest.Name != "b524_wire_logs_seed_corpus" {
		t.Fatalf("manifest name = %q; want b524_wire_logs_seed_corpus", manifest.Name)
	}
	if manifest.ValidationScope != "corpus_presence_only" {
		t.Fatalf("validation_scope = %q; want corpus_presence_only", manifest.ValidationScope)
	}
	if manifest.MirrorSourceWorkspace != "_work_register_mapping/B524/" {
		t.Fatalf("mirror_source_workspace_path = %q; want _work_register_mapping/B524/", manifest.MirrorSourceWorkspace)
	}
	required := map[string]bool{
		"b524_discovery_scan_log":         false,
		"b524_groups_scan_schema_log":     false,
		"b524_discovery_scan_tail":        false,
		"b524_groups_scan_schema_tail":    false,
		"b524_groups_scan_schema_decoded": false,
	}
	if len(manifest.Cases) == 0 {
		t.Fatalf("cases = 0; want non-empty mirrored wire-log inventory")
	}

	for _, tc := range manifest.Cases {
		if tc.Name == "" || tc.File == "" {
			t.Fatalf("wire log case %+v is missing required metadata", tc)
		}
		if _, ok := required[tc.Name]; ok {
			required[tc.Name] = true
		}
		if _, err := os.Stat(filepath.Join(dir, tc.File)); err != nil {
			t.Fatalf("wire log file %s missing: %v", tc.File, err)
		}
	}
	for name, seen := range required {
		if !seen {
			t.Fatalf("required wire log corpus case %q missing from %s", name, manifestPath)
		}
	}
}

func readReplayJSON(t *testing.T, path string, target any) {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("Unmarshal(%s) error = %v", path, err)
	}
}
