package ebusgateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type replayCorpus struct {
	Name            string `json:"name"`
	ValidationScope string `json:"validation_scope"`
	Cases           []struct {
		Name string `json:"name"`
	} `json:"cases"`
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
	if corpus.ValidationScope != "fixture_presence_and_shape_only" {
		t.Fatalf("validation_scope = %q; want fixture_presence_and_shape_only", corpus.ValidationScope)
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
