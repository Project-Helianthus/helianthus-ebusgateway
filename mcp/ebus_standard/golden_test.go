package ebus_standard_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	estd "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
)

// Envelope-golden fixtures. Re-generate with UPDATE=1 and include rationale
// in the PR body per canonical plan §"ENVELOPE_GOLDEN_TESTS".
//
// Determinism requirements (canonical plan §"DETERMINISM_HASH"):
//   - data_hash in meta is canonical-JSON SHA-256; stable across runs.
//   - data field is emitted with sorted keys via marshalWithSortedKeys so
//     the on-disk fixture is byte-stable under Go marshal reordering.

func goldenPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join("testdata", name+".golden.json")
}

func marshalEnvelope(t *testing.T, env map[string]any) []byte {
	t.Helper()
	// Use json.Marshal — Go sorts map keys alphabetically on marshal for
	// map[string]any, which is sufficient for stable golden output.
	buf, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return append(buf, '\n')
}

func compareOrUpdate(t *testing.T, name string, got []byte) {
	t.Helper()
	path := goldenPath(t, name)
	if os.Getenv("UPDATE") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v (run with UPDATE=1 to create)", path, err)
	}
	if string(got) != string(want) {
		t.Fatalf("golden mismatch for %s:\n--- want ---\n%s\n--- got ---\n%s",
			path, want, got)
	}
}

// fixed timestamp so fixtures are stable across runs.
var fixedTS = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestGolden_ServicesList(t *testing.T) {
	srv := estd.NewServer(syntheticGoldenCatalog())
	data := srv.ServicesList()
	env := estd.NewEnvelope(data, nil, fixedTS)
	// Overwrite the data_hash with a deterministic recomputation so the
	// value on disk matches DataHash(data). This is redundant (NewEnvelope
	// already computes it) but pins the contract in the test.
	env["meta"].(map[string]any)["data_hash"] = estd.DataHash(data)
	compareOrUpdate(t, "services_list", marshalEnvelope(t, env))
}

func TestGolden_CommandsList(t *testing.T) {
	srv := estd.NewServer(syntheticGoldenCatalog())
	pb := uint8(0x03)
	data, err := srv.CommandsList(&pb)
	if err != nil {
		t.Fatal(err)
	}
	env := estd.NewEnvelope(data, nil, fixedTS)
	compareOrUpdate(t, "commands_list_pb03", marshalEnvelope(t, env))
}

func TestGolden_CommandGet(t *testing.T) {
	srv := estd.NewServer(syntheticGoldenCatalog())
	data, err := srv.CommandGet("ebus_standard.golden.alpha")
	if err != nil {
		t.Fatal(err)
	}
	env := estd.NewEnvelope(data, nil, fixedTS)
	compareOrUpdate(t, "command_get_alpha", marshalEnvelope(t, env))
}

func TestGolden_Decode(t *testing.T) {
	srv := estd.NewServer(syntheticGoldenCatalog())
	data, err := srv.Decode(estd.DecodeInput{
		PB: 0x03, SB: 0x04,
		Direction:  "request",
		FrameType:  "addressed",
		PayloadHex: "0102",
	})
	if err != nil {
		t.Fatal(err)
	}
	env := estd.NewEnvelope(data, nil, fixedTS)
	compareOrUpdate(t, "decode_alpha", marshalEnvelope(t, env))
}
