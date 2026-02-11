package ebusgateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestResolveRegisterDumpJSONPath(t *testing.T) {
	cfg := smokeConfig{}
	base := filepath.Join(t.TempDir(), "dump.log")
	got := resolveRegisterDumpJSONPath(cfg, base)
	want := filepath.Join(filepath.Dir(base), "dump.json")
	if got != want {
		t.Fatalf("resolveRegisterDumpJSONPath() = %q; want %q", got, want)
	}

	cfg.Smoke.RegisterDumpJSONOutput = filepath.Join(t.TempDir(), "custom.json")
	got = resolveRegisterDumpJSONPath(cfg, base)
	if got != cfg.Smoke.RegisterDumpJSONOutput {
		t.Fatalf("resolveRegisterDumpJSONPath() = %q; want %q", got, cfg.Smoke.RegisterDumpJSONOutput)
	}
}

func TestWriteRegisterDumpJSON(t *testing.T) {
	ts := time.Date(2026, 2, 11, 12, 0, 0, 0, time.UTC)
	payload := buildRegisterDumpJSON(ts, 0x15, "./data/vaillant/15.720.annotated.tsp", []registerDumpJSONEntry{
		{
			Method:   "get_ext_register",
			Group:    "0x01",
			Instance: "0x02",
			Address:  "0x0100",
			Raw:      "010203",
			Decoded:  "value=7",
		},
	})

	dir := t.TempDir()
	path := filepath.Join(dir, "register_dump.json")
	if err := writeRegisterDumpJSON(path, payload); err != nil {
		t.Fatalf("writeRegisterDumpJSON error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile error = %v", err)
	}

	var decoded registerDumpJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error = %v", err)
	}

	if decoded.Metadata.Timestamp != ts.Format(time.RFC3339Nano) {
		t.Fatalf("timestamp = %q; want %q", decoded.Metadata.Timestamp, ts.Format(time.RFC3339Nano))
	}
	if decoded.Metadata.Target != "0x15" {
		t.Fatalf("target = %q; want 0x15", decoded.Metadata.Target)
	}
	if decoded.Metadata.TSPSource != "./data/vaillant/15.720.annotated.tsp" {
		t.Fatalf("tsp = %q; want %q", decoded.Metadata.TSPSource, "./data/vaillant/15.720.annotated.tsp")
	}
	if decoded.Metadata.EntryCount != 1 {
		t.Fatalf("entry_count = %d; want 1", decoded.Metadata.EntryCount)
	}
	if len(decoded.Entries) != 1 {
		t.Fatalf("entries len = %d; want 1", len(decoded.Entries))
	}
	entry := decoded.Entries[0]
	if entry.Method != "get_ext_register" || entry.Group != "0x01" || entry.Instance != "0x02" || entry.Address != "0x0100" {
		t.Fatalf("entry mismatch: %+v", entry)
	}
	if entry.Raw != "010203" || entry.Decoded != "value=7" {
		t.Fatalf("entry payload mismatch: %+v", entry)
	}
}
