package ebusdscan

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAgentLocal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "AGENT-local.md")
	content := "text\n" +
		"```yaml\n" +
		"enh:\n" +
		"  host: \"10.0.0.2\"\n" +
		"  port: 9999\n" +
		"smoke:\n" +
		"  source_address: 0x31\n" +
		"  register_dump_tsp: \"./data/foo.tsp\"\n" +
		"  identify_b509_28xx: true\n" +
		"```\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	defaults, err := LoadAgentLocal(path)
	if err != nil {
		t.Fatalf("LoadAgentLocal error: %v", err)
	}
	if defaults.Host != "10.0.0.2" {
		t.Fatalf("host = %q; want 10.0.0.2", defaults.Host)
	}
	if defaults.Port != 9999 {
		t.Fatalf("port = %d; want 9999", defaults.Port)
	}
	if defaults.TSP != "./data/foo.tsp" {
		t.Fatalf("tsp = %q; want ./data/foo.tsp", defaults.TSP)
	}
	if !defaults.HasSource || defaults.Source != 0x31 {
		t.Fatalf("source = 0x%02x set=%v; want 0x31 set", defaults.Source, defaults.HasSource)
	}
	if !defaults.IdentifyB50928xx {
		t.Fatalf("identify flag = false; want true")
	}
}
