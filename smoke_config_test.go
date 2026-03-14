package ebusgateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractYAMLBlocks(t *testing.T) {
	input := "\ntext\n```yaml\nenh:\n  type: unix\n  path: /tmp/ebusd.sock\n```\nskip\n```bash\necho \"noop\"\n```\n```yaml\nsmoke:\n  scan_timeout_sec: 7\n```\n"
	got := strings.TrimSpace(extractYAMLBlocks(input))
	want := strings.TrimSpace(`enh:
  type: unix
  path: /tmp/ebusd.sock
smoke:
  scan_timeout_sec: 7`)
	if got != want {
		t.Fatalf("extractYAMLBlocks() = %q; want %q", got, want)
	}
}

func TestLoadSmokeConfigFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "AGENT-local.md")
	content := "```yaml\nenh:\n  type: tcp\n  host: \"127.0.0.1\"\n  port: 7624\n  timeout_sec: 3\nexpected_devices:\n  - address: 0x08\n    description: \"Boiler\"\nsmoke:\n  source_address: 0xf0\n  verbose_frames: true\n  scan_timeout_sec: 4\n  method_timeout_sec: 6\n```\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}

	cfg, err := loadSmokeConfigFile(path)
	if err != nil {
		t.Fatalf("loadSmokeConfigFile error = %v", err)
	}
	if cfg.ENH.Type != "tcp" || cfg.ENH.Host != "127.0.0.1" || cfg.ENH.Port != 7624 {
		t.Fatalf("unexpected enh config: %+v", cfg.ENH)
	}
	if cfg.ENH.TimeoutSec != 3 {
		t.Fatalf("timeout = %d; want 3", cfg.ENH.TimeoutSec)
	}
	if cfg.Smoke.SourceAddress.Byte() != 0xF0 {
		t.Fatalf("source address = 0x%02x; want 0xf0", cfg.Smoke.SourceAddress.Byte())
	}
	if !cfg.Smoke.VerboseFrames || cfg.Smoke.ScanTimeoutSec != 4 || cfg.Smoke.MethodTimeoutSec != 6 {
		t.Fatalf("unexpected smoke config: %+v", cfg.Smoke)
	}
	if cfg.Smoke.Profile != string(TransportENH) {
		t.Fatalf("profile = %q; want %q", cfg.Smoke.Profile, string(TransportENH))
	}
	if len(cfg.ExpectedDevices) != 1 || cfg.ExpectedDevices[0].Address.Byte() != 0x08 {
		t.Fatalf("unexpected expected devices: %+v", cfg.ExpectedDevices)
	}
}

func TestLoadSmokeConfigFileProfileValidValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "AGENT-local.md")
	content := "```yaml\nenh:\n  type: tcp\n  host: \"127.0.0.1\"\n  port: 7624\nsmoke:\n  profile: ebusd-tcp\n```\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}

	cfg, err := loadSmokeConfigFile(path)
	if err != nil {
		t.Fatalf("loadSmokeConfigFile error = %v", err)
	}
	if cfg.Smoke.Profile != string(TransportEbusdTCP) {
		t.Fatalf("profile = %q; want %q", cfg.Smoke.Profile, string(TransportEbusdTCP))
	}
}

func TestLoadSmokeConfigFileProfileEbusdAlias(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "AGENT-local.md")
	content := "```yaml\nenh:\n  type: tcp\n  host: \"127.0.0.1\"\n  port: 7624\nsmoke:\n  profile: ebusd\n```\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}

	cfg, err := loadSmokeConfigFile(path)
	if err != nil {
		t.Fatalf("loadSmokeConfigFile error = %v", err)
	}
	if cfg.Smoke.Profile != string(TransportEbusdTCP) {
		t.Fatalf("profile = %q; want %q", cfg.Smoke.Profile, string(TransportEbusdTCP))
	}
}

func TestLoadSmokeConfigFileProfileENSValidValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "AGENT-local.md")
	content := "```yaml\nenh:\n  type: tcp\n  host: \"127.0.0.1\"\n  port: 7624\nsmoke:\n  profile: ens\n```\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}

	cfg, err := loadSmokeConfigFile(path)
	if err != nil {
		t.Fatalf("loadSmokeConfigFile error = %v", err)
	}
	if cfg.Smoke.Profile != string(TransportENS) {
		t.Fatalf("profile = %q; want %q", cfg.Smoke.Profile, string(TransportENS))
	}
}

func TestLoadSmokeConfigFileProfileInvalidValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "AGENT-local.md")
	content := "```yaml\nenh:\n  type: tcp\n  host: \"127.0.0.1\"\n  port: 7624\nsmoke:\n  profile: nope\n```\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}

	_, err := loadSmokeConfigFile(path)
	if err == nil {
		t.Fatalf("expected error for invalid profile")
	}
	if !strings.Contains(err.Error(), "unsupported smoke.profile") {
		t.Fatalf("error = %v; want unsupported smoke.profile", err)
	}
}

func TestLoadSmokeConfigMissingFile(t *testing.T) {
	_, err := loadSmokeConfigFile(filepath.Join(t.TempDir(), "AGENT-local.md"))
	if !errors.Is(err, errSmokeConfigMissing) {
		t.Fatalf("expected errSmokeConfigMissing, got %v", err)
	}
}

func TestRunSmokeFromEnvMissingConfig(t *testing.T) {
	t.Setenv("EBUS_SMOKE", "1")
	err := RunSmokeFromEnv(context.Background(), SmokeOptions{RootDir: t.TempDir()})
	if !errors.Is(err, errSmokeConfigMissing) {
		t.Fatalf("expected errSmokeConfigMissing, got %v", err)
	}
}

func TestLoadSmokeConfigFallbackToAGENTSLocal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "AGENTS-local.md")
	content := "```yaml\nenh:\n  type: tcp\n  host: \"127.0.0.1\"\n  port: 7624\nsmoke:\n  profile: ens\n```\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}

	cfg, loadedPath, err := loadSmokeConfig(dir)
	if err != nil {
		t.Fatalf("loadSmokeConfig error = %v", err)
	}
	if loadedPath != path {
		t.Fatalf("loaded path = %q; want %q", loadedPath, path)
	}
	if cfg.Smoke.Profile != string(TransportENS) {
		t.Fatalf("profile = %q; want %q", cfg.Smoke.Profile, string(TransportENS))
	}
}
