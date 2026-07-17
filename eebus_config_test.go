package ebusgateway

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestDefaultConfig_DisablesEEBusScaffold(t *testing.T) {
	cfg := DefaultConfig().EEBusConfig
	if cfg.Enabled {
		t.Fatal("EEBusConfig.Enabled = true; want false")
	}
	if cfg.ListenPort != 0 {
		t.Fatalf("EEBusConfig.ListenPort = %d; want 0", cfg.ListenPort)
	}
	if len(cfg.Interfaces) != 0 || len(cfg.Subnets) != 0 || len(cfg.RemoteSKIAllowlist) != 0 {
		t.Fatalf(
			"EEBusConfig lists = interfaces:%v subnets:%v allowlist:%v; want empty",
			cfg.Interfaces,
			cfg.Subnets,
			cfg.RemoteSKIAllowlist,
		)
	}
	if cfg.CertificatePath != "" || cfg.PrivateKeyPath != "" || cfg.TrustStorePath != "" {
		t.Fatalf(
			"EEBusConfig paths = cert:%q key:%q store:%q; want empty",
			cfg.CertificatePath,
			cfg.PrivateKeyPath,
			cfg.TrustStorePath,
		)
	}
	if cfg.PairingWindowMode != EEBusPairingWindowClosed {
		t.Fatalf(
			"EEBusConfig.PairingWindowMode = %q; want %q",
			cfg.PairingWindowMode,
			EEBusPairingWindowClosed,
		)
	}
}

func TestDefaultEEBusConfig_ReturnsIndependentEmptySlices(t *testing.T) {
	first := DefaultEEBusConfig()
	second := DefaultEEBusConfig()
	first.Interfaces = append(first.Interfaces, "en0")
	first.Subnets = append(first.Subnets, "192.0.2.0/24")
	first.RemoteSKIAllowlist = append(first.RemoteSKIAllowlist, strings.Repeat("a", 40))

	if len(second.Interfaces) != 0 || len(second.Subnets) != 0 || len(second.RemoteSKIAllowlist) != 0 {
		t.Fatalf("defaults share list backing state: %+v", second)
	}
}

func TestM5AEEBusScaffold_HasNoRuntimeOrModuleCoupling(t *testing.T) {
	const forbiddenImport = "github.com/Project-Helianthus/helianthus-eebusreg"
	goMod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	if strings.Contains(string(goMod), forbiddenImport) {
		t.Fatalf("go.mod contains forbidden M5A dependency %q", forbiddenImport)
	}

	allowedReferences := []string{
		"cmd/gateway/eebus_config_flags.go",
		"config.go",
	}
	var unexpected []string
	err = filepath.WalkDir(".", func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path == ".git" || strings.HasPrefix(path, ".git"+string(filepath.Separator)) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		normalized := filepath.ToSlash(strings.TrimPrefix(path, "."+string(filepath.Separator)))
		if strings.Contains(string(content), forbiddenImport) {
			unexpected = append(unexpected, normalized+": forbidden import")
		}
		if strings.Contains(string(content), "EEBusConfig") && !slices.Contains(allowedReferences, normalized) {
			unexpected = append(unexpected, normalized+": runtime reference outside config scaffold")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk production Go files: %v", err)
	}
	if len(unexpected) != 0 {
		t.Fatalf("M5A eeBUS boundary violations: %v", unexpected)
	}
}
