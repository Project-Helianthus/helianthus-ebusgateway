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
	if cfg.StateRoot != "" {
		t.Fatalf("EEBusConfig.StateRoot = %q; want empty", cfg.StateRoot)
	}
	if cfg.DiscoveryEnabled {
		t.Fatal("EEBusConfig.DiscoveryEnabled = true; want false")
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

func TestMSP05BEEBusRuntimeCouplingIsConfinedToPrivateGatewaySeams(t *testing.T) {
	const runtimeImport = "github.com/Project-Helianthus/helianthus-eebusreg"
	goMod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	if !strings.Contains(string(goMod), runtimeImport) {
		t.Fatalf("go.mod is missing required MSP-05A-R1 dependency %q", runtimeImport)
	}

	allowedConfigReferences := []string{
		"cmd/gateway/eebus_config_flags.go",
		"cmd/gateway/eebus_runtime_adapter.go",
		"cmd/gateway/eebus_runtime_config.go",
		"cmd/gateway/main.go",
		"config.go",
	}
	allowedRuntimeImports := map[string]bool{
		"cmd/gateway/eebus_runtime_adapter.go": false,
		"cmd/gateway/eebus_runtime_config.go":  false,
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
		if strings.Contains(string(content), runtimeImport) {
			if _, allowed := allowedRuntimeImports[normalized]; !allowed {
				unexpected = append(unexpected, normalized+": runtime import outside private gateway seams")
			} else {
				allowedRuntimeImports[normalized] = true
			}
		}
		if strings.Contains(string(content), "EEBusConfig") && !slices.Contains(allowedConfigReferences, normalized) {
			unexpected = append(unexpected, normalized+": eeBUS config reference outside private gateway seams")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk production Go files: %v", err)
	}
	if len(unexpected) != 0 {
		t.Fatalf("MSP-05A-R1 eeBUS boundary violations: %v", unexpected)
	}
	for path, found := range allowedRuntimeImports {
		if !found {
			t.Errorf("%s does not import %q", path, runtimeImport)
		}
	}
}
