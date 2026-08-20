package ebusgateway

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfig_DisablesDumpUploadPath(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.DumpUploadPath != "" {
		t.Fatalf("expected DumpUploadPath empty by default, got %q", cfg.DumpUploadPath)
	}
}

func TestApplyDefaults_DoesNotEnableDumpUploadPath(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.DumpUploadPath != "" {
		t.Fatalf("expected DumpUploadPath empty after defaults, got %q", cfg.DumpUploadPath)
	}
}

func TestDefaultConfig_SetsScanSource(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ScanSource != 0xF0 {
		t.Fatalf("expected ScanSource=0xf0 by default, got 0x%02x", cfg.ScanSource)
	}
}

func TestApplyDefaults_SetsScanSource(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.ScanSource != 0xF0 {
		t.Fatalf("expected ScanSource=0xf0 after defaults, got 0x%02x", cfg.ScanSource)
	}
}

func TestApplyDefaults_PreservesAutoScanSource(t *testing.T) {
	cfg := applyDefaults(Config{
		ScanSource:     0x00,
		ScanSourceAuto: true,
	})
	if cfg.ScanSource != 0x00 {
		t.Fatalf("expected ScanSource=0x00 for auto mode, got 0x%02x", cfg.ScanSource)
	}
}

func TestDefaultConfig_SetsBootLiveTimeout(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.BootLiveTimeout != 2*time.Minute {
		t.Fatalf("expected BootLiveTimeout=2m by default, got %s", cfg.BootLiveTimeout)
	}
}

func TestApplyDefaults_SetsBootLiveTimeout(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.BootLiveTimeout != 2*time.Minute {
		t.Fatalf("expected BootLiveTimeout=2m after defaults, got %s", cfg.BootLiveTimeout)
	}
}

func TestDefaultConfig_SetsSemanticCachePath(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.SemanticCachePath != "./semantic_cache.json" {
		t.Fatalf("expected SemanticCachePath=./semantic_cache.json by default, got %q", cfg.SemanticCachePath)
	}
}

func TestApplyDefaults_SetsSemanticCachePath(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.SemanticCachePath != "./semantic_cache.json" {
		t.Fatalf("expected SemanticCachePath=./semantic_cache.json after defaults, got %q", cfg.SemanticCachePath)
	}
}

func TestDefaultConfig_SetsSemanticReadBreakerDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.SemanticReadBreakerFailureBudget != DefaultSemanticReadFailureBudget {
		t.Fatalf("expected SemanticReadBreakerFailureBudget=%d by default, got %d", DefaultSemanticReadFailureBudget, cfg.SemanticReadBreakerFailureBudget)
	}
	if cfg.SemanticReadBreakerOpenCooldown != DefaultSemanticReadOpenCooldown {
		t.Fatalf("expected SemanticReadBreakerOpenCooldown=%s by default, got %s", DefaultSemanticReadOpenCooldown, cfg.SemanticReadBreakerOpenCooldown)
	}
	if cfg.SemanticReadBreakerHalfOpenProbeLimit != DefaultSemanticReadHalfOpenProbeLimit {
		t.Fatalf("expected SemanticReadBreakerHalfOpenProbeLimit=%d by default, got %d", DefaultSemanticReadHalfOpenProbeLimit, cfg.SemanticReadBreakerHalfOpenProbeLimit)
	}
}

func TestApplyDefaults_SetsSemanticReadBreakerDefaults(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.SemanticReadBreakerFailureBudget != DefaultSemanticReadFailureBudget {
		t.Fatalf("expected SemanticReadBreakerFailureBudget=%d after defaults, got %d", DefaultSemanticReadFailureBudget, cfg.SemanticReadBreakerFailureBudget)
	}
	if cfg.SemanticReadBreakerOpenCooldown != DefaultSemanticReadOpenCooldown {
		t.Fatalf("expected SemanticReadBreakerOpenCooldown=%s after defaults, got %s", DefaultSemanticReadOpenCooldown, cfg.SemanticReadBreakerOpenCooldown)
	}
	if cfg.SemanticReadBreakerHalfOpenProbeLimit != DefaultSemanticReadHalfOpenProbeLimit {
		t.Fatalf("expected SemanticReadBreakerHalfOpenProbeLimit=%d after defaults, got %d", DefaultSemanticReadHalfOpenProbeLimit, cfg.SemanticReadBreakerHalfOpenProbeLimit)
	}
}

func TestApplyDefaults_PreservesExplicitDisabledSemanticReadBreakerBudget(t *testing.T) {
	cfg := applyDefaults(Config{
		SemanticReadBreakerFailureBudget:    0,
		SemanticReadBreakerFailureBudgetSet: true,
	})
	if cfg.SemanticReadBreakerFailureBudget != 0 {
		t.Fatalf("expected SemanticReadBreakerFailureBudget=0 to remain disabled, got %d", cfg.SemanticReadBreakerFailureBudget)
	}
}

func TestApplyDefaults_UsesDefaultBudgetForPartialBreakerOverrides(t *testing.T) {
	cfg := applyDefaults(Config{
		SemanticReadBreakerOpenCooldown: 20 * time.Second,
	})
	if cfg.SemanticReadBreakerFailureBudget != DefaultSemanticReadFailureBudget {
		t.Fatalf("expected SemanticReadBreakerFailureBudget=%d with partial override, got %d", DefaultSemanticReadFailureBudget, cfg.SemanticReadBreakerFailureBudget)
	}
	if cfg.SemanticReadBreakerOpenCooldown != 20*time.Second {
		t.Fatalf("expected SemanticReadBreakerOpenCooldown=20s, got %s", cfg.SemanticReadBreakerOpenCooldown)
	}
}

func TestDefaultConfig_SetsSemanticZonePresenceThresholdDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.SemanticZonePresenceMissThreshold != DefaultSemanticZonePresenceMissThreshold {
		t.Fatalf("expected SemanticZonePresenceMissThreshold=%d by default, got %d", DefaultSemanticZonePresenceMissThreshold, cfg.SemanticZonePresenceMissThreshold)
	}
	if cfg.SemanticZonePresenceHitThreshold != DefaultSemanticZonePresenceHitThreshold {
		t.Fatalf("expected SemanticZonePresenceHitThreshold=%d by default, got %d", DefaultSemanticZonePresenceHitThreshold, cfg.SemanticZonePresenceHitThreshold)
	}
}

func TestApplyDefaults_SetsSemanticZonePresenceThresholdDefaults(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.SemanticZonePresenceMissThreshold != DefaultSemanticZonePresenceMissThreshold {
		t.Fatalf("expected SemanticZonePresenceMissThreshold=%d after defaults, got %d", DefaultSemanticZonePresenceMissThreshold, cfg.SemanticZonePresenceMissThreshold)
	}
	if cfg.SemanticZonePresenceHitThreshold != DefaultSemanticZonePresenceHitThreshold {
		t.Fatalf("expected SemanticZonePresenceHitThreshold=%d after defaults, got %d", DefaultSemanticZonePresenceHitThreshold, cfg.SemanticZonePresenceHitThreshold)
	}
}

func TestApplyDefaults_PreservesExplicitSemanticZonePresenceThresholds(t *testing.T) {
	cfg := applyDefaults(Config{
		SemanticZonePresenceMissThreshold: 5,
		SemanticZonePresenceHitThreshold:  4,
	})
	if cfg.SemanticZonePresenceMissThreshold != 5 {
		t.Fatalf("expected SemanticZonePresenceMissThreshold=5, got %d", cfg.SemanticZonePresenceMissThreshold)
	}
	if cfg.SemanticZonePresenceHitThreshold != 4 {
		t.Fatalf("expected SemanticZonePresenceHitThreshold=4, got %d", cfg.SemanticZonePresenceHitThreshold)
	}
}

func TestDefaultConfig_SetsSemanticDHWStaleTTL(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.SemanticDHWStaleTTL != DefaultSemanticDHWStaleTTL {
		t.Fatalf("expected SemanticDHWStaleTTL=%s by default, got %s", DefaultSemanticDHWStaleTTL, cfg.SemanticDHWStaleTTL)
	}
}

func TestApplyDefaults_SetsSemanticDHWStaleTTL(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.SemanticDHWStaleTTL != DefaultSemanticDHWStaleTTL {
		t.Fatalf("expected SemanticDHWStaleTTL=%s after defaults, got %s", DefaultSemanticDHWStaleTTL, cfg.SemanticDHWStaleTTL)
	}
}

func TestApplyDefaults_PreservesExplicitSemanticDHWStaleTTL(t *testing.T) {
	cfg := applyDefaults(Config{
		SemanticDHWStaleTTL: 42 * time.Minute,
	})
	if cfg.SemanticDHWStaleTTL != 42*time.Minute {
		t.Fatalf("expected SemanticDHWStaleTTL=42m, got %s", cfg.SemanticDHWStaleTTL)
	}
}

func TestDefaultConfig_SetsSemanticEnergyInterval(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.SemanticEnergyInterval != DefaultSemanticEnergyInterval {
		t.Fatalf("expected SemanticEnergyInterval=%s by default, got %s", DefaultSemanticEnergyInterval, cfg.SemanticEnergyInterval)
	}
}

func TestApplyDefaults_SetsSemanticEnergyInterval(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.SemanticEnergyInterval != DefaultSemanticEnergyInterval {
		t.Fatalf("expected SemanticEnergyInterval=%s after defaults, got %s", DefaultSemanticEnergyInterval, cfg.SemanticEnergyInterval)
	}
}

func TestApplyDefaults_PreservesExplicitSemanticEnergyInterval(t *testing.T) {
	cfg := applyDefaults(Config{
		SemanticEnergyInterval: 12 * time.Minute,
	})
	if cfg.SemanticEnergyInterval != 12*time.Minute {
		t.Fatalf("expected SemanticEnergyInterval=12m, got %s", cfg.SemanticEnergyInterval)
	}
}

func TestDefaultConfig_SetsPortalPath(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.PortalPath != "/portal" {
		t.Fatalf("expected PortalPath=/portal by default, got %q", cfg.PortalPath)
	}
}

func TestIssue817ConfigHasNoEEBusSpecificAdminOrCredentialSurface(t *testing.T) {
	content, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(content)
	for _, forbidden := range []string{
		"type EEBusAdminConfig struct",
		"DefaultEEBusAdminConfig",
		"EEBusAdminConfig",
	} {
		if strings.Contains(source, forbidden) {
			t.Errorf("config.go retains eeBUS-specific auth/config symbol %q", forbidden)
		}
	}
}

func TestApplyDefaults_SetsPortalPath(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.PortalPath != "/portal" {
		t.Fatalf("expected PortalPath=/portal after defaults, got %q", cfg.PortalPath)
	}
}

func TestDefaultConfig_DisablesM2MGraphQL(t *testing.T) {
	config := DefaultConfig().M2MGraphQL
	if !config.Disabled() || config.Validate() != nil {
		t.Fatalf("default M2M GraphQL config is not inert: %+v", config)
	}
}

func TestM2MGraphQLConfig_RejectsIncompleteActiveConfiguration(t *testing.T) {
	config := M2MGraphQLConfig{ListenAddr: "127.0.0.1:8443", AllowedAssets: []string{"pv-one"}}
	if err := config.Validate(); err == nil {
		t.Fatal("incomplete M2M GraphQL config was accepted")
	}
}

func TestM2MGraphQLConfig_RejectsMalformedOrDuplicateAuthority(t *testing.T) {
	base := M2MGraphQLConfig{
		ListenAddr: "127.0.0.1:8443", ServerName: "m2m.gateway.test", ClientCAFile: "ca.pem",
		ServerCertFile: "server.pem", ServerKeyFile: "server-key.pem", AllowedAssets: []string{"pv-one"}, KnownAssets: []string{"pv-one"},
	}
	malformed := base
	malformed.DeniedPrincipalFingerprints = []string{strings.Repeat("z", 64)}
	duplicate := base
	duplicate.AllowedAssets = []string{"pv-one", "pv-one"}
	for _, config := range []M2MGraphQLConfig{malformed, duplicate} {
		if err := config.Validate(); err == nil {
			t.Fatalf("invalid M2M GraphQL authority accepted: %+v", config)
		}
	}
}

func TestM2MGraphQLConfig_RejectsKnownAssetOutsideAllowlist(t *testing.T) {
	config := M2MGraphQLConfig{
		ListenAddr: "127.0.0.1:8443", ServerName: "m2m.gateway.test", ClientCAFile: "ca.pem",
		ServerCertFile: "server.pem", ServerKeyFile: "server-key.pem", AllowedAssets: []string{"pv-one"}, KnownAssets: []string{"pv-two"},
	}
	if err := config.Validate(); err == nil {
		t.Fatal("known asset outside authorization allowlist was accepted")
	}
}
