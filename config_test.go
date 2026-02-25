package ebusgateway

import (
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

func TestDefaultConfig_SetsPortalPath(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.PortalPath != "/portal" {
		t.Fatalf("expected PortalPath=/portal by default, got %q", cfg.PortalPath)
	}
}

func TestApplyDefaults_SetsPortalPath(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.PortalPath != "/portal" {
		t.Fatalf("expected PortalPath=/portal after defaults, got %q", cfg.PortalPath)
	}
}
