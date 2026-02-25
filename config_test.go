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
