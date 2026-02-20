package ebusgateway

import "testing"

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
