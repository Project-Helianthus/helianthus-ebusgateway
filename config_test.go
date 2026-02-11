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
