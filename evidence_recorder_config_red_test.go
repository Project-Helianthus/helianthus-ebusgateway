package ebusgateway

import "testing"

func TestMSP065EvidenceRecorderConfigV1IsDisabledAndInertByDefault(t *testing.T) {
	cfg := DefaultConfig().EvidenceRecorderConfig
	if cfg.Version != 1 {
		t.Fatalf("default evidence recorder version = %d, want 1", cfg.Version)
	}
	if cfg.Enabled {
		t.Fatal("evidence recorder is enabled by default")
	}
	if cfg.StateRoot != "" || cfg.Retention != 0 || cfg.QuotaBytes != 0 {
		t.Fatalf("disabled recorder contains active fields: %#v", cfg)
	}
	if err := ValidateEvidenceRecorderConfig(cfg); err != nil {
		t.Fatalf("disabled default config rejected: %v", err)
	}
}

func TestMSP065EvidenceRecorderConfigRejectsPartialOrUnsafeEnablement(t *testing.T) {
	base := DefaultEvidenceRecorderConfig()
	base.Enabled = true
	tests := []struct {
		name   string
		mutate func(*EvidenceRecorderConfig)
	}{
		{name: "missing root"},
		{name: "relative root", mutate: func(cfg *EvidenceRecorderConfig) { cfg.StateRoot = "relative" }},
		{name: "filesystem root", mutate: func(cfg *EvidenceRecorderConfig) { cfg.StateRoot = "/" }},
		{name: "missing retention", mutate: func(cfg *EvidenceRecorderConfig) { cfg.StateRoot = "/data/evidence" }},
		{name: "missing quota", mutate: func(cfg *EvidenceRecorderConfig) {
			cfg.StateRoot = "/data/evidence"
			cfg.Retention = DefaultEvidenceRecorderRetention
		}},
		{name: "above hard limit", mutate: func(cfg *EvidenceRecorderConfig) {
			cfg.StateRoot = "/data/evidence"
			cfg.Retention = DefaultEvidenceRecorderRetention
			cfg.QuotaBytes = DefaultEvidenceRecorderQuotaBytes
			cfg.Limits.MaxSources = HardMaxEvidenceSources + 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := base
			if test.mutate != nil {
				test.mutate(&cfg)
			}
			if err := ValidateEvidenceRecorderConfig(cfg); err == nil {
				t.Fatalf("ValidateEvidenceRecorderConfig(%#v) succeeded", cfg)
			}
		})
	}
}
