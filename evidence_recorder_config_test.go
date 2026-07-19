package ebusgateway

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestEvidenceRecorderConfigAcceptsExplicitBoundedV1(t *testing.T) {
	cfg := DefaultEvidenceRecorderConfig()
	cfg.Enabled = true
	cfg.Scope = EvidenceRecorderScopeV1
	cfg.StateRoot = "/data/evidence"
	cfg.Retention = DefaultEvidenceRecorderRetention
	cfg.QuotaBytes = DefaultEvidenceRecorderQuotaBytes
	cfg.Limits = DefaultEvidenceRecorderLimits()

	if err := ValidateEvidenceRecorderConfig(cfg); err != nil {
		t.Fatalf("ValidateEvidenceRecorderConfig() error = %v", err)
	}
}

func TestEvidenceRecorderConfigRejectsActiveFieldsWhileDisabled(t *testing.T) {
	cfg := DefaultEvidenceRecorderConfig()
	cfg.StateRoot = "/data/evidence"

	if err := ValidateEvidenceRecorderConfig(cfg); !errors.Is(err, ErrEvidenceRecorderDisabled) {
		t.Fatalf("ValidateEvidenceRecorderConfig() error = %v, want %v", err, ErrEvidenceRecorderDisabled)
	}
}

func TestEvidenceRecorderConfigRejectsEveryV1LimitAboveCeiling(t *testing.T) {
	valid := DefaultEvidenceRecorderConfig()
	valid.Enabled = true
	valid.Scope = EvidenceRecorderScopeV1
	valid.StateRoot = "/data/evidence"
	valid.Retention = DefaultEvidenceRecorderRetention
	valid.QuotaBytes = DefaultEvidenceRecorderQuotaBytes
	valid.Limits = DefaultEvidenceRecorderLimits()

	tests := []struct {
		name   string
		mutate func(*EvidenceRecorderLimits)
	}{
		{name: "sources", mutate: func(v *EvidenceRecorderLimits) { v.MaxSources = HardMaxEvidenceSources + 1 }},
		{name: "items", mutate: func(v *EvidenceRecorderLimits) { v.MaxItemsPerSource = HardMaxEvidenceItemsPerSource + 1 }},
		{name: "artifact bytes", mutate: func(v *EvidenceRecorderLimits) { v.MaxArtifactBytes = HardMaxEvidenceArtifactBytes + 1 }},
		{name: "bundle bytes", mutate: func(v *EvidenceRecorderLimits) { v.MaxBundleBytes = HardMaxEvidenceBundleBytes + 1 }},
		{name: "depth", mutate: func(v *EvidenceRecorderLimits) { v.MaxDepth = HardMaxEvidenceDepth + 1 }},
		{name: "string bytes", mutate: func(v *EvidenceRecorderLimits) { v.MaxStringBytes = HardMaxEvidenceStringBytes + 1 }},
		{name: "capture duration", mutate: func(v *EvidenceRecorderLimits) { v.MaxCaptureDuration = HardMaxEvidenceCaptureDuration + 1 }},
		{name: "source duration", mutate: func(v *EvidenceRecorderLimits) { v.MaxSourceDuration = HardMaxEvidenceSourceDuration + 1 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg.Limits)
			if err := ValidateEvidenceRecorderConfig(cfg); !errors.Is(err, ErrEvidenceRecorderLimits) {
				t.Fatalf("ValidateEvidenceRecorderConfig() error = %v, want %v", err, ErrEvidenceRecorderLimits)
			}
		})
	}
}

func TestGatewayRejectsUnsafeEvidenceRecorderBeforeTransportDial(t *testing.T) {
	dialed := false
	cfg := Config{
		EvidenceRecorderConfig: EvidenceRecorderConfig{
			Version: EvidenceRecorderConfigVersion,
			Enabled: true,
		},
		TransportConfig: TransportConfig{
			Network: "tcp",
			Address: "127.0.0.1:9999",
			Dial: func(context.Context, string, string, time.Duration) (net.Conn, error) {
				dialed = true
				return nil, errors.New("unexpected dial")
			},
		},
	}

	if _, err := New(context.Background(), cfg); !errors.Is(err, ErrEvidenceRecorderScope) {
		t.Fatalf("New() error = %v, want %v", err, ErrEvidenceRecorderScope)
	}
	if dialed {
		t.Fatal("transport dialed before evidence recorder configuration was rejected")
	}
}
