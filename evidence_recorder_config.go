package ebusgateway

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const (
	EvidenceRecorderConfigVersion = 1
	EvidenceRecorderScopeV1       = "SYNCHRONIZED_EVIDENCE_ONLY"

	DefaultEvidenceRecorderRetention  = 7 * 24 * time.Hour
	DefaultEvidenceRecorderQuotaBytes = int64(256 << 20)

	HardMaxEvidenceSources         = 64
	HardMaxEvidenceItemsPerSource  = 4096
	HardMaxEvidenceArtifactBytes   = 1 << 20
	HardMaxEvidenceBundleBytes     = 64 << 20
	HardMaxEvidenceDepth           = 32
	HardMaxEvidenceStringBytes     = 64 << 10
	HardMaxEvidenceCaptureDuration = 15 * time.Minute
	HardMaxEvidenceSourceDuration  = time.Minute
	HardMaxEvidenceClockSkew       = time.Second
)

var (
	ErrEvidenceRecorderConfigVersion = errors.New("evidence recorder config: unsupported version")
	ErrEvidenceRecorderDisabled      = errors.New("evidence recorder config: disabled configuration must be inert")
	ErrEvidenceRecorderStateRoot     = errors.New("evidence recorder config: unsafe state root")
	ErrEvidenceRecorderScope         = errors.New("evidence recorder config: scope is required")
	ErrEvidenceRecorderRetention     = errors.New("evidence recorder config: retention must be positive")
	ErrEvidenceRecorderQuota         = errors.New("evidence recorder config: quota cannot hold one maximum bundle")
	ErrEvidenceRecorderLimits        = errors.New("evidence recorder config: limits must be positive and within V1 ceilings")
	ErrSynchronizedEvidenceOwnership = errors.New("synchronized evidence: generic recorder and one-shot control cannot both be enabled")
)

// EvidenceRecorderLimits records the effective, hash-bound V1 capture limits.
// All fields must be explicit when the recorder is enabled.
type EvidenceRecorderLimits struct {
	MaxSources         int
	MaxItemsPerSource  int
	MaxArtifactBytes   int
	MaxBundleBytes     int
	MaxDepth           int
	MaxStringBytes     int
	MaxCaptureDuration time.Duration
	MaxSourceDuration  time.Duration
}

// EvidenceRecorderConfig is intentionally inert unless Enabled is explicit.
// It does not itself open a store or acquire any source.
type EvidenceRecorderConfig struct {
	Version    int
	Enabled    bool
	Scope      string
	StateRoot  string
	Retention  time.Duration
	QuotaBytes int64
	Limits     EvidenceRecorderLimits
}

func DefaultEvidenceRecorderConfig() EvidenceRecorderConfig {
	return EvidenceRecorderConfig{Version: EvidenceRecorderConfigVersion}
}

func DefaultEvidenceRecorderLimits() EvidenceRecorderLimits {
	return EvidenceRecorderLimits{
		MaxSources:         16,
		MaxItemsPerSource:  4096,
		MaxArtifactBytes:   1 << 20,
		MaxBundleBytes:     32 << 20,
		MaxDepth:           32,
		MaxStringBytes:     64 << 10,
		MaxCaptureDuration: 15 * time.Minute,
		MaxSourceDuration:  time.Minute,
	}
}

func ValidateSynchronizedEvidenceConfig(cfg Config) error {
	if err := ValidateEvidenceRecorderConfig(cfg.EvidenceRecorderConfig); err != nil {
		return err
	}
	if cfg.EvidenceRecorderConfig.Enabled && cfg.EvidenceOneShotEnabled {
		return ErrSynchronizedEvidenceOwnership
	}
	return nil
}

func ValidateEvidenceRecorderConfig(cfg EvidenceRecorderConfig) error {
	if cfg.Version != EvidenceRecorderConfigVersion {
		return ErrEvidenceRecorderConfigVersion
	}
	if !cfg.Enabled {
		if cfg.Scope != "" || cfg.StateRoot != "" || cfg.Retention != 0 || cfg.QuotaBytes != 0 || cfg.Limits != (EvidenceRecorderLimits{}) {
			return ErrEvidenceRecorderDisabled
		}
		return nil
	}

	if cfg.Scope != EvidenceRecorderScopeV1 {
		return ErrEvidenceRecorderScope
	}
	if err := validateEvidenceRecorderStateRoot(cfg.StateRoot); err != nil {
		return err
	}
	if cfg.Retention <= 0 {
		return ErrEvidenceRecorderRetention
	}
	if cfg.QuotaBytes <= 0 || cfg.Limits.MaxBundleBytes <= 0 || cfg.QuotaBytes < int64(cfg.Limits.MaxBundleBytes) {
		return ErrEvidenceRecorderQuota
	}
	if err := validateEvidenceRecorderLimits(cfg.Limits); err != nil {
		return err
	}
	return nil
}

func validateEvidenceRecorderStateRoot(root string) error {
	if root == "" || strings.ContainsRune(root, '\x00') || !filepath.IsAbs(root) {
		return ErrEvidenceRecorderStateRoot
	}
	clean := filepath.Clean(root)
	if clean == string(filepath.Separator) || clean != root {
		return ErrEvidenceRecorderStateRoot
	}
	return nil
}

func validateEvidenceRecorderLimits(limits EvidenceRecorderLimits) error {
	checks := []struct {
		name string
		got  int64
		max  int64
	}{
		{name: "max_sources", got: int64(limits.MaxSources), max: HardMaxEvidenceSources},
		{name: "max_items_per_source", got: int64(limits.MaxItemsPerSource), max: HardMaxEvidenceItemsPerSource},
		{name: "max_artifact_bytes", got: int64(limits.MaxArtifactBytes), max: HardMaxEvidenceArtifactBytes},
		{name: "max_bundle_bytes", got: int64(limits.MaxBundleBytes), max: HardMaxEvidenceBundleBytes},
		{name: "max_depth", got: int64(limits.MaxDepth), max: HardMaxEvidenceDepth},
		{name: "max_string_bytes", got: int64(limits.MaxStringBytes), max: HardMaxEvidenceStringBytes},
		{name: "max_capture_duration", got: int64(limits.MaxCaptureDuration), max: int64(HardMaxEvidenceCaptureDuration)},
		{name: "max_source_duration", got: int64(limits.MaxSourceDuration), max: int64(HardMaxEvidenceSourceDuration)},
	}
	for _, check := range checks {
		if check.got <= 0 || check.got > check.max {
			return fmt.Errorf("%w: %s", ErrEvidenceRecorderLimits, check.name)
		}
	}
	if limits.MaxSourceDuration > limits.MaxCaptureDuration {
		return fmt.Errorf("%w: max_source_duration", ErrEvidenceRecorderLimits)
	}
	if limits.MaxArtifactBytes > limits.MaxBundleBytes {
		return fmt.Errorf("%w: max_artifact_bytes", ErrEvidenceRecorderLimits)
	}
	return nil
}
