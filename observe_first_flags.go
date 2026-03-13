package ebusgateway

import (
	"fmt"
	"strings"
)

type ObserveFirstExternalWritePolicy string

const (
	ObserveFirstExternalWritePolicyInvalidateOnly      ObserveFirstExternalWritePolicy = "invalidate_only"
	ObserveFirstExternalWritePolicyRecordOnly          ObserveFirstExternalWritePolicy = "record_only"
	ObserveFirstExternalWritePolicyRecordAndInvalidate ObserveFirstExternalWritePolicy = "record_and_invalidate"
)

type ObserveFirstFeatureFlagNormalizationReason string

const (
	ObserveFirstFeatureFlagNormalizationReasonMasterOffClamp             ObserveFirstFeatureFlagNormalizationReason = "master_off_clamp"
	ObserveFirstFeatureFlagNormalizationReasonConfigRequiresState        ObserveFirstFeatureFlagNormalizationReason = "config_requires_state"
	ObserveFirstFeatureFlagNormalizationReasonConfigRequiresInvalidation ObserveFirstFeatureFlagNormalizationReason = "config_requires_invalidation"
)

var observeFirstExternalWritePolicies = []ObserveFirstExternalWritePolicy{
	ObserveFirstExternalWritePolicyInvalidateOnly,
	ObserveFirstExternalWritePolicyRecordOnly,
	ObserveFirstExternalWritePolicyRecordAndInvalidate,
}

var observeFirstFeatureFlagNormalizationReasons = []ObserveFirstFeatureFlagNormalizationReason{
	ObserveFirstFeatureFlagNormalizationReasonMasterOffClamp,
	ObserveFirstFeatureFlagNormalizationReasonConfigRequiresState,
	ObserveFirstFeatureFlagNormalizationReasonConfigRequiresInvalidation,
}

type ObserveFirstFeatureFlagView interface {
	ObserveFirstEnabled() bool
	PassiveStateDirectApply() bool
	PassiveConfigDirectApply() bool
	ExternalWritePolicy() ObserveFirstExternalWritePolicy
}

type ObserveFirstFeatureFlagState struct {
	ObserveFirstEnabled      bool                            `json:"observe_first_enabled"`
	PassiveStateDirectApply  bool                            `json:"passive_state_direct_apply"`
	PassiveConfigDirectApply bool                            `json:"passive_config_direct_apply"`
	ExternalWritePolicy      ObserveFirstExternalWritePolicy `json:"external_write_policy"`
	Normalizations           []string                        `json:"normalizations,omitempty"`
}

type ObserveFirstFeatureFlags struct {
	observeFirstEnabled      bool
	passiveStateDirectApply  bool
	passiveConfigDirectApply bool
	externalWritePolicy      ObserveFirstExternalWritePolicy
	normalizationReasons     []ObserveFirstFeatureFlagNormalizationReason
}

func DefaultObserveFirstFeatureFlags() ObserveFirstFeatureFlags {
	return NormalizeObserveFirstFeatureFlags(false, false, false, ObserveFirstExternalWritePolicyRecordOnly)
}

func ParseObserveFirstExternalWritePolicy(value string) (ObserveFirstExternalWritePolicy, error) {
	policy := normalizeObserveFirstExternalWritePolicy(ObserveFirstExternalWritePolicy(value))
	if policy == "" {
		return "", fmt.Errorf("invalid external-write-policy %q", value)
	}
	return policy, nil
}

func NormalizeObserveFirstFeatureFlags(enabled, stateDirect, configDirect bool, policy ObserveFirstExternalWritePolicy) ObserveFirstFeatureFlags {
	policy = normalizeObserveFirstExternalWritePolicy(policy)
	if policy == "" {
		policy = ObserveFirstExternalWritePolicyRecordOnly
	}

	flags := ObserveFirstFeatureFlags{
		observeFirstEnabled:      enabled,
		passiveStateDirectApply:  stateDirect,
		passiveConfigDirectApply: configDirect,
		externalWritePolicy:      policy,
	}
	reasons := make(map[ObserveFirstFeatureFlagNormalizationReason]struct{}, 4)

	addReason := func(reason ObserveFirstFeatureFlagNormalizationReason) {
		reasons[reason] = struct{}{}
	}

	// Normalization order is fixed by the canonical execution plan.
	if !flags.observeFirstEnabled {
		if flags.passiveStateDirectApply || flags.passiveConfigDirectApply || flags.externalWritePolicy != ObserveFirstExternalWritePolicyRecordOnly {
			addReason(ObserveFirstFeatureFlagNormalizationReasonMasterOffClamp)
		}
		flags.passiveStateDirectApply = false
		flags.passiveConfigDirectApply = false
		flags.externalWritePolicy = ObserveFirstExternalWritePolicyRecordOnly
	}

	if !flags.passiveStateDirectApply {
		if flags.passiveConfigDirectApply {
			addReason(ObserveFirstFeatureFlagNormalizationReasonConfigRequiresState)
		}
		flags.passiveConfigDirectApply = false
	}

	if flags.passiveConfigDirectApply && flags.externalWritePolicy == ObserveFirstExternalWritePolicyRecordOnly {
		addReason(ObserveFirstFeatureFlagNormalizationReasonConfigRequiresInvalidation)
		flags.externalWritePolicy = ObserveFirstExternalWritePolicyRecordAndInvalidate
	}

	if len(reasons) == 0 {
		return flags
	}

	flags.normalizationReasons = make([]ObserveFirstFeatureFlagNormalizationReason, 0, len(reasons))
	for _, reason := range observeFirstFeatureFlagNormalizationReasons {
		if _, ok := reasons[reason]; ok {
			flags.normalizationReasons = append(flags.normalizationReasons, reason)
		}
	}
	return flags
}

func NormalizeObserveFirstFeatureFlagsFromView(view ObserveFirstFeatureFlagView) ObserveFirstFeatureFlags {
	if view == nil {
		return DefaultObserveFirstFeatureFlags()
	}
	return NormalizeObserveFirstFeatureFlags(
		view.ObserveFirstEnabled(),
		view.PassiveStateDirectApply(),
		view.PassiveConfigDirectApply(),
		view.ExternalWritePolicy(),
	)
}

func (flags ObserveFirstFeatureFlags) ObserveFirstEnabled() bool {
	return flags.observeFirstEnabled
}

func (flags ObserveFirstFeatureFlags) PassiveStateDirectApply() bool {
	return flags.passiveStateDirectApply
}

func (flags ObserveFirstFeatureFlags) PassiveConfigDirectApply() bool {
	return flags.passiveConfigDirectApply
}

func (flags ObserveFirstFeatureFlags) ExternalWritePolicy() ObserveFirstExternalWritePolicy {
	return flags.externalWritePolicy
}

func (flags ObserveFirstFeatureFlags) NormalizationReasons() []ObserveFirstFeatureFlagNormalizationReason {
	if len(flags.normalizationReasons) == 0 {
		return nil
	}
	out := make([]ObserveFirstFeatureFlagNormalizationReason, len(flags.normalizationReasons))
	copy(out, flags.normalizationReasons)
	return out
}

func (flags ObserveFirstFeatureFlags) State() ObserveFirstFeatureFlagState {
	state := ObserveFirstFeatureFlagState{
		ObserveFirstEnabled:      flags.observeFirstEnabled,
		PassiveStateDirectApply:  flags.passiveStateDirectApply,
		PassiveConfigDirectApply: flags.passiveConfigDirectApply,
		ExternalWritePolicy:      flags.externalWritePolicy,
	}
	if len(flags.normalizationReasons) == 0 {
		return state
	}
	state.Normalizations = make([]string, len(flags.normalizationReasons))
	for index, reason := range flags.normalizationReasons {
		state.Normalizations[index] = string(reason)
	}
	return state
}

func (flags ObserveFirstFeatureFlags) configured() bool {
	return flags.externalWritePolicy != ""
}

func normalizeObserveFirstExternalWritePolicy(policy ObserveFirstExternalWritePolicy) ObserveFirstExternalWritePolicy {
	value := strings.TrimSpace(strings.ToLower(string(policy)))
	value = strings.ReplaceAll(value, "-", "_")
	switch ObserveFirstExternalWritePolicy(value) {
	case ObserveFirstExternalWritePolicyInvalidateOnly,
		ObserveFirstExternalWritePolicyRecordOnly,
		ObserveFirstExternalWritePolicyRecordAndInvalidate:
		return ObserveFirstExternalWritePolicy(value)
	default:
		return ""
	}
}
