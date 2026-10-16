package graphql

import (
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/systemconfig"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type ConfigBound = systemconfig.ConfigBound

const (
	SystemConfigFieldDHWBivalencePoint = systemconfig.SystemConfigFieldDHWBivalencePoint
	ConfigBoundSourceControllerProfile = systemconfig.ConfigBoundSourceControllerProfile
	ConfigBoundSourceUnmatchedPolicy   = systemconfig.ConfigBoundSourceUnmatchedPolicy
	ConfigBoundReasonUnknownProfile    = systemconfig.ConfigBoundReasonUnknownProfile
	ConfigBoundReasonUnknownRegister   = systemconfig.ConfigBoundReasonUnknownRegister
	ConfigBoundReasonUnqualifiedBound  = systemconfig.ConfigBoundReasonUnqualifiedBound
	ConfigBoundReasonInvalidBound      = systemconfig.ConfigBoundReasonInvalidBound
)

func ControllerIdentityFromSnapshot(entry registry.DeviceEntrySnapshot) systemconfig.ControllerIdentity {
	return systemconfig.ControllerIdentityFromSnapshot(entry)
}
func controllerIdentityFromRegistry(reg InvokeRegistry) systemconfig.ControllerIdentity {
	return systemconfig.ControllerIdentityFromRegistry(reg)
}
func ResolveDHWBivalenceBound(identity systemconfig.ControllerIdentity) ConfigBound {
	return systemconfig.ResolveDHWBivalenceBound(identity)
}
func EncodeSystemConfigValueForController(fieldName, rawValue string, identity systemconfig.ControllerIdentity) ([]byte, configFieldSpec, *ConfigBound, error) {
	payload, spec, bound, err := systemconfig.EncodeSystemConfigValueForController(fieldName, rawValue, identity)
	return payload, adaptSystemConfigSpec(spec), bound, err
}
