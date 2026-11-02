package graphql

import (
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/systemconfig"
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
