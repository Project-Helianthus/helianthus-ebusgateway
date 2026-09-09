package main

import (
	"fmt"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

type runtimeStatusProvider struct {
	daemon                 graphql.ServiceStatus
	daemonUpdatesAvailable func() bool
	semantic               graphql.SemanticProvider
	admittedSource         func() (byte, bool)
}

type runtimeGatewayIdentityProvider struct {
	instanceGUID string
}

func (p runtimeStatusProvider) DaemonStatus() graphql.ServiceStatus {
	status := p.daemon
	if p.daemonUpdatesAvailable != nil {
		status.UpdatesAvailable = p.daemonUpdatesAvailable()
	}
	status.InitiatorAddress = ""
	if p.admittedSource != nil {
		if source, ok := p.admittedSource(); ok && source != 0 {
			status.InitiatorAddress = formatConfiguredInitiator(source, false)
		}
	}
	return status
}

func (p runtimeStatusProvider) AdapterStatus() graphql.ServiceStatus {
	return adapterGraphQLStatusFromSemantic(p.semantic)
}

func (p runtimeGatewayIdentityProvider) GatewayIdentity() graphql.GatewayIdentity {
	return graphql.GatewayIdentity{InstanceGUID: p.instanceGUID}
}

func newRuntimeStatusProvider(semantic graphql.SemanticProvider, admittedSource func() (byte, bool)) graphql.StatusProvider {
	return newRuntimeStatusProviderForBuild(semantic, admittedSource, gatewayBuildInfo{}, nil)
}

// newRuntimeStatusProviderForBuild exposes the already-validated process
// release and only reads a cached release comparison. It must never perform
// network I/O because GraphQL calls this provider in its request path.
func newRuntimeStatusProviderForBuild(semantic graphql.SemanticProvider, admittedSource func() (byte, bool), buildInfo gatewayBuildInfo, updatesAvailable func() bool) graphql.StatusProvider {
	return runtimeStatusProvider{
		daemon: graphql.ServiceStatus{
			Status:           "running",
			FirmwareVersion:  buildInfo.ReleaseVersion,
			UpdatesAvailable: false,
		},
		daemonUpdatesAvailable: updatesAvailable,
		semantic:               semantic,
		admittedSource:         admittedSource,
	}
}

func newRuntimeGatewayIdentityProvider(cfg ebusgateway.Config) graphql.GatewayIdentityProvider {
	return runtimeGatewayIdentityProvider{instanceGUID: cfg.InstanceGUID}
}

type runtimeMCPStatusProvider struct {
	daemon                 mcp.ServiceStatus
	daemonUpdatesAvailable func() bool
	semantic               graphql.SemanticProvider
	admittedSource         func() (byte, bool)
}

func (p runtimeMCPStatusProvider) DaemonStatus() mcp.ServiceStatus {
	status := p.daemon
	if p.daemonUpdatesAvailable != nil {
		status.UpdatesAvailable = p.daemonUpdatesAvailable()
	}
	status.InitiatorAddress = "auto"
	if p.admittedSource != nil {
		if source, ok := p.admittedSource(); ok && source != 0 {
			status.InitiatorAddress = formatConfiguredInitiator(source, false)
		}
	}
	return status
}

func (p runtimeMCPStatusProvider) AdapterStatus() mcp.ServiceStatus {
	status := adapterGraphQLStatusFromSemantic(p.semantic)
	return mcp.ServiceStatus{
		Status:           status.Status,
		FirmwareVersion:  status.FirmwareVersion,
		UpdatesAvailable: status.UpdatesAvailable,
		InitiatorAddress: status.InitiatorAddress,
	}
}

func (p runtimeMCPStatusProvider) VaillantRegulatorCapability() string {
	if p.semantic == nil {
		return string(graphql.VaillantRegulatorCapabilityUnknown)
	}
	provider, ok := p.semantic.(graphql.VaillantRegulatorCapabilityProvider)
	if !ok {
		return string(graphql.VaillantRegulatorCapabilityUnknown)
	}
	return string(provider.VaillantRegulatorCapability())
}

func newMCPRuntimeStatusProvider(semantic graphql.SemanticProvider, admittedSource func() (byte, bool)) mcp.StatusProvider {
	return newMCPRuntimeStatusProviderForBuild(semantic, admittedSource, gatewayBuildInfo{}, nil)
}

// newMCPRuntimeStatusProviderForBuild shares the GraphQL provider's cached
// release comparison so MCP and GraphQL retain identical daemon status values.
func newMCPRuntimeStatusProviderForBuild(semantic graphql.SemanticProvider, admittedSource func() (byte, bool), buildInfo gatewayBuildInfo, updatesAvailable func() bool) mcp.StatusProvider {
	return runtimeMCPStatusProvider{
		daemon: mcp.ServiceStatus{
			Status:           "running",
			FirmwareVersion:  buildInfo.ReleaseVersion,
			UpdatesAvailable: false,
		},
		daemonUpdatesAvailable: updatesAvailable,
		semantic:               semantic,
		admittedSource:         admittedSource,
	}
}

func adapterGraphQLStatusFromSemantic(semantic graphql.SemanticProvider) graphql.ServiceStatus {
	status := graphql.ServiceStatus{
		Status:           "unknown",
		FirmwareVersion:  "",
		UpdatesAvailable: false,
	}
	if semantic == nil {
		return status
	}
	info := semantic.AdapterHardwareInfo()
	if info == nil {
		return status
	}
	status.FirmwareVersion = info.FirmwareVersion
	if info.FirmwareVersion != "" {
		status.Status = "running"
	}
	return status
}

func formatConfiguredInitiator(source byte, auto bool) string {
	if auto && source == 0x00 {
		return "auto"
	}
	return fmt.Sprintf("0x%02X", source)
}
