package main

import (
	"fmt"
	"sync"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

type runtimeStatusProvider struct {
	daemon                 graphql.ServiceStatus
	daemonUpdatesAvailable func() bool
	semantic               graphql.SemanticProvider
	admittedSource         func() (byte, bool)
	// adapterLifecycle, when set, reports the adapter status replacement
	// while no eBUS generation is natively admitted ("" while one is).
	adapterLifecycle func() string
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
	return gateAdapterStatus(adapterGraphQLStatusFromSemantic(p.semantic), p.adapterLifecycle)
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
	adapterLifecycle       func() string
	// adapterCondition names a degraded condition of the admitted
	// generation (a failed start-critical task), "" when there is none.
	adapterCondition func() string
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
	status := gateAdapterStatus(adapterGraphQLStatusFromSemantic(p.semantic), p.adapterLifecycle)
	out := mcp.ServiceStatus{
		Status:           status.Status,
		FirmwareVersion:  status.FirmwareVersion,
		UpdatesAvailable: status.UpdatesAvailable,
		InitiatorAddress: status.InitiatorAddress,
	}
	if status.Status == adapterStatusDegraded && p.adapterCondition != nil {
		out.Condition = p.adapterCondition()
	}
	return out
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

// Adapter status values while no eBUS generation is natively admitted. The
// retained firmware version stays visible with its original meaning; it is
// identity evidence, not a claim that the adapter is running.
const (
	adapterStatusStopped     = "stopped"
	adapterStatusUnavailable = "unavailable"
	// adapterStatusDegraded: a generation is admitted but one of its
	// start-critical post-start tasks failed; MCP names it in condition.
	adapterStatusDegraded = "degraded"
)

// ebusAdapterConditions maps a composed eBUS driver controller to the
// condition reader of its control path (the failed start-critical task of
// the admitted generation). The control composition registers it when it
// binds the generation tasks, so the status surfaces built inside the HTTP
// shell read it without a new constructor parameter.
var ebusAdapterConditions sync.Map // *ebusDriverController -> func() string

func ebusAdapterConditionFor(controller *ebusDriverController) func() string {
	if controller == nil {
		return nil
	}
	return func() string {
		if reader, ok := ebusAdapterConditions.Load(controller); ok {
			return reader.(func() string)()
		}
		return ""
	}
}

// newRuntimeStatusProviderForControl is newRuntimeStatusProviderForBuild
// with adapter status gated on current native admission: retained firmware
// or cached identity cannot make a STOPPED or FAILED adapter appear running.
// Daemon status keeps describing the process.
func newRuntimeStatusProviderForControl(semantic graphql.SemanticProvider, admittedSource func() (byte, bool), buildInfo gatewayBuildInfo, updatesAvailable func() bool, adapterLifecycle func() string) graphql.StatusProvider {
	provider := newRuntimeStatusProviderForBuild(semantic, admittedSource, buildInfo, updatesAvailable).(runtimeStatusProvider)
	provider.adapterLifecycle = adapterLifecycle
	return provider
}

// newMCPRuntimeStatusProviderForControl is the MCP counterpart; MCP and
// GraphQL keep identical daemon and adapter status values.
func newMCPRuntimeStatusProviderForControl(semantic graphql.SemanticProvider, admittedSource func() (byte, bool), buildInfo gatewayBuildInfo, updatesAvailable func() bool, adapterLifecycle func() string, adapterCondition func() string) mcp.StatusProvider {
	provider := newMCPRuntimeStatusProviderForBuild(semantic, admittedSource, buildInfo, updatesAvailable).(runtimeMCPStatusProvider)
	provider.adapterLifecycle = adapterLifecycle
	provider.adapterCondition = adapterCondition
	return provider
}

func gateAdapterStatus(status graphql.ServiceStatus, lifecycle func() string) graphql.ServiceStatus {
	if lifecycle == nil {
		return status
	}
	if replacement := lifecycle(); replacement != "" {
		status.Status = replacement
	}
	return status
}

// ebusAdapterLifecycleStatus derives the adapter status replacement from the
// cached DriverManager snapshot (no endpoint I/O): "" while a generation is
// admitted (RUNNING or DEGRADED), "stopped" while intentionally stopped or
// disabled, and "unavailable" otherwise (starting, backoff, stopping,
// failed). An admitted generation whose start-critical task failed reports
// "degraded". A nil controller keeps the historical projection.
func ebusAdapterLifecycleStatus(controller *ebusDriverController) func() string {
	if controller == nil || controller.manager == nil {
		return nil
	}
	condition := ebusAdapterConditionFor(controller)
	return func() string {
		switch controller.Snapshot().ObservedState {
		case drivermanager.ObservedRunning, drivermanager.ObservedDegraded:
			if condition() != "" {
				return adapterStatusDegraded
			}
			return ""
		case drivermanager.ObservedStopped, drivermanager.ObservedDisabled:
			return adapterStatusStopped
		default:
			return adapterStatusUnavailable
		}
	}
}
