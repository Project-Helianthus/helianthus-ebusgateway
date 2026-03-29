package main

import (
	"fmt"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

type runtimeStatusProvider struct {
	daemon   graphql.ServiceStatus
	semantic graphql.SemanticProvider
}

func (p runtimeStatusProvider) DaemonStatus() graphql.ServiceStatus {
	return p.daemon
}

func (p runtimeStatusProvider) AdapterStatus() graphql.ServiceStatus {
	return adapterGraphQLStatusFromSemantic(p.semantic)
}

func newRuntimeStatusProvider(cfg ebusgateway.Config, semantic graphql.SemanticProvider) graphql.StatusProvider {
	return runtimeStatusProvider{
		daemon: graphql.ServiceStatus{
			Status:           "running",
			FirmwareVersion:  "",
			UpdatesAvailable: false,
			InitiatorAddress: formatConfiguredInitiator(cfg.ScanSource, cfg.ScanSourceAuto),
		},
		semantic: semantic,
	}
}

type runtimeMCPStatusProvider struct {
	daemon   mcp.ServiceStatus
	semantic graphql.SemanticProvider
}

func (p runtimeMCPStatusProvider) DaemonStatus() mcp.ServiceStatus {
	return p.daemon
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

func newMCPRuntimeStatusProvider(cfg ebusgateway.Config, semantic graphql.SemanticProvider) mcp.StatusProvider {
	return runtimeMCPStatusProvider{
		daemon: mcp.ServiceStatus{
			Status:           "running",
			FirmwareVersion:  "",
			UpdatesAvailable: false,
			InitiatorAddress: formatConfiguredInitiator(cfg.ScanSource, cfg.ScanSourceAuto),
		},
		semantic: semantic,
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
