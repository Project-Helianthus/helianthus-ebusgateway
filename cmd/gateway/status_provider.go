package main

import (
	"fmt"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

type runtimeStatusProvider struct {
	daemon  graphql.ServiceStatus
	adapter graphql.ServiceStatus
}

func (p runtimeStatusProvider) DaemonStatus() graphql.ServiceStatus {
	return p.daemon
}

func (p runtimeStatusProvider) AdapterStatus() graphql.ServiceStatus {
	return p.adapter
}

func newRuntimeStatusProvider(cfg ebusgateway.Config) graphql.StatusProvider {
	return runtimeStatusProvider{
		daemon: graphql.ServiceStatus{
			Status:           "running",
			FirmwareVersion:  "",
			UpdatesAvailable: false,
			InitiatorAddress: formatConfiguredInitiator(cfg.ScanSource, cfg.ScanSourceAuto),
		},
		adapter: graphql.ServiceStatus{
			Status:           "unknown",
			FirmwareVersion:  "",
			UpdatesAvailable: false,
		},
	}
}

type runtimeMCPStatusProvider struct {
	daemon  mcp.ServiceStatus
	adapter mcp.ServiceStatus
}

func (p runtimeMCPStatusProvider) DaemonStatus() mcp.ServiceStatus {
	return p.daemon
}

func (p runtimeMCPStatusProvider) AdapterStatus() mcp.ServiceStatus {
	return p.adapter
}

func newMCPRuntimeStatusProvider(cfg ebusgateway.Config) mcp.StatusProvider {
	return runtimeMCPStatusProvider{
		daemon: mcp.ServiceStatus{
			Status:           "running",
			FirmwareVersion:  "",
			UpdatesAvailable: false,
			InitiatorAddress: formatConfiguredInitiator(cfg.ScanSource, cfg.ScanSourceAuto),
		},
		adapter: mcp.ServiceStatus{
			Status:           "unknown",
			FirmwareVersion:  "",
			UpdatesAvailable: false,
		},
	}
}

func formatConfiguredInitiator(source byte, auto bool) string {
	if auto && source == 0x00 {
		return "auto"
	}
	return fmt.Sprintf("0x%02X", source)
}
