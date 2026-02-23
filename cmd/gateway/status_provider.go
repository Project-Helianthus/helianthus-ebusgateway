package main

import (
	"fmt"

	"github.com/d3vi1/helianthus-ebusgateway"
	"github.com/d3vi1/helianthus-ebusgateway/graphql"
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

func formatConfiguredInitiator(source byte, auto bool) string {
	if auto && source == 0x00 {
		return "auto"
	}
	return fmt.Sprintf("0x%02X", source)
}
