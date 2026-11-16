package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// gatewayHTTPPortalSurface holds the collaborators of the Portal surface that
// the HTTP server mounts when a Portal path is configured. gateway, builder,
// mcpServer and portalConfig are required; every other collaborator keeps its
// own nil handling inside the Portal.
type gatewayHTTPPortalSurface struct {
	cfg                ebusgateway.Config
	gateway            *ebusgateway.Gateway
	builder            *graphql.Builder
	mcpServer          *mcp.Server
	semanticProvider   graphql.SemanticProvider
	eebusProvider      mcp.EEBusV1Provider
	eebusLifecycle     *eebusRuntimeLifecycle
	ebusProxyReadiness func() string
	ebusSourceProvider func() (byte, bool)
	modbusProvider     mcp.ModbusV1Provider
	busObservability   *ebusgateway.BusObservabilityStore
	buildInfo          gatewayBuildInfo
	runtimeControl     *gatewayRuntimeControl
	portalConfig       *gatewayPortalConfigRuntime
	semanticPV         func(context.Context) (portal.ForwardedResponse, error)
	semanticStorage    func(context.Context) (portal.ForwardedResponse, error)
	storageAvailable   bool
}

func (surface gatewayHTTPPortalSurface) validate() error {
	switch {
	case surface.gateway == nil:
		return fmt.Errorf("gateway missing for http portal surface")
	case surface.builder == nil:
		return fmt.Errorf("graphql builder missing for http portal surface")
	case surface.mcpServer == nil:
		return fmt.Errorf("mcp server missing for http portal surface")
	case surface.portalConfig == nil:
		return fmt.Errorf("portal configuration missing for http portal surface")
	}
	return nil
}

// newGatewayHTTPPortalOptions builds the Portal options of the mounted Portal
// path. The shared semantic read, the catalog route and the mount itself are
// added by the HTTP server after these options are built.
func newGatewayHTTPPortalOptions(surface gatewayHTTPPortalSurface) (portal.Options, error) {
	if err := surface.validate(); err != nil {
		return portal.Options{}, err
	}
	cfg := surface.cfg
	portalModbusProvider := portalTCPModbusProvider(surface.modbusProvider)
	getPortalBusObservability := gatewayPortalBusObservability(surface.busObservability)
	return portal.Options{
		Config:                 surface.portalConfig.callbacks(cfg, surface.runtimeControl),
		GraphQLPath:            cfg.GraphQLPath,
		SnapshotPath:           cfg.SnapshotPath,
		SubscriptionPath:       cfg.SubscriptionPath,
		MCPPath:                cfg.MCPPath,
		EEBusAdminPath:         "/admin/eebus/v1",
		Readiness:              gatewayPortalReadiness(surface.eebusProvider, surface.eebusLifecycle, surface.ebusProxyReadiness),
		GatewayVersion:         surface.buildInfo.ReleaseVersion,
		BuildID:                surface.buildInfo.BuildID,
		SemanticPVEnabled:      cfg.PortalPV.SemanticEnabled,
		SemanticStorageEnabled: cfg.PortalStorage.SemanticEnabled && surface.storageAvailable,
		RawModbusEnabled:       cfg.PortalPV.RawReadEnabled,
		SemanticPV:             surface.semanticPV,
		SemanticStorage:        surface.semanticStorage,
		ModbusProvider:         portalModbusProvider,
		RawModbusAudit:         logGatewayPortalRawModbusAudit,
		ListRegistry:           gatewayPortalRegistryDevices(surface.builder, surface.gateway),
		ListSemantic:           gatewayPortalSemanticSnapshot(surface.semanticProvider),
		GetBusObservability:    getPortalBusObservability,
		ListProjections:        gatewayPortalProjectionDevices(surface.builder),
		GetProjection:          gatewayPortalProjectionGraph(surface.builder),
		ExplorerBus:            surface.gateway.Bus,
		ExplorerSourceProvider: surface.ebusSourceProvider,
		// The in-process L7 catalog sub-server is the instance the MCP server
		// installed, so both surfaces reach the same embedded catalog. A nil
		// server would make the Portal's ebus-standard routes answer 404.
		EbusStandardServer: surface.mcpServer.EbusStandardServer(),
	}, nil
}

// logGatewayPortalRawModbusAudit writes one sanitized audit line per Portal
// raw Modbus read.
func logGatewayPortalRawModbusAudit(event portal.RawModbusAuditEvent) {
	log.Printf("portal_modbus_raw_audit request_id=%s surface=%s tool=%s unit_id=%s function=%s offset=%s quantity=%s outcome=%s error_code=%s duration_ms=%d endpoint_ref=%s timestamp=%s",
		event.RequestID, event.Surface, event.Tool, auditNumber(event.UnitID), auditNumber(event.Function),
		auditNumber(event.Offset), auditNumber(event.Quantity), event.Outcome, event.ErrorCode,
		event.DurationMS, event.EndpointRef, event.At.Format(time.RFC3339Nano))
}

// gatewayPortalBusObservability reads the bus summary per call; no store
// leaves the read unset.
func gatewayPortalBusObservability(store *ebusgateway.BusObservabilityStore) func() any {
	if store == nil {
		return nil
	}
	return func() any {
		return store.Snapshot().Summary
	}
}

// gatewayPortalRegistryDevices lists the registry devices with their planes and
// method names, labelled with the display name and role of the current schema.
func gatewayPortalRegistryDevices(builder *graphql.Builder, gateway *ebusgateway.Gateway) func() []portal.RegistryDevice {
	return func() []portal.RegistryDevice {
		schemaSnapshot := builder.FreshSchema()
		schemaByAddr := make(map[byte]graphql.Device, len(schemaSnapshot.Devices))
		for _, sd := range schemaSnapshot.Devices {
			schemaByAddr[sd.Address] = sd
		}
		items := make([]portal.RegistryDevice, 0)
		gateway.Registry.Iterate(func(entry registry.DeviceEntry) bool {
			if entry == nil {
				return true
			}
			device := gatewayPortalRegistryDevice(entry)
			if sd, ok := schemaByAddr[entry.PrimaryDisplayAddress()]; ok {
				device.DisplayName = sd.DisplayName
				device.Role = sd.Role
			}
			items = append(items, device)
			return true
		})
		return items
	}
}

func gatewayPortalRegistryDevice(entry registry.DeviceEntry) portal.RegistryDevice {
	rawAddrs := entry.Addresses()
	intAddrs := make([]int, len(rawAddrs))
	for i, a := range rawAddrs {
		intAddrs[i] = int(a)
	}
	device := portal.RegistryDevice{
		Address:      int(entry.PrimaryDisplayAddress()),
		Addresses:    intAddrs,
		Manufacturer: entry.Manufacturer(),
		DeviceID:     entry.DeviceID(),
		SerialNumber: entry.SerialNumber(),
		Software:     entry.SoftwareVersion(),
		Hardware:     entry.HardwareVersion(),
		Planes:       make([]portal.RegistryPlane, 0),
	}
	for _, plane := range entry.Planes() {
		if plane == nil {
			continue
		}
		methods := plane.Methods()
		methodNames := make([]string, 0, len(methods))
		for _, method := range methods {
			if method == nil {
				continue
			}
			methodNames = append(methodNames, method.Name())
		}
		device.Planes = append(device.Planes, portal.RegistryPlane{
			Name:    plane.Name(),
			Methods: methodNames,
		})
	}
	return device
}

// gatewayPortalSemanticSnapshot maps the semantic provider into one Portal
// snapshot per call; no provider gives the empty snapshot.
func gatewayPortalSemanticSnapshot(semanticProvider graphql.SemanticProvider) func() portal.SemanticSnapshot {
	return func() portal.SemanticSnapshot {
		if semanticProvider == nil {
			return portal.SemanticSnapshot{}
		}
		verdict := portalFM5Interpretation(semanticProvider)
		var degradedReason *string
		if verdict.DegradedReason != "" {
			reason := string(verdict.DegradedReason)
			degradedReason = &reason
		}
		return portal.SemanticSnapshot{
			Zones:               mapPortalZones(semanticProvider.Zones()),
			DHW:                 mapPortalDHW(semanticProvider.DHW()),
			Energy:              mapPortalEnergyTotals(semanticProvider.EnergyTotals()),
			BoilerStatus:        mapPortalBoilerStatus(semanticProvider.BoilerStatus()),
			System:              mapPortalSystemStatus(semanticProvider.System()),
			Circuits:            mapPortalCircuits(semanticProvider.Circuits()),
			RadioDevices:        mapPortalRadioDevices(semanticProvider.RadioDevices()),
			FM5Mode:             string(verdict.Mode),
			FM5DegradedReason:   degradedReason,
			FM5EvidenceRevision: verdict.EvidenceRevision,
			Solar:               mapPortalSolarStatus(semanticProvider.Solar()),
			Cylinders:           mapPortalCylinders(semanticProvider.Cylinders()),
			AdapterInfo:         mapPortalAdapterInfo(semanticProvider.AdapterHardwareInfo()),
			CapturedUTC:         time.Now().UTC().Format(time.RFC3339),
		}
	}
}

// gatewayPortalProjectionDevices summarises the projections of every schema
// device by plane with node and edge counts.
func gatewayPortalProjectionDevices(builder *graphql.Builder) func() []portal.ProjectionDevice {
	return func() []portal.ProjectionDevice {
		snapshot := builder.FreshSchema()
		items := make([]portal.ProjectionDevice, 0, len(snapshot.Devices))
		for _, device := range snapshot.Devices {
			summaries := make([]portal.ProjectionSummary, 0, len(device.Projections))
			for _, projection := range device.Projections {
				summaries = append(summaries, portal.ProjectionSummary{
					Plane:     projection.Plane,
					NodeCount: len(projection.Nodes),
					EdgeCount: len(projection.Edges),
				})
			}
			items = append(items, portal.ProjectionDevice{
				Address:      device.Address,
				DeviceID:     device.DeviceID,
				DisplayName:  device.DisplayName,
				Manufacturer: device.Manufacturer,
				Projections:  summaries,
			})
		}
		return items
	}
}

// gatewayPortalProjectionGraph returns the first projection of the device at
// address whose plane matches case-insensitively.
func gatewayPortalProjectionGraph(builder *graphql.Builder) func(address byte, plane string) (portal.ProjectionGraph, bool) {
	return func(address byte, plane string) (portal.ProjectionGraph, bool) {
		snapshot := builder.FreshSchema()
		for _, device := range snapshot.Devices {
			if device.Address != address {
				continue
			}
			for _, projection := range device.Projections {
				if !strings.EqualFold(projection.Plane, plane) {
					continue
				}
				return gatewayPortalProjection(address, projection), true
			}
		}
		return portal.ProjectionGraph{}, false
	}
}

func gatewayPortalProjection(address byte, projection graphql.Projection) portal.ProjectionGraph {
	nodes := make([]portal.ProjectionNode, 0, len(projection.Nodes))
	for _, node := range projection.Nodes {
		nodes = append(nodes, portal.ProjectionNode{
			ID:            node.ID,
			Path:          node.Path,
			CanonicalPath: node.CanonicalPath,
		})
	}
	edges := make([]portal.ProjectionEdge, 0, len(projection.Edges))
	for _, edge := range projection.Edges {
		edges = append(edges, portal.ProjectionEdge{
			ID:   edge.ID,
			From: edge.From,
			To:   edge.To,
		})
	}
	return portal.ProjectionGraph{
		Address: address,
		Plane:   projection.Plane,
		Nodes:   nodes,
		Edges:   edges,
	}
}
