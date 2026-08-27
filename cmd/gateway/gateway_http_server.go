package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func startHTTPServer(
	ctx context.Context,
	cfg ebusgateway.Config,
	gateway *ebusgateway.Gateway,
	builder *graphql.Builder,
	hub *graphql.BroadcastHub,
	semanticProvider graphql.SemanticProvider,
	eebusProvider mcp.EEBusV1Provider,
	eebusCommandRouter mcp.EEBusV1CommandRouter,
	modbusProvider mcp.ModbusV1Provider,
	scheduleWriter mcp.ScheduleWriter,
	configWriter mcp.ConfigWriter,
	busObservability *ebusgateway.BusObservabilityStore,
	watchSummaryProvider mcp.WatchSummaryProvider,
	eebusAdminHandler http.Handler,
	eebusLifecycle *eebusRuntimeLifecycle,
	ebusProxyReadiness func() string,
	ebusSourceProvider func() (byte, bool),
	buildInfo gatewayBuildInfo,
	ebusDriver *ebusDriverController,
) (*http.Server, mdns.Advertiser, error) {
	if cfg.HTTPAddr == "" {
		return nil, nil, nil
	}
	if gateway == nil {
		return nil, nil, fmt.Errorf("gateway missing for http server")
	}
	if builder == nil {
		return nil, nil, fmt.Errorf("graphql builder missing for http server")
	}
	if hub == nil {
		return nil, nil, fmt.Errorf("graphql broadcast hub missing for http server")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cfg.ValidatePortalPV(); err != nil {
		return nil, nil, fmt.Errorf("validate Portal PV configuration: %w", err)
	}
	portalPVClient, err := newPortalPVClient(cfg.PortalPV)
	if err != nil {
		return nil, nil, fmt.Errorf("portal PV configuration: %w", err)
	}

	queryHandler, snapshotHandler, subscriptionHandler, err := newHTTPControlPlaneGraphQLHandlers(gateway, builder, hub)
	if err != nil {
		return nil, nil, err
	}
	mcpServer, err := mcp.NewServer(gateway.Registry, gateway.Router)
	if err != nil {
		return nil, nil, err
	}
	if err := mcpServer.SetServerVersion(buildInfo.ReleaseVersion); err != nil {
		return nil, nil, fmt.Errorf("configure MCP build identity: %w", err)
	}
	if eebusProvider != nil {
		if err := mcpServer.RegisterEEBusV1Provider(eebusProvider); err != nil {
			return nil, nil, fmt.Errorf("register eeBUS MCP provider: %w", err)
		}
	}
	if eebusCommandRouter != nil {
		if err := mcpServer.RegisterEEBusV1CommandRouter(eebusCommandRouter); err != nil {
			return nil, nil, fmt.Errorf("register eeBUS MCP command router: %w", err)
		}
	}
	mcp.RegisterModbusV1Tools(mcpServer, modbusProvider)
	if source, ok := eebusProvider.(mcp.LeafPromotionCaptureSource); ok {
		if capture := source.LeafPromotionCapture(); capture != nil {
			if err := mcpServer.RegisterLeafPromotionCapture(capture); err != nil {
				return nil, nil, fmt.Errorf("register leaf promotion capture: %w", err)
			}
		}
	}
	oneShotRuntime, err := newSynchronizedEvidenceOneShotRuntime(
		cfg.EvidenceOneShotEnabled,
		eebusProvider,
		eebusCommandRouter,
		buildInfo,
	)
	if err != nil {
		return nil, nil, err
	}
	if oneShotRuntime != nil {
		if err := mcpServer.RegisterSynchronizedEvidenceCapture(oneShotRuntime); err != nil {
			return nil, nil, fmt.Errorf("register synchronized evidence capture: %w", err)
		}
	}
	mcpServer.SetAdmittedRPCSourceProvider(ebusSourceProvider)
	mcpServer.SetStatusProvider(newMCPRuntimeStatusProvider(semanticProvider, ebusSourceProvider))
	if busObservability != nil {
		mcpServer.SetBusObservabilityProvider(newMCPBusObservabilityProvider(busObservability))
	}
	if watchSummaryProvider != nil {
		mcpServer.SetWatchSummaryProvider(watchSummaryProvider)
	}
	mcpServer.SetSemanticProvider(newMCPSemanticProvider(semanticProvider))

	// M2a_GATEWAY_MCP (execution-plans#19): install Vaillant B503 MCP tool
	// surface. Uses a deferred dispatcher stub — production B524-style raw
	// RPC wiring for the 2-byte (family, selector) frame is scheduled as a
	// follow-up under the M2b / M3 rollout (the MCP substrate currently
	// models dispatch as (plane, method, params) through the catalog, and
	// B503 has not yet been added to the catalog — intentional, per plan
	// AD01: Vaillant protocol knowledge stays out of ebusreg).
	//
	// Registering here ensures:
	//   - `ebus.v1.vaillant.*` tools are listed by tools/list (P1 lint from
	//     Codex review of M2a — tools MUST be reachable by clients, even
	//     if production dispatch is not yet wired);
	//   - capability signal is emitted;
	//   - forbidden-surface guards (TestNoVaillantInstallWriteTools) apply
	//     to the production build, not just the test harness.
	//
	// With the stub dispatcher, read tools surface `UPSTREAM_RPC_FAILED`
	// with the "production wiring pending" message; live-monitor action
	// paths use the real session FSM so EXPIRED normalization, session
	// epochs, and owner-conditional release are all exercised — only the
	// raw bus dispatch is stubbed.
	b503rt := installVaillantB503(mcpServer, gateway, &cfg, ebusSourceProvider)
	if b503rt != nil && ebusDriver != nil {
		ebusDriver.SetLifecycleObserver(b503rt)
	}
	// M2b_GATEWAY_GRAPHQL (execution-plans#19): wire the GraphQL B503
	// provider to the same Manager + Dispatcher the MCP surface uses. A
	// single Manager across both surfaces is mandatory — GraphQL
	// Enable/Read/Disable operating on a separate Manager would break
	// the single-owner session invariant.
	if b503rt != nil {
		builder.SetVaillantB503Provider(newB503GraphQLProvider(b503rt))
	}

	// M4c2: populate the package-level responder capability provider
	// based on the active transport protocol. Consumers apply fail-closed
	// semantics on absence, so this MUST be called before any MCP
	// surface serves its first envelope. The provider closure is
	// evaluated per-envelope; hot-path cost is a single pointer load +
	// struct copy. See decision doc @ 567a6798 §4.2 + §5.
	//
	// A nil return from buildResponderCapabilityProvider means the raw
	// transport protocol does not canonicalize to any of the three
	// locked rows at v1.1 (ENH / ENS / ebusd-tcp). In that case we omit
	// the capability entirely so consumers fall back to §4.3 rule 1
	// (absence → scope=none, fail-closed). This preserves invariant I1
	// (exactly three rows at v1.1) and I2 (active.transport MUST appear
	// in transports[]) without widening the schema.
	// Pass the live transport instance (gateway.Transport) so the provider
	// can type-assert against ebusgoTransport.ResponderTransport. The
	// adapter-direct mux returns a RawTransport that does NOT satisfy
	// ResponderTransport, so config-string "enh" + mux active path
	// correctly downgrades to scope=none (see Codex P1 on PR #509).
	if provider := buildResponderCapabilityProvider(cfg, gateway.Transport); provider != nil {
		ebus_standard.SetResponderCapabilityProvider(provider)
	} else {
		log.Printf("warning: meta.capabilities.responder omitted: transport protocol %q does not canonicalize to ENH/ENS/ebusd-tcp", cfg.TransportConfig.Protocol)
	}
	if scheduleWriter != nil {
		mcpServer.SetScheduleWriter(scheduleWriter)
	}
	if configWriter != nil {
		mcpServer.SetConfigWriter(configWriter)
	}

	mux := http.NewServeMux()
	registerHTTPControlPlaneCoreRoutes(
		mux, cfg, busObservability, queryHandler, snapshotHandler, subscriptionHandler, mcpServer, eebusAdminHandler,
	)
	if cfg.PortalPath != "" {
		portalPath := normalizeMountPath(cfg.PortalPath, "/portal")
		var getPortalBusObservability func() any
		if busObservability != nil {
			getPortalBusObservability = func() any {
				return busObservability.Snapshot().Summary
			}
		}
		portalHandler := portal.NewHandler(portal.Options{
			GraphQLPath:      cfg.GraphQLPath,
			SnapshotPath:     cfg.SnapshotPath,
			SubscriptionPath: cfg.SubscriptionPath,
			MCPPath:          cfg.MCPPath,
			EEBusAdminPath:   "/admin/eebus/v1",
			Readiness: func() portal.RuntimeReadiness {
				return projectGatewayReadiness(ebusProxyReadiness, eebusLifecycle.LifecycleSnapshot())
			},
			GatewayVersion:    buildInfo.ReleaseVersion,
			BuildID:           buildInfo.BuildID,
			SemanticPVEnabled: cfg.PortalPV.SemanticEnabled,
			RawModbusEnabled:  cfg.PortalPV.RawReadEnabled,
			SemanticPV:        portalPVClient,
			ModbusProvider:    modbusProvider,
			RawModbusAudit: func(event portal.RawModbusAuditEvent) {
				log.Printf("portal_modbus_raw_audit request_id=%s surface=%s tool=%s unit_id=%s function=%s offset=%s quantity=%s outcome=%s error_code=%s duration_ms=%d endpoint_ref=%s timestamp=%s",
					event.RequestID, event.Surface, event.Tool, auditNumber(event.UnitID), auditNumber(event.Function),
					auditNumber(event.Offset), auditNumber(event.Quantity), event.Outcome, event.ErrorCode,
					event.DurationMS, event.EndpointRef, event.At.Format(time.RFC3339Nano))
			},
			ListRegistry: func() []portal.RegistryDevice {
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
					if sd, ok := schemaByAddr[entry.PrimaryDisplayAddress()]; ok {
						device.DisplayName = sd.DisplayName
						device.Role = sd.Role
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
					items = append(items, device)
					return true
				})
				return items
			},
			ListSemantic: func() portal.SemanticSnapshot {
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
			},
			GetBusObservability: getPortalBusObservability,
			ListProjections: func() []portal.ProjectionDevice {
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
			},
			GetProjection: func(address byte, plane string) (portal.ProjectionGraph, bool) {
				snapshot := builder.FreshSchema()
				for _, device := range snapshot.Devices {
					if device.Address != address {
						continue
					}
					for _, projection := range device.Projections {
						if !strings.EqualFold(projection.Plane, plane) {
							continue
						}
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
						}, true
					}
				}
				return portal.ProjectionGraph{}, false
			},
			ExplorerBus:            gateway.Bus,
			ExplorerSourceProvider: ebusSourceProvider,
			// Wire the in-process L7 catalog sub-server (M5_PORTAL).
			// mcpServer.EbusStandardServer() returns the same instance
			// RegisterEbusStandardTools installed inside mcp.NewServer;
			// sharing it between MCP + portal surfaces guarantees both
			// reach the identical SHA256-pinned embedded catalog. Nil
			// here would make /api/v1/ebus-standard/* routes 404 in
			// production (handler.go nil-guard) — see PR #507.
			EbusStandardServer: mcpServer.EbusStandardServer(),
		})
		mux.Handle(portalPath+"/", http.StripPrefix(portalPath, portalHandler))
		mux.HandleFunc(portalPath, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, portalPath+"/", http.StatusMovedPermanently)
		})
	}

	return startHTTPControlPlaneListener(ctx, cfg, mux, gateway, mcpServer, eebusProvider)
}
