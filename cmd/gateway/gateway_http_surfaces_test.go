package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/schema"
)

// Synthetic registry fixture: one plane with one read-only method and one
// observability projection with two nodes and one edge.
type httpSurfaceFixturePlane struct{}

func (httpSurfaceFixturePlane) Name() string { return "fixture" }
func (httpSurfaceFixturePlane) Methods() []registry.Method {
	return []registry.Method{httpSurfaceFixtureMethod{}}
}

type httpSurfaceFixtureMethod struct{}

func (httpSurfaceFixtureMethod) Name() string   { return "get_status" }
func (httpSurfaceFixtureMethod) ReadOnly() bool { return true }
func (httpSurfaceFixtureMethod) Template() registry.FrameTemplate {
	return httpSurfaceFixtureTemplate{}
}
func (httpSurfaceFixtureMethod) ResponseSchema() schema.SchemaSelector {
	return schema.SchemaSelector{Default: schema.Schema{Fields: []schema.SchemaField{{Name: "status", Type: types.DATA1b{}}}}}
}

type httpSurfaceFixtureTemplate struct{}

func (httpSurfaceFixtureTemplate) Primary() byte   { return 0x07 }
func (httpSurfaceFixtureTemplate) Secondary() byte { return 0x04 }

type httpSurfaceFixtureProvider struct{}

func (httpSurfaceFixtureProvider) Name() string                   { return "fixture" }
func (httpSurfaceFixtureProvider) Match(registry.DeviceInfo) bool { return true }
func (httpSurfaceFixtureProvider) CreatePlanes(registry.DeviceInfo) []registry.Plane {
	return []registry.Plane{httpSurfaceFixturePlane{}}
}
func (httpSurfaceFixtureProvider) CreateProjections(info registry.DeviceInfo, _ []registry.Plane) []registry.Projection {
	base := []registry.PathSegment{{Name: "ebus"}, {Name: fmt.Sprintf("addr@%02X", info.Address)}, {Name: "device@" + info.DeviceID}}
	child := append(append([]registry.PathSegment(nil), base...), registry.PathSegment{Name: "method@get_status"})
	root, err := registry.NewNode(registry.ProjectionPath{Plane: "Observability", Segments: base}, registry.ProjectionPath{Plane: registry.ServicePlane, Segments: base})
	if err != nil {
		return nil
	}
	leaf, err := registry.NewNode(registry.ProjectionPath{Plane: "Observability", Segments: child}, registry.ProjectionPath{Plane: registry.ServicePlane, Segments: child})
	if err != nil {
		return nil
	}
	edge, err := registry.NewEdge("Observability", root.ID, leaf.ID)
	if err != nil {
		return nil
	}
	projection, err := registry.NewProjection("Observability", []registry.Node{root, leaf}, []registry.Edge{edge})
	if err != nil {
		return nil
	}
	return []registry.Projection{projection}
}

// httpSurfaceSemanticFixture fills the zone, adapter and FM5 interpretation
// branches of the Portal semantic snapshot, including the degraded reason.
type httpSurfaceSemanticFixture struct{}

func (httpSurfaceSemanticFixture) Zones() []graphql.Zone {
	return []graphql.Zone{{ID: "zone-fixture", Name: "Fixture zone"}}
}
func (httpSurfaceSemanticFixture) DHW() *graphql.DhwStatus                  { return nil }
func (httpSurfaceSemanticFixture) Circuits() []graphql.CircuitStatus        { return nil }
func (httpSurfaceSemanticFixture) RadioDevices() []graphql.RadioDevice      { return nil }
func (httpSurfaceSemanticFixture) FM5SemanticMode() graphql.Fm5SemanticMode { return "" }
func (httpSurfaceSemanticFixture) Solar() *graphql.SolarStatus              { return nil }
func (httpSurfaceSemanticFixture) Cylinders() []graphql.CylinderStatus      { return nil }
func (httpSurfaceSemanticFixture) EnergyTotals() *graphql.EnergyTotals      { return nil }
func (httpSurfaceSemanticFixture) BoilerStatus() *graphql.BoilerStatus      { return nil }
func (httpSurfaceSemanticFixture) System() *graphql.SystemStatus            { return nil }
func (httpSurfaceSemanticFixture) Schedules() *graphql.ScheduleStatus       { return nil }
func (httpSurfaceSemanticFixture) AdapterHardwareInfo() *graphql.AdapterHardwareInfo {
	return &graphql.AdapterHardwareInfo{FirmwareVersion: "fixture-firmware"}
}
func (httpSurfaceSemanticFixture) FM5Interpretation() graphql.Fm5Interpretation {
	return graphql.Fm5Interpretation{
		Mode:             graphql.Fm5SemanticModeGPIOOnly,
		DegradedReason:   graphql.Fm5SemanticDegradedReasonConfigurationNotInterpretable,
		EvidenceRevision: "fixture-revision",
	}
}

type httpSurfaceFixture struct {
	surface gatewayHTTPPortalSurface
}

func newHTTPSurfaceFixture(t *testing.T) httpSurfaceFixture {
	t.Helper()
	reg := registry.NewDeviceRegistry([]registry.PlaneProvider{httpSurfaceFixtureProvider{}})
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Fixture", DeviceID: "fixture-device", SerialNumber: "synthetic-serial", SoftwareVersion: "0100", HardwareVersion: "0200"})
	gateway := &ebusgateway.Gateway{Registry: reg}
	builder := graphql.NewBuilder(reg, nil)
	server, err := mcp.NewServer(reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := ebusgateway.DefaultConfig()
	cfg.GraphQLPath, cfg.SnapshotPath, cfg.SubscriptionPath, cfg.MCPPath = "/fixture/graphql", "/fixture/snapshot", "/fixture/subscriptions", "/fixture/mcp"
	cfg.PortalPV.SemanticEnabled, cfg.PortalPV.RawReadEnabled, cfg.PortalStorage.SemanticEnabled = true, true, true
	cfg.PortalConfigRoot = t.TempDir()
	portalConfig := openGatewayPortalConfigRuntime(cfg)
	t.Cleanup(func() { _ = portalConfig.Close() })
	forwarded := func(context.Context) (portal.ForwardedResponse, error) {
		return portal.ForwardedResponse{Status: 204}, nil
	}
	return httpSurfaceFixture{surface: gatewayHTTPPortalSurface{
		cfg: cfg, gateway: gateway, builder: builder, mcpServer: server,
		semanticProvider:   httpSurfaceSemanticFixture{},
		ebusProxyReadiness: func() string { return "READY" },
		ebusSourceProvider: func() (byte, bool) { return 0x31, true },
		busObservability:   ebusgateway.NewBusObservabilityStore(ebusgateway.DefaultConfig()),
		buildInfo:          gatewayBuildInfo{ReleaseVersion: "0.0.0-fixture", BuildID: "fixture-build"},
		portalConfig:       portalConfig,
		semanticPV:         forwarded,
		semanticStorage:    forwarded,
		storageAvailable:   true,
	}}
}

// httpSurfaceInlinePortalOptions is the Portal options literal that the HTTP
// server composed inline before the Portal surface had its own builder. It is
// the reference the named composition must reproduce.
func httpSurfaceInlinePortalOptions(s gatewayHTTPPortalSurface) portal.Options {
	cfg, gateway, builder, semanticProvider := s.cfg, s.gateway, s.builder, s.semanticProvider
	portalModbusProvider := portalTCPModbusProvider(s.modbusProvider)
	var getPortalBusObservability func() any
	if s.busObservability != nil {
		getPortalBusObservability = func() any {
			return s.busObservability.Snapshot().Summary
		}
	}
	return portal.Options{
		Config:                 s.portalConfig.callbacks(cfg, s.runtimeControl),
		GraphQLPath:            cfg.GraphQLPath,
		SnapshotPath:           cfg.SnapshotPath,
		SubscriptionPath:       cfg.SubscriptionPath,
		MCPPath:                cfg.MCPPath,
		EEBusAdminPath:         "/admin/eebus/v1",
		Readiness:              gatewayPortalReadiness(s.eebusProvider, s.eebusLifecycle, s.ebusProxyReadiness),
		GatewayVersion:         s.buildInfo.ReleaseVersion,
		BuildID:                s.buildInfo.BuildID,
		SemanticPVEnabled:      cfg.PortalPV.SemanticEnabled,
		SemanticStorageEnabled: cfg.PortalStorage.SemanticEnabled && s.storageAvailable,
		RawModbusEnabled:       cfg.PortalPV.RawReadEnabled,
		SemanticPV:             s.semanticPV,
		SemanticStorage:        s.semanticStorage,
		ModbusProvider:         portalModbusProvider,
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
					Address: int(entry.PrimaryDisplayAddress()), Addresses: intAddrs, Manufacturer: entry.Manufacturer(),
					DeviceID: entry.DeviceID(), SerialNumber: entry.SerialNumber(), Software: entry.SoftwareVersion(),
					Hardware: entry.HardwareVersion(), Planes: make([]portal.RegistryPlane, 0),
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
					device.Planes = append(device.Planes, portal.RegistryPlane{Name: plane.Name(), Methods: methodNames})
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
				Zones: mapPortalZones(semanticProvider.Zones()), DHW: mapPortalDHW(semanticProvider.DHW()),
				Energy: mapPortalEnergyTotals(semanticProvider.EnergyTotals()), BoilerStatus: mapPortalBoilerStatus(semanticProvider.BoilerStatus()),
				System: mapPortalSystemStatus(semanticProvider.System()), Circuits: mapPortalCircuits(semanticProvider.Circuits()),
				RadioDevices: mapPortalRadioDevices(semanticProvider.RadioDevices()), FM5Mode: string(verdict.Mode),
				FM5DegradedReason: degradedReason, FM5EvidenceRevision: verdict.EvidenceRevision,
				Solar: mapPortalSolarStatus(semanticProvider.Solar()), Cylinders: mapPortalCylinders(semanticProvider.Cylinders()),
				AdapterInfo: mapPortalAdapterInfo(semanticProvider.AdapterHardwareInfo()), CapturedUTC: time.Now().UTC().Format(time.RFC3339),
			}
		},
		GetBusObservability: getPortalBusObservability,
		ListProjections: func() []portal.ProjectionDevice {
			snapshot := builder.FreshSchema()
			items := make([]portal.ProjectionDevice, 0, len(snapshot.Devices))
			for _, device := range snapshot.Devices {
				summaries := make([]portal.ProjectionSummary, 0, len(device.Projections))
				for _, projection := range device.Projections {
					summaries = append(summaries, portal.ProjectionSummary{Plane: projection.Plane, NodeCount: len(projection.Nodes), EdgeCount: len(projection.Edges)})
				}
				items = append(items, portal.ProjectionDevice{Address: device.Address, DeviceID: device.DeviceID, DisplayName: device.DisplayName, Manufacturer: device.Manufacturer, Projections: summaries})
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
						nodes = append(nodes, portal.ProjectionNode{ID: node.ID, Path: node.Path, CanonicalPath: node.CanonicalPath})
					}
					edges := make([]portal.ProjectionEdge, 0, len(projection.Edges))
					for _, edge := range projection.Edges {
						edges = append(edges, portal.ProjectionEdge{ID: edge.ID, From: edge.From, To: edge.To})
					}
					return portal.ProjectionGraph{Address: address, Plane: projection.Plane, Nodes: nodes, Edges: edges}, true
				}
			}
			return portal.ProjectionGraph{}, false
		},
		ExplorerBus:            gateway.Bus,
		ExplorerSourceProvider: s.ebusSourceProvider,
		EbusStandardServer:     s.mcpServer.EbusStandardServer(),
	}
}

// httpSurfacePortalView evaluates every field of the options: values are kept,
// functions are invoked on fixed inputs, and functions that would forward a
// request are compared by identity. A Portal option field this view does not
// know fails the test, so a new field cannot escape the comparison.
func httpSurfacePortalView(t *testing.T, opts portal.Options, audit *bytes.Buffer) map[string]any {
	t.Helper()
	view := map[string]any{}
	value := reflect.ValueOf(opts)
	for i := 0; i < value.NumField(); i++ {
		name := value.Type().Field(i).Name
		field := value.Field(i)
		if field.Kind() != reflect.Func && field.Kind() != reflect.Ptr {
			view[name] = field.Interface()
			continue
		}
		if field.IsNil() {
			view[name] = "<nil>"
			continue
		}
		switch name {
		case "Config":
			callbacks := opts.Config
			record, err := callbacks.Read(context.Background())
			view[name+".Read"] = fmt.Sprint(record, err)
			view[name+".Bindings"] = callbacks.Bindings != nil
			view[name+".Update"] = callbacks.Update != nil
			view[name+".Drivers"] = callbacks.Drivers(context.Background(), nil)
		case "Contributions":
			view[name] = *opts.Contributions
		case "SharedSemanticView":
			view[name] = "<set>"
		case "Readiness":
			view[name] = opts.Readiness()
		case "ListRegistry":
			view[name] = opts.ListRegistry()
		case "ListSemantic":
			// The capture time is the only value that may differ between calls.
			snapshot := opts.ListSemantic()
			view[name+".Captured"] = snapshot.CapturedUTC != ""
			snapshot.CapturedUTC = ""
			view[name] = snapshot
		case "GetBusObservability":
			view[name] = opts.GetBusObservability()
		case "ListProjections":
			view[name] = opts.ListProjections()
		case "GetProjection":
			for _, query := range []struct {
				address byte
				plane   string
			}{{0x15, "observability"}, {0x15, "Observability"}, {0x15, "service"}, {0x16, "Observability"}} {
				graph, ok := opts.GetProjection(query.address, query.plane)
				view[fmt.Sprintf("%s(%02X,%s)", name, query.address, query.plane)] = fmt.Sprint(graph, ok)
			}
		case "ExplorerSourceProvider":
			source, ok := opts.ExplorerSourceProvider()
			view[name] = fmt.Sprint(field.Pointer(), source, ok)
		case "SemanticPV", "SemanticStorage":
			view[name] = field.Pointer()
		case "RawModbusAudit":
			audit.Reset()
			unit, function, offset, quantity := uint64(3), uint64(4), uint64(100), uint64(2)
			opts.RawModbusAudit(portal.RawModbusAuditEvent{
				RequestID: "fixture-request", Surface: "portal", Tool: "raw_read", UnitID: &unit, Function: &function, Offset: &offset, Quantity: &quantity, ErrorCode: "fixture-code",
				Outcome: "ok", DurationMS: 7, EndpointRef: "fixture-endpoint", At: time.Unix(1, 0).UTC(),
			})
			line := audit.String()
			index := strings.Index(line, "portal_modbus_raw_audit request_id=fixture-request")
			if index < 0 {
				t.Fatalf("audit line missing: %q", line)
			}
			view[name] = strings.SplitN(line[index:], "\n", 2)[0]
		default:
			t.Fatalf("Portal option %s is not covered by the comparison", name)
		}
	}
	return view
}

func captureHTTPSurfaceAudit(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	writer, flags := log.Writer(), log.Flags()
	log.SetOutput(&buffer)
	t.Cleanup(func() {
		log.SetOutput(writer)
		log.SetFlags(flags)
	})
	return &buffer
}

func TestGatewayHTTPSurfacesPortalOptionsMatchInlineComposition(t *testing.T) {
	audit := captureHTTPSurfaceAudit(t)
	fixture := newHTTPSurfaceFixture(t)
	// The second configuration takes every absent-collaborator branch.
	sparse := fixture.surface
	sparse.storageAvailable, sparse.busObservability, sparse.semanticProvider = false, nil, nil
	sparse.cfg.PortalPV.SemanticEnabled = false
	for name, surface := range map[string]gatewayHTTPPortalSurface{"composed": fixture.surface, "sparse": sparse} {
		options, err := newGatewayHTTPPortalOptions(surface)
		if err != nil {
			t.Fatal(err)
		}
		gotView := httpSurfacePortalView(t, options, audit)
		wantView := httpSurfacePortalView(t, httpSurfaceInlinePortalOptions(surface), audit)
		if !reflect.DeepEqual(gotView, wantView) {
			for key := range wantView {
				if !reflect.DeepEqual(gotView[key], wantView[key]) {
					t.Errorf("%s %s: got %#v want %#v", name, key, gotView[key], wantView[key])
				}
			}
			t.Fatalf("%s Portal options differ from the inline composition", name)
		}
	}
	got, err := newGatewayHTTPPortalOptions(fixture.surface)
	if err != nil {
		t.Fatal(err)
	}
	gotView := httpSurfacePortalView(t, got, audit)
	// The fixture must reach every branch the comparison relies on.
	devices := got.ListRegistry()
	if len(devices) != 1 || len(devices[0].Planes) != 1 || len(devices[0].Planes[0].Methods) != 1 || devices[0].SerialNumber != "synthetic-serial" {
		t.Fatalf("registry fixture not exercised: %+v", devices)
	}
	if graph, ok := got.GetProjection(0x15, "observability"); !ok || len(graph.Nodes) != 2 || len(graph.Edges) != 1 {
		t.Fatalf("projection fixture not exercised: %+v %t", graph, ok)
	}
	if semantic := got.ListSemantic(); semantic.FM5DegradedReason == nil || len(semantic.Zones) != 1 || semantic.AdapterInfo == nil {
		t.Fatalf("semantic fixture not exercised: %+v", semantic)
	}
	if got.GetBusObservability == nil || got.Config == nil || got.EbusStandardServer == nil || got.ModbusProvider != nil {
		t.Fatal("fixture options lost a composed collaborator")
	}

	// The comparison is not vacuous: a changed composition is reported.
	changed := fixture.surface
	changed.storageAvailable = false
	changed.cfg.MCPPath = "/fixture/other"
	if reflect.DeepEqual(httpSurfacePortalView(t, httpSurfaceInlinePortalOptions(changed), audit), gotView) {
		t.Fatal("comparison accepted a different composition")
	}
	withoutStore := fixture.surface
	withoutStore.busObservability = nil
	options, err := newGatewayHTTPPortalOptions(withoutStore)
	if err != nil || options.GetBusObservability != nil {
		t.Fatalf("no bus store must leave the bus read unset: err=%v", err)
	}
}

func TestGatewayHTTPSurfacesRejectMissingPortalWiring(t *testing.T) {
	for name, mutate := range map[string]func(*gatewayHTTPPortalSurface){
		"gateway":              func(s *gatewayHTTPPortalSurface) { s.gateway = nil },
		"builder":              func(s *gatewayHTTPPortalSurface) { s.builder = nil },
		"mcp server":           func(s *gatewayHTTPPortalSurface) { s.mcpServer = nil },
		"portal configuration": func(s *gatewayHTTPPortalSurface) { s.portalConfig = nil },
	} {
		t.Run(name, func(t *testing.T) {
			surface := newHTTPSurfaceFixture(t).surface
			mutate(&surface)
			options, err := newGatewayHTTPPortalOptions(surface)
			if err == nil || !strings.Contains(err.Error(), name+" missing for http portal surface") {
				t.Fatalf("err = %v; want missing %s", err, name)
			}
			if !reflect.DeepEqual(httpSurfaceOptionPresence(options), httpSurfaceOptionPresence(portal.Options{})) {
				t.Fatal("rejected Portal surface returned composed options")
			}
		})
	}
}

func httpSurfaceOptionPresence(opts portal.Options) []bool {
	value := reflect.ValueOf(opts)
	present := make([]bool, value.NumField())
	for i := range present {
		present[i] = !value.Field(i).IsZero()
	}
	return present
}

func httpSurfaceMCPFixture(t *testing.T) gatewayHTTPMCPSurface {
	t.Helper()
	reg := registry.NewDeviceRegistry(nil)
	server, err := mcp.NewServer(reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := ebusgateway.DefaultConfig()
	// A protocol outside the published responder rows leaves the package-level
	// responder capability untouched.
	cfg.TransportConfig.Protocol = "fixture-unpublished"
	return gatewayHTTPMCPSurface{server: server, gateway: &ebusgateway.Gateway{Registry: reg}, builder: graphql.NewBuilder(reg, nil), cfg: &cfg}
}

func httpSurfaceVaillantTools(t *testing.T, server *mcp.Server) []string {
	t.Helper()
	tools := []string{}
	for _, name := range gatewayInventoryNames(t, server) {
		if strings.Contains(name, "vaillant") {
			tools = append(tools, name)
		}
	}
	return tools
}

func TestGatewayHTTPSurfacesRejectMissingMCPWiring(t *testing.T) {
	for name, mutate := range map[string]func(*gatewayHTTPMCPSurface){
		"mcp server":      func(s *gatewayHTTPMCPSurface) { s.server = nil },
		"gateway":         func(s *gatewayHTTPMCPSurface) { s.gateway = nil },
		"graphql builder": func(s *gatewayHTTPMCPSurface) { s.builder = nil },
		"configuration":   func(s *gatewayHTTPMCPSurface) { s.cfg = nil },
	} {
		t.Run(name, func(t *testing.T) {
			surface := httpSurfaceMCPFixture(t)
			server := surface.server
			mutate(&surface)
			err := installGatewayHTTPMCPSurface(surface)
			if err == nil || !strings.Contains(err.Error(), name+" missing for http mcp surface") {
				t.Fatalf("err = %v; want missing %s", err, name)
			}
			if tools := httpSurfaceVaillantTools(t, server); len(tools) != 0 {
				t.Fatalf("rejected MCP surface installed %v", tools)
			}
		})
	}
}

func TestGatewayHTTPSurfacesInstallMCPSurface(t *testing.T) {
	surface := httpSurfaceMCPFixture(t)
	if tools := httpSurfaceVaillantTools(t, surface.server); len(tools) != 0 {
		t.Fatalf("fixture server already lists %v", tools)
	}
	if err := installGatewayHTTPMCPSurface(surface); err != nil {
		t.Fatal(err)
	}
	if tools := httpSurfaceVaillantTools(t, surface.server); len(tools) == 0 {
		t.Fatal("installed MCP surface lists no Vaillant B503 tools")
	}
}

func TestGatewayHTTPSurfacesServerRejectsMissingWiring(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	gateway := &ebusgateway.Gateway{Registry: reg}
	builder := graphql.NewBuilder(reg, nil)
	hub := graphql.NewBroadcastHub(nil)
	cfg := ebusgateway.DefaultConfig()
	cfg.HTTPAddr = "127.0.0.1:0"
	for name, inputs := range map[string]struct {
		gateway *ebusgateway.Gateway
		builder *graphql.Builder
		hub     *graphql.BroadcastHub
		want    string
	}{
		"gateway": {nil, builder, hub, "gateway missing for http server"},
		"builder": {gateway, nil, hub, "graphql builder missing for http server"},
		"hub":     {gateway, builder, nil, "graphql broadcast hub missing for http server"},
	} {
		t.Run(name, func(t *testing.T) {
			server, advertiser, err := startHTTPServer(context.Background(), cfg, inputs.gateway, inputs.builder, inputs.hub,
				nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, gatewayBuildInfo{}, nil, nil, nil)
			if server != nil || advertiser != nil || err == nil || err.Error() != inputs.want {
				t.Fatalf("server=%v advertiser=%v err=%v; want %q", server, advertiser, err, inputs.want)
			}
		})
	}
}
