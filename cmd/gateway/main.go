package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	"github.com/Project-Helianthus/helianthus-ebusgateway/ui"
	vaillantproviders "github.com/Project-Helianthus/helianthus-ebusreg/providers/vaillant"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

var (
	buildVersion = "dev"
	buildID      = "unknown"
)

func main() {
	cfg := ebusgateway.DefaultConfig()
	bindFlags(flag.CommandLine, &cfg)
	flag.Parse()
	applyTransportSourcePolicy(&cfg)

	if cfg.ScanSource == 0x31 && !cfg.ScanSourceAuto {
		log.Printf("warning: source-addr=0x31 is commonly used by ebusd; when running alongside ebusd pick a different source address (e.g. --source-addr=0xf0)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		log.Fatalf("gateway: %v", err)
	}
}

func run(ctx context.Context, cfg ebusgateway.Config) error {
	applyTransportSourcePolicy(&cfg)

	if len(cfg.Providers) == 0 {
		cfg.Providers = vaillantproviders.Default()
	}

	busObservability, deduplicator, err := wireObserveFirstObservers(&cfg)
	if err != nil {
		return err
	}

	gateway, err := ebusgateway.New(ctx, cfg)
	if err != nil {
		return err
	}

	defer func() {
		if err := gateway.Close(); err != nil {
			log.Printf("gateway close: %v", err)
		}
	}()

	gateway.Start(ctx)

	builder := graphql.NewBuilder(gateway.Registry, nil)
	builder.SetStatusProvider(newRuntimeStatusProvider(cfg))
	hub := graphql.NewBroadcastHub(nil)
	gateway.AddRouterPlane(hub)
	gateway.RefreshRouterPlanes()

	semanticRuntime := graphql.WireSemantic(builder, gateway.Router, hub)
	semanticRuntime.SetBootLiveTimeout(cfg.BootLiveTimeout)
	semanticRuntime.Start(ctx)
	semanticPoller := startVaillantSemanticPolling(ctx, cfg, gateway, semanticRuntime.Provider(), hub)
	if semanticPoller != nil {
		builder.SetBoilerConfigWriter(semanticPoller)
		builder.SetScheduleWriter(semanticPoller)
	}

	if err := builder.Start(ctx); err != nil {
		return err
	}

	startDiscoveryScanLoop(ctx, cfg, gateway, builder)

	var scheduleWriter mcp.ScheduleWriter
	if semanticPoller != nil {
		scheduleWriter = semanticPoller
	}
	server, advertiser, err := startHTTPServer(ctx, cfg, gateway, builder, hub, semanticRuntime.Provider(), scheduleWriter, busObservability)
	if err != nil {
		return err
	}
	var listener *ebusgateway.BroadcastListener
	var reconstructor *ebusgateway.PassiveTransactionReconstructor
	if cfg.BroadcastListen {
		reconstructor, err = ebusgateway.StartPassiveTransactionReconstructor(ctx, cfg)
		if err != nil {
			if advertiser != nil {
				_ = advertiser.Close()
			}
			if server != nil {
				_ = server.Close()
			}
			return err
		}
		if busObservability != nil {
			if err := busObservability.AttachReconstructor(ctx, reconstructor); err != nil {
				_ = reconstructor.Close()
				if advertiser != nil {
					_ = advertiser.Close()
				}
				if server != nil {
					_ = server.Close()
				}
				return err
			}
		}
		if deduplicator != nil {
			if err := deduplicator.AttachReconstructor(ctx, reconstructor); err != nil {
				_ = reconstructor.Close()
				if advertiser != nil {
					_ = advertiser.Close()
				}
				if server != nil {
					_ = server.Close()
				}
				return err
			}
		}
		listener, err = ebusgateway.StartBroadcastListenerWithReconstructor(ctx, gateway.Router, reconstructor)
		if err != nil {
			_ = reconstructor.Close()
			if advertiser != nil {
				_ = advertiser.Close()
			}
			if server != nil {
				_ = server.Close()
			}
			return err
		}
	}
	defer func() {
		if listener != nil {
			if err := listener.Close(); err != nil {
				log.Printf("broadcast listener close: %v", err)
			}
		}
		if deduplicator != nil {
			if err := deduplicator.Close(); err != nil {
				log.Printf("deduplicator close: %v", err)
			}
		}
		if reconstructor != nil {
			if err := reconstructor.Close(); err != nil {
				log.Printf("reconstructor close: %v", err)
			}
		}
		if busObservability != nil {
			if err := busObservability.Close(); err != nil {
				log.Printf("bus observability close: %v", err)
			}
		}
		if advertiser != nil {
			if err := advertiser.Close(); err != nil {
				log.Printf("mdns close: %v", err)
			}
		}
		if server != nil {
			if err := server.Close(); err != nil {
				log.Printf("http server close: %v", err)
			}
		}
	}()

	<-ctx.Done()
	return nil
}

func wireObserveFirstObservers(cfg *ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
	if cfg == nil {
		return nil, nil, nil
	}

	observerCfg := *cfg

	var deduplicator *ebusgateway.ActivePassiveDeduplicator
	if cfg.BroadcastListen {
		dedup, err := ebusgateway.NewActivePassiveDeduplicator(observerCfg)
		if err != nil {
			return nil, nil, err
		}
		deduplicator = dedup
		observerCfg.LocalAddressSnapshotter = deduplicator
	}

	busObservability := ebusgateway.NewBusObservabilityStore(observerCfg)
	cfg.BusConfig.Observer = ebusgateway.ChainBusObservers(cfg.BusConfig.Observer, busObservability)
	if deduplicator != nil {
		cfg.BusConfig.Observer = ebusgateway.ChainBusObservers(cfg.BusConfig.Observer, deduplicator)
	}

	return busObservability, deduplicator, nil
}

func applyTransportSourcePolicy(cfg *ebusgateway.Config) {
	if cfg == nil {
		return
	}

	protocol := strings.TrimSpace(strings.ToLower(string(cfg.TransportConfig.Protocol)))
	switch protocol {
	case "ebusd", "ebusd-tcp":
		if cfg.ScanSourceAuto || cfg.ScanSource == 0xF0 {
			cfg.ScanSource = 0x31
		}
	default:
		if cfg.ScanSourceAuto {
			cfg.ScanSource = 0x00
		}
	}
}

func bindFlags(fs *flag.FlagSet, cfg *ebusgateway.Config) {
	if fs == nil || cfg == nil {
		return
	}

	fs.StringVar((*string)(&cfg.TransportConfig.Protocol), "transport", string(cfg.TransportConfig.Protocol), "transport protocol: enh, ens, udp-plain, tcp-plain, or ebusd-tcp")
	fs.StringVar(&cfg.TransportConfig.Network, "network", cfg.TransportConfig.Network, "transport network: unix, tcp, or udp")
	fs.StringVar(&cfg.TransportConfig.Address, "address", cfg.TransportConfig.Address, "transport address (unix socket path or host:port)")
	fs.DurationVar(&cfg.TransportConfig.ReadTimeout, "read-timeout", cfg.TransportConfig.ReadTimeout, "transport read timeout")
	fs.DurationVar(&cfg.TransportConfig.WriteTimeout, "write-timeout", cfg.TransportConfig.WriteTimeout, "transport write timeout")
	fs.DurationVar(&cfg.TransportConfig.DialTimeout, "dial-timeout", cfg.TransportConfig.DialTimeout, "transport dial timeout")
	fs.IntVar(&cfg.QueueCapacity, "queue-capacity", cfg.QueueCapacity, "bus queue capacity (0 uses protocol default)")
	fs.BoolVar(&cfg.ScanOnStart, "scan", cfg.ScanOnStart, "scan bus on startup")
	fs.DurationVar(&cfg.ScanTimeout, "scan-timeout", cfg.ScanTimeout, "startup scan timeout")
	fs.DurationVar(&cfg.ScanRequestTimeout, "scan-request-timeout", cfg.ScanRequestTimeout, "startup scan per-request timeout")
	fs.DurationVar(&cfg.ScanInterval, "scan-interval", cfg.ScanInterval, "startup scan retry interval (when scan finds 0 devices)")
	fs.DurationVar(&cfg.BootLiveTimeout, "boot-live-timeout", cfg.BootLiveTimeout, "semantic startup timeout before entering degraded mode")
	fs.DurationVar(&cfg.SemanticDiscoveryInterval, "semantic-discovery-interval", cfg.SemanticDiscoveryInterval, "semantic discovery polling interval")
	fs.DurationVar(&cfg.SemanticConfigInterval, "semantic-config-interval", cfg.SemanticConfigInterval, "semantic config polling interval")
	fs.DurationVar(&cfg.SemanticStateInterval, "semantic-state-interval", cfg.SemanticStateInterval, "semantic state polling interval")
	fs.DurationVar(&cfg.SemanticEnergyInterval, "semantic-energy-interval", cfg.SemanticEnergyInterval, "semantic energy polling interval")
	fs.DurationVar(&cfg.SemanticRequestTimeout, "semantic-request-timeout", cfg.SemanticRequestTimeout, "semantic per-request timeout")
	fs.Func("semantic-read-breaker-failure-budget", "semantic read breaker consecutive failure budget (<=0 disables)", func(value string) error {
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("invalid semantic-read-breaker-failure-budget %q", value)
		}
		cfg.SemanticReadBreakerFailureBudget = parsed
		cfg.SemanticReadBreakerFailureBudgetSet = true
		return nil
	})
	fs.DurationVar(&cfg.SemanticReadBreakerOpenCooldown, "semantic-read-breaker-open-cooldown", cfg.SemanticReadBreakerOpenCooldown, "semantic read breaker open-state cooldown before probe")
	fs.IntVar(&cfg.SemanticReadBreakerHalfOpenProbeLimit, "semantic-read-breaker-half-open-probe-limit", cfg.SemanticReadBreakerHalfOpenProbeLimit, "semantic read breaker half-open probes per cooldown window")
	fs.IntVar(&cfg.SemanticZonePresenceMissThreshold, "semantic-zone-presence-miss-threshold", cfg.SemanticZonePresenceMissThreshold, "consecutive discovery misses required before a zone is marked absent")
	fs.IntVar(&cfg.SemanticZonePresenceHitThreshold, "semantic-zone-presence-hit-threshold", cfg.SemanticZonePresenceHitThreshold, "consecutive discovery hits required before an absent zone is marked present")
	fs.DurationVar(&cfg.SemanticDHWStaleTTL, "semantic-dhw-stale-ttl", cfg.SemanticDHWStaleTTL, "maximum age to keep DHW last-known state during cache-sourced/transient failures")
	fs.DurationVar(&cfg.SemanticRegulatorRecheckInterval, "semantic-regulator-recheck-interval", cfg.SemanticRegulatorRecheckInterval, "regulator capability re-evaluation interval")
	fs.DurationVar(&cfg.SemanticRegulatorAbsenceGrace, "semantic-regulator-absence-grace", cfg.SemanticRegulatorAbsenceGrace, "grace window before WARN_NO_REGULATOR after regulator disappears")
	fs.StringVar(&cfg.SemanticCachePath, "semantic-cache-path", cfg.SemanticCachePath, "semantic cache file path for startup preload and live persistence")
	fs.Func("semantic-interval", "DEPRECATED: semantic state polling interval", func(value string) error {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid semantic-interval %q", value)
		}
		cfg.SemanticInterval = duration
		cfg.SemanticStateInterval = duration
		return nil
	})
	fs.BoolVar(&cfg.BroadcastListen, "broadcast", cfg.BroadcastListen, "enable broadcast listener (separate connection)")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "http listen address (empty disables)")
	fs.StringVar(&cfg.GraphQLPath, "graphql-path", cfg.GraphQLPath, "graphql endpoint path")
	fs.StringVar(&cfg.SnapshotPath, "snapshot-path", cfg.SnapshotPath, "projection snapshot endpoint path")
	fs.StringVar(&cfg.SubscriptionPath, "subscription-path", cfg.SubscriptionPath, "graphql subscriptions path")
	fs.StringVar(&cfg.MCPPath, "mcp-path", cfg.MCPPath, "mcp endpoint path")
	fs.StringVar(&cfg.UIPath, "ui-path", cfg.UIPath, "portal ui path")
	fs.StringVar(&cfg.PortalPath, "portal-path", cfg.PortalPath, "dynamic portal path")
	fs.StringVar(&cfg.DumpUploadPath, "dump-upload-path", cfg.DumpUploadPath, "register dump upload endpoint path")
	fs.BoolVar(&cfg.MDNSAdvertise, "mdns", cfg.MDNSAdvertise, "advertise graphql endpoint via mdns")
	fs.StringVar(&cfg.MDNSInstance, "mdns-instance", cfg.MDNSInstance, "mdns instance name")
	fs.StringVar(&cfg.DumpOutputDir, "dump-output-dir", cfg.DumpOutputDir, "unknown device dump output dir")
	fs.StringVar(&cfg.DumpUploadURL, "dump-upload-url", cfg.DumpUploadURL, "unknown device dump upload url (internal)")
	fs.BoolVar(&cfg.DumpIncludePII, "dump-include-pii", cfg.DumpIncludePII, "include identifiers in unknown device dumps")

	fs.Func("source-addr", "source address for scans/semantic reads (e.g. 0xf0, 0x00, or auto)", func(value string) error {
		value = strings.TrimSpace(strings.ToLower(value))
		if value == "" {
			return nil
		}
		if value == "auto" {
			cfg.ScanSource = 0x00
			cfg.ScanSourceAuto = true
			return nil
		}
		parsed, err := strconv.ParseUint(value, 0, 8)
		if err != nil {
			return fmt.Errorf("invalid source-addr %q", value)
		}
		cfg.ScanSource = byte(parsed)
		cfg.ScanSourceAuto = cfg.ScanSource == 0x00
		return nil
	})
}

func startHTTPServer(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, builder *graphql.Builder, hub *graphql.BroadcastHub, semanticProvider graphql.SemanticProvider, scheduleWriter mcp.ScheduleWriter, busObservability *ebusgateway.BusObservabilityStore) (*http.Server, mdns.Advertiser, error) {
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

	queryHandler, err := graphql.NewInvokeHandler(builder, gateway.Registry, gateway.Router)
	if err != nil {
		return nil, nil, err
	}
	snapshotHandler, err := graphql.NewProjectionSnapshotHandler(builder)
	if err != nil {
		return nil, nil, err
	}
	subscriptionHandler, err := graphql.NewSubscriptionHandler(builder, gateway.Registry, gateway.Router, hub)
	if err != nil {
		return nil, nil, err
	}
	mcpServer, err := mcp.NewServer(gateway.Registry, gateway.Router)
	if err != nil {
		return nil, nil, err
	}
	mcpServer.SetStatusProvider(newMCPRuntimeStatusProvider(cfg))
	mcpServer.SetSemanticProvider(newMCPSemanticProvider(semanticProvider))
	if scheduleWriter != nil {
		mcpServer.SetScheduleWriter(scheduleWriter)
	}

	mux := http.NewServeMux()
	if busObservability != nil {
		mux.Handle(normalizeMountPath(cfg.MetricsPath, ebusgateway.DefaultMetricsPath), busObservability.MetricsHandler())
	}
	mux.Handle(cfg.GraphQLPath, queryHandler)
	mux.Handle(cfg.SnapshotPath, snapshotHandler)
	mux.Handle(cfg.SubscriptionPath, subscriptionHandler)
	mux.Handle(cfg.MCPPath, mcpServer.Handler())
	if cfg.DumpUploadPath != "" {
		uploadPath := cfg.DumpUploadPath
		if !strings.HasPrefix(uploadPath, "/") {
			uploadPath = "/" + uploadPath
		}
		mux.Handle(uploadPath, ebusgateway.NewRegisterDumpUploadHandler(cfg.DumpOutputDir))
	}
	if cfg.UIPath != "" {
		uiPath := normalizeMountPath(cfg.UIPath, "/ui")
		uiHandler := ui.NewHandler(cfg.GraphQLPath)
		mux.Handle(uiPath+"/", http.StripPrefix(uiPath, uiHandler))
		mux.HandleFunc(uiPath, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, uiPath+"/", http.StatusMovedPermanently)
		})
	}
	if cfg.PortalPath != "" {
		portalPath := normalizeMountPath(cfg.PortalPath, "/portal")
		portalHandler := portal.NewHandler(portal.Options{
			GraphQLPath:      cfg.GraphQLPath,
			SnapshotPath:     cfg.SnapshotPath,
			SubscriptionPath: cfg.SubscriptionPath,
			MCPPath:          cfg.MCPPath,
			GatewayVersion:   buildVersion,
			BuildID:          buildID,
			ListRegistry: func() []portal.RegistryDevice {
				schemaSnapshot := builder.Schema()
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
						Address:      int(entry.Address()),
						Addresses:    intAddrs,
						Manufacturer: entry.Manufacturer(),
						DeviceID:     entry.DeviceID(),
						SerialNumber: entry.SerialNumber(),
						Software:     entry.SoftwareVersion(),
						Hardware:     entry.HardwareVersion(),
						Planes:       make([]portal.RegistryPlane, 0),
					}
					if sd, ok := schemaByAddr[entry.Address()]; ok {
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
				return portal.SemanticSnapshot{
					Zones:        mapPortalZones(semanticProvider.Zones()),
					DHW:          mapPortalDHW(semanticProvider.DHW()),
					Energy:       mapPortalEnergyTotals(semanticProvider.EnergyTotals()),
					BoilerStatus: mapPortalBoilerStatus(semanticProvider.BoilerStatus()),
					System:       mapPortalSystemStatus(semanticProvider.System()),
					Circuits:     mapPortalCircuits(semanticProvider.Circuits()),
					RadioDevices: mapPortalRadioDevices(semanticProvider.RadioDevices()),
					FM5Mode:      string(semanticProvider.FM5SemanticMode()),
					Solar:        mapPortalSolarStatus(semanticProvider.Solar()),
					Cylinders:    mapPortalCylinders(semanticProvider.Cylinders()),
					CapturedUTC:  time.Now().UTC().Format(time.RFC3339),
				}
			},
			ListProjections: func() []portal.ProjectionDevice {
				snapshot := builder.Schema()
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
				snapshot := builder.Schema()
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
			ExplorerBus:    gateway.Bus,
			ExplorerSource: cfg.ScanSource,
		})
		mux.Handle(portalPath+"/", http.StripPrefix(portalPath, portalHandler))
		mux.HandleFunc(portalPath, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, portalPath+"/", http.StatusMovedPermanently)
		})
	}

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return nil, nil, err
	}

	server := &http.Server{
		Handler: mux,
	}

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("http server error: %v", err)
		}
	}()

	var advertiser mdns.Advertiser
	if cfg.MDNSAdvertise {
		port := listener.Addr().(*net.TCPAddr).Port
		advertiser, err = mdns.Advertise(ctx, mdns.Service{
			Instance: cfg.MDNSInstance,
			Service:  mdns.ServiceTypeGateway,
			Port:     port,
			Text: []string{
				"path=" + cfg.GraphQLPath,
				"transport=http",
				"version=1",
			},
		})
		if err != nil {
			_ = server.Close()
			return nil, nil, err
		}
	}

	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()

	return server, advertiser, nil
}

func normalizeMountPath(path string, fallback string) string {
	normalized := strings.TrimSpace(path)
	if normalized == "" {
		normalized = fallback
	}
	if !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	if normalized != "/" {
		normalized = strings.TrimRight(normalized, "/")
	}
	if normalized == "/" {
		return fallback
	}
	return normalized
}

func mapPortalZones(zones []graphql.Zone) []portal.SemanticZone {
	if len(zones) == 0 {
		return nil
	}
	items := make([]portal.SemanticZone, 0, len(zones))
	for _, zone := range zones {
		items = append(items, portal.SemanticZone{
			ID:   zone.ID,
			Name: zone.Name,
			State: portal.SemanticZoneState{
				CurrentTempC:       cloneFloatPtr(zone.State.CurrentTempC),
				CurrentHumidityPct: cloneFloatPtr(zone.State.CurrentHumidityPct),
				HvacAction:         zone.State.HvacAction,
				SpecialFunction:    zone.State.SpecialFunction,
				HeatingDemandPct:   cloneFloatPtr(zone.State.HeatingDemandPct),
				ValvePositionPct:   cloneFloatPtr(zone.State.ValvePositionPct),
			},
			Config: portal.SemanticZoneConfig{
				OperatingMode:              zone.Config.OperatingMode,
				Preset:                     zone.Config.Preset,
				TargetTempC:                cloneFloatPtr(zone.Config.TargetTempC),
				AllowedModes:               append([]string(nil), zone.Config.AllowedModes...),
				CircuitType:                zone.Config.CircuitType,
				AssociatedCircuit:          cloneIntPtr(zone.Config.AssociatedCircuit),
				RoomTemperatureZoneMapping: cloneIntPtr(zone.Config.RoomTemperatureZoneMapping),
			},
		})
	}
	return items
}

func mapPortalDHW(status *graphql.DhwStatus) *portal.SemanticDHW {
	if status == nil {
		return nil
	}
	return &portal.SemanticDHW{
		State: portal.SemanticDhwState{
			CurrentTempC:     cloneFloatPtr(status.State.CurrentTempC),
			SpecialFunction:  status.State.SpecialFunction,
			HeatingDemandPct: cloneFloatPtr(status.State.HeatingDemandPct),
		},
		Config: portal.SemanticDhwConfig{
			OperatingMode: status.Config.OperatingMode,
			Preset:        status.Config.Preset,
			TargetTempC:   cloneFloatPtr(status.Config.TargetTempC),
		},
	}
}

func mapPortalEnergyTotals(value *graphql.EnergyTotals) *portal.SemanticEnergyTotals {
	if value == nil {
		return nil
	}
	return &portal.SemanticEnergyTotals{
		Gas:      mapPortalEnergyChannel(value.Gas),
		Electric: mapPortalEnergyChannel(value.Electric),
		Solar:    mapPortalEnergyChannel(value.Solar),
	}
}

func mapPortalEnergyChannel(channel graphql.EnergyChannel) portal.SemanticEnergyChannel {
	return portal.SemanticEnergyChannel{
		DHW:     mapPortalEnergySeries(channel.DHW),
		Climate: mapPortalEnergySeries(channel.Climate),
	}
}

func mapPortalEnergySeries(series graphql.EnergySeries) portal.SemanticEnergySeries {
	out := portal.SemanticEnergySeries{Today: series.Today}
	if len(series.Yearly) > 0 {
		out.Yearly = append([]float64(nil), series.Yearly...)
	}
	return out
}

func mapPortalBoilerStatus(status *graphql.BoilerStatus) *portal.SemanticBoilerStatus {
	if status == nil {
		return nil
	}
	return &portal.SemanticBoilerStatus{
		State: portal.SemanticBoilerState{
			FlowTemperatureC:         cloneFloatPtr(status.State.FlowTemperatureC),
			ReturnTemperatureC:       cloneFloatPtr(status.State.ReturnTemperatureC),
			CentralHeatingPumpActive: cloneBoolPtr(status.State.CentralHeatingPumpActive),
			WaterPressureBar:         cloneFloatPtr(status.State.WaterPressureBar),
			ExternalPumpActive:       cloneBoolPtr(status.State.ExternalPumpActive),
			CirculationPumpActive:    cloneBoolPtr(status.State.CirculationPumpActive),
			GasValveActive:           cloneBoolPtr(status.State.GasValveActive),
			FlameActive:              cloneBoolPtr(status.State.FlameActive),
			DiverterValvePositionPct: cloneFloatPtr(status.State.DiverterValvePositionPct),
			FanSpeedRpm:              cloneIntPtr(status.State.FanSpeedRpm),
			TargetFanSpeedRpm:        cloneIntPtr(status.State.TargetFanSpeedRpm),
			IonisationVoltageUa:      cloneFloatPtr(status.State.IonisationVoltageUa),
			DhwWaterFlowLpm:          cloneFloatPtr(status.State.DhwWaterFlowLpm),
			DhwDemandActive:          cloneBoolPtr(status.State.DhwDemandActive),
			HeatingSwitchActive:      cloneBoolPtr(status.State.HeatingSwitchActive),
			StorageLoadPumpPct:       cloneFloatPtr(status.State.StorageLoadPumpPct),
			ModulationPct:            cloneFloatPtr(status.State.ModulationPct),
			PrimaryCircuitFlowLpm:    cloneFloatPtr(status.State.PrimaryCircuitFlowLpm),
			FlowTempDesiredC:         cloneFloatPtr(status.State.FlowTempDesiredC),
			DhwTempDesiredC:          cloneFloatPtr(status.State.DhwTempDesiredC),
			StateNumber:              cloneIntPtr(status.State.StateNumber),
			DhwTemperatureC:          cloneFloatPtr(status.State.DhwTemperatureC),
			DhwTargetTemperatureC:    cloneFloatPtr(status.State.DhwTargetTemperatureC),
		},
		Config: portal.SemanticBoilerConfig{
			DhwOperatingMode: cloneStringPtr(status.Config.DhwOperatingMode),
			FlowsetHcMaxC:    cloneFloatPtr(status.Config.FlowsetHcMaxC),
			FlowsetHwcMaxC:   cloneFloatPtr(status.Config.FlowsetHwcMaxC),
			PartloadHcKW:     cloneFloatPtr(status.Config.PartloadHcKW),
			PartloadHwcKW:    cloneFloatPtr(status.Config.PartloadHwcKW),
		},
		Diagnostics: portal.SemanticBoilerDiagnostics{
			HeatingStatusRaw:         cloneIntPtr(status.Diagnostics.HeatingStatusRaw),
			DhwStatusRaw:             cloneIntPtr(status.Diagnostics.DhwStatusRaw),
			CentralHeatingHours:      cloneFloatPtr(status.Diagnostics.CentralHeatingHours),
			DhwHours:                 cloneFloatPtr(status.Diagnostics.DhwHours),
			CentralHeatingStarts:     cloneIntPtr(status.Diagnostics.CentralHeatingStarts),
			DhwStarts:                cloneIntPtr(status.Diagnostics.DhwStarts),
			PumpHours:                cloneFloatPtr(status.Diagnostics.PumpHours),
			FanHours:                 cloneFloatPtr(status.Diagnostics.FanHours),
			DeactivationsIFC:         cloneIntPtr(status.Diagnostics.DeactivationsIFC),
			DeactivationsTemplimiter: cloneIntPtr(status.Diagnostics.DeactivationsTemplimiter),
		},
	}
}

func mapPortalSystemStatus(status *graphql.SystemStatus) *portal.SemanticSystemStatus {
	if status == nil {
		return nil
	}
	return &portal.SemanticSystemStatus{
		State: portal.SemanticSystemState{
			SystemOff:                    cloneBoolPtr(status.State.SystemOff),
			SystemWaterPressure:          cloneFloatPtr(status.State.SystemWaterPressure),
			SystemFlowTemperature:        cloneFloatPtr(status.State.SystemFlowTemperature),
			OutdoorTemperature:           cloneFloatPtr(status.State.OutdoorTemperature),
			OutdoorTemperatureAvg24h:     cloneFloatPtr(status.State.OutdoorTemperatureAvg24h),
			MaintenanceDue:               cloneBoolPtr(status.State.MaintenanceDue),
			HwcCylinderTemperatureTop:    cloneFloatPtr(status.State.HwcCylinderTemperatureTop),
			HwcCylinderTemperatureBottom: cloneFloatPtr(status.State.HwcCylinderTemperatureBottom),
		},
		Config: portal.SemanticSystemConfig{
			AdaptiveHeatingCurve:         cloneBoolPtr(status.Config.AdaptiveHeatingCurve),
			AlternativePoint:             cloneFloatPtr(status.Config.AlternativePoint),
			HeatingCircuitBivalencePoint: cloneFloatPtr(status.Config.HeatingCircuitBivalencePoint),
			DhwBivalencePoint:            cloneFloatPtr(status.Config.DhwBivalencePoint),
			HcEmergencyTemperature:       cloneFloatPtr(status.Config.HcEmergencyTemperature),
			HwcMaxFlowTempDesired:        cloneFloatPtr(status.Config.HwcMaxFlowTempDesired),
			MaxRoomHumidity:              cloneIntPtr(status.Config.MaxRoomHumidity),
		},
		Properties: portal.SemanticSystemProperties{
			SystemScheme:            cloneIntPtr(status.Properties.SystemScheme),
			ModuleConfigurationVR71: cloneIntPtr(status.Properties.ModuleConfigurationVR71),
		},
	}
}

func mapPortalCircuits(circuits []graphql.CircuitStatus) []portal.SemanticCircuit {
	if len(circuits) == 0 {
		return nil
	}
	items := make([]portal.SemanticCircuit, 0, len(circuits))
	for _, circuit := range circuits {
		items = append(items, portal.SemanticCircuit{
			Index:       circuit.Index,
			CircuitType: circuit.CircuitType,
			HasMixer:    circuit.HasMixer,
			State: portal.SemanticCircuitState{
				PumpActive:       cloneBoolPtr(circuit.State.PumpActive),
				MixerPositionPct: cloneFloatPtr(circuit.State.MixerPositionPct),
				FlowTemperatureC: cloneFloatPtr(circuit.State.FlowTemperatureC),
				FlowSetpointC:    cloneFloatPtr(circuit.State.FlowSetpointC),
				CalcFlowTempC:    cloneFloatPtr(circuit.State.CalcFlowTempC),
				CircuitState:     circuit.State.CircuitState,
				Humidity:         cloneFloatPtr(circuit.State.Humidity),
				DewPoint:         cloneFloatPtr(circuit.State.DewPoint),
				PumpHours:        cloneFloatPtr(circuit.State.PumpHours),
				PumpStarts:       cloneIntPtr(circuit.State.PumpStarts),
			},
			Config: portal.SemanticCircuitConfig{
				HeatingCurve:    cloneFloatPtr(circuit.Config.HeatingCurve),
				FlowTempMaxC:    cloneFloatPtr(circuit.Config.FlowTempMaxC),
				FlowTempMinC:    cloneFloatPtr(circuit.Config.FlowTempMinC),
				SummerLimitC:    cloneFloatPtr(circuit.Config.SummerLimitC),
				FrostProtC:      cloneFloatPtr(circuit.Config.FrostProtC),
				RoomTempControl: circuit.Config.RoomTempControl,
				CoolingEnabled:  cloneBoolPtr(circuit.Config.CoolingEnabled),
			},
			ManagingDevice: portal.SemanticManagingDevice{
				Role:     string(circuit.ManagingDevice.Role),
				DeviceID: cloneStringPtr(circuit.ManagingDevice.DeviceID),
				Address:  cloneIntPtr(circuit.ManagingDevice.Address),
			},
		})
	}
	return items
}

func mapPortalRadioDevices(devices []graphql.RadioDevice) []portal.SemanticRadioDevice {
	if len(devices) == 0 {
		return nil
	}
	items := make([]portal.SemanticRadioDevice, 0, len(devices))
	for _, device := range devices {
		items = append(items, portal.SemanticRadioDevice{
			Group:                device.Group,
			Instance:             device.Instance,
			SlotMode:             device.SlotMode,
			DeviceConnected:      cloneBoolPtr(device.DeviceConnected),
			DeviceClassAddress:   cloneIntPtr(device.DeviceClassAddress),
			DeviceModel:          device.DeviceModel,
			FirmwareVersion:      cloneStringPtr(device.FirmwareVersion),
			HardwareIdentifier:   cloneIntPtr(device.HardwareIdentifier),
			RemoteControlAddress: cloneIntPtr(device.RemoteControlAddress),
			DevicePaired:         cloneBoolPtr(device.DevicePaired),
			ReceptionStrength:    cloneIntPtr(device.ReceptionStrength),
			ZoneAssignment:       cloneIntPtr(device.ZoneAssignment),
			RoomTemperatureC:     cloneFloatPtr(device.RoomTemperatureC),
			RoomHumidityPct:      cloneFloatPtr(device.RoomHumidityPct),
		})
	}
	return items
}

func mapPortalSolarStatus(status *graphql.SolarStatus) *portal.SemanticSolarStatus {
	if status == nil {
		return nil
	}
	return &portal.SemanticSolarStatus{
		CollectorTemperatureC: cloneFloatPtr(status.CollectorTemperatureC),
		ReturnTemperatureC:    cloneFloatPtr(status.ReturnTemperatureC),
		PumpActive:            cloneBoolPtr(status.PumpActive),
		CurrentYield:          cloneFloatPtr(status.CurrentYield),
		PumpHours:             cloneFloatPtr(status.PumpHours),
		SolarEnabled:          cloneBoolPtr(status.SolarEnabled),
		FunctionMode:          cloneBoolPtr(status.FunctionMode),
	}
}

func mapPortalCylinders(cylinders []graphql.CylinderStatus) []portal.SemanticCylinder {
	if len(cylinders) == 0 {
		return nil
	}
	items := make([]portal.SemanticCylinder, 0, len(cylinders))
	for _, cylinder := range cylinders {
		items = append(items, portal.SemanticCylinder{
			Index:             cylinder.Index,
			TemperatureC:      cloneFloatPtr(cylinder.TemperatureC),
			MaxSetpointC:      cloneFloatPtr(cylinder.MaxSetpointC),
			ChargeHysteresisC: cloneFloatPtr(cylinder.ChargeHysteresisC),
			ChargeOffsetC:     cloneFloatPtr(cylinder.ChargeOffsetC),
		})
	}
	return items
}
