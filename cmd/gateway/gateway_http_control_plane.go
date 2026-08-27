package main

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgateway/ui"
)

func newHTTPControlPlaneGraphQLHandlers(
	gateway *ebusgateway.Gateway,
	builder *graphql.Builder,
	hub *graphql.BroadcastHub,
) (http.Handler, http.Handler, http.Handler, error) {
	queryHandler, err := graphql.NewInvokeHandler(builder, gateway.Registry, gateway.Router)
	if err != nil {
		return nil, nil, nil, err
	}
	snapshotHandler, err := graphql.NewProjectionSnapshotHandler(builder)
	if err != nil {
		return nil, nil, nil, err
	}
	subscriptionHandler, err := graphql.NewSubscriptionHandler(builder, gateway.Registry, gateway.Router, hub)
	if err != nil {
		return nil, nil, nil, err
	}
	return queryHandler, snapshotHandler, subscriptionHandler, nil
}

type httpControlPlaneRoutePlan struct {
	metricsPath      string
	graphqlPath      string
	snapshotPath     string
	subscriptionPath string
	mcpPath          string
	dumpUploadPath   string
	uiPath           string
	portalPath       string
}

func newHTTPControlPlaneRoutePlan(cfg ebusgateway.Config, hasBusObservability bool) httpControlPlaneRoutePlan {
	plan := httpControlPlaneRoutePlan{
		graphqlPath:      cfg.GraphQLPath,
		snapshotPath:     cfg.SnapshotPath,
		subscriptionPath: cfg.SubscriptionPath,
		mcpPath:          cfg.MCPPath,
	}
	if hasBusObservability {
		plan.metricsPath = normalizeMountPath(cfg.MetricsPath, ebusgateway.DefaultMetricsPath)
	}
	if cfg.DumpUploadPath != "" {
		plan.dumpUploadPath = cfg.DumpUploadPath
		if !strings.HasPrefix(plan.dumpUploadPath, "/") {
			plan.dumpUploadPath = "/" + plan.dumpUploadPath
		}
	}
	if cfg.UIPath != "" {
		plan.uiPath = normalizeMountPath(cfg.UIPath, "/ui")
	}
	if cfg.PortalPath != "" {
		plan.portalPath = normalizeMountPath(cfg.PortalPath, "/portal")
	}
	return plan
}

func (plan httpControlPlaneRoutePlan) manifest() []string {
	routes := make([]string, 0, 13)
	if plan.metricsPath != "" {
		routes = append(routes, plan.metricsPath)
	}
	routes = append(routes,
		"/debug/vars",
		"/debug/v8/admin-events",
		plan.graphqlPath,
		plan.snapshotPath,
		plan.subscriptionPath,
		plan.mcpPath,
		"/admin/eebus/v1/",
	)
	if plan.dumpUploadPath != "" {
		routes = append(routes, plan.dumpUploadPath)
	}
	if plan.uiPath != "" {
		routes = append(routes, plan.uiPath+"/", plan.uiPath)
	}
	if plan.portalPath != "" {
		routes = append(routes, plan.portalPath+"/", plan.portalPath)
	}
	return routes
}

func httpControlPlaneRouteManifest(cfg ebusgateway.Config, hasBusObservability bool) []string {
	return newHTTPControlPlaneRoutePlan(cfg, hasBusObservability).manifest()
}

func registerHTTPControlPlaneCoreRoutes(
	mux *http.ServeMux,
	plan httpControlPlaneRoutePlan,
	dumpOutputDir string,
	busObservability *ebusgateway.BusObservabilityStore,
	queryHandler http.Handler,
	snapshotHandler http.Handler,
	subscriptionHandler http.Handler,
	mcpServer *mcp.Server,
	eebusAdminHandler http.Handler,
) {
	if plan.metricsPath != "" {
		mux.Handle(plan.metricsPath, busObservability.MetricsHandler())
	}
	// The gateway owns its mux, so expvar's default-mux registration must be
	// mounted explicitly.
	mux.Handle("/debug/vars", expvar.Handler())
	mux.HandleFunc("/debug/v8/admin-events", handleV8AdminEvents)
	mux.Handle(plan.graphqlPath, queryHandler)
	mux.Handle(plan.snapshotPath, snapshotHandler)
	mux.Handle(plan.subscriptionPath, subscriptionHandler)
	mux.Handle(plan.mcpPath, mcpServer.Handler())
	mux.Handle("/admin/eebus/v1/", eebusAdminHandler)
	if plan.dumpUploadPath != "" {
		mux.Handle(plan.dumpUploadPath, ebusgateway.NewRegisterDumpUploadHandler(dumpOutputDir))
	}
	if plan.uiPath != "" {
		uiHandler := ui.NewHandler(plan.graphqlPath)
		mux.Handle(plan.uiPath+"/", http.StripPrefix(plan.uiPath, uiHandler))
		mux.HandleFunc(plan.uiPath, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, plan.uiPath+"/", http.StatusMovedPermanently)
		})
	}
}

func startHTTPControlPlaneListener(
	ctx context.Context,
	cfg ebusgateway.Config,
	mux *http.ServeMux,
	gateway *ebusgateway.Gateway,
	mcpServer *mcp.Server,
	eebusProvider mcp.EEBusV1Provider,
) (*http.Server, mdns.Advertiser, error) {
	var operatorEndpoint io.Closer
	var err error
	if eebusProvider != nil {
		operatorEndpoint, err = mcpServer.StartEEBusV1OperatorEndpoint(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("start eeBUS operator MCP endpoint: %w", err)
		}
	}

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		if operatorEndpoint != nil {
			_ = operatorEndpoint.Close()
		}
		return nil, nil, err
	}

	server := &http.Server{Handler: mux}
	if operatorEndpoint != nil {
		server.RegisterOnShutdown(func() {
			_ = operatorEndpoint.Close()
		})
	}
	go func() {
		defer func() {
			if operatorEndpoint != nil {
				_ = operatorEndpoint.Close()
			}
		}()
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
			Text:     gatewayMDNSText(cfg),
		})
		if err != nil {
			_ = server.Close()
			if operatorEndpoint != nil {
				_ = operatorEndpoint.Close()
			}
			return nil, nil, err
		}
	}

	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
		if operatorEndpoint != nil {
			_ = operatorEndpoint.Close()
		}
	}()
	return server, advertiser, nil
}
