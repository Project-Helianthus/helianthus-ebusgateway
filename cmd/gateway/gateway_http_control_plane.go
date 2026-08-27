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

func httpControlPlaneRouteManifest(cfg ebusgateway.Config, hasBusObservability bool) []string {
	routes := make([]string, 0, 13)
	if hasBusObservability {
		routes = append(routes, normalizeMountPath(cfg.MetricsPath, ebusgateway.DefaultMetricsPath))
	}
	routes = append(routes,
		"/debug/vars",
		"/debug/v8/admin-events",
		cfg.GraphQLPath,
		cfg.SnapshotPath,
		cfg.SubscriptionPath,
		cfg.MCPPath,
		"/admin/eebus/v1/",
	)
	if cfg.DumpUploadPath != "" {
		uploadPath := cfg.DumpUploadPath
		if !strings.HasPrefix(uploadPath, "/") {
			uploadPath = "/" + uploadPath
		}
		routes = append(routes, uploadPath)
	}
	if cfg.UIPath != "" {
		uiPath := normalizeMountPath(cfg.UIPath, "/ui")
		routes = append(routes, uiPath+"/", uiPath)
	}
	if cfg.PortalPath != "" {
		portalPath := normalizeMountPath(cfg.PortalPath, "/portal")
		routes = append(routes, portalPath+"/", portalPath)
	}
	return routes
}

func registerHTTPControlPlaneCoreRoutes(
	mux *http.ServeMux,
	cfg ebusgateway.Config,
	busObservability *ebusgateway.BusObservabilityStore,
	queryHandler http.Handler,
	snapshotHandler http.Handler,
	subscriptionHandler http.Handler,
	mcpServer *mcp.Server,
	eebusAdminHandler http.Handler,
) {
	if busObservability != nil {
		mux.Handle(normalizeMountPath(cfg.MetricsPath, ebusgateway.DefaultMetricsPath), busObservability.MetricsHandler())
	}
	// The gateway owns its mux, so expvar's default-mux registration must be
	// mounted explicitly.
	mux.Handle("/debug/vars", expvar.Handler())
	mux.HandleFunc("/debug/v8/admin-events", handleV8AdminEvents)
	mux.Handle(cfg.GraphQLPath, queryHandler)
	mux.Handle(cfg.SnapshotPath, snapshotHandler)
	mux.Handle(cfg.SubscriptionPath, subscriptionHandler)
	mux.Handle(cfg.MCPPath, mcpServer.Handler())
	mux.Handle("/admin/eebus/v1/", eebusAdminHandler)
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
