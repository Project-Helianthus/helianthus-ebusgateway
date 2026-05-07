package ebusgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	vaillantproviders "github.com/Project-Helianthus/helianthus-ebusreg/providers/vaillant"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
	"github.com/Project-Helianthus/helianthus-ebusreg/schema"
)

const defaultSmokeSource = byte(0x10)
const defaultSmokeReportPath = "artifacts/smoke-report.json"

type SmokeOptions struct {
	RootDir        string
	Providers      []registry.PlaneProvider
	Logger         *log.Logger
	SourceAddress  byte
	OnGatewayReady func(ctx context.Context, gateway *Gateway, logger *log.Logger)
	GraphQLCheck   func(ctx context.Context, gateway *Gateway) SmokeCheckResult
	MCPCheck       func(ctx context.Context, gateway *Gateway) SmokeCheckResult
}

type SmokeCheckResult struct {
	OK      bool
	Details string
	Error   string
}

type smokeReport struct {
	Version    string                `json:"version"`
	Profile    string                `json:"profile"`
	ReadOnly   bool                  `json:"read_only"`
	Success    bool                  `json:"success"`
	StartedAt  string                `json:"started_at"`
	FinishedAt string                `json:"finished_at"`
	DurationMS int64                 `json:"duration_ms"`
	Transport  smokeTransportSummary `json:"transport"`
	Startup    smokeCheckSummary     `json:"startup"`
	Scan       smokeScanSummary      `json:"scan"`
	GraphQL    smokeCheckSummary     `json:"graphql"`
	MCP        smokeCheckSummary     `json:"mcp"`
	Error      string                `json:"error,omitempty"`
}

type smokeTransportSummary struct {
	Protocol string `json:"protocol"`
	Network  string `json:"network"`
	Address  string `json:"address"`
}

type smokeCheckSummary struct {
	OK      bool   `json:"ok"`
	Details string `json:"details,omitempty"`
	Error   string `json:"error,omitempty"`
}

type smokeScanSummary struct {
	OK      bool `json:"ok"`
	Devices int  `json:"devices"`
}

func RunSmokeFromEnv(ctx context.Context, opts SmokeOptions) error {
	if os.Getenv("EBUS_SMOKE") != "1" {
		return nil
	}

	cfg, _, err := loadSmokeConfig(opts.RootDir)
	if err != nil {
		return err
	}

	return RunSmoke(ctx, cfg, opts)
}

func RunSmoke(ctx context.Context, cfg smokeConfig, opts SmokeOptions) (runErr error) {
	if ctx == nil {
		ctx = context.Background()
	}

	logger := opts.Logger
	if logger == nil {
		logger = log.New(os.Stdout, "smoke: ", log.LstdFlags)
	}

	reportPath, err := resolveSmokeReportPath(cfg.Smoke.ReportJSONOutput, opts.RootDir)
	if err != nil {
		return err
	}

	profile := strings.TrimSpace(cfg.Smoke.Profile)
	if profile == "" {
		profile = string(TransportENH)
	}
	startTime := time.Now()
	report := smokeReport{
		Version:   "1",
		Profile:   profile,
		ReadOnly:  true,
		StartedAt: startTime.UTC().Format(time.RFC3339Nano),
		Transport: smokeTransportSummary{Protocol: profile},
		Startup:   smokeCheckSummary{OK: false},
		Scan:      smokeScanSummary{OK: false},
		GraphQL:   smokeCheckSummary{OK: false},
		MCP:       smokeCheckSummary{OK: false},
	}
	defer func() {
		report.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		report.DurationMS = time.Since(startTime).Milliseconds()
		report.Success = runErr == nil
		if runErr != nil {
			report.Error = runErr.Error()
		}
		if err := writeSmokeReport(reportPath, report); err != nil {
			logger.Printf("smoke report write: %v", err)
			if runErr == nil {
				runErr = err
			} else {
				runErr = fmt.Errorf("%w; smoke report write: %v", runErr, err)
			}
			return
		}
		logger.Printf("smoke report written: %s", reportPath)
	}()
	transportCfg, err := transportConfigFromSmoke(cfg)
	if err != nil {
		return err
	}
	report.Transport = smokeTransportSummary{
		Protocol: string(transportCfg.Protocol),
		Network:  transportCfg.Network,
		Address:  transportCfg.Address,
	}

	providers := opts.Providers
	if len(providers) == 0 {
		providers = defaultSmokeProviders()
		if len(providers) == 0 {
			logger.Printf("warning: no plane providers configured; read-only invokes will be skipped")
		}
	}

	gatewayCfg := DefaultConfig()
	gatewayCfg.TransportConfig = transportCfg
	gatewayCfg.Providers = providers

	var deduplicator *ActivePassiveDeduplicator
	if !cfg.Smoke.RegisterDumpProbeOnly {
		dedup, err := NewActivePassiveDeduplicator(gatewayCfg)
		if err != nil {
			return err
		}
		gatewayCfg.BusConfig.Observer = ChainBusObservers(gatewayCfg.BusConfig.Observer, dedup)
		deduplicator = dedup
	}

	wireLogger, err := newWireLogger(cfg.Smoke.WireLogPath)
	if err != nil {
		return err
	}
	if wireLogger != nil {
		defer func() {
			if err := wireLogger.Close(); err != nil {
				logger.Printf("wire log close: %v", err)
			}
		}()
	}

	var wrap func(transport.RawTransport) transport.RawTransport
	if wireLogger != nil {
		wrap = chainTransportWrap(wrap, func(inner transport.RawTransport) transport.RawTransport {
			return newWireLogTransport(inner, wireLogger, "bus")
		})
	}
	if cfg.Smoke.VerboseFrames {
		wrap = chainTransportWrap(wrap, func(inner transport.RawTransport) transport.RawTransport {
			return &loggingTransport{inner: inner, logger: logger}
		})
	}

	gateway, err := newGatewayWithTransport(ctx, gatewayCfg, wrap)
	if err != nil {
		return err
	}
	var broadcastListener *BroadcastListener
	var reconstructor *PassiveTransactionReconstructor
	defer func() {
		if broadcastListener != nil {
			if err := broadcastListener.Close(); err != nil {
				logger.Printf("broadcast listener close: %v", err)
			}
		}
		if deduplicator != nil {
			if err := deduplicator.Close(); err != nil {
				logger.Printf("deduplicator close: %v", err)
			}
		}
		if reconstructor != nil {
			if err := reconstructor.Close(); err != nil {
				logger.Printf("reconstructor close: %v", err)
			}
		}
		if err := gateway.Close(); err != nil {
			logger.Printf("gateway close: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	gateway.Start(ctx)
	report.Startup = smokeCheckSummary{
		OK:      true,
		Details: "gateway started",
	}

	source := opts.SourceAddress
	if source == 0 {
		if cfg.Smoke.SourceAddress.Byte() != 0 {
			source = cfg.Smoke.SourceAddress.Byte()
		} else {
			source = defaultSmokeSource
		}
	}

	if cfg.Smoke.RegisterDumpProbeOnly {
		target := cfg.Smoke.RegisterDumpTarget.Byte()
		if target == 0 {
			return fmt.Errorf("smoke probe-only requires register_dump_target")
		}
		manufacturer := strings.TrimSpace(cfg.Smoke.RegisterDumpProbeManufacturer)
		if manufacturer == "" {
			manufacturer = "Vaillant"
		}
		deviceRegistry := registry.NewDeviceRegistry(providers)
		entry := deviceRegistry.Register(registry.DeviceInfo{
			Address:      target,
			Manufacturer: manufacturer,
		})
		systemPlane, ok := findSystemPlane(entry.Planes())
		if !ok {
			return fmt.Errorf("smoke probe-only: no system plane for target 0x%02x", target)
		}
		if err := runRegisterProbe(ctx, cfg, gateway, systemPlane, source, nil); err != nil {
			return err
		}
		logger.Printf("probe-only completed")
		return nil
	}

	scanBus := &timeoutBus{
		bus:     gateway.Bus,
		timeout: time.Duration(cfg.Smoke.ScanTimeoutSec) * time.Second,
	}

	targets := scanTargetsFromExpectedDevices(cfg.ExpectedDevices)
	entries, err := registry.Scan(ctx, scanBus, gateway.Registry, source, targets)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return fmt.Errorf("smoke scan found no devices")
	}
	report.Scan = smokeScanSummary{
		OK:      true,
		Devices: len(entries),
	}

	logDeviceInfo(logger, entries)
	logExpectedDevices(logger, cfg.ExpectedDevices, entries)

	dumpBus := &timeoutBus{
		bus:     gateway.Bus,
		timeout: time.Duration(cfg.Smoke.MethodTimeoutSec) * time.Second,
	}
	dumpResults, err := DumpUnknownDevices(ctx, dumpBus, entries, UnknownDeviceDumpOptions{
		OutputDir:      gatewayCfg.DumpOutputDir,
		UploadURL:      gatewayCfg.DumpUploadURL,
		IncludePII:     gatewayCfg.DumpIncludePII,
		IncludeTraffic: false,
		SourceAddress:  source,
		Logger:         logger,
	})
	if err != nil {
		return err
	}
	if len(dumpResults) > 0 {
		logger.Printf("unknown device dump bundles generated: %d", len(dumpResults))
	}

	_ = gateway.RefreshRouterPlanes()
	report.GraphQL = runSmokeGraphQLCheck(ctx, gateway, opts)
	if !report.GraphQL.OK {
		return fmt.Errorf("smoke graphql check failed: %s", smokeCheckError(report.GraphQL))
	}
	report.MCP = runSmokeMCPCheck(ctx, gateway, opts)
	if !report.MCP.OK {
		return fmt.Errorf("smoke mcp check failed: %s", smokeCheckError(report.MCP))
	}
	if broadcastListener == nil {
		var broadcastWrap func(transport.RawTransport) transport.RawTransport
		if wireLogger != nil {
			broadcastWrap = chainTransportWrap(broadcastWrap, func(inner transport.RawTransport) transport.RawTransport {
				return newWireLogTransport(inner, wireLogger, "broadcast")
			})
		}
		if cfg.Smoke.VerboseFrames {
			broadcastWrap = chainTransportWrap(broadcastWrap, func(inner transport.RawTransport) transport.RawTransport {
				return &loggingTransport{inner: inner, logger: logger}
			})
		}

		reconstructor, err = StartPassiveTransactionReconstructorWithTransport(ctx, gatewayCfg, broadcastWrap)
		if err != nil {
			logger.Printf("broadcast listener start: %v", err)
		} else {
			if deduplicator != nil {
				if err := deduplicator.AttachReconstructor(ctx, reconstructor); err != nil {
					logger.Printf("deduplicator attach: %v", err)
					_ = reconstructor.Close()
					reconstructor = nil
				}
			}
			if reconstructor != nil {
				listener, err := StartBroadcastListenerWithReconstructor(ctx, gateway.Router, reconstructor)
				if err != nil {
					logger.Printf("broadcast listener start: %v", err)
					_ = reconstructor.Close()
					reconstructor = nil
				} else {
					broadcastListener = listener
					logger.Printf("broadcast listener started")
				}
			}
		}
	}
	if opts.OnGatewayReady != nil {
		opts.OnGatewayReady(ctx, gateway, logger)
	}

	if err := runRegisterDump(ctx, cfg, gateway, entries, source, wireLogger, nil); err != nil {
		return err
	}

	var invokeErrors []string
	for _, entry := range entries {
		planes := entry.Planes()
		if len(planes) == 0 {
			logger.Printf("device 0x%02x has no planes; running identify", entry.PrimaryDisplayAddress())
			if err := invokeIdentify(ctx, gateway.Router, entry, source, time.Duration(cfg.Smoke.MethodTimeoutSec)*time.Second, logger); err != nil {
				invokeErrors = append(invokeErrors, fmt.Sprintf("device 0x%02x identify: %v", entry.PrimaryDisplayAddress(), err))
			}
			continue
		}
		for _, plane := range planes {
			invokePlane, direct := smokeInvocationPlane(entry, plane, source)

			hasReadOnly := false
			invoked := false
			for _, method := range invokePlane.Methods() {
				if method == nil || !method.ReadOnly() {
					continue
				}
				hasReadOnly = true

				params, ok := smokeParams(entry, invokePlane.Name(), method.Name(), source)
				if !ok {
					if smokeMethodNeedsParams(method) {
						logger.Printf("device 0x%02x plane %s method %s skipped: cannot determine params", entry.PrimaryDisplayAddress(), invokePlane.Name(), method.Name())
						continue
					}
					params = map[string]any{}
					if direct {
						params["source"] = source
					}
				}

				methodTimeout := time.Duration(cfg.Smoke.MethodTimeoutSec) * time.Second
				ctxMethod, cancelMethod := context.WithTimeout(ctx, methodTimeout)
				result, err := gateway.Router.Invoke(ctxMethod, invokePlane, method.Name(), params)
				if err != nil && method.Name() == "get_ext_register" {
					if opcode, ok := params["opcode"].(byte); ok && opcode == 0x02 {
						retryParams := make(map[string]any, len(params))
						for k, v := range params {
							retryParams[k] = v
						}
						retryParams["opcode"] = byte(0x06)
						result, err = gateway.Router.Invoke(ctxMethod, invokePlane, method.Name(), retryParams)
					}
				}
				cancelMethod()
				invoked = true
				if err != nil {
					if errors.Is(err, ebuserrors.ErrInvalidPayload) {
						logger.Printf("device 0x%02x plane %s method %s skipped: %v", entry.PrimaryDisplayAddress(), invokePlane.Name(), method.Name(), err)
						continue
					}
					invokeErrors = append(invokeErrors, fmt.Sprintf("device 0x%02x plane %s method %s: %v", entry.PrimaryDisplayAddress(), invokePlane.Name(), method.Name(), err))
					break
				}
				logger.Printf("device 0x%02x plane %s method %s ok: %+v", entry.PrimaryDisplayAddress(), invokePlane.Name(), method.Name(), result)
				break
			}
			if !hasReadOnly {
				logger.Printf("device 0x%02x plane %s has no read-only methods", entry.PrimaryDisplayAddress(), invokePlane.Name())
				continue
			}
			if !invoked {
				logger.Printf("device 0x%02x plane %s has no invokable read-only methods", entry.PrimaryDisplayAddress(), invokePlane.Name())
				continue
			}
		}
	}

	if len(invokeErrors) > 0 {
		return fmt.Errorf("smoke invoke errors: %s", strings.Join(invokeErrors, "; "))
	}

	logger.Printf("smoke test completed: devices=%d", len(entries))
	return nil
}

func invokeIdentify(ctx context.Context, router *router.BusEventRouter, entry registry.DeviceEntry, source byte, timeout time.Duration, logger *log.Logger) error {
	if router == nil {
		return fmt.Errorf("identify missing router: %w", ebuserrors.ErrInvalidPayload)
	}
	ctxMethod, cancelMethod := context.WithTimeout(ctx, timeout)
	defer cancelMethod()
	plane := &identifyPlane{entry: entry, source: source}
	result, err := router.Invoke(ctxMethod, plane, identifyMethodName, map[string]any{})
	if err != nil {
		return err
	}
	if logger != nil {
		logger.Printf("device 0x%02x identify ok: %+v", entry.PrimaryDisplayAddress(), result)
	}
	return nil
}

func transportConfigFromSmoke(smokeCfg smokeConfig) (TransportConfig, error) {
	cfg := DefaultConfig().TransportConfig
	profile := strings.TrimSpace(smokeCfg.Smoke.Profile)
	if profile == "" {
		profile = string(TransportENH)
	}
	switch profile {
	case string(TransportENH):
		cfg.Protocol = TransportENH
	case string(TransportENS):
		cfg.Protocol = TransportENS
	case string(TransportEbusdTCP), "ebusd":
		cfg.Protocol = TransportEbusdTCP
	default:
		return TransportConfig{}, fmt.Errorf("smoke config unsupported smoke.profile %q (allowed: enh, ens, ebusd-tcp)", profile)
	}

	enh := smokeCfg.ENH
	switch enh.Type {
	case "unix":
		cfg.Network = "unix"
		cfg.Address = enh.Path
	case "tcp":
		cfg.Network = "tcp"
		cfg.Address = net.JoinHostPort(enh.Host, fmt.Sprintf("%d", enh.Port))
	default:
		return TransportConfig{}, fmt.Errorf("smoke config unsupported enh.type %q", enh.Type)
	}
	if cfg.Protocol == TransportEbusdTCP && cfg.Network != "tcp" {
		return TransportConfig{}, fmt.Errorf("smoke profile %q requires enh.type tcp", profile)
	}
	timeout := time.Duration(enh.TimeoutSec) * time.Second
	if timeout > 0 {
		cfg.ReadTimeout = timeout
		cfg.WriteTimeout = timeout
		cfg.DialTimeout = timeout
	}
	cfg = clampEbusdTCPTimeouts(cfg, DefaultConfig().ScanRequestTimeout)
	return cfg, nil
}

func defaultSmokeProviders() []registry.PlaneProvider {
	return []registry.PlaneProvider{
		vaillantproviders.System(),
		vaillantproviders.Heating(),
		vaillantproviders.DHW(),
	}
}

func logDeviceInfo(logger *log.Logger, entries []registry.DeviceEntry) {
	for _, entry := range entries {
		logger.Printf("device 0x%02x: manufacturer=%s device=%s sw=%s hw=%s",
			entry.PrimaryDisplayAddress(),
			entry.Manufacturer(),
			entry.DeviceID(),
			entry.SoftwareVersion(),
			entry.HardwareVersion(),
		)
	}
}

func logExpectedDevices(logger *log.Logger, expected []expectedDevice, entries []registry.DeviceEntry) {
	if len(expected) == 0 {
		return
	}
	expectedMap := make(map[byte]expectedDevice, len(expected))
	for _, item := range expected {
		expectedMap[item.Address.Byte()] = item
	}
	found := make(map[byte]registry.DeviceEntry, len(entries))
	for _, entry := range entries {
		found[entry.PrimaryDisplayAddress()] = entry
	}

	var missing []string
	for addr, item := range expectedMap {
		if _, ok := found[addr]; !ok {
			label := item.Description
			if label == "" {
				label = fmt.Sprintf("0x%02x", addr)
			}
			missing = append(missing, label)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		logger.Printf("warning: expected devices missing: %s", strings.Join(missing, ", "))
	}

	var unexpected []string
	for addr := range found {
		if _, ok := expectedMap[addr]; !ok {
			unexpected = append(unexpected, fmt.Sprintf("0x%02x", addr))
		}
	}
	if len(unexpected) > 0 {
		sort.Strings(unexpected)
		logger.Printf("warning: unexpected devices found: %s", strings.Join(unexpected, ", "))
	}
}

func firstReadOnlyMethod(methods []registry.Method) (registry.Method, bool) {
	for _, method := range methods {
		if method != nil && method.ReadOnly() {
			return method, true
		}
	}
	return nil, false
}

func smokeInvocationPlane(entry registry.DeviceEntry, plane registry.Plane, source byte) (router.Plane, bool) {
	if plane == nil {
		return newSmokePlane(entry, plane, source), false
	}
	if typed, ok := plane.(router.Plane); ok {
		return typed, true
	}
	return newSmokePlane(entry, plane, source), false
}

func smokeParams(entry registry.DeviceEntry, planeName, methodName string, source byte) (map[string]any, bool) {
	if entry == nil {
		return nil, false
	}

	deviceID := strings.TrimSpace(entry.DeviceID())
	switch {
	case deviceID == "BAI00" && planeName == "system" && methodName == "get_operational_data":
		return map[string]any{
			"source": source,
			"op":     byte(0x00),
		}, true
	case deviceID == "BASV2" && planeName == "system" && methodName == "get_ext_register":
		return map[string]any{
			"source":   source,
			"opcode":   byte(0x02),
			"group":    byte(0x00),
			"instance": byte(0x00),
			"addr":     uint16(0x5C00),
		}, true
	default:
		return nil, false
	}
}

func smokeMethodNeedsParams(method registry.Method) bool {
	if method == nil {
		return false
	}
	template := method.Template()
	if template == nil {
		return false
	}

	if schemaProvider, ok := template.(interface{ ParamSchema() schema.Schema }); ok {
		return len(schemaProvider.ParamSchema().Fields) > 0
	}
	if _, ok := template.(interface {
		Build(params map[string]any) ([]byte, error)
	}); ok {
		builder := template.(interface {
			Build(params map[string]any) ([]byte, error)
		})
		if _, err := builder.Build(map[string]any{}); err == nil {
			return false
		}
		return true
	}
	return false
}

func newGatewayWithTransport(ctx context.Context, cfg Config, wrap func(transport.RawTransport) transport.RawTransport) (*Gateway, error) {
	cfg = applyDefaults(cfg)

	transportLayer, closeFn, err := resolveTransport(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if wrap != nil {
		transportLayer = wrap(transportLayer)
	}

	bus := protocol.NewBus(transportLayer, cfg.BusConfig, cfg.QueueCapacity)
	deviceRegistry := registry.NewDeviceRegistry(cfg.Providers)
	eventRouter := router.NewBusEventRouter(bus)

	return &Gateway{
		Transport: transportLayer,
		Bus:       bus,
		Registry:  deviceRegistry,
		Router:    eventRouter,
		closeFn:   closeFn,
	}, nil
}

func chainTransportWrap(base, next func(transport.RawTransport) transport.RawTransport) func(transport.RawTransport) transport.RawTransport {
	if base == nil {
		return next
	}
	if next == nil {
		return base
	}
	return func(inner transport.RawTransport) transport.RawTransport {
		return next(base(inner))
	}
}

type timeoutBus struct {
	bus     registry.ScanBus
	timeout time.Duration
}

func (b *timeoutBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	if b == nil || b.bus == nil {
		return nil, fmt.Errorf("timeout bus missing: %w", ebuserrors.ErrInvalidPayload)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if b.timeout <= 0 {
		return b.bus.Send(ctx, frame)
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= b.timeout {
			return b.bus.Send(ctx, frame)
		}
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	return b.bus.Send(ctxTimeout, frame)
}

type loggingTransport struct {
	inner  transport.RawTransport
	logger *log.Logger
}

func (l *loggingTransport) ReadByte() (byte, error) {
	if l == nil || l.inner == nil {
		return 0, fmt.Errorf("logging transport missing: %w", ebuserrors.ErrInvalidPayload)
	}
	b, err := l.inner.ReadByte()
	if l.logger != nil {
		if err != nil {
			l.logger.Printf("read error: %v", err)
		} else {
			l.logger.Printf("read 0x%02x", b)
		}
	}
	return b, err
}

func (l *loggingTransport) ReadEvent() (transport.StreamEvent, error) {
	if l == nil || l.inner == nil {
		return transport.StreamEvent{}, fmt.Errorf("logging transport missing: %w", ebuserrors.ErrInvalidPayload)
	}
	if reader, ok := l.inner.(transport.StreamEventReader); ok {
		event, err := reader.ReadEvent()
		if l.logger != nil {
			if err != nil {
				l.logger.Printf("read event error: %v", err)
			} else if event.Kind == transport.StreamEventReset {
				l.logger.Printf("read reset")
			} else {
				l.logger.Printf("read 0x%02x", event.Byte)
			}
		}
		return event, err
	}

	value, err := l.ReadByte()
	if err != nil {
		return transport.StreamEvent{}, err
	}
	return transport.StreamEvent{Kind: transport.StreamEventByte, Byte: value}, nil
}

func (l *loggingTransport) Write(data []byte) (int, error) {
	if l == nil || l.inner == nil {
		return 0, fmt.Errorf("logging transport missing: %w", ebuserrors.ErrInvalidPayload)
	}
	if l.logger != nil {
		l.logger.Printf("write %x", data)
	}
	n, err := l.inner.Write(data)
	if l.logger != nil && err != nil {
		l.logger.Printf("write error: %v", err)
	}
	return n, err
}

func (l *loggingTransport) Close() error {
	if l == nil || l.inner == nil {
		return nil
	}
	return l.inner.Close()
}

type smokePlane struct {
	entry  registry.DeviceEntry
	plane  registry.Plane
	source byte
}

func newSmokePlane(entry registry.DeviceEntry, plane registry.Plane, source byte) *smokePlane {
	return &smokePlane{
		entry:  entry,
		plane:  plane,
		source: source,
	}
}

func (p *smokePlane) Name() string {
	if p == nil || p.plane == nil {
		return ""
	}
	return p.plane.Name()
}

func (p *smokePlane) Methods() []registry.Method {
	if p == nil || p.plane == nil {
		return nil
	}
	return p.plane.Methods()
}

func (p *smokePlane) Subscriptions() []router.Subscription {
	if p == nil || p.plane == nil {
		return nil
	}
	if plane, ok := p.plane.(interface {
		Subscriptions() []router.Subscription
	}); ok {
		return plane.Subscriptions()
	}
	return nil
}

func (p *smokePlane) OnBroadcast(frame protocol.Frame) error {
	if p == nil || p.plane == nil {
		return nil
	}
	if plane, ok := p.plane.(interface {
		OnBroadcast(protocol.Frame) error
	}); ok {
		return plane.OnBroadcast(frame)
	}
	return nil
}

func (p *smokePlane) BuildRequest(method registry.Method, params map[string]any) (protocol.Frame, error) {
	if p == nil || p.plane == nil || method == nil {
		return protocol.Frame{}, fmt.Errorf("smoke build request missing data: %w", ebuserrors.ErrInvalidPayload)
	}
	template := method.Template()
	if template == nil {
		return protocol.Frame{}, fmt.Errorf("smoke build request missing template: %w", ebuserrors.ErrInvalidPayload)
	}

	var data []byte
	if schemaProvider, ok := template.(interface{ ParamSchema() schema.Schema }); ok {
		encoded, err := schemaProvider.ParamSchema().Encode(params)
		if err != nil {
			return protocol.Frame{}, err
		}
		data = encoded
	} else if builder, ok := template.(interface {
		Build(map[string]any) ([]byte, error)
	}); ok {
		encoded, err := builder.Build(params)
		if err != nil {
			return protocol.Frame{}, err
		}
		data = encoded
	} else if len(params) > 0 {
		return protocol.Frame{}, fmt.Errorf("smoke build request unexpected params: %w", ebuserrors.ErrInvalidPayload)
	}

	return protocol.Frame{
		Source:    p.source,
		Target:    targetAddressForRouting(p.entry),
		Primary:   template.Primary(),
		Secondary: template.Secondary(),
		Data:      data,
	}, nil
}

func (p *smokePlane) DecodeResponse(method registry.Method, response protocol.Frame, _ map[string]any) (any, error) {
	if p == nil || method == nil {
		return nil, fmt.Errorf("smoke decode response missing data: %w", ebuserrors.ErrInvalidPayload)
	}
	selector := method.ResponseSchema()
	schemaValue := selector.Select(p.entry.PrimaryDisplayAddress(), p.entry.HardwareVersion())
	return schemaValue.Decode(response.Data)
}

var _ router.Plane = (*smokePlane)(nil)

const identifyMethodName = "identify"

type identifyPlane struct {
	entry  registry.DeviceEntry
	source byte
}

func (p *identifyPlane) Name() string {
	return "device_info"
}

func (p *identifyPlane) Methods() []registry.Method {
	return []registry.Method{identifyMethod{}}
}

func (p *identifyPlane) Subscriptions() []router.Subscription {
	return nil
}

func (p *identifyPlane) OnBroadcast(protocol.Frame) error {
	return nil
}

func (p *identifyPlane) BuildRequest(registry.Method, map[string]any) (protocol.Frame, error) {
	return protocol.Frame{
		Source:    p.source,
		Target:    targetAddressForRouting(p.entry),
		Primary:   0x07,
		Secondary: 0x04,
	}, nil
}

func (p *identifyPlane) DecodeResponse(_ registry.Method, response protocol.Frame, _ map[string]any) (any, error) {
	return decodeDeviceInfoPayload(response.Data)
}

type identifyMethod struct{}

func (identifyMethod) Name() string                     { return identifyMethodName }
func (identifyMethod) ReadOnly() bool                   { return true }
func (identifyMethod) Template() registry.FrameTemplate { return identifyTemplate{} }
func (identifyMethod) ResponseSchema() schema.SchemaSelector {
	return schema.SchemaSelector{}
}

type identifyTemplate struct{}

func (identifyTemplate) Primary() byte   { return 0x07 }
func (identifyTemplate) Secondary() byte { return 0x04 }

func scanTargetsFromExpectedDevices(expected []expectedDevice) []byte {
	if len(expected) == 0 {
		return nil
	}
	seen := make(map[byte]struct{}, len(expected))
	out := make([]byte, 0, len(expected))
	for _, item := range expected {
		addr := item.Address.Byte()
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}
	return out
}

func runSmokeGraphQLCheck(ctx context.Context, gateway *Gateway, opts SmokeOptions) smokeCheckSummary {
	if opts.GraphQLCheck != nil {
		return smokeCheckSummaryFromResult(opts.GraphQLCheck(ctx, gateway))
	}
	return smokeCheckSummary{
		OK:      true,
		Details: "graphql read-only check skipped (not configured)",
	}
}

func runSmokeMCPCheck(ctx context.Context, gateway *Gateway, opts SmokeOptions) smokeCheckSummary {
	if opts.MCPCheck != nil {
		return smokeCheckSummaryFromResult(opts.MCPCheck(ctx, gateway))
	}
	if gateway == nil || gateway.Registry == nil {
		return smokeCheckSummary{
			OK:    false,
			Error: "mcp check missing gateway registry",
		}
	}

	server, err := mcp.NewServer(gateway.Registry, gateway.Router)
	if err != nil {
		return smokeCheckSummary{
			OK:    false,
			Error: err.Error(),
		}
	}

	handler := server.Handler()
	tools, err := mcpToolsListCheck(ctx, handler)
	if err != nil {
		return smokeCheckSummary{
			OK:    false,
			Error: err.Error(),
		}
	}
	if containsString(tools, "ebus.devices") {
		if err := mcpDevicesCallCheck(ctx, handler); err != nil {
			return smokeCheckSummary{
				OK:    false,
				Error: err.Error(),
			}
		}
		return smokeCheckSummary{
			OK:      true,
			Details: fmt.Sprintf("tools=%d (read-only tools/list + ebus.devices)", len(tools)),
		}
	}
	return smokeCheckSummary{
		OK:      true,
		Details: fmt.Sprintf("tools=%d (listing only; no safe read-only MCP call available)", len(tools)),
	}
}

func mcpToolsListCheck(ctx context.Context, handler http.Handler) ([]string, error) {
	response, err := invokeSmokeJSONRPC(ctx, handler, `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	if err != nil {
		return nil, err
	}

	result, ok := response["result"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("mcp tools/list response missing result object")
	}
	rawTools, ok := result["tools"].([]any)
	if !ok {
		return nil, fmt.Errorf("mcp tools/list response missing tools array")
	}
	if len(rawTools) == 0 {
		return nil, fmt.Errorf("mcp tools/list returned no tools")
	}

	toolNames := make([]string, 0, len(rawTools))
	for _, rawTool := range rawTools {
		tool, ok := rawTool.(map[string]any)
		if !ok {
			continue
		}
		name, _ := tool["name"].(string)
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		toolNames = append(toolNames, name)
	}
	if len(toolNames) == 0 {
		return nil, fmt.Errorf("mcp tools/list has no valid tool names")
	}
	return toolNames, nil
}

func mcpDevicesCallCheck(ctx context.Context, handler http.Handler) error {
	response, err := invokeSmokeJSONRPC(ctx, handler, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"ebus.devices","arguments":{}}}`)
	if err != nil {
		return err
	}
	result, ok := response["result"].(map[string]any)
	if !ok {
		return fmt.Errorf("mcp tools/call response missing result object")
	}
	if isError, _ := result["isError"].(bool); isError {
		return fmt.Errorf("mcp tools/call ebus.devices returned isError=true")
	}
	content, ok := result["content"].([]any)
	if !ok || len(content) == 0 {
		return fmt.Errorf("mcp tools/call ebus.devices missing content")
	}
	first, ok := content[0].(map[string]any)
	if !ok {
		return fmt.Errorf("mcp tools/call ebus.devices content item invalid")
	}
	text, _ := first["text"].(string)
	if strings.TrimSpace(text) == "" {
		return fmt.Errorf("mcp tools/call ebus.devices returned empty text")
	}
	return nil
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func invokeSmokeJSONRPC(ctx context.Context, handler http.Handler, payload string) (map[string]any, error) {
	if handler == nil {
		return nil, fmt.Errorf("json-rpc handler missing")
	}
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte(payload)))
	req.Header.Set("Content-Type", "application/json")
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		return nil, fmt.Errorf("json-rpc status %d: %s", recorder.Code, strings.TrimSpace(recorder.Body.String()))
	}

	raw, err := io.ReadAll(recorder.Body)
	if err != nil {
		return nil, err
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("json-rpc decode: %w", err)
	}
	if rpcErr, ok := parsed["error"].(map[string]any); ok {
		message, _ := rpcErr["message"].(string)
		if message == "" {
			message = fmt.Sprintf("%v", rpcErr)
		}
		return nil, fmt.Errorf("json-rpc error: %s", message)
	}
	return parsed, nil
}

func smokeCheckSummaryFromResult(result SmokeCheckResult) smokeCheckSummary {
	return smokeCheckSummary{
		OK:      result.OK,
		Details: strings.TrimSpace(result.Details),
		Error:   strings.TrimSpace(result.Error),
	}
}

func smokeCheckError(summary smokeCheckSummary) string {
	if strings.TrimSpace(summary.Error) != "" {
		return strings.TrimSpace(summary.Error)
	}
	if strings.TrimSpace(summary.Details) != "" {
		return strings.TrimSpace(summary.Details)
	}
	return "check failed"
}

func resolveSmokeReportPath(outputPath, rootDir string) (string, error) {
	path := strings.TrimSpace(outputPath)
	if path == "" {
		path = defaultSmokeReportPath
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}

	base := strings.TrimSpace(rootDir)
	if base == "" {
		resolvedRoot, err := findRepoRoot()
		if err != nil {
			return "", err
		}
		base = resolvedRoot
	}
	return filepath.Clean(filepath.Join(base, path)), nil
}

func writeSmokeReport(path string, report smokeReport) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("smoke report path missing")
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	payload = append(payload, '\n')

	tmpFile, err := os.CreateTemp(dir, "smoke-report-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	if _, err := tmpFile.Write(payload); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}
