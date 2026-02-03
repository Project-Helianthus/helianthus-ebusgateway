package ebusgateway

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusgo/transport"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/router"
	"github.com/d3vi1/helianthus-ebusreg/schema"
	vaillantproviders "github.com/d3vi1/helianthus-ebusreg/providers/vaillant"
)

const defaultSmokeSource = byte(0x10)

type SmokeOptions struct {
	RootDir       string
	Providers     []registry.PlaneProvider
	Logger        *log.Logger
	SourceAddress byte
	OnGatewayReady func(ctx context.Context, gateway *Gateway, logger *log.Logger)
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

func RunSmoke(ctx context.Context, cfg smokeConfig, opts SmokeOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}

	logger := opts.Logger
	if logger == nil {
		logger = log.New(os.Stdout, "smoke: ", log.LstdFlags)
	}

	transportCfg, err := transportConfigFromSmoke(cfg.ENH)
	if err != nil {
		return err
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

	var wrap func(transport.RawTransport) transport.RawTransport
	if cfg.Smoke.VerboseFrames {
		wrap = func(inner transport.RawTransport) transport.RawTransport {
			return &loggingTransport{inner: inner, logger: logger}
		}
	}

	gateway, err := newGatewayWithTransport(ctx, gatewayCfg, wrap)
	if err != nil {
		return err
	}
	var broadcastListener *BroadcastListener
	defer func() {
		if broadcastListener != nil {
			if err := broadcastListener.Close(); err != nil {
				logger.Printf("broadcast listener close: %v", err)
			}
		}
		if err := gateway.Close(); err != nil {
			logger.Printf("gateway close: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	gateway.Start(ctx)

	source := opts.SourceAddress
	if source == 0 {
		if cfg.Smoke.SourceAddress.Byte() != 0 {
			source = cfg.Smoke.SourceAddress.Byte()
		} else {
			source = defaultSmokeSource
		}
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

	logDeviceInfo(logger, entries)
	logExpectedDevices(logger, cfg.ExpectedDevices, entries)

	_ = gateway.RefreshRouterPlanes()
	if broadcastListener == nil {
		listener, err := StartBroadcastListener(ctx, gatewayCfg, gateway.Router)
		if err != nil {
			logger.Printf("broadcast listener start: %v", err)
		} else {
			broadcastListener = listener
			logger.Printf("broadcast listener started")
		}
	}
	if opts.OnGatewayReady != nil {
		opts.OnGatewayReady(ctx, gateway, logger)
	}

	var invokeErrors []string
	for _, entry := range entries {
		planes := entry.Planes()
		if len(planes) == 0 {
			logger.Printf("device 0x%02x has no planes; running identify", entry.Address())
			if err := invokeIdentify(ctx, gateway.Router, entry, source, time.Duration(cfg.Smoke.MethodTimeoutSec)*time.Second, logger); err != nil {
				invokeErrors = append(invokeErrors, fmt.Sprintf("device 0x%02x identify: %v", entry.Address(), err))
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
						logger.Printf("device 0x%02x plane %s method %s skipped: cannot determine params", entry.Address(), invokePlane.Name(), method.Name())
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
				cancelMethod()
				invoked = true
				if err != nil {
					invokeErrors = append(invokeErrors, fmt.Sprintf("device 0x%02x plane %s method %s: %v", entry.Address(), invokePlane.Name(), method.Name(), err))
					break
				}
				logger.Printf("device 0x%02x plane %s method %s ok: %+v", entry.Address(), invokePlane.Name(), method.Name(), result)
				break
			}
			if !hasReadOnly {
				logger.Printf("device 0x%02x plane %s has no read-only methods", entry.Address(), invokePlane.Name())
				continue
			}
			if !invoked {
				logger.Printf("device 0x%02x plane %s has no invokable read-only methods", entry.Address(), invokePlane.Name())
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
		logger.Printf("device 0x%02x identify ok: %+v", entry.Address(), result)
	}
	return nil
}

func transportConfigFromSmoke(enh enhConfig) (TransportConfig, error) {
	cfg := DefaultConfig().TransportConfig
	cfg.Protocol = TransportENH
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
	timeout := time.Duration(enh.TimeoutSec) * time.Second
	if timeout > 0 {
		cfg.ReadTimeout = timeout
		cfg.WriteTimeout = timeout
		cfg.DialTimeout = timeout
	}
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
			entry.Address(),
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
		found[entry.Address()] = entry
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
		Target:    p.entry.Address(),
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
	schemaValue := selector.Select(p.entry.Address(), p.entry.HardwareVersion())
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
		Target:    p.entry.Address(),
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

func decodeDeviceInfoPayload(payload []byte) (map[string]string, error) {
	if len(payload) < 10 {
		return nil, fmt.Errorf("identify short payload: %w", ebuserrors.ErrInvalidPayload)
	}

	manufacturer := fmt.Sprintf("0x%02X", payload[0])
	if payload[0] == 0xB5 {
		manufacturer = "Vaillant"
	}

	return map[string]string{
		"manufacturer": manufacturer,
		"device_id":    strings.Trim(string(payload[1:6]), " \x00"),
		"sw_version":   fmt.Sprintf("%02X%02X", payload[6], payload[7]),
		"hw_version":   fmt.Sprintf("%02X%02X", payload[8], payload[9]),
	}, nil
}

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
