package main

import (
	"context"
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"log"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/d3vi1/helianthus-ebusgateway"
	"github.com/d3vi1/helianthus-ebusgateway/graphql"
	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusreg/registry"
)

const (
	vaillantExtRegisterPrimary   = byte(0xB5)
	vaillantExtRegisterSecondary = byte(0x24)
	vaillantB524OpcodeRead       = byte(0x06)
	vaillantB524OpcodeLocal      = byte(0x02)
	vaillantB524OpRead           = byte(0x00)

	vaillantGroupDHW      = byte(0x01)
	vaillantGroupCircuits = byte(0x02)
	vaillantGroupZones    = byte(0x03)

	zoneRegName                 = uint16(0x0016)
	zoneRegNamePrefix           = uint16(0x0017)
	zoneRegNameSuffix           = uint16(0x0018)
	zoneRegIndex                = uint16(0x001C)
	zoneRegHeatingOpMode        = uint16(0x0006) // configuration.heating.operation_mode
	zoneRegCurrentTemp          = uint16(0x000F) // state.current_room_temperature
	zoneRegTargetTemp           = uint16(0x0022) // configuration.heating.desired_setpoint
	zoneRegFallbackManualTemp   = uint16(0x0014) // configuration.heating.manual_mode_setpoint
	zoneRegSpecialFunction      = uint16(0x000E) // state.current_special_function
	zoneRegValveStatus          = uint16(0x0012) // state.valve_status
	zoneRegAssociatedCircuitRaw = uint16(0x0013) // configuration.associated_circuit_index
	zoneRegCurrentHumidity      = uint16(0x0028) // state.current_room_humidity

	circuitRegType = uint16(0x0002) // configuration.heating_circuit_type / mixer_circuit_type_external

	dhwRegOperationMode   = uint16(0x0003) // configuration.domestic_hot_water.operation_mode
	dhwRegTargetTemp      = uint16(0x0004) // configuration.domestic_hot_water.tapping_setpoint
	dhwRegCurrentTemp     = uint16(0x0005) // state.current_dhw_temperature
	dhwRegSpecialFunction = uint16(0x000D) // state.current_special_function
	dhwInstance           = byte(0x00)
)

var (
	semanticReadBreakerTransitionsTotal = expvar.NewMap("semantic_read_breaker_transitions_total")
	semanticReadBreakerSuppressedTotal  = expvar.NewMap("semantic_read_breaker_suppressed_total")
)

func startVaillantSemanticPolling(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub) {
	if gateway == nil || gateway.Bus == nil || gateway.Registry == nil || provider == nil {
		return
	}

	cacheStore := newSemanticCacheStore(cfg.SemanticCachePath, log.Printf)
	cacheSnapshot, cacheLoaded := preloadSemanticCache(provider, cacheStore)
	poller := newVaillantSemanticPoller(cfg, gateway, provider, hub, cacheStore)
	if cacheLoaded {
		poller.hydrateFromCache(cacheSnapshot)
	}
	poller.Start(ctx)
}

type vaillantSemanticPoller struct {
	scheduler *ebusgateway.SemanticReadScheduler
	tasks     *semanticTaskScheduler

	reg      *registry.DeviceRegistry
	bus      *protocol.Bus
	provider *graphql.LiveSemanticProvider
	hub      *graphql.BroadcastHub
	cache    semanticCachePersister

	transportConfig ebusgateway.TransportConfig

	source            byte
	requestTimeout    time.Duration
	discoveryInterval time.Duration
	configInterval    time.Duration
	stateInterval     time.Duration

	pollMu sync.Mutex
	readMu sync.Mutex

	mu         sync.Mutex
	controller byte
	zones      map[byte]*vaillantZoneSnapshot
	dhw        *vaillantDhwSnapshot
}

type vaillantZoneSnapshot struct {
	Instance byte
	Present  bool

	Name string

	OperatingMode string
	Preset        string
	HvacAction    string
	AllowedModes  []string

	CurrentTempC *float64
	TargetTempC  *float64
	HumidityPct  *float64

	ConfigurationHeatingOperationMode string
	StateSpecialFunction              string
	ConfigurationAssociatedCircuitRaw *uint16
	ConfigurationCircuitTypeRaw       *uint16
	StateValveStatusRaw               *uint16
}

type vaillantDhwSnapshot struct {
	OperatingMode string
	Preset        string

	CurrentTempC *float64
	TargetTempC  *float64

	ConfigurationDHWOperationMode string
	StateSpecialFunction          string
}

type semanticSnapshotSource uint8

const (
	semanticSnapshotSourceCache semanticSnapshotSource = iota
	semanticSnapshotSourceLive
)

func newVaillantSemanticPoller(cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub, cache semanticCachePersister) *vaillantSemanticPoller {
	poller := &vaillantSemanticPoller{
		scheduler:       ebusgateway.NewSemanticReadScheduler(),
		tasks:           newSemanticTaskScheduler(),
		reg:             gateway.Registry,
		bus:             gateway.Bus,
		provider:        provider,
		hub:             hub,
		cache:           cache,
		transportConfig: cfg.TransportConfig,
		source:          cfg.ScanSource,

		requestTimeout:    cfg.SemanticRequestTimeout,
		discoveryInterval: cfg.SemanticDiscoveryInterval,
		configInterval:    cfg.SemanticConfigInterval,
		stateInterval:     cfg.SemanticStateInterval,

		zones: make(map[byte]*vaillantZoneSnapshot),
	}
	poller.scheduler.SetCircuitBreaker(ebusgateway.SemanticReadCircuitBreakerOptions{
		FailureBudget:      cfg.SemanticReadBreakerFailureBudget,
		OpenCooldown:       cfg.SemanticReadBreakerOpenCooldown,
		HalfOpenProbeLimit: cfg.SemanticReadBreakerHalfOpenProbeLimit,
		OnTransition:       poller.onSemanticReadBreakerTransition,
		OnSuppressed:       poller.onSemanticReadBreakerSuppressed,
	})
	return poller
}

func (p *vaillantSemanticPoller) onSemanticReadBreakerTransition(event ebusgateway.SemanticReadCircuitBreakerTransition) {
	if p == nil {
		return
	}
	semanticReadBreakerTransitionsTotal.Add(fmt.Sprintf("%s->%s", event.From, event.To), 1)
	log.Printf(
		"semantic_read_breaker_transition key=%q from=%s to=%s consecutive_failures=%d",
		event.Key,
		event.From,
		event.To,
		event.ConsecutiveFailures,
	)
}

func (p *vaillantSemanticPoller) onSemanticReadBreakerSuppressed(event ebusgateway.SemanticReadCircuitBreakerSuppression) {
	if p == nil {
		return
	}
	semanticReadBreakerSuppressedTotal.Add(string(event.State), 1)
	log.Printf(
		"semantic_read_breaker_suppressed key=%q state=%s retry_after=%s suppressed_total=%d",
		event.Key,
		event.State,
		event.RetryAfter.Round(time.Millisecond),
		event.SuppressedTotal,
	)
}

func preloadSemanticCache(provider *graphql.LiveSemanticProvider, cacheStore *semanticCacheStore) (semanticCacheSnapshot, bool) {
	if provider == nil || cacheStore == nil {
		return semanticCacheSnapshot{}, false
	}
	snapshot, ok := cacheStore.Load()
	if !ok {
		return semanticCacheSnapshot{}, false
	}
	if len(snapshot.Zones) > 0 {
		provider.SetZonesFromCache(snapshot.Zones)
	}
	if snapshot.DHW != nil {
		provider.SetDHWFromCache(snapshot.DHW)
	}
	return snapshot, true
}

func (p *vaillantSemanticPoller) hydrateFromCache(snapshot semanticCacheSnapshot) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.zones == nil {
		p.zones = make(map[byte]*vaillantZoneSnapshot)
	}
	for idx, zone := range snapshot.Zones {
		instance := zoneInstanceFromSemanticID(zone.ID, idx)
		p.zones[instance] = zoneSnapshotFromSemanticZone(instance, zone)
	}
	if snapshot.DHW != nil {
		p.dhw = dhwSnapshotFromSemanticStatus(snapshot.DHW)
	}
}

func zoneInstanceFromSemanticID(id string, fallbackIndex int) byte {
	trimmed := strings.TrimSpace(strings.ToLower(id))
	if strings.HasPrefix(trimmed, "zone-") {
		ordinalRaw := strings.TrimPrefix(trimmed, "zone-")
		if ordinal, err := strconv.Atoi(ordinalRaw); err == nil && ordinal > 0 && ordinal <= 256 {
			return byte(ordinal - 1)
		}
	}
	if fallbackIndex < 0 {
		return 0
	}
	if fallbackIndex > 255 {
		return 255
	}
	return byte(fallbackIndex)
}

func zoneSnapshotFromSemanticZone(instance byte, zone graphql.Zone) *vaillantZoneSnapshot {
	return &vaillantZoneSnapshot{
		Instance:                          instance,
		Present:                           true,
		Name:                              zone.Name,
		OperatingMode:                     zone.OperatingMode,
		Preset:                            zone.Preset,
		HvacAction:                        zone.HvacAction,
		AllowedModes:                      append([]string(nil), zone.AllowedModes...),
		CurrentTempC:                      cloneFloat64Ptr(zone.CurrentTempC),
		TargetTempC:                       cloneFloat64Ptr(zone.TargetTempC),
		HumidityPct:                       cloneFloat64Ptr(zone.CurrentHumidityPct),
		ConfigurationHeatingOperationMode: zone.ZoneOperationModeRaw,
		StateSpecialFunction:              zone.ZoneSpecialFunctionRaw,
		ConfigurationAssociatedCircuitRaw: parseUint16Token(zone.ZoneCircuitIndexRaw),
		ConfigurationCircuitTypeRaw:       parseUint16Token(zone.CircuitTypeRaw),
		StateValveStatusRaw:               parseUint16Token(zone.ZoneValveStatusRaw),
	}
}

func dhwSnapshotFromSemanticStatus(status *graphql.DhwStatus) *vaillantDhwSnapshot {
	if status == nil {
		return nil
	}
	return &vaillantDhwSnapshot{
		OperatingMode:                 status.OperatingMode,
		Preset:                        status.Preset,
		CurrentTempC:                  cloneFloat64Ptr(status.CurrentTempC),
		TargetTempC:                   cloneFloat64Ptr(status.TargetTempC),
		ConfigurationDHWOperationMode: status.DhwOperationModeRaw,
		StateSpecialFunction:          status.DhwSpecialFunctionRaw,
	}
}

func parseUint16Token(token string) *uint16 {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil
	}
	value, err := strconv.ParseUint(token, 10, 16)
	if err != nil {
		return nil
	}
	out := uint16(value)
	return &out
}

func cloneFloat64Ptr(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func (p *vaillantSemanticPoller) Start(ctx context.Context) {
	if p == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	go p.tasks.run(ctx)

	// Prime quickly so HA can create entities on first coordinator refresh.
	p.enqueueTask(semanticTaskPriorityHigh, p.refreshDiscovery)
	p.enqueueTask(semanticTaskPriorityHigh, p.refreshConfig)
	p.enqueueTask(semanticTaskPriorityHigh, p.refreshState)

	go p.runLoop(ctx, p.discoveryInterval, semanticTaskPriorityLow, p.refreshDiscovery)
	go p.runLoop(ctx, p.configInterval, semanticTaskPriorityMedium, p.refreshConfig)
	go p.runLoop(ctx, p.stateInterval, semanticTaskPriorityHigh, p.refreshState)
}

func (p *vaillantSemanticPoller) runLoop(ctx context.Context, interval time.Duration, priority semanticTaskPriority, fn func(context.Context)) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.enqueueTask(priority, fn)
		}
	}
}

func (p *vaillantSemanticPoller) enqueueTask(priority semanticTaskPriority, fn func(context.Context)) {
	if p == nil || fn == nil {
		return
	}
	scheduler := p.tasks
	if scheduler == nil {
		p.withPollLock(context.Background(), fn)
		return
	}
	err := scheduler.submit(priority, func(taskCtx context.Context) {
		p.withPollLock(taskCtx, fn)
	})
	if errors.Is(err, errSemanticTaskQueueOverloaded) {
		log.Printf("semantic poll scheduler overloaded: skipping task (priority=%d)", priority)
		return
	}
	if err != nil {
		log.Printf("semantic poll scheduler submit failed: %v", err)
	}
}

func (p *vaillantSemanticPoller) withPollLock(ctx context.Context, fn func(context.Context)) {
	if p == nil || fn == nil {
		return
	}
	p.pollMu.Lock()
	defer p.pollMu.Unlock()
	fn(ctx)
}

func (p *vaillantSemanticPoller) refreshDiscovery(ctx context.Context) {
	controller, ok := findDeviceAddressByPrefix(p.reg, "BASV")
	if !ok {
		p.mu.Lock()
		p.controller = 0
		p.zones = make(map[byte]*vaillantZoneSnapshot)
		p.dhw = nil
		p.mu.Unlock()
		p.publishZones(semanticSnapshotSourceCache)
		p.publishDHW(semanticSnapshotSourceCache)
		return
	}

	p.mu.Lock()
	p.controller = controller
	p.mu.Unlock()

	present := make(map[byte]bool, 4)
	checked := make(map[byte]bool, 11)
	for instance := byte(0x00); instance <= 0x0A; instance++ {
		indexBytes, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegIndex)
		if !ok {
			continue
		}
		checked[instance] = true
		if len(indexBytes) < 1 || indexBytes[0] == 0xFF {
			continue
		}
		present[instance] = true
	}

	p.mu.Lock()
	for instance := range present {
		entry := p.zones[instance]
		if entry == nil {
			entry = &vaillantZoneSnapshot{Instance: instance}
			p.zones[instance] = entry
		}
		entry.Present = true
	}
	for instance := range p.zones {
		if checked[instance] && !present[instance] {
			delete(p.zones, instance)
		}
	}
	p.mu.Unlock()

	source := semanticSnapshotSourceLive
	if len(present) == 0 {
		source = sourceFromEbusdGrab(p.refreshFromEbusdGrab(ctx))
	}

	p.publishZones(source)
}

func (p *vaillantSemanticPoller) refreshConfig(ctx context.Context) {
	controller, zones := p.snapshotZones()
	grabHydrated := false
	if controller == 0 || len(zones) == 0 {
		p.refreshDiscovery(ctx)
		controller, zones = p.snapshotZones()
	}
	if controller == 0 || len(zones) == 0 {
		if p.refreshFromEbusdGrab(ctx) {
			controller, zones = p.snapshotZones()
			grabHydrated = true
		}
	}
	if controller == 0 || len(zones) == 0 {
		return
	}

	liveReadSuccess := false
	for _, instance := range zones {
		primaryName, primaryOK := p.readB524ZoneNamePart(ctx, instance, zoneRegName)
		prefix, prefixOK := p.readB524ZoneNamePart(ctx, instance, zoneRegNamePrefix)
		suffix, suffixOK := p.readB524ZoneNamePart(ctx, instance, zoneRegNameSuffix)
		if primaryOK || prefixOK || suffixOK {
			liveReadSuccess = true
		}

		name := composeZoneName(primaryName, prefix, suffix)
		if strings.TrimSpace(name) == "" {
			continue
		}

		p.mu.Lock()
		if entry := p.zones[instance]; entry != nil {
			entry.Name = name
		}
		p.mu.Unlock()
	}
	if !liveReadSuccess && p.refreshFromEbusdGrab(ctx) {
		grabHydrated = true
	}

	source := semanticSnapshotSourceCache
	if liveReadSuccess || grabHydrated {
		source = semanticSnapshotSourceLive
	}
	p.publishZones(source)
}

func (p *vaillantSemanticPoller) refreshState(ctx context.Context) {
	controller, zones := p.snapshotZones()
	grabHydrated := false
	if controller == 0 || len(zones) == 0 {
		p.refreshDiscovery(ctx)
		controller, zones = p.snapshotZones()
	}
	if controller == 0 || len(zones) == 0 {
		if p.refreshFromEbusdGrab(ctx) {
			controller, zones = p.snapshotZones()
			grabHydrated = true
		}
	}
	if controller == 0 || len(zones) == 0 {
		dhwSource := p.refreshDHW(ctx)
		p.publishDHW(dhwSource)
		return
	}

	liveReadSuccess := false
	for _, instance := range zones {
		var (
			currentPtr *float64
			targetPtr  *float64
			humidity   *float64
		)
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegCurrentTemp); ok {
			current := value
			currentPtr = &current
			liveReadSuccess = true
		}

		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegTargetTemp); ok {
			target := value
			targetPtr = &target
			liveReadSuccess = true
		}
		if targetPtr == nil {
			if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegFallbackManualTemp); ok {
				target := value
				targetPtr = &target
				liveReadSuccess = true
			}
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegCurrentHumidity); ok {
			currentHumidity := value
			humidity = &currentHumidity
			liveReadSuccess = true
		}

		zoneOpMode, zoneOpModeOK := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegHeatingOpMode)
		zoneSF, zoneSFOK := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegSpecialFunction)
		zoneValve, zoneValveOK := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegValveStatus)
		zoneCircuitRaw, zoneCircuitRawOK := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegAssociatedCircuitRaw)
		if zoneOpModeOK || zoneSFOK || zoneValveOK || zoneCircuitRawOK {
			liveReadSuccess = true
		}
		circuitInstance := resolveCircuitInstance(zoneCircuitRaw, instance)
		circuitType, hasCircuitType := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, circuitInstance, circuitRegType)
		if hasCircuitType {
			liveReadSuccess = true
		}

		operatingMode, preset, allowedModes := deriveZoneModeAndPreset(zoneOpMode, zoneSF, circuitType, hasCircuitType)
		hvacAction := deriveZoneHvacAction(zoneValve, circuitType, hasCircuitType)

		p.mu.Lock()
		if entry := p.zones[instance]; entry != nil {
			if currentPtr != nil {
				entry.CurrentTempC = currentPtr
			}
			if targetPtr != nil {
				entry.TargetTempC = targetPtr
			}
			if humidity != nil {
				entry.HumidityPct = humidity
			}
			if operatingMode != "" {
				entry.OperatingMode = operatingMode
			}
			if preset != "" {
				entry.Preset = preset
			}
			if hvacAction != "" {
				entry.HvacAction = hvacAction
			}
			if len(allowedModes) > 0 {
				entry.AllowedModes = allowedModes
			}
			if zoneOpMode != nil {
				entry.ConfigurationHeatingOperationMode = formatUintToken(*zoneOpMode)
			}
			if zoneSF != nil {
				entry.StateSpecialFunction = formatUintToken(*zoneSF)
			}
			entry.ConfigurationAssociatedCircuitRaw = zoneCircuitRaw
			entry.StateValveStatusRaw = zoneValve
			if hasCircuitType {
				entry.ConfigurationCircuitTypeRaw = circuitType
			}
		}
		p.mu.Unlock()
	}
	if !liveReadSuccess && p.refreshFromEbusdGrab(ctx) {
		grabHydrated = true
	}

	zoneSource := semanticSnapshotSourceCache
	if liveReadSuccess || grabHydrated {
		zoneSource = semanticSnapshotSourceLive
	}

	dhwSource := p.refreshDHW(ctx)
	p.publishZones(zoneSource)
	p.publishDHW(dhwSource)
}

func (p *vaillantSemanticPoller) snapshotZones() (byte, []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	controller := p.controller
	if controller == 0 || len(p.zones) == 0 {
		return controller, nil
	}
	instances := make([]byte, 0, len(p.zones))
	for instance := range p.zones {
		instances = append(instances, instance)
	}
	slices.Sort(instances)
	return controller, instances
}

func (p *vaillantSemanticPoller) publishZones(source semanticSnapshotSource) {
	if p.provider == nil {
		return
	}

	p.mu.Lock()
	instances := make([]byte, 0, len(p.zones))
	for instance := range p.zones {
		instances = append(instances, instance)
	}
	slices.Sort(instances)
	zones := make([]graphql.Zone, 0, len(instances))
	for _, instance := range instances {
		entry := p.zones[instance]
		if entry == nil {
			continue
		}

		name := entry.Name
		if strings.TrimSpace(name) == "" {
			name = fmt.Sprintf("Zone %d", instance+1)
		}

		zone := graphql.Zone{
			ID:                     fmt.Sprintf("zone-%d", instance+1),
			Name:                   name,
			OperatingMode:          entry.OperatingMode,
			Preset:                 entry.Preset,
			HvacAction:             entry.HvacAction,
			AllowedModes:           append([]string(nil), entry.AllowedModes...),
			CurrentTempC:           entry.CurrentTempC,
			TargetTempC:            entry.TargetTempC,
			CurrentHumidityPct:     entry.HumidityPct,
			SpecialFunction:        entry.StateSpecialFunction,
			CircuitTypeRaw:         optionalUintToken(entry.ConfigurationCircuitTypeRaw),
			ZoneCircuitIndexRaw:    optionalUintToken(entry.ConfigurationAssociatedCircuitRaw),
			ZoneOperationModeRaw:   entry.ConfigurationHeatingOperationMode,
			ZoneValveStatusRaw:     optionalUintToken(entry.StateValveStatusRaw),
			ZoneSpecialFunctionRaw: entry.StateSpecialFunction,
		}
		zones = append(zones, zone)
	}
	p.mu.Unlock()

	previous := p.provider.Zones()
	if source == semanticSnapshotSourceCache && len(zones) == 0 && len(previous) > 0 {
		return
	}
	switch source {
	case semanticSnapshotSourceCache:
		p.provider.SetZonesFromCache(zones)
	default:
		p.provider.SetZones(zones)
	}

	if p.hub != nil {
		prevByID := make(map[string]graphql.Zone, len(previous))
		for _, z := range previous {
			prevByID[z.ID] = z
		}
		for _, zone := range zones {
			if !zoneEquals(prevByID[zone.ID], zone) {
				p.hub.PublishZoneUpdate(zone)
			}
		}
	}
	p.persistSemanticCache(source)
}

func zoneEquals(a, b graphql.Zone) bool {
	if a.ID != b.ID || a.Name != b.Name {
		return false
	}
	if a.OperatingMode != b.OperatingMode || a.Preset != b.Preset {
		return false
	}
	if a.HvacAction != b.HvacAction || a.SpecialFunction != b.SpecialFunction {
		return false
	}
	if a.CircuitTypeRaw != b.CircuitTypeRaw ||
		a.ZoneCircuitIndexRaw != b.ZoneCircuitIndexRaw ||
		a.ZoneOperationModeRaw != b.ZoneOperationModeRaw ||
		a.ZoneValveStatusRaw != b.ZoneValveStatusRaw ||
		a.ZoneSpecialFunctionRaw != b.ZoneSpecialFunctionRaw {
		return false
	}
	if !slices.Equal(a.AllowedModes, b.AllowedModes) {
		return false
	}
	if !floatPtrEquals(a.CurrentTempC, b.CurrentTempC) {
		return false
	}
	if !floatPtrEquals(a.TargetTempC, b.TargetTempC) {
		return false
	}
	if !floatPtrEquals(a.CurrentHumidityPct, b.CurrentHumidityPct) {
		return false
	}
	return true
}

func (p *vaillantSemanticPoller) refreshDHW(ctx context.Context) semanticSnapshotSource {
	controller, _ := p.snapshotZones()
	if controller == 0 {
		if found, ok := findDeviceAddressByPrefix(p.reg, "BASV"); ok {
			p.mu.Lock()
			p.controller = found
			p.mu.Unlock()
			controller = found
		}
	}
	if controller == 0 {
		return sourceFromEbusdGrab(p.refreshDHWFromEbusdGrab(ctx))
	}

	liveReadSuccess := false
	currentPtr := p.readDhwFloat(ctx, dhwRegCurrentTemp)
	if currentPtr != nil {
		liveReadSuccess = true
	}
	targetPtr := p.readDhwFloat(ctx, dhwRegTargetTemp)
	if targetPtr != nil {
		liveReadSuccess = true
	}
	opModeRaw, opModeOK := p.readDhwUint16(ctx, dhwRegOperationMode)
	sfModeRaw, sfModeOK := p.readDhwUint16(ctx, dhwRegSpecialFunction)
	if opModeOK || sfModeOK {
		liveReadSuccess = true
	}

	operatingMode, preset := deriveDhwModeAndPreset(opModeRaw, sfModeRaw)
	if operatingMode == "" && preset == "" && currentPtr == nil && targetPtr == nil {
		return sourceFromEbusdGrab(p.refreshDHWFromEbusdGrab(ctx))
	}

	status := &vaillantDhwSnapshot{
		OperatingMode: operatingMode,
		Preset:        preset,
		CurrentTempC:  currentPtr,
		TargetTempC:   targetPtr,
	}
	if opModeRaw != nil {
		status.ConfigurationDHWOperationMode = formatUintToken(*opModeRaw)
	}
	if sfModeRaw != nil {
		status.StateSpecialFunction = formatUintToken(*sfModeRaw)
	}

	p.mu.Lock()
	p.dhw = status
	p.mu.Unlock()
	if liveReadSuccess {
		return semanticSnapshotSourceLive
	}
	return semanticSnapshotSourceCache
}

func sourceFromEbusdGrab(ok bool) semanticSnapshotSource {
	if ok {
		return semanticSnapshotSourceLive
	}
	return semanticSnapshotSourceCache
}

func (p *vaillantSemanticPoller) readDhwFloat(ctx context.Context, addr uint16) *float64 {
	for _, opcode := range []byte{vaillantB524OpcodeLocal, vaillantB524OpcodeRead} {
		value, ok := p.readB524Float32LE(ctx, opcode, vaillantGroupDHW, dhwInstance, addr)
		if !ok {
			continue
		}
		floatValue := value
		return &floatValue
	}
	return nil
}

func (p *vaillantSemanticPoller) readDhwUint16(ctx context.Context, addr uint16) (*uint16, bool) {
	for _, opcode := range []byte{vaillantB524OpcodeLocal, vaillantB524OpcodeRead} {
		value, ok := p.readB524Uint16(ctx, opcode, vaillantGroupDHW, dhwInstance, addr)
		if ok {
			return value, true
		}
	}
	return nil, false
}

func (p *vaillantSemanticPoller) publishDHW(source semanticSnapshotSource) {
	if p.provider == nil {
		return
	}

	p.mu.Lock()
	snapshot := p.dhw
	p.mu.Unlock()

	previous := p.provider.DHW()
	if snapshot == nil {
		if source == semanticSnapshotSourceCache && previous != nil {
			return
		}
		switch source {
		case semanticSnapshotSourceCache:
			p.provider.SetDHWFromCache(nil)
		default:
			p.provider.SetDHW(nil)
		}
		if previous != nil && p.hub != nil {
			p.hub.PublishDHWUpdate(nil)
		}
		p.persistSemanticCache(source)
		return
	}

	current := &graphql.DhwStatus{
		OperatingMode:         snapshot.OperatingMode,
		Preset:                snapshot.Preset,
		CurrentTempC:          snapshot.CurrentTempC,
		TargetTempC:           snapshot.TargetTempC,
		SpecialFunction:       snapshot.StateSpecialFunction,
		DhwOperationModeRaw:   snapshot.ConfigurationDHWOperationMode,
		DhwSpecialFunctionRaw: snapshot.StateSpecialFunction,
	}

	switch source {
	case semanticSnapshotSourceCache:
		p.provider.SetDHWFromCache(current)
	default:
		p.provider.SetDHW(current)
	}
	if p.hub != nil && !dhwEquals(previous, current) {
		p.hub.PublishDHWUpdate(current)
	}
	p.persistSemanticCache(source)
}

func (p *vaillantSemanticPoller) persistSemanticCache(source semanticSnapshotSource) {
	if p == nil || source != semanticSnapshotSourceLive || p.cache == nil || p.provider == nil {
		return
	}
	_ = p.cache.Save(semanticCacheSnapshot{
		Zones: p.provider.Zones(),
		DHW:   p.provider.DHW(),
	})
}

func dhwEquals(a, b *graphql.DhwStatus) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.OperatingMode != b.OperatingMode || a.Preset != b.Preset {
		return false
	}
	if a.SpecialFunction != b.SpecialFunction ||
		a.DhwOperationModeRaw != b.DhwOperationModeRaw ||
		a.DhwSpecialFunctionRaw != b.DhwSpecialFunctionRaw {
		return false
	}
	if !floatPtrEquals(a.CurrentTempC, b.CurrentTempC) || !floatPtrEquals(a.TargetTempC, b.TargetTempC) {
		return false
	}
	return true
}

func optionalUintToken(value *uint16) string {
	if value == nil {
		return ""
	}
	return formatUintToken(*value)
}

func floatPtrEquals(a, b *float64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func findDeviceAddressByPrefix(reg *registry.DeviceRegistry, wantedPrefix string) (byte, bool) {
	if reg == nil {
		return 0, false
	}
	wantedPrefix = normalizeDeviceID(wantedPrefix)
	if wantedPrefix == "" {
		return 0, false
	}

	var addr byte
	var found bool
	reg.Iterate(func(entry registry.DeviceEntry) bool {
		if entry == nil {
			return true
		}
		if strings.HasPrefix(normalizeDeviceID(entry.DeviceID()), wantedPrefix) {
			addr = entry.Address()
			found = true
			return false
		}
		return true
	})
	return addr, found
}

func normalizeDeviceID(value string) string {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "_", "")
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, " ", "")
	return normalized
}

func (p *vaillantSemanticPoller) readB524Value(ctx context.Context, opcode, group, instance byte, addr uint16) ([]byte, bool) {
	if p == nil || p.bus == nil {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	target := p.controller
	timeout := p.requestTimeout
	p.mu.Unlock()

	if target == 0 {
		return nil, false
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	key := semanticReadBreakerKey(opcode, group, instance, addr)
	value, err := p.scheduler.Get(ctx, key, 500*time.Millisecond, func(ctx context.Context) ([]byte, error) {
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			p.readMu.Lock()

			reqCtx, cancel := context.WithTimeout(ctx, timeout)
			request := protocol.Frame{
				Source:    source,
				Target:    target,
				Primary:   vaillantExtRegisterPrimary,
				Secondary: vaillantExtRegisterSecondary,
				Data:      buildB524ReadSelector(opcode, group, instance, addr),
			}
			response, err := p.bus.Send(reqCtx, request)
			cancel()
			p.readMu.Unlock()

			if err != nil {
				lastErr = err
			} else if response == nil {
				lastErr = fmt.Errorf("b524 read returned nil response")
			} else {
				payload, ok := parseB524ReadPayload(response.Data, opcode, group, instance, addr)
				if ok {
					return payload, nil
				}
				lastErr = fmt.Errorf(
					"b524 read failed: opcode=0x%02x group=0x%02x instance=0x%02x addr=0x%04x",
					opcode,
					group,
					instance,
					addr,
				)
			}

			if attempt < 2 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(75 * time.Millisecond):
				}
			}
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("b524 read failed")
		}
		return nil, lastErr
	})
	if err != nil {
		return nil, false
	}
	return value, true
}

func semanticReadBreakerKey(opcode, group, instance byte, addr uint16) string {
	return fmt.Sprintf("b524:%02x:%02x:%02x:%04x", opcode, group, instance, addr)
}

func buildB524ReadSelector(opcode, group, instance byte, addr uint16) []byte {
	return []byte{
		vaillantB524OpcodeRead,
		opcode,
		vaillantB524OpRead,
		group,
		instance,
		byte(addr),
		byte(addr >> 8),
	}
}

func parseB524ReadPayload(payload []byte, opcode, group, instance byte, addr uint16) ([]byte, bool) {
	if len(payload) == 0 {
		return nil, false
	}

	if len(payload) == 1 && payload[0] == 0x00 {
		return nil, false
	}
	if len(payload) < 4 {
		return nil, false
	}

	replyKind := payload[0]

	if len(payload) >= 5 {
		replyInstance := payload[1]
		replyGroup := payload[2]
		replyAddr := uint16(payload[3]) | uint16(payload[4])<<8
		if replyGroup == group && replyAddr == addr {
			if !matchesB524ReplyInstance(replyInstance, instance) {
				log.Printf(
					"b524 read mismatch: want opcode=0x%02x group=0x%02x instance=0x%02x addr=0x%04x; got reply-instance=0x%02x (group=0x%02x addr=0x%04x)",
					opcode,
					group,
					instance,
					addr,
					replyInstance,
					replyGroup,
					replyAddr,
				)
				return nil, false
			}
			if len(payload) == 5 {
				return nil, false
			}
			return payload[5:], true
		}
	}

	replyGroup := payload[1]
	replyAddr := uint16(payload[2]) | uint16(payload[3])<<8
	if replyGroup != group || replyAddr != addr {
		log.Printf(
			"b524 read mismatch: want opcode=0x%02x group=0x%02x instance=0x%02x addr=0x%04x; got kind=0x%02x group=0x%02x addr=0x%04x len=%d",
			opcode,
			group,
			instance,
			addr,
			replyKind,
			replyGroup,
			replyAddr,
			len(payload),
		)
		return nil, false
	}
	if len(payload) == 4 {
		return nil, false
	}
	return payload[4:], true
}

func matchesB524ReplyInstance(replyInstance, requestedInstance byte) bool {
	if replyInstance == requestedInstance {
		return true
	}
	if requestedInstance < 0xFF && replyInstance == requestedInstance+1 {
		return true
	}
	return false
}

func (p *vaillantSemanticPoller) readB524Float32LE(ctx context.Context, opcode, group, instance byte, addr uint16) (float64, bool) {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || len(raw) < 4 {
		return 0, false
	}
	bits := binary.LittleEndian.Uint32(raw[:4])
	value := float64(math.Float32frombits(bits))
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return value, true
}

func (p *vaillantSemanticPoller) readB524CString(ctx context.Context, opcode, group, instance byte, addr uint16) (string, bool) {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || len(raw) == 0 {
		return "", false
	}
	trimmed := raw
	for i, b := range trimmed {
		if b == 0x00 {
			trimmed = trimmed[:i]
			break
		}
	}
	return string(trimmed), true
}

func (p *vaillantSemanticPoller) readB524ZoneNamePart(ctx context.Context, instance byte, addr uint16) (string, bool) {
	raw, ok := p.readB524CString(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, addr)
	if !ok {
		return "", false
	}
	return strings.TrimSpace(raw), true
}

func (p *vaillantSemanticPoller) readB524Uint16(ctx context.Context, opcode, group, instance byte, addr uint16) (*uint16, bool) {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || len(raw) == 0 {
		return nil, false
	}
	parsed, ok := decodeB524Uint16(raw)
	if !ok {
		return nil, false
	}
	return &parsed, true
}

func resolveCircuitInstance(associatedCircuit *uint16, zoneInstance byte) byte {
	if associatedCircuit == nil {
		return zoneInstance
	}
	value := *associatedCircuit
	switch value {
	case 0xFF, 0xFFFF:
		return zoneInstance
	default:
		if value <= 0x1F {
			return byte(value)
		}
		return zoneInstance
	}
}

func deriveZoneModeAndPreset(opMode, specialFunction, circuitType *uint16, hasCircuitType bool) (string, string, []string) {
	allowed := deriveZoneAllowedModes(circuitType, hasCircuitType)
	var mode string
	switch value := uint16Value(opMode, 1); value {
	case 0:
		mode = "off"
	case 1:
		mode = "auto"
	case 2:
		mode = deriveManualOperatingMode(circuitType, hasCircuitType)
	default:
		mode = "auto"
	}

	preset := derivePresetFromSpecialFunctionAndOpMode(specialFunction, opMode)
	if preset == "" {
		switch mode {
		case "auto":
			preset = "schedule"
		default:
			preset = "manual"
		}
	}
	return mode, preset, allowed
}

func deriveDhwModeAndPreset(opMode, specialFunction *uint16) (string, string) {
	var mode string
	switch value := uint16Value(opMode, 1); value {
	case 0:
		mode = "off"
	case 1:
		mode = "auto"
	case 2:
		mode = "heat"
	default:
		mode = "auto"
	}
	preset := derivePresetFromSpecialFunctionAndOpMode(specialFunction, opMode)
	if preset == "" {
		switch mode {
		case "auto":
			preset = "schedule"
		default:
			preset = "manual"
		}
	}
	return mode, preset
}

func derivePresetFromSpecialFunctionAndOpMode(specialFunction, opMode *uint16) string {
	if token := normalizeSpecialFunctionToken(specialFunction); token != "" {
		switch token {
		case "quickveto":
			return "quickveto"
		case "away":
			return "away"
		}
	}
	switch uint16Value(opMode, 1) {
	case 1:
		return "schedule"
	case 2:
		return "manual"
	}
	return ""
}

func normalizeSpecialFunctionToken(specialFunction *uint16) string {
	if specialFunction == nil {
		return ""
	}
	switch *specialFunction {
	case 2:
		return "quickveto"
	case 3, 4:
		return "away"
	default:
		return ""
	}
}

func deriveManualOperatingMode(circuitType *uint16, hasCircuitType bool) string {
	if !hasCircuitType || circuitType == nil {
		return "heat"
	}
	switch *circuitType {
	case 2:
		return "cool"
	default:
		return "heat"
	}
}

func deriveZoneAllowedModes(circuitType *uint16, hasCircuitType bool) []string {
	if !hasCircuitType || circuitType == nil {
		return []string{"off", "auto", "heat"}
	}
	switch *circuitType {
	case 0:
		return []string{"off"}
	case 2:
		return []string{"off", "auto", "cool"}
	default:
		return []string{"off", "auto", "heat"}
	}
}

func deriveZoneHvacAction(valveStatus, circuitType *uint16, hasCircuitType bool) string {
	if valveStatus == nil {
		return ""
	}
	switch *valveStatus {
	case 0:
		return "idle"
	case 1:
		if hasCircuitType && circuitType != nil && *circuitType == 2 {
			return "cooling"
		}
		return "heating"
	default:
		return ""
	}
}

func uint16Value(value *uint16, fallback uint16) uint16 {
	if value == nil {
		return fallback
	}
	return *value
}

func formatUintToken(value uint16) string {
	return fmt.Sprintf("%d", value)
}

func decodeB524Uint16(payload []byte) (uint16, bool) {
	if len(payload) == 0 {
		return 0, false
	}
	if len(payload) == 1 {
		return uint16(payload[0]), true
	}
	return binary.LittleEndian.Uint16(payload[:2]), true
}

func composeZoneName(primary, prefix, suffix string) string {
	if primary = strings.TrimSpace(primary); primary != "" {
		return primary
	}
	parts := make([]string, 0, 2)
	if prefix = strings.TrimSpace(prefix); prefix != "" {
		parts = append(parts, prefix)
	}
	if suffix = strings.TrimSpace(suffix); suffix != "" {
		parts = append(parts, suffix)
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}
