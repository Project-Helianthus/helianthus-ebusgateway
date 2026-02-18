package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"slices"
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
	vaillantB524OpcodeLocal      = byte(0x02)
	vaillantB524OpRead           = byte(0x00)

	vaillantGroupZones = byte(0x03)

	zoneRegName        = uint16(0x0016)
	zoneRegIndex       = uint16(0x001C)
	zoneRegCurrentTemp = uint16(0x000F)
	zoneRegTargetTemp  = uint16(0x0022)
)

func startVaillantSemanticPolling(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub) {
	if gateway == nil || gateway.Bus == nil || gateway.Registry == nil || provider == nil {
		return
	}

	poller := newVaillantSemanticPoller(cfg, gateway, provider, hub)
	poller.Start(ctx)
}

type vaillantSemanticPoller struct {
	scheduler *ebusgateway.SemanticReadScheduler

	reg      *registry.DeviceRegistry
	bus      *protocol.Bus
	provider *graphql.LiveSemanticProvider
	hub      *graphql.BroadcastHub

	source            byte
	requestTimeout    time.Duration
	discoveryInterval time.Duration
	configInterval    time.Duration
	stateInterval     time.Duration

	mu         sync.Mutex
	controller byte
	zones      map[byte]*vaillantZoneSnapshot
}

type vaillantZoneSnapshot struct {
	Instance byte
	Present  bool

	Name string

	CurrentTempC *float64
	TargetTempC  *float64
}

func newVaillantSemanticPoller(cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub) *vaillantSemanticPoller {
	return &vaillantSemanticPoller{
		scheduler: ebusgateway.NewSemanticReadScheduler(),
		reg:       gateway.Registry,
		bus:       gateway.Bus,
		provider:  provider,
		hub:       hub,
		source:    cfg.ScanSource,

		requestTimeout:    cfg.SemanticRequestTimeout,
		discoveryInterval: cfg.SemanticDiscoveryInterval,
		configInterval:    cfg.SemanticConfigInterval,
		stateInterval:     cfg.SemanticStateInterval,

		zones: make(map[byte]*vaillantZoneSnapshot),
	}
}

func (p *vaillantSemanticPoller) Start(ctx context.Context) {
	if p == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Prime quickly so HA can create entities on first coordinator refresh.
	go func() {
		p.refreshDiscovery(ctx)
		p.refreshConfig(ctx)
		p.refreshState(ctx)
	}()

	go p.runLoop(ctx, p.discoveryInterval, p.refreshDiscovery)
	go p.runLoop(ctx, p.configInterval, p.refreshConfig)
	go p.runLoop(ctx, p.stateInterval, p.refreshState)
}

func (p *vaillantSemanticPoller) runLoop(ctx context.Context, interval time.Duration, fn func(context.Context)) {
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
			fn(ctx)
		}
	}
}

func (p *vaillantSemanticPoller) refreshDiscovery(ctx context.Context) {
	controller, ok := findDeviceAddressByPrefix(p.reg, "BASV")
	if !ok {
		p.mu.Lock()
		p.controller = 0
		p.zones = make(map[byte]*vaillantZoneSnapshot)
		p.mu.Unlock()
		p.publishZones()
		return
	}

	p.mu.Lock()
	p.controller = controller
	p.mu.Unlock()

	present := make(map[byte]bool, 4)
	for instance := byte(0x00); instance <= 0x0A; instance++ {
		indexBytes, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegIndex)
		if !ok || len(indexBytes) < 1 || indexBytes[0] == 0xFF {
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
		if !present[instance] {
			delete(p.zones, instance)
		}
	}
	p.mu.Unlock()

	p.publishZones()
}

func (p *vaillantSemanticPoller) refreshConfig(ctx context.Context) {
	controller, zones := p.snapshotZones()
	if controller == 0 || len(zones) == 0 {
		return
	}

	for _, instance := range zones {
		if rawName, ok := p.readB524CString(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegName); ok {
			if trimmed := strings.TrimSpace(rawName); trimmed != "" {
				p.mu.Lock()
				if entry := p.zones[instance]; entry != nil {
					entry.Name = trimmed
				}
				p.mu.Unlock()
			}
		}
	}

	p.publishZones()
}

func (p *vaillantSemanticPoller) refreshState(ctx context.Context) {
	controller, zones := p.snapshotZones()
	if controller == 0 || len(zones) == 0 {
		return
	}

	for _, instance := range zones {
		var currentPtr *float64
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegCurrentTemp); ok {
			current := value
			currentPtr = &current
		}

		var targetPtr *float64
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegTargetTemp); ok {
			target := value
			targetPtr = &target
		}

		p.mu.Lock()
		if entry := p.zones[instance]; entry != nil {
			entry.CurrentTempC = currentPtr
			entry.TargetTempC = targetPtr
		}
		p.mu.Unlock()
	}

	p.publishZones()
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

func (p *vaillantSemanticPoller) publishZones() {
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
			ID:           fmt.Sprintf("zone-%d", instance+1),
			Name:         name,
			CurrentTempC: entry.CurrentTempC,
			TargetTempC:  entry.TargetTempC,
		}
		zones = append(zones, zone)
	}
	p.mu.Unlock()

	previous := p.provider.Zones()
	p.provider.SetZones(zones)

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
}

func zoneEquals(a, b graphql.Zone) bool {
	if a.ID != b.ID || a.Name != b.Name {
		return false
	}
	if !floatPtrEquals(a.CurrentTempC, b.CurrentTempC) {
		return false
	}
	if !floatPtrEquals(a.TargetTempC, b.TargetTempC) {
		return false
	}
	return true
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

	key := fmt.Sprintf("b524:%02x:%02x:%02x:%02x:%04x", target, opcode, group, instance, addr)
	value, err := p.scheduler.Get(ctx, key, 500*time.Millisecond, func(ctx context.Context) ([]byte, error) {
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		request := protocol.Frame{
			Source:    source,
			Target:    target,
			Primary:   vaillantExtRegisterPrimary,
			Secondary: vaillantExtRegisterSecondary,
			Data:      []byte{opcode, vaillantB524OpRead, group, instance, byte(addr), byte(addr >> 8)},
		}
		response, err := p.bus.Send(reqCtx, request)
		if err != nil {
			return nil, err
		}
		if response == nil {
			return nil, fmt.Errorf("b524 read returned nil response")
		}
		payload, ok := parseB524ReadPayload(response.Data, opcode, group, instance, addr)
		if !ok {
			return nil, fmt.Errorf("b524 read failed: opcode=0x%02x group=0x%02x instance=0x%02x addr=0x%04x", opcode, group, instance, addr)
		}
		return payload, nil
	})
	if err != nil {
		return nil, false
	}
	return value, true
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
