package main

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
)

// eebusPromotedSemanticProvider keeps the overlay of eeBUS heating values that
// GraphQL and Portal compose under the eBUS values. Each refresh reads the
// values through the inbound reader. In the managed start path the shell
// provider serves the overlay of the worker of the running generation. The
// overlay names no native runtime type: the remote binding of the last cycle is
// kept in the inbound reader's own result.
type eebusPromotedSemanticProvider struct {
	adapter   *eebusRuntimeAdapter
	refreshMu sync.Mutex
	mu        sync.RWMutex
	overlay   graphql.PromotedSemanticOverlay
	values    map[string]promotioncapture.TypedValue
	last      eebusInboundRead
	startOnce sync.Once
	cancel    context.CancelFunc
	done      chan struct{}
}

func newEEBusPromotedSemanticProvider(adapter *eebusRuntimeAdapter) *eebusPromotedSemanticProvider {
	if adapter == nil || (adapter.runtime == nil && adapter.managed == nil) {
		return nil
	}
	return &eebusPromotedSemanticProvider{adapter: adapter, done: make(chan struct{})}
}

// wireEEBusPromotedSemanticGraphQL composes the public-safe overlay for
// GraphQL and its serialized Portal consumer. The base provider remains the
// eBUS owner used by polling, MCP, and status.
func wireEEBusPromotedSemanticGraphQL(ctx context.Context, builder *graphql.Builder, base graphql.SemanticProvider, adapter *eebusRuntimeAdapter) graphql.SemanticProvider {
	if builder == nil {
		return base
	}
	promoted := newEEBusPromotedSemanticProvider(adapter)
	if promoted == nil {
		return base
	}
	if adapter.managed == nil {
		promoted.Start(ctx)
	}
	composed := graphql.NewPromotedSemanticProvider(base, promoted.Overlay)
	builder.SetSemanticProvider(composed)
	return composed
}

func (p *eebusPromotedSemanticProvider) Overlay() graphql.PromotedSemanticOverlay {
	if p == nil {
		return graphql.PromotedSemanticOverlay{}
	}
	if p.adapter != nil && p.adapter.managed != nil {
		return p.adapter.managed.eebusPromotedOverlay()
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return graphql.ClonePromotedSemanticOverlay(p.overlay)
}

func (p *eebusPromotedSemanticProvider) Start(ctx context.Context) {
	if p == nil {
		return
	}
	p.startOnce.Do(func() {
		if p.done == nil {
			p.done = make(chan struct{})
		}
		ctx, p.cancel = context.WithCancel(ctx)
		go func() {
			defer close(p.done)
			p.Refresh(ctx)
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					p.Refresh(ctx)
				}
			}
		}()
	})
}

func (p *eebusPromotedSemanticProvider) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if p.cancel != nil {
		p.cancel()
	}
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Refresh offers the native snapshot to the SemReg publication boundary and
// fills the overlay from the values the inbound reader returns for the same
// snapshot. A cycle whose runtime is not ready, or whose remote binding changes,
// replaces the overlay with an empty one. A value whose admitted read fails
// keeps its last-known value while the remote binding stays the same.
func (p *eebusPromotedSemanticProvider) Refresh(ctx context.Context) {
	if p == nil || p.adapter == nil || ctx.Err() != nil {
		return
	}
	runtime := p.adapter.runtime
	if runtime == nil {
		return
	}
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()
	slotGeneration, isSlot := eebusPromotedSlotGeneration(runtime)
	snapshot, _ := p.adapter.refreshSemregSnapshot(ctx)
	if ctx.Err() != nil {
		return
	}
	read, err := readEEBusInboundValues(ctx, runtime, snapshot)
	if ctx.Err() != nil {
		return
	}
	if err == nil && isSlot {
		if generation, _ := eebusPromotedSlotGeneration(runtime); generation != slotGeneration {
			err = errEEBusInboundBindingChanged
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.overlay, p.values, p.last = graphql.PromotedSemanticOverlay{}, nil, eebusInboundRead{}
		return
	}
	values := make(map[string]promotioncapture.TypedValue, len(read.Values))
	for _, value := range read.Values {
		values[value.SemanticPath] = value.Value
	}
	if read.Binding == p.last.Binding {
		for _, path := range read.Unread {
			if last, ok := p.values[path]; ok {
				values[path] = last
			}
		}
	}
	overlay := graphql.PromotedSemanticOverlay{}
	for path, value := range values {
		applyPromotedValue(&overlay, path, value)
	}
	p.overlay, p.values, p.last = overlay, values, eebusInboundRead{Binding: read.Binding}
}

func eebusPromotedSlotGeneration(runtime any) (uint64, bool) {
	slot, ok := runtime.(*eebusRuntimeSlot)
	if !ok || slot == nil {
		return 0, false
	}
	slot.operationMu.Lock()
	defer slot.operationMu.Unlock()
	return slot.publicationGeneration, true
}

func applyPromotedValue(overlay *graphql.PromotedSemanticOverlay, path string, value promotioncapture.TypedValue) {
	if overlay == nil {
		return
	}
	floatValue := func() *float64 {
		if value.Decimal == nil {
			return nil
		}
		v := float64(value.Decimal.Number)
		if value.Decimal.Scale >= 0 {
			for i := 0; i < value.Decimal.Scale; i++ {
				v *= 10
			}
		} else {
			for i := 0; i > value.Decimal.Scale; i-- {
				v /= 10
			}
		}
		return &v
	}
	stringValue := func() *string {
		if value.Enum != nil {
			v := *value.Enum
			return &v
		}
		if value.String != nil {
			v := *value.String
			return &v
		}
		return nil
	}
	boolValue := func() *bool {
		if value.Boolean == nil {
			return nil
		}
		v := *value.Boolean
		return &v
	}
	switch path {
	case "/dhw/temperature_c":
		overlay.DHW.CurrentTempC = floatValue()
	case "/dhw/target_temperature_c":
		overlay.DHW.TargetTempC = floatValue()
	case "/dhw/operating_mode":
		overlay.DHW.OperatingMode = stringValue()
	case "/dhw/operation_mode_changeable":
		overlay.DHW.OperationModeChangeable = boolValue()
	case "/dhw/overrun_active":
		overlay.DHW.OverrunActive = boolValue()
	case "/system/outside_air_temperature_c":
		overlay.System.OutdoorTemperature = floatValue()
	case "/system/gateway_brand":
		overlay.System.GatewayBrand = stringValue()
	case "/system/gateway_vendor":
		overlay.System.GatewayVendor = stringValue()
	default:
		number, field, ok := promotedZonePath(path)
		if !ok {
			return
		}
		id := "zone-" + number
		if overlay.Zones == nil {
			overlay.Zones = map[string]graphql.PromotedZoneOverlay{}
		}
		zone := overlay.Zones[id]
		switch field {
		case "room_temperature_c":
			zone.CurrentTempC = floatValue()
		case "target_temperature_c":
			zone.TargetTempC = floatValue()
		case "operating_mode":
			zone.OperatingMode = stringValue()
		case "operation_mode_changeable":
			zone.OperationModeChangeable = boolValue()
		case "name":
			// The native zone name is a household label; the overlay publishes
			// only the generic zone label.
			label := "Zone " + number
			zone.SourceLabel = &label
		}
		overlay.Zones[id] = zone
	}
}

// promotedZonePath splits "/zones/zone_<n>/<field>" into the canonical
// positive zone number and a known field.
func promotedZonePath(path string) (string, string, bool) {
	rest, ok := strings.CutPrefix(path, "/zones/zone_")
	if !ok {
		return "", "", false
	}
	number, field, ok := strings.Cut(rest, "/")
	if !ok {
		return "", "", false
	}
	value, err := strconv.ParseUint(number, 10, 32)
	if err != nil || value == 0 || strconv.FormatUint(value, 10) != number {
		return "", "", false
	}
	switch field {
	case "room_temperature_c", "target_temperature_c", "operating_mode", "operation_mode_changeable", "name":
		return number, field, true
	}
	return "", "", false
}
