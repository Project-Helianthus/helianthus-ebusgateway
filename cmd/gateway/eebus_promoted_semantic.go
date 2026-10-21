package main

import (
	"context"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
)

// The compatibility overlay stays empty until the shared SemReg consumer
// admits a documented mapping. Refresh only supplies native observations.
type eebusPromotedSemanticProvider struct {
	adapter   *eebusRuntimeAdapter
	mu        sync.RWMutex
	overlay   graphql.PromotedSemanticOverlay
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

// Refresh delegates to the adapter publication boundary without native feature
// reads or direct semantic promotion.
func (p *eebusPromotedSemanticProvider) Refresh(ctx context.Context) {
	if p == nil || p.adapter == nil || ctx.Err() != nil {
		return
	}
	_, _ = p.adapter.refreshSemregSnapshot(ctx)
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
	zone := func(id string) graphql.PromotedZoneOverlay {
		if overlay.Zones == nil {
			overlay.Zones = map[string]graphql.PromotedZoneOverlay{}
		}
		return overlay.Zones[id]
	}
	putZone := func(id string, v graphql.PromotedZoneOverlay) { overlay.Zones[id] = v }
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
	case "/zones/zone_1/room_temperature_c":
		v := zone("zone-1")
		v.CurrentTempC = floatValue()
		putZone("zone-1", v)
	case "/zones/zone_1/target_temperature_c":
		v := zone("zone-1")
		v.TargetTempC = floatValue()
		putZone("zone-1", v)
	case "/zones/zone_1/operating_mode":
		v := zone("zone-1")
		v.OperatingMode = stringValue()
		putZone("zone-1", v)
	case "/zones/zone_1/operation_mode_changeable":
		v := zone("zone-1")
		v.OperationModeChangeable = boolValue()
		putZone("zone-1", v)
	case "/zones/zone_1/name":
		v := zone("zone-1")
		label := "Zone 1"
		v.SourceLabel = &label
		putZone("zone-1", v)
	case "/zones/zone_2/room_temperature_c":
		v := zone("zone-2")
		v.CurrentTempC = floatValue()
		putZone("zone-2", v)
	case "/zones/zone_2/target_temperature_c":
		v := zone("zone-2")
		v.TargetTempC = floatValue()
		putZone("zone-2", v)
	case "/zones/zone_2/operating_mode":
		v := zone("zone-2")
		v.OperatingMode = stringValue()
		putZone("zone-2", v)
	case "/zones/zone_2/operation_mode_changeable":
		v := zone("zone-2")
		v.OperationModeChangeable = boolValue()
		putZone("zone-2", v)
	case "/zones/zone_2/name":
		v := zone("zone-2")
		label := "Zone 2"
		v.SourceLabel = &label
		putZone("zone-2", v)
	}
}
