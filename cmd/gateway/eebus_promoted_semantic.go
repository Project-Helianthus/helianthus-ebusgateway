package main

import (
	"context"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

// eebusPromotedSemanticProvider is deliberately an adapter-bound reader. It
// converts the fixed, already-promoted catalog into protocol-neutral GraphQL
// overlay values and retains neither raw payloads nor eeBUS identity.
type eebusPromotedSemanticProvider struct {
	runtime leafPromotionEEBusRuntime
	mu      sync.RWMutex
	overlay graphql.PromotedSemanticOverlay
}

func newEEBusPromotedSemanticProvider(adapter *eebusRuntimeAdapter) *eebusPromotedSemanticProvider {
	if adapter == nil || adapter.runtime == nil {
		return nil
	}
	return &eebusPromotedSemanticProvider{runtime: adapter.runtime}
}

// wireEEBusPromotedSemanticGraphQL composes only the public-safe overlay into
// GraphQL. The base provider remains the eBUS owner used by polling, MCP, and
// Portal wiring until their separately serialized M9 milestones.
func wireEEBusPromotedSemanticGraphQL(ctx context.Context, builder *graphql.Builder, base graphql.SemanticProvider, adapter *eebusRuntimeAdapter) {
	if builder == nil {
		return
	}
	promoted := newEEBusPromotedSemanticProvider(adapter)
	if promoted == nil {
		return
	}
	promoted.Start(ctx)
	builder.SetSemanticProvider(graphql.NewPromotedSemanticProvider(base, promoted.Overlay))
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
	go func() {
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
}

// Refresh replaces only the eeBUS-owned overlay. Any unavailable/degraded
// cycle clears it atomically, leaving the eBUS base provider untouched.
func (p *eebusPromotedSemanticProvider) Refresh(ctx context.Context) {
	if p == nil || p.runtime == nil {
		return
	}
	next := graphql.PromotedSemanticOverlay{Zones: map[string]graphql.PromotedZoneOverlay{}}
	snapshot, err := p.runtime.Snapshot()
	if err != nil || snapshot.Validate() != nil || snapshot.Meta.MaskTier != eebusraw.MaskTierRaw || snapshot.Status.State != "ready" {
		p.replace(next)
		return
	}
	slots, err := leafPromotionResolveSlots(snapshot)
	if err != nil {
		p.replace(next)
		return
	}
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		p.replace(next)
		return
	}
	for _, candidate := range registry.Candidates() {
		if candidate.SemanticPath == nil || candidate.EEBusSource == nil ||
			(candidate.ProtocolEligibility != promotioncapture.ProtocolCrossProtocol && candidate.ProtocolEligibility != promotioncapture.ProtocolEEBusNative) {
			continue
		}
		locator, locatorErr := leafPromotionLocatorForSource(snapshot, slots, *candidate.EEBusSource)
		if locatorErr != nil || len(candidate.EEBusSource.ValueFunctions) == 0 {
			continue
		}
		if !p.inventoryReadable(ctx, locator) {
			continue
		}
		value, ok := p.readValue(ctx, locator, candidate.EEBusSource.ValueFunctions[0])
		if !ok {
			continue
		}
		_, normalized, _, decodeErr := leafPromotionDecodeEEBus(candidate, value)
		if decodeErr == nil {
			applyPromotedValue(&next, *candidate.SemanticPath, normalized)
		}
	}
	p.replace(next)
}

func (p *eebusPromotedSemanticProvider) replace(overlay graphql.PromotedSemanticOverlay) {
	p.mu.Lock()
	p.overlay = overlay
	p.mu.Unlock()
}

func (p *eebusPromotedSemanticProvider) inventoryReadable(ctx context.Context, locator eebusraw.FeatureLocatorV1) bool {
	auth := eebusraw.ReadAuthorizationV1{PrincipalClass: "owner", Scope: eebusraw.AuthScopeV1RawRead, Tool: eebusraw.ToolV1FeaturesGet, MaskTier: eebusraw.MaskTierRaw}
	request := eebusraw.FeaturesGetRequestV1{Target: locator.Clone()}
	data, terminal := p.runtime.FeaturesGet(ctx, auth, request)
	return terminal == nil && eebusraw.ValidateFeaturesGetDataV1(request, data) == nil
}

func (p *eebusPromotedSemanticProvider) readValue(ctx context.Context, locator eebusraw.FeatureLocatorV1, function string) (any, bool) {
	target := eebusraw.FeatureTargetV1{RemoteSKI: locator.RemoteSKI, SHIPID: locator.SHIPID, DeviceAddress: locator.DeviceAddress, EntityAddress: append([]uint64(nil), locator.EntityAddress...), FeatureAddress: locator.FeatureAddress, FeatureType: locator.FeatureType, FeatureRole: locator.FeatureRole, Function: function, Operation: eebusraw.OperationV1Read}
	request := eebusraw.FeatureDataGetRequestV1{Targets: []eebusraw.FeatureTargetV1{target}, TimeoutMS: 3000}
	auth := eebusraw.ReadAuthorizationV1{PrincipalClass: "owner", Scope: eebusraw.AuthScopeV1RawRead, Tool: eebusraw.ToolV1FeaturesDataGet, MaskTier: eebusraw.MaskTierRaw}
	data, terminal := p.runtime.FeaturesDataGet(ctx, auth, request)
	if terminal != nil || eebusraw.ValidateFeatureDataGetDataV1(request, data, terminal) != nil || !data.Complete || len(data.Results) != 1 || len(data.Failures) != 0 {
		return nil, false
	}
	observation := data.Results[0]
	if observation.Target.Function != function || !leafPromotionLocatorEqual(observation.Target.Locator(), target.Locator()) {
		return nil, false
	}
	return observation.Value.Value(), true
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
