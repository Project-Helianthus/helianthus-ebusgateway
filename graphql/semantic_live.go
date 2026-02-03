package graphql

import (
	"sync"
	"time"

	"github.com/d3vi1/helianthus-ebusgo/types"
	"github.com/d3vi1/helianthus-ebusreg/router"
)

// LiveSemanticProvider maintains semantic snapshots derived from bus data.
type LiveSemanticProvider struct {
	mu     sync.RWMutex
	zones  []Zone
	dhw    *DhwStatus
	energy *EnergyTotals
}

func NewLiveSemanticProvider() *LiveSemanticProvider {
	return &LiveSemanticProvider{}
}

func (provider *LiveSemanticProvider) Zones() []Zone {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if len(provider.zones) == 0 {
		return nil
	}
	zones := make([]Zone, len(provider.zones))
	copy(zones, provider.zones)
	return zones
}

func (provider *LiveSemanticProvider) DHW() *DhwStatus {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if provider.dhw == nil {
		return nil
	}
	copy := *provider.dhw
	return &copy
}

func (provider *LiveSemanticProvider) EnergyTotals() *EnergyTotals {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if provider.energy == nil {
		return nil
	}
	copy := *provider.energy
	copy.Gas = cloneEnergyChannel(copy.Gas)
	copy.Electric = cloneEnergyChannel(copy.Electric)
	copy.Solar = cloneEnergyChannel(copy.Solar)
	return &copy
}

func (provider *LiveSemanticProvider) SetZones(zones []Zone) {
	if provider == nil {
		return
	}
	zonesCopy := make([]Zone, len(zones))
	copy(zonesCopy, zones)
	provider.mu.Lock()
	provider.zones = zonesCopy
	provider.mu.Unlock()
}

func (provider *LiveSemanticProvider) SetDHW(status *DhwStatus) {
	if provider == nil {
		return
	}
	provider.mu.Lock()
	if status == nil {
		provider.dhw = nil
		provider.mu.Unlock()
		return
	}
	copy := *status
	provider.dhw = &copy
	provider.mu.Unlock()
}

// ApplyBroadcast updates semantic snapshots based on router broadcasts.
// Returns the updated totals if energy data changed.
func (provider *LiveSemanticProvider) ApplyBroadcast(event router.BroadcastEvent) (*EnergyTotals, bool) {
	if provider == nil {
		return nil, false
	}
	if len(event.Values) == 0 {
		return nil, false
	}

	if updated := provider.applyEnergy(event.Values, time.Now()); !updated {
		return nil, false
	}
	return provider.EnergyTotals(), true
}

func (provider *LiveSemanticProvider) applyEnergy(values map[string]types.Value, now time.Time) bool {
	wh, ok := floatValue(values, "wh")
	if !ok {
		return false
	}
	source, ok := stringValue(values, "source")
	if !ok {
		return false
	}
	usage, ok := stringValue(values, "usage")
	if !ok {
		return false
	}
	period, ok := stringValue(values, "period")
	if !ok {
		return false
	}

	var channel *EnergyChannel
	provider.mu.Lock()
	if provider.energy == nil {
		provider.energy = &EnergyTotals{}
	}
	switch source {
	case "gas":
		channel = &provider.energy.Gas
	case "electricity":
		channel = &provider.energy.Electric
	case "solar":
		channel = &provider.energy.Solar
	default:
		provider.mu.Unlock()
		return false
	}

	series := selectUsageSeries(channel, usage)
	if series == nil {
		provider.mu.Unlock()
		return false
	}

	updated := false
	kwh := wh / 1000.0

	switch period {
	case "day":
		if !matchesToday(values, now) {
			provider.mu.Unlock()
			return false
		}
		series.Today = kwh
		updated = true
	case "year":
		yearKind, ok := stringValue(values, "year_kind")
		if !ok {
			provider.mu.Unlock()
			return false
		}
		index := -1
		switch yearKind {
		case "previous":
			index = 0
		case "current":
			index = 1
		}
		if index < 0 {
			provider.mu.Unlock()
			return false
		}
		if len(series.Yearly) < 2 {
			series.Yearly = make([]float64, 2)
		}
		series.Yearly[index] = kwh
		updated = true
	}

	provider.mu.Unlock()
	return updated
}

func selectUsageSeries(channel *EnergyChannel, usage string) *EnergySeries {
	if channel == nil {
		return nil
	}
	switch usage {
	case "hot_water":
		return &channel.DHW
	case "heating", "cooling":
		return &channel.Climate
	default:
		return nil
	}
}

func matchesToday(values map[string]types.Value, now time.Time) bool {
	day, okDay := uintValue(values, "day")
	month, okMonth := uintValue(values, "month")
	if okMonth && int(month) != int(now.Month()) {
		return false
	}
	if okDay && int(day) != now.Day() {
		return false
	}
	return true
}

func stringValue(values map[string]types.Value, key string) (string, bool) {
	value, ok := values[key]
	if !ok || !value.Valid {
		return "", false
	}
	stringValue, ok := value.Value.(string)
	if !ok {
		return "", false
	}
	return stringValue, true
}

func floatValue(values map[string]types.Value, key string) (float64, bool) {
	value, ok := values[key]
	if !ok || !value.Valid {
		return 0, false
	}
	switch typed := value.Value.(type) {
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	default:
		return 0, false
	}
}

func uintValue(values map[string]types.Value, key string) (uint, bool) {
	value, ok := values[key]
	if !ok || !value.Valid {
		return 0, false
	}
	switch typed := value.Value.(type) {
	case uint8:
		return uint(typed), true
	case uint16:
		return uint(typed), true
	case uint32:
		return uint(typed), true
	case uint64:
		return uint(typed), true
	case uint:
		return typed, true
	case int:
		if typed < 0 {
			return 0, false
		}
		return uint(typed), true
	case int32:
		if typed < 0 {
			return 0, false
		}
		return uint(typed), true
	case int64:
		if typed < 0 {
			return 0, false
		}
		return uint(typed), true
	default:
		return 0, false
	}
}

func cloneEnergyChannel(channel EnergyChannel) EnergyChannel {
	channel.DHW = cloneEnergySeries(channel.DHW)
	channel.Climate = cloneEnergySeries(channel.Climate)
	return channel
}

func cloneEnergySeries(series EnergySeries) EnergySeries {
	if len(series.Yearly) > 0 {
		copySlice := make([]float64, len(series.Yearly))
		copy(copySlice, series.Yearly)
		series.Yearly = copySlice
	}
	return series
}

var _ SemanticProvider = (*LiveSemanticProvider)(nil)
