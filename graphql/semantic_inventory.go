package graphql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// SemanticRegistryLeaf identifies one materialized leaf owned by the eBUS
// semantic provider. It carries no value payload.
type SemanticRegistryLeaf struct {
	Path           string
	PromotionState string
	Source         string
}

// SemanticRegistrySnapshot is a read-only inventory from the semantic owner.
type SemanticRegistrySnapshot struct {
	Authority string
	Leaves    []SemanticRegistryLeaf
}

// SemanticRegistryState snapshots only owner-materialized semantic state.
func (provider *LiveSemanticProvider) SemanticRegistryState() (SemanticRegistrySnapshot, error) {
	state := SemanticRegistrySnapshot{
		Authority: "ebus.promoted",
		Leaves:    []SemanticRegistryLeaf{},
	}
	if provider == nil {
		return state, nil
	}

	provider.mu.RLock()
	values := []struct {
		path  string
		value any
	}{
		{path: "/zones", value: provider.zonesForInventoryLocked()},
		{path: "/dhw", value: cloneDhwStatus(provider.dhw)},
		{path: "/circuits", value: provider.circuitsForInventoryLocked()},
		{path: "/radio", value: provider.radioForInventoryLocked()},
		{path: "/solar", value: cloneSolarStatus(provider.solar)},
		{path: "/cylinders", value: cloneCylinderStatuses(provider.cylinders)},
		{path: "/boiler", value: provider.boilerForInventoryLocked()},
		{path: "/system", value: semanticSystemForInventoryLocked(provider)},
		{path: "/schedules", value: semanticSchedulesForInventoryLocked(provider)},
		{path: "/adapter", value: semanticAdapterForInventoryLocked(provider)},
	}
	if provider.fm5Mode != Fm5SemanticModeAbsent && provider.fm5Mode != "" {
		values = append(values, struct {
			path  string
			value any
		}{path: "/fm5", value: provider.fm5Mode})
	}
	provider.mu.RUnlock()

	energyRevision := uint64(0)
	var energy *EnergyTotals
	if provider.energyMerge != nil {
		energyRevision = provider.energyMerge.Revision()
		if energyRevision > 0 {
			energy = provider.EnergyTotals()
		}
		if provider.energyMerge.Revision() != energyRevision {
			return SemanticRegistrySnapshot{}, fmt.Errorf("inventory semantic owner energy changed during snapshot")
		}
	}
	if energyRevision > 0 {
		values = append(values, struct {
			path  string
			value any
		}{path: "/energy", value: energy})
	}

	for _, item := range values {
		paths, err := semanticMaterializedLeafPaths(item.path, item.value)
		if err != nil {
			return SemanticRegistrySnapshot{}, fmt.Errorf("inventory semantic owner %s: %w", item.path, err)
		}
		for _, path := range paths {
			state.Leaves = append(state.Leaves, SemanticRegistryLeaf{
				Path: path, PromotionState: "PROMOTED", Source: "ebus",
			})
		}
	}
	return state, nil
}

func (provider *LiveSemanticProvider) zonesForInventoryLocked() []Zone {
	if !provider.zonePublished {
		return nil
	}
	zones := make([]Zone, len(provider.zones))
	for i, zone := range provider.zones {
		zones[i] = cloneZone(zone)
	}
	return zones
}

func (provider *LiveSemanticProvider) circuitsForInventoryLocked() []CircuitStatus {
	if !provider.circuitPublished {
		return nil
	}
	circuits := make([]CircuitStatus, len(provider.circuits))
	for i, circuit := range provider.circuits {
		circuits[i] = cloneCircuitStatus(circuit)
	}
	return circuits
}

func (provider *LiveSemanticProvider) radioForInventoryLocked() []RadioDevice {
	if !provider.radioPublished {
		return nil
	}
	radio := make([]RadioDevice, len(provider.radio))
	for i, device := range provider.radio {
		radio[i] = cloneRadioDevice(device)
	}
	return radio
}

func (provider *LiveSemanticProvider) boilerForInventoryLocked() *BoilerStatus {
	if !provider.boilerPublished || provider.boiler == nil {
		return nil
	}
	boiler := *provider.boiler
	boiler.State = cloneBoilerState(boiler.State)
	boiler.Config = cloneBoilerConfig(boiler.Config)
	boiler.Diagnostics = cloneBoilerDiagnostics(boiler.Diagnostics)
	return &boiler
}

func semanticSystemForInventoryLocked(provider *LiveSemanticProvider) *SystemStatus {
	stored, ok := liveSystemSnapshots.Load(provider)
	if !ok {
		return nil
	}
	status, _ := stored.(*SystemStatus)
	return cloneSystemStatus(status)
}

func semanticSchedulesForInventoryLocked(provider *LiveSemanticProvider) *ScheduleStatus {
	stored, ok := liveScheduleSnapshots.Load(provider)
	if !ok {
		return nil
	}
	status, _ := stored.(*ScheduleStatus)
	return cloneScheduleStatus(status)
}

func semanticAdapterForInventoryLocked(provider *LiveSemanticProvider) *AdapterHardwareInfo {
	stored, ok := liveAdapterHWInfoSnapshots.Load(provider)
	if !ok {
		return nil
	}
	info, _ := stored.(*AdapterHardwareInfo)
	return cloneAdapterHardwareInfo(info)
}

func semanticMaterializedLeafPaths(root string, value any) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	paths := []string{}
	var walk func(string, any)
	walk = func(path string, current any) {
		switch typed := current.(type) {
		case nil:
			return
		case map[string]any:
			keys := make([]string, 0, len(typed))
			for key := range typed {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				escaped := strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
				walk(path+"/"+escaped, typed[key])
			}
		case []any:
			for index, item := range typed {
				walk(path+"/"+strconv.Itoa(index), item)
			}
		default:
			paths = append(paths, path)
		}
	}
	walk(root, decoded)
	return paths, nil
}
