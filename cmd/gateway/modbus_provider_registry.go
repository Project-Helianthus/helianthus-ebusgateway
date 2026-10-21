package main

import (
	"errors"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

type modbusProviderProfileID string

const (
	modbusProviderSunSpec           modbusProviderProfileID = modbusreg.SunSpecThreePhaseMonitoringCapabilityID
	modbusProviderGrowattBMS        modbusProviderProfileID = "growatt.bms.rs485.v2.02"
	modbusProviderTeslaRetained     modbusProviderProfileID = "tesla.gen3.retained"
	modbusProviderSmartLogger       modbusProviderProfileID = "huawei.smartlogger"
	modbusProviderEMMA              modbusProviderProfileID = "huawei.emma"
	modbusProviderSDongle           modbusProviderProfileID = "huawei.sdongle"
	modbusProviderGrowattProtocolII modbusProviderProfileID = "growatt.protocol_ii"
	modbusProviderOutBack           modbusProviderProfileID = "outback"
)

type modbusProviderState string

const (
	modbusProviderPresent  modbusProviderState = "present"
	modbusProviderWithheld modbusProviderState = "withheld"
)

var (
	errModbusProviderUnknown     = errors.New("unknown modbus provider profile")
	errModbusProviderDuplicate   = errors.New("duplicate modbus provider profile")
	errModbusProviderUnavailable = errors.New("modbus provider profile unavailable")
)

type modbusProviderOwners struct {
	tcp     *modbusadapter.Adapter
	growatt *growattBMSRS485ProductionProvider
	tesla   *teslaHSCRetainedOwner
}

type modbusProviderFactory func(*gatewayModbusMCPProvider, modbusProviderOwners) mcp.ModbusV1Provider

type modbusProviderEntry struct {
	id        modbusProviderProfileID
	state     modbusProviderState
	available func(modbusProviderOwners) bool
	factory   modbusProviderFactory
	tools     []string
}

type modbusProviderRegistry map[modbusProviderProfileID]modbusProviderEntry

func newModbusProviderRegistry(entries []modbusProviderEntry) (modbusProviderRegistry, error) {
	registry := make(modbusProviderRegistry, len(entries))
	for _, entry := range entries {
		if _, exists := registry[entry.id]; exists {
			return nil, errModbusProviderDuplicate
		}
		registry[entry.id] = entry
	}
	return registry, nil
}

// resolve selects an inert owner-wrapper factory; it never invokes it.
func (registry modbusProviderRegistry) resolve(id modbusProviderProfileID, owners modbusProviderOwners) (modbusProviderFactory, error) {
	entry, ok := registry[id]
	if !ok {
		return nil, errModbusProviderUnknown
	}
	if entry.state != modbusProviderPresent || entry.factory == nil || entry.available == nil || !entry.available(owners) {
		return nil, errModbusProviderUnavailable
	}
	return entry.factory, nil
}

func modbusProviderEntries() []modbusProviderEntry {
	return []modbusProviderEntry{
		{id: modbusProviderSunSpec, state: modbusProviderPresent, available: func(o modbusProviderOwners) bool { return o.tcp != nil }, factory: func(core *gatewayModbusMCPProvider, o modbusProviderOwners) mcp.ModbusV1Provider {
			core.adapter = o.tcp
			core.portalPV = o.tcp
			return core
		}},
		{id: modbusProviderGrowattBMS, state: modbusProviderPresent, available: func(o modbusProviderOwners) bool { return o.growatt != nil }, factory: func(core *gatewayModbusMCPProvider, o modbusProviderOwners) mcp.ModbusV1Provider {
			return gatewayGrowattBMSMCPProvider{gatewayModbusMCPProvider: core, growatt: o.growatt, storage: o.growatt}
		}, tools: []string{mcp.GrowattBMSRS485V202StatusGetTool, mcp.SemanticV1GrowattStorageCurrentGetTool}},
		{id: modbusProviderTeslaRetained, state: modbusProviderPresent, available: func(o modbusProviderOwners) bool { return o.tesla != nil }, factory: func(core *gatewayModbusMCPProvider, o modbusProviderOwners) mcp.ModbusV1Provider {
			return &gatewayModbusRuntimeProvider{gatewayModbusMCPProvider: core, growatt: o.growatt, tesla: o.tesla}
		}, tools: []string{mcp.TeslaGen3EVSECurrentLimitV1GetTool, mcp.SemanticV1EVSECurrentGetTool}},
		{id: modbusProviderSmartLogger, state: modbusProviderWithheld},
		{id: modbusProviderEMMA, state: modbusProviderWithheld},
		{id: modbusProviderSDongle, state: modbusProviderWithheld},
		{id: modbusProviderGrowattProtocolII, state: modbusProviderWithheld},
		{id: modbusProviderOutBack, state: modbusProviderWithheld},
	}
}

func composeModbusProviders(core *gatewayModbusMCPProvider, owners modbusProviderOwners) mcp.ModbusV1Provider {
	entries := modbusProviderEntries()
	registry, err := newModbusProviderRegistry(entries)
	if err != nil {
		return nil
	}
	var provider mcp.ModbusV1Provider
	for _, entry := range entries {
		factory, err := registry.resolve(entry.id, owners)
		if errors.Is(err, errModbusProviderUnavailable) {
			continue
		}
		if err != nil {
			return nil
		}
		provider = factory(core, owners)
	}
	return provider
}

func modbusProviderOptionalToolAvailable(tool string, owners modbusProviderOwners) bool {
	entries := modbusProviderEntries()
	registry, err := newModbusProviderRegistry(entries)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		for _, name := range entry.tools {
			if name == tool {
				_, err := registry.resolve(entry.id, owners)
				return err == nil
			}
		}
	}
	return false
}
