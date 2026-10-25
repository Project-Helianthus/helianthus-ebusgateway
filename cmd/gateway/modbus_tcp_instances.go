package main

import (
	"errors"
	"log"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

// modbusTCPFamily is one value of the closed family table of the Modbus
// multi-endpoint runtime contract. The family is declared per endpoint and is
// the only qualification candidate; there is no detection across families.
type modbusTCPFamily string

const (
	modbusTCPFamilySunSpec           modbusTCPFamily = "sunspec"
	modbusTCPFamilyHuaweiSmartLogger modbusTCPFamily = "huawei.smartlogger"
	modbusTCPFamilyHuaweiEMMA        modbusTCPFamily = "huawei.emma"
	modbusTCPFamilyHuaweiSDongle     modbusTCPFamily = "huawei.sdongle"
)

// modbusTCPDefaultLabel is reserved for the unsuffixed driver ID. Together
// with the sunspec family it names the instance that configuration v1 maps to
// and that the single-endpoint surfaces keep addressing.
const modbusTCPDefaultLabel = "default"

// modbusTCPFamilySpec is one row of the closed family table. A family is
// composed only when the gateway has an admitted qualification for it; every
// other family registers withheld and sends nothing.
type modbusTCPFamilySpec struct {
	base     runtimecontrol.DriverID
	composed bool
	// cycle is the family's default acquisition cycle when the entry gives
	// none. A family contract may raise the contract default, never widen a
	// bound.
	cycle time.Duration
}

var modbusTCPFamilyTable = map[modbusTCPFamily]modbusTCPFamilySpec{
	modbusTCPFamilySunSpec:           {base: managedSunSpecID, composed: true, cycle: sunSpecLiveSmokeCycleInterval},
	modbusTCPFamilyHuaweiSmartLogger: {base: "modbus.huawei.smartlogger", cycle: modbusTCPDefaultCycle},
	modbusTCPFamilyHuaweiEMMA:        {base: "modbus.huawei.emma", cycle: modbusTCPDefaultCycle},
	modbusTCPFamilyHuaweiSDongle:     {base: "modbus.huawei.sdongle", cycle: modbusTCPDefaultCycle},
}

// modbusTCPWithheldReason is a closed instance qualification reason.
type modbusTCPWithheldReason string

const modbusTCPReasonFamilyNotAdmitted modbusTCPWithheldReason = "FAMILY_NOT_ADMITTED"

var errModbusTCPFamilyNotAdmitted = errors.New("modbus TCP instance withheld (FAMILY_NOT_ADMITTED)")

// modbusTCPDriverID derives an instance driver ID: the family base, then a dot
// and the label unless the label is the reserved default. Labels contain no
// dot and no family base is a dotted prefix of another, so the derivation is
// injective. No endpoint, host, port or device value enters an ID.
func modbusTCPDriverID(family modbusTCPFamily, label string) (runtimecontrol.DriverID, bool) {
	spec, ok := modbusTCPFamilyTable[family]
	if !ok || !validModbusTCPLabel(label) {
		return "", false
	}
	if label == modbusTCPDefaultLabel {
		return spec.base, true
	}
	return spec.base + runtimecontrol.DriverID("."+label), true
}

// modbusTCPInstance is the frozen construction input of one managed Modbus TCP
// driver instance. config carries only that instance's endpoint and dial
// timeout; pacing is nil for configuration v1, whose transport limits stay
// exactly as they were.
type modbusTCPInstance struct {
	label    string
	family   modbusTCPFamily
	driverID runtimecontrol.DriverID
	enabled  bool
	config   ebusgateway.ModbusTCPConfig
	pacing   *ebusgateway.ModbusTCPPacing
	unit     byte
	cycle    time.Duration
	// v1Slot marks the instance that the single-endpoint surfaces address:
	// raw reads, retained observations, the PV projection and its Portal panel.
	v1Slot bool
	// v2 is set for every instance of configuration v2.
	v2       bool
	withheld modbusTCPWithheldReason
}

// modbusTCPConfigurationV2 reports whether the endpoints document selected
// configuration v2.
func modbusTCPConfigurationV2(cfg ebusgateway.ModbusTCPConfig) bool {
	return len(cfg.Endpoints) > 0
}

// modbusTCPV1SlotConfigured reports whether the instance addressed by the
// single-endpoint surfaces is configured: the enabled v1 endpoint, or the v2
// entry with family sunspec and label default.
func modbusTCPV1SlotConfigured(cfg ebusgateway.ModbusTCPConfig) bool {
	if !modbusTCPConfigurationV2(cfg) {
		return cfg.Enabled
	}
	for _, entry := range cfg.Endpoints {
		if modbusTCPFamily(entry.Family) == modbusTCPFamilySunSpec && entry.Label == modbusTCPDefaultLabel {
			return true
		}
	}
	return false
}

var errModbusTCPInstanceSet = errors.New("modbus TCP instance set refused")

// modbusTCPInstancesFor derives the Modbus TCP instance set in configuration
// order. Configuration v1 is always exactly one sunspec instance with the
// existing driver ID, enabled only when the runtime is enabled. Configuration
// v2 is revalidated as a whole: any invalid entry, or a duplicate label,
// endpoint or driver ID, refuses the complete set before anything is
// registered or constructed.
func modbusTCPInstancesFor(cfg ebusgateway.ModbusTCPConfig) ([]modbusTCPInstance, error) {
	if !modbusTCPConfigurationV2(cfg) {
		v1 := ebusgateway.ModbusTCPConfig{Enabled: cfg.Enabled, Endpoint: cfg.Endpoint, DialTimeout: cfg.DialTimeout}
		return []modbusTCPInstance{{
			label: modbusTCPDefaultLabel, family: modbusTCPFamilySunSpec, driverID: managedSunSpecID,
			enabled: cfg.Enabled, config: v1, unit: sunSpecLiveSmokeUnitID, cycle: sunSpecLiveSmokeCycleInterval, v1Slot: true,
		}}, nil
	}
	if !cfg.Enabled || cfg.Endpoint != "" || cfg.DialTimeout != 0 || len(cfg.Endpoints) > maxModbusTCPEndpoints {
		return nil, errModbusTCPInstanceSet
	}
	instances := make([]modbusTCPInstance, 0, len(cfg.Endpoints))
	labels := make(map[string]bool, len(cfg.Endpoints))
	endpoints := make(map[string]bool, len(cfg.Endpoints))
	ids := make(map[runtimecontrol.DriverID]bool, len(cfg.Endpoints))
	for _, entry := range cfg.Endpoints {
		family := modbusTCPFamily(entry.Family)
		id, ok := modbusTCPDriverID(family, entry.Label)
		normalized, endpointErr := normalizeModbusTCPEndpoint(entry.Endpoint)
		if !ok || endpointErr != nil || !validModbusTCPEntryBounds(family, entry) {
			return nil, errModbusTCPInstanceSet
		}
		if labels[entry.Label] || endpoints[normalized] || ids[id] {
			return nil, errModbusTCPInstanceSet
		}
		labels[entry.Label], endpoints[normalized], ids[id] = true, true, true
		pacing := entry.Pacing
		instance := modbusTCPInstance{
			label: entry.Label, family: family, driverID: id, enabled: true,
			config: ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: entry.Endpoint, DialTimeout: entry.DialTimeout},
			pacing: &pacing, unit: sunSpecLiveSmokeUnitID, cycle: pacing.Cycle, v2: true,
			v1Slot: family == modbusTCPFamilySunSpec && entry.Label == modbusTCPDefaultLabel,
		}
		if len(entry.Units) > 0 {
			instance.unit = entry.Units[0]
		}
		if !modbusTCPFamilyTable[family].composed {
			instance.withheld = modbusTCPReasonFamilyNotAdmitted
		}
		instances = append(instances, instance)
	}
	return instances, nil
}

// validModbusTCPEntryBounds rechecks the resolved bounds of one entry so a
// programmatic configuration cannot bypass the document validation.
func validModbusTCPEntryBounds(family modbusTCPFamily, entry ebusgateway.ModbusTCPEndpointConfig) bool {
	if entry.DialTimeout < minModbusTCPDialTimeout || entry.DialTimeout > maxModbusTCPDialTimeout {
		return false
	}
	if !validModbusTCPUnits(family, entry.Units) {
		return false
	}
	return validModbusTCPPacing(entry.Pacing)
}

// modbusTCPWithheldFailure is the closed executor failure of a withheld
// instance: it lists as FAILED with PROVIDER_UNAVAILABLE, is never retried,
// and constructs, dials and publishes nothing.
func modbusTCPWithheldFailure(instance modbusTCPInstance) error {
	log.Printf("Modbus TCP instance withheld driver=%s family=%s posture=WITHHELD reason=%s", instance.driverID, instance.family, instance.withheld)
	return ebusControlFailure(drivermanager.ReasonProviderUnavailable, errModbusTCPFamilyNotAdmitted)
}

// modbusTCPLogf keys categorical live-smoke lines by instance. The v1 slot
// keeps its historical line unchanged; every other instance appends its
// driver ID, never an endpoint.
func modbusTCPLogf(instance modbusTCPInstance) func(string, ...any) {
	if instance.v1Slot {
		return log.Printf
	}
	return func(format string, args ...any) {
		log.Printf(format+" driver=%s", append(args, instance.driverID)...)
	}
}
