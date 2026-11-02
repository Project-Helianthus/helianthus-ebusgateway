package systemconfig

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func TestCaptureBoundRegistryIdentityBindsAddressAndObservation(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	info := registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "controller-family"}
	reg.Register(info)
	info.Address = 0x35
	reg.Register(info)
	first := CaptureBoundRegistryIdentity(reg, 0x15)
	if !first.Valid || first == CaptureBoundRegistryIdentity(reg, 0x35) {
		t.Fatal("identical identities at different addresses must have distinct provenance")
	}
	info.Address = 0x15
	reg.Register(info)
	if first == CaptureBoundRegistryIdentity(reg, 0x15) {
		t.Fatal("same-identity re-observation must retire provenance")
	}
	if CaptureBoundRegistryIdentity(reg, 0x75).Valid || CaptureBoundRegistryIdentity(struct{}{}, 0x15).Valid {
		t.Fatal("missing entry or snapshot support must fail closed")
	}
}

type changingBoundRegistry struct {
	generation uint64
}

func (reg *changingBoundRegistry) WithObservationGeneration(fn func(uint64)) bool {
	fn(reg.generation)
	return true
}

func (reg *changingBoundRegistry) LookupEntrySnapshot(byte) (registry.DeviceEntrySnapshot, bool) {
	reg.generation++
	return registry.DeviceEntrySnapshot{DeviceID: "controller-family"}, true
}

func TestCaptureBoundRegistryIdentityRefusesChangingSnapshot(t *testing.T) {
	if CaptureBoundRegistryIdentity(&changingBoundRegistry{}, 0x15).Valid {
		t.Fatal("observation during snapshot capture must fail closed")
	}
}
