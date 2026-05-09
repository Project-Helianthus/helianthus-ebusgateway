package graphql

import (
	"errors"
	"testing"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// P9.5 (operator-directed pivot 2026-05-09) — findControllerEntry
// uses canonical regulator target addresses, not BASV-prefix scan.
//
// The previous BASV-prefix DeviceID scan silently skipped non-BASV
// controllers (CTLV*/CTLS*/CTLR*/BASS*/future identifiers) and was
// also race-prone with concurrent Register writes. The new algorithm
// looks up canonical eBUS regulator target addresses in priority
// order; the first registered entry is the controller.

// findControllerCanonicalLookupRegistry is a minimal Registry mock
// that only implements Lookup. Used to verify findControllerEntry's
// canonical-address-iteration contract: each canonical address gets
// looked up in priority order until one returns a registered entry.
type findControllerCanonicalLookupRegistry struct {
	lookups []byte // ordered list of addresses queried
	entries map[byte]registry.DeviceEntry
}

func (r *findControllerCanonicalLookupRegistry) Lookup(addr byte) (registry.DeviceEntry, bool) {
	r.lookups = append(r.lookups, addr)
	entry, ok := r.entries[addr]
	return entry, ok
}

// minimalEntry is a stub DeviceEntry returned by the mock's Lookup.
type minimalEntry struct {
	primaryAddress byte
	deviceID       string
}

func (e *minimalEntry) AddressByRole(registry.SlotRole) (byte, bool) { return e.primaryAddress, true }
func (e *minimalEntry) PrimaryDisplayAddress() byte                  { return e.primaryAddress }
func (e *minimalEntry) Addresses() []byte                            { return []byte{e.primaryAddress} }
func (e *minimalEntry) Manufacturer() string                         { return "Vaillant" }
func (e *minimalEntry) DeviceID() string                             { return e.deviceID }
func (e *minimalEntry) SerialNumber() string                         { return "" }
func (e *minimalEntry) MacAddress() string                           { return "" }
func (e *minimalEntry) SoftwareVersion() string                      { return "" }
func (e *minimalEntry) HardwareVersion() string                      { return "" }
func (e *minimalEntry) Planes() []registry.Plane                     { return nil }
func (e *minimalEntry) Projections() []registry.Projection           { return nil }

// TestFindControllerEntry_FindsRegulatorAtPrimaryCanonicalAddress
// verifies the primary canonical regulator target address (0x15) is
// the FIRST address tried, and the entry there is returned without
// further Lookup calls.
func TestFindControllerEntry_FindsRegulatorAtPrimaryCanonicalAddress(t *testing.T) {
	t.Parallel()

	primaryRegulator := &minimalEntry{primaryAddress: 0x15, deviceID: "BASV2X"}
	reg := &findControllerCanonicalLookupRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: primaryRegulator,
		},
	}

	got, err := findControllerEntry(reg)
	if err != nil {
		t.Fatalf("findControllerEntry error = %v", err)
	}
	if got != primaryRegulator {
		t.Errorf("returned entry = %v; want primary regulator at 0x15", got)
	}
	if len(reg.lookups) != 1 || reg.lookups[0] != 0x15 {
		t.Errorf("lookups = %v; want [0x15] (primary canonical address only)", reg.lookups)
	}
}

// TestFindControllerEntry_FallsThroughToAlternateCanonicalAddresses
// verifies that when 0x15 has no registered entry, the function
// tries the alternate canonical regulator target addresses in the
// documented priority order: 0x15, 0x35, 0x75, 0xF5, 0x76, 0xF6.
func TestFindControllerEntry_FallsThroughToAlternateCanonicalAddresses(t *testing.T) {
	t.Parallel()

	// Register a regulator only at 0x35 (P0 Heating circuit reg. 1).
	circuitReg1 := &minimalEntry{primaryAddress: 0x35, deviceID: "CTLV2X"}
	reg := &findControllerCanonicalLookupRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x35: circuitReg1,
		},
	}

	got, err := findControllerEntry(reg)
	if err != nil {
		t.Fatalf("findControllerEntry error = %v", err)
	}
	if got != circuitReg1 {
		t.Errorf("returned entry = %v; want circuit regulator at 0x35", got)
	}
	want := []byte{0x15, 0x35}
	if len(reg.lookups) != len(want) {
		t.Fatalf("lookups = %v; want exactly [0x15, 0x35] (stops on first hit)", reg.lookups)
	}
	for i, addr := range want {
		if reg.lookups[i] != addr {
			t.Errorf("lookups[%d] = 0x%02X; want 0x%02X", i, reg.lookups[i], addr)
		}
	}
}

// TestFindControllerEntry_AcceptsNonBASVDeviceIDs verifies the
// operator-directed core contract: a regulator at 0x15 with a NON-BASV
// DeviceID (e.g. CTLV2X, CTLS3, CTLR9, BASS4, or a hypothetical
// future identifier) is STILL returned as the controller. The
// previous DeviceID-prefix scan would have silently skipped this
// device.
func TestFindControllerEntry_AcceptsNonBASVDeviceIDs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		deviceID string
	}{
		{name: "CTLV", deviceID: "CTLV2X"},
		{name: "CTLS", deviceID: "CTLS3Y"},
		{name: "CTLR", deviceID: "CTLR9Z"},
		{name: "BASS", deviceID: "BASS4Q"},
		{name: "future identifier", deviceID: "FUTURE99"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry := &minimalEntry{primaryAddress: 0x15, deviceID: tc.deviceID}
			reg := &findControllerCanonicalLookupRegistry{
				entries: map[byte]registry.DeviceEntry{
					0x15: entry,
				},
			}

			got, err := findControllerEntry(reg)
			if err != nil {
				t.Fatalf("findControllerEntry(%s) error = %v", tc.deviceID, err)
			}
			if got != entry {
				t.Errorf("returned entry = %v; want regulator at 0x15 (DeviceID=%q)", got, tc.deviceID)
			}
		})
	}
}

// TestFindControllerEntry_ReturnsErrorWhenNoCanonicalRegulator
// verifies the new error path: when none of the canonical regulator
// target addresses has a registered entry, the function returns an
// error wrapping ErrInvalidPayload (not just a hardcoded fallback).
func TestFindControllerEntry_ReturnsErrorWhenNoCanonicalRegulator(t *testing.T) {
	t.Parallel()

	reg := &findControllerCanonicalLookupRegistry{
		entries: map[byte]registry.DeviceEntry{
			// Only an off-canonical entry — should NOT match.
			0x42: &minimalEntry{primaryAddress: 0x42, deviceID: "BASV2X"},
		},
	}

	_, err := findControllerEntry(reg)
	if err == nil {
		t.Fatal("findControllerEntry: err=nil; want error when no canonical regulator")
	}
	if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
		t.Errorf("err = %v; want errors.Is(err, ErrInvalidPayload)", err)
	}
	// Verify it tried EVERY canonical address before giving up.
	want := []byte{0x15, 0x35, 0x75, 0xF5, 0x76, 0xF6}
	if len(reg.lookups) != len(want) {
		t.Fatalf("lookups = %v; want all %d canonical addresses tried", reg.lookups, len(want))
	}
	for i, addr := range want {
		if reg.lookups[i] != addr {
			t.Errorf("lookups[%d] = 0x%02X; want 0x%02X (priority order)", i, reg.lookups[i], addr)
		}
	}
}
