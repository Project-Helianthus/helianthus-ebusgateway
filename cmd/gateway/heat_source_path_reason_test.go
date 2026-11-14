package main

// Issue #1526: the startup reason no_supported_heat_source_path. The poller
// sets it while there is no B524 root and no boiler-class heat source and the
// inventory holds a heat-pump-class identity, and clears it as soon as a root
// or a boiler-class heat source exists. Identities and serials are synthetic:
// zero date, plant and sequence fields around a public product-catalog part
// number selected by catalog role at run time.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

// captureHeatSourcePathLog returns the semantic_startup_reason lines logged
// while fn runs.
func captureHeatSourcePathLog(t *testing.T, fn func()) []string {
	t.Helper()
	var buf bytes.Buffer
	writer, flags, prefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetOutput(&buf)
	log.SetFlags(0)
	log.SetPrefix("")
	defer func() {
		log.SetOutput(writer)
		log.SetFlags(flags)
		log.SetPrefix(prefix)
	}()
	fn()
	var lines []string
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.HasPrefix(line, "semantic_startup_reason ") {
			lines = append(lines, line)
		}
	}
	return lines
}

func newHeatSourcePathPoller(t *testing.T, devices ...registry.DeviceInfo) *vaillantSemanticPoller {
	t.Helper()
	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	return &vaillantSemanticPoller{
		reg:               newTestRegistry(devices...),
		provider:          graphql.NewLiveSemanticProvider(),
		catalog:           catalog,
		tasks:             newSemanticTaskScheduler(),
		zones:             make(map[byte]*vaillantZoneSnapshot),
		presence:          make(map[byte]*zonePresenceRecord),
		circuits:          make(map[byte]*vaillantCircuitSnapshot),
		zoneMissThreshold: 3,
		zoneHitThreshold:  2,
		// Every frame fails at once and pauses take no time: no device answers.
		sendFrameFn: func(context.Context, protocol.Frame) (*protocol.Frame, error) {
			return nil, ebuserrors.ErrTimeout
		},
		pauseFn: func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil },
	}
}

const heatSourcePathSetLine = "semantic_startup_reason reason=no_supported_heat_source_path state=set heat_source=0x%02x"

// Each case varies one input of the condition. Inputs and the production
// mutation each one catches:
//   - heat-pump identity, device-ID family (EHP, HMU, VWL, VWS, VWZ): a family
//     dropped from or misspelt in heatPumpDeviceIDFamilies;
//   - heat-pump identity, catalog role: the catalog-role branch removed,
//     the manufacturer or catalog-availability check removed, or a role other
//     than "Heat Pump" accepted;
//   - unknown identity: any identity treated as heat-pump class;
//   - boiler-class identity: boiler precedence in the predicate removed;
//   - B524 root (controller): the root check removed;
//   - boiler-class heat source (boiler address): the boiler check removed;
//   - root discovered earlier: the controllerEverDiscovered guard removed;
//   - regulator capability PRESENT: the capability guard removed;
//   - routing address: the lowest heat-pump-class address not reported.
func TestHeatSourcePathReasonCondition(t *testing.T) {
	heatPumpSerial := controllerlessSerial(controllerlessCatalogPart(t, "Heat Pump"))
	boilerSerial := controllerlessSerial(controllerlessCatalogPart(t, "Boiler"))
	vaillant := func(address byte, deviceID, serial string) registry.DeviceInfo {
		return registry.DeviceInfo{Address: address, Manufacturer: "Vaillant", DeviceID: deviceID, SerialNumber: serial}
	}
	for _, tc := range []struct {
		name           string
		devices        []registry.DeviceInfo
		controller     byte
		boilerAddress  byte
		everDiscovered bool
		capability     productids.ControllerCapability
		catalogErr     error
		wantAddress    byte // 0: no reason
	}{
		{name: "device-ID family EHP", devices: []registry.DeviceInfo{vaillant(0x08, "EHP00", "")}, wantAddress: 0x08},
		{name: "device-ID family HMU", devices: []registry.DeviceInfo{vaillant(0x08, "HMU00", "")}, wantAddress: 0x08},
		{name: "device-ID family VWL", devices: []registry.DeviceInfo{vaillant(0x08, "VWL00", "")}, wantAddress: 0x08},
		{name: "device-ID family VWS", devices: []registry.DeviceInfo{vaillant(0x08, "VWS00", "")}, wantAddress: 0x08},
		{name: "device-ID family VWZ", devices: []registry.DeviceInfo{vaillant(0x76, "VWZIO", "")}, wantAddress: 0x76},
		{name: "lowest heat-pump-class address", devices: []registry.DeviceInfo{vaillant(0x76, "VWZIO", ""), vaillant(0x08, "HMU00", "")}, wantAddress: 0x08},
		{name: "device-ID family without the catalog", devices: []registry.DeviceInfo{vaillant(0x08, "EHP00", "")}, catalogErr: errors.New("synthetic"), wantAddress: 0x08},
		{name: "catalog role heat pump", devices: []registry.DeviceInfo{vaillant(0x08, "XYZ00", heatPumpSerial)}, wantAddress: 0x08},
		{name: "catalog role heat pump, catalog unavailable", devices: []registry.DeviceInfo{vaillant(0x08, "XYZ00", heatPumpSerial)}, catalogErr: errors.New("synthetic")},
		{name: "catalog role heat pump, other manufacturer", devices: []registry.DeviceInfo{{Address: 0x08, Manufacturer: "Other", DeviceID: "XYZ00", SerialNumber: heatPumpSerial}}},
		{name: "catalog role boiler, unknown device ID", devices: []registry.DeviceInfo{vaillant(0x08, "XYZ00", boilerSerial)}},
		{name: "unknown identity", devices: []registry.DeviceInfo{vaillant(0x08, "XYZ00", "")}},
		{name: "boiler-class device ID with a heat-pump part", devices: []registry.DeviceInfo{vaillant(0x08, "BAI00", heatPumpSerial)}},
		{name: "empty inventory"},
		{name: "B524 root", devices: []registry.DeviceInfo{vaillant(0x08, "EHP00", heatPumpSerial)}, controller: 0x15},
		{name: "boiler-class heat source", devices: []registry.DeviceInfo{vaillant(0x08, "EHP00", heatPumpSerial)}, boilerAddress: 0x1F},
		{name: "root discovered earlier", devices: []registry.DeviceInfo{vaillant(0x08, "EHP00", heatPumpSerial)}, everDiscovered: true},
		{name: "regulator capability present", devices: []registry.DeviceInfo{vaillant(0x08, "EHP00", heatPumpSerial)}, capability: productids.ControllerPresent},
		{name: "regulator capability none", devices: []registry.DeviceInfo{vaillant(0x08, "EHP00", heatPumpSerial)}, capability: productids.ControllerNone, wantAddress: 0x08},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newHeatSourcePathPoller(t, tc.devices...)
			p.catalogErr = tc.catalogErr
			p.controller, p.boilerAddress = tc.controller, tc.boilerAddress
			p.controllerEverDiscovered, p.regulatorCapability = tc.everDiscovered, tc.capability
			lines := captureHeatSourcePathLog(t, p.refreshHeatSourcePathReason)

			wantReason, wantLines := "", []string(nil)
			if tc.wantAddress != 0 {
				wantReason = graphql.SemanticStartupReasonNoSupportedHeatSourcePath
				wantLines = []string{fmt.Sprintf(heatSourcePathSetLine, tc.wantAddress)}
			}
			if got := p.provider.StartupReason(); got != wantReason {
				t.Errorf("StartupReason() = %q; want %q", got, wantReason)
			}
			if strings.Join(lines, "\n") != strings.Join(wantLines, "\n") {
				t.Errorf("log lines = %q; want %q", lines, wantLines)
			}
			if got := p.provider.StartupPhase(); got != graphql.SemanticStartupPhaseBootInit {
				t.Errorf("phase = %s; want BOOT_INIT (the reason is not a phase)", got)
			}
			// A second evaluation of an unchanged state logs nothing.
			if again := captureHeatSourcePathLog(t, p.refreshHeatSourcePathReason); len(again) != 0 {
				t.Errorf("repeated evaluation logged %q; want nothing", again)
			}
		})
	}
}

// The reason follows the discovery passes: set by a pass without a root,
// cleared by a pass that adopts the heat-pump-class unit as root (root
// discovery is unchanged and still probes the heat-source address), and kept
// clear afterwards because a root was discovered once.
func TestHeatSourcePathReasonFollowsDiscoveryPasses(t *testing.T) {
	p := newHeatSourcePathPoller(t, registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "EHP00",
		SerialNumber: controllerlessSerial(controllerlessCatalogPart(t, "Heat Pump"))})
	rootAt := byte(0)
	p.b524ProbeFn = func(_ context.Context, target, _, _, _ byte, _ uint16) bool { return rootAt != 0 && target == rootAt }

	lines := captureHeatSourcePathLog(t, func() { p.refreshDiscovery(context.Background()) })
	if got := p.provider.StartupReason(); got != graphql.SemanticStartupReasonNoSupportedHeatSourcePath {
		t.Fatalf("StartupReason() after a pass without a root = %q; want the reason", got)
	}
	if want := []string{"semantic_startup_reason reason=no_supported_heat_source_path state=set heat_source=0x08"}; strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("log lines = %q; want %q", lines, want)
	}

	// The heat-pump-class unit answers the root probe coherently. The startup
	// priming of a first root (a wall-clock retry budget) is not under test.
	rootAt = 0x08
	p.mu.Lock()
	p.startupSemanticPrimed = true
	p.mu.Unlock()
	lines = captureHeatSourcePathLog(t, func() { p.refreshDiscovery(context.Background()) })
	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller != 0x08 {
		t.Fatalf("controller = 0x%02x; want the heat-pump-class unit adopted as root at 0x08", controller)
	}
	if got := p.provider.StartupReason(); got != "" {
		t.Fatalf("StartupReason() with a root = %q; want empty", got)
	}
	if want := []string{"semantic_startup_reason reason=no_supported_heat_source_path state=cleared controller=0x08 boiler=0x00"}; strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("log lines = %q; want %q", lines, want)
	}

	// The root stops answering: a failing read, not a bus without a path.
	rootAt = 0
	lines = captureHeatSourcePathLog(t, func() { p.refreshDiscovery(context.Background()) })
	if got := p.provider.StartupReason(); got != "" || len(lines) != 0 {
		t.Fatalf("after losing a discovered root: StartupReason() = %q, log %q; want empty and no line", got, lines)
	}
}

// A boiler-class heat source that appears later clears the reason on the
// registry-only boiler refresh of the state and configuration ticks.
func TestHeatSourcePathReasonClearedByBoilerClassHeatSource(t *testing.T) {
	p := newHeatSourcePathPoller(t, registry.DeviceInfo{Address: 0x76, Manufacturer: "Vaillant", DeviceID: "VWZIO"})
	p.refreshBoilerAddress(context.Background())
	if got := p.provider.StartupReason(); got != graphql.SemanticStartupReasonNoSupportedHeatSourcePath {
		t.Fatalf("StartupReason() = %q; want the reason", got)
	}
	p.reg.Register(registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"})
	lines := captureHeatSourcePathLog(t, func() { p.refreshBoilerAddress(context.Background()) })
	if got := p.provider.StartupReason(); got != "" {
		t.Fatalf("StartupReason() with a boiler-class heat source = %q; want empty", got)
	}
	if want := []string{"semantic_startup_reason reason=no_supported_heat_source_path state=cleared controller=0x00 boiler=0x08"}; strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("log lines = %q; want %q", lines, want)
	}
}

// A registry entry enriched with a serial the catalog classifies as a
// controller clears the reason at the next regulator capability recheck, with
// an unchanged device count (no discovery pass is queued). Mutation caught:
// the reason evaluation removed from refreshRegulatorCapability.
func TestHeatSourcePathReasonClearedByCapabilityRecheck(t *testing.T) {
	regulatorSerial := controllerlessSerial(controllerlessCatalogPartWith(t, "Regulator", productids.ControllerPresent))
	p := newHeatSourcePathPoller(t,
		registry.DeviceInfo{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "EHP00"},
		registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV"},
	)
	p.refreshRegulatorCapability(context.Background())
	if got := p.provider.StartupReason(); got != graphql.SemanticStartupReasonNoSupportedHeatSourcePath {
		t.Fatalf("StartupReason() before enrichment = %q; want the reason", got)
	}
	p.reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV", SerialNumber: regulatorSerial})
	p.refreshRegulatorCapability(context.Background())
	if got := p.provider.StartupReason(); got != "" {
		t.Fatalf("StartupReason() after a controller serial = %q; want empty", got)
	}
}
