package main

// Issue #1469: offline acceptance of the site profile in which the eBUS carries
// a heat source and the vendor's internet module but never a system controller.
//
// The fixture is synthetic. Device classes, target addresses and the frame
// shapes it answers come from the public protocol documentation in
// helianthus-docs-ebus: protocols/ebus-services/ebus-service-07h.md (07 04
// identification), protocols/vaillant/ebus-vaillant-b509.md (B509 register
// reads and the 24..27 serial windows) and protocols/vaillant/ebus-vaillant.md
// (device-class table: heat source target 0x08 without B524, internet module
// target 0xF6 with B524 as initiator only). No device answers B524, the worst
// case for retries. Serial numbers are synthetic: zero date, plant and
// sequence fields around a public product-catalogue part number that the test
// selects by catalogue role at run time. No identifier of a real installation
// is used.
//
// The scenario runs the production startup scan loop, registry scan, B524 root
// probe, semantic poller, read scheduler and poll schedule with default
// configuration. Every gateway frame reaches the recording bus below through
// the scan bus or the poller's frame seam; the gateway's own protocol bus runs
// over a counting loopback that must stay silent. Time is simulated from
// process start, in production order:
//   - the semantic runtime and its boot-live deadline start at time 0, then the
//     passive warm-up of source selection, then the startup scan, then semantic
//     polling once the barrier opens;
//   - each frame takes simulated bus time, and every pause the gateway inserts
//     (scan pacing, probe retry back-off, read retry and post-read pauses, the
//     scan loop interval) takes its production duration on the simulated clock;
//   - the poller, its task queue, its read scheduler and its shadow cache all
//     run on that clock, and recurring tasks are enqueued at the instants their
//     production tickers fire, also while another task runs.
//
// Two timing profiles bound real bus behaviour. Wire timing: a frame costs its
// symbols at 2400 Bd, and an unanswered request costs one more symbol (the SYN
// that replaces the acknowledgement). Timeout timing: an unanswered request
// lasts its whole per-attempt timeout (400 ms scan request, 5 s root probe).
// The passive observation path is not simulated: the internet module is
// identified through the per-address identity probe that the passive
// address-table inserter calls when it sees the module's traffic.
//
// Expected results, asserted below:
//   - startup, automatic source (add-on default): one scan pass and one root
//     confirmation probe call; the active probe releases semantic polling.
//     Explicit source: the same single pass; root confirmation finds no root
//     and the catalog classifies every Vaillant identity as a non-controller,
//     so active confirmation admits. With an unclassified or a regulator
//     identity at the controller address the explicit source still needs a
//     root: two passes, then admission failure while semantic polling starts;
//   - phases: a boiler-class heat source reaches LIVE_READY from the boiler
//     stream alone, before the boot-live deadline in both timing profiles and
//     both admissions; with an explicit source, an unclassified identity at the
//     controller address and timeout timing the deadline passes first
//     (DEGRADED, then LIVE_READY). A heat-pump-class heat source publishes no
//     stream and the deadline moves BOOT_INIT to DEGRADED; from the first
//     discovery pass the startup block carries the reason
//     no_supported_heat_source_path in GraphQL and MCP and the DEGRADED
//     transition names it. A heat source of unknown class gets no reason and
//     the plain DEGRADED transition. A heat-pump-class unit that answers the
//     root probe is adopted as root as before, which clears the reason;
//   - controller-class traffic is B524 root discovery only: each candidate
//     (0x15 first, then 0x08, 0x26 and any identified device) gets one
//     production probe call per discovery pass, three read attempts each with
//     the regulator selector. Until the absence state is ABSENT a pass runs on
//     every state tick (60 s), config tick (5 min) and discovery tick (10 min);
//     once ABSENT only the discovery tick and event-driven refreshes run one.
//     No other B524 frame, no B555 read and no write is ever sent, and nothing
//     runs as a tight loop;
//   - heat-source B509 registers are re-read natively when the 30 s read cache
//     has expired: fast-tier registers once a minute, medium-tier every 5 min,
//     slow-tier every 10 min;
//   - published: boiler status from the heat source only, an ABSENT FM5 mode,
//     empty zone, circuit, radio and cylinder lists, an all-null solar plane, a
//     null energy plane in GraphQL and MCP (no energy point is ever accepted);
//     nothing for DHW, system or schedules; no controller-owned boiler mirror
//     field;
//   - absence: vaillant_regulator_capability is NONE from the first discovery
//     (UNKNOWN when an identified module has no readable serial), identical in
//     GraphQL and MCP; the absence state goes UNKNOWN (not yet evaluated) ->
//     ABSENCE_GRACE at the first recheck -> ABSENT five minutes after that.
//
// Nothing here writes to a bus or answers as a controller: the synthetic bus
// leaves every B524 request unanswered and the test fails on any write frame.

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
	graphqlgo "github.com/graphql-go/graphql"
)

const (
	controllerlessSource         = byte(0xF0)
	controllerlessCompanion      = byte(0xF5)
	controllerlessHeatSource     = byte(0x08)
	controllerlessHeatSourceInit = byte(0x03)
	controllerlessRegulator      = byte(0x15)
	controllerlessModuleSlot     = byte(0x26)
	controllerlessInternetModule = byte(0xF6)
	controllerlessHorizon        = 30 * time.Minute
	// One eBUS symbol: 10 bits at 2400 Bd.
	controllerlessSymbol = time.Second * 10 / 2400
	// registry.Scan pauses this long after every identification probe.
	controllerlessScanPacing = 100 * time.Millisecond
	// Production probe attempts per call against an unanswered address.
	controllerlessProbeAttempts = 3
	// controllerlessTaskLimit bounds the tasks that may run without simulated
	// time advancing; a queue that keeps refilling itself is a tight loop.
	controllerlessTaskLimit = 64
)

// Frame classes the synthetic bus records.
const (
	controllerlessIdentify     = "identification_07_04"
	controllerlessSerialWindow = "b509_serial_window"
	controllerlessB509Read     = "b509_register_read"
	controllerlessB524Frame    = "b524_frame"
	controllerlessWriteFrame   = "write_frame"
	controllerlessOtherFrame   = "other_frame"
)

type controllerlessTiming int

const (
	controllerlessWireTiming controllerlessTiming = iota
	controllerlessTimeoutTiming
)

func (timing controllerlessTiming) String() string {
	if timing == controllerlessTimeoutTiming {
		return "timeout timing"
	}
	return "wire timing"
}

type controllerlessClock struct {
	mu        sync.Mutex
	base      time.Time
	offset    time.Duration
	onAdvance func(time.Duration)
}

func (c *controllerlessClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.base.Add(c.offset)
}

func (c *controllerlessClock) elapsed() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offset
}

func (c *controllerlessClock) advance(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	c.offset += d
	at, hook := c.offset, c.onAdvance
	c.mu.Unlock()
	if hook != nil {
		hook(at)
	}
}

func (c *controllerlessClock) advanceTo(at time.Duration) {
	c.advance(at - c.elapsed())
}

type controllerlessDevice struct {
	deviceID    string // five ASCII characters returned by 07 04
	serialRaw   string // 28 characters for the 24..27 windows; empty leaves them unanswered
	answersB509 bool   // answers B509 0D register reads
}

type controllerlessEvent struct {
	at       time.Duration
	kind     string
	target   byte
	register uint16
	data     []byte
}

type controllerlessObservation struct {
	at    time.Duration
	value string
}

func controllerlessNote(series *[]controllerlessObservation, at time.Duration, value string) {
	if n := len(*series); n > 0 && (*series)[n-1].value == value {
		return
	}
	*series = append(*series, controllerlessObservation{at: at, value: value})
}

// controllerlessSite is one simulated installation: the synthetic bus, the
// clock and the semantic provider that the gateway starts at process start.
type controllerlessSite struct {
	t        *testing.T
	ctx      context.Context
	timing   controllerlessTiming
	clock    *controllerlessClock
	provider *graphql.LiveSemanticProvider
	deadline time.Duration

	// onFrame, when set, sees every recorded frame; stopPolling ends the
	// running simulation (the task in progress sees a cancelled context).
	onFrame     func(controllerlessEvent)
	stopPolling context.CancelFunc

	mu              sync.Mutex
	devices         map[byte]controllerlessDevice
	events          []controllerlessEvent
	phases          []controllerlessObservation
	deadlinePassed  bool
	phaseAtDeadline graphql.SemanticStartupPhase

	// transitions holds the phase transition lines the boot-live timer's
	// logger received.
	transitionMu sync.Mutex
	transitions  []string
}

// controllerlessHeatSourceRegisters holds synthetic B509 values for the
// registers whose published value the scenario checks. Every other register
// the gateway reads answers with two zero bytes.
var controllerlessHeatSourceRegisters = map[uint16][]byte{
	boiler_b509_water_pressure:     {0x80, 0x01}, // DATA2b 1.5 bar
	boiler_b509_flame_active:       {0xF0},       // off
	boiler_b509_flowset_hc_max_c:   {0xB0, 0x04}, // DATA2c 75 °C
	boiler_b509_dhw_temp_desired_c: {0x20, 0x03}, // DATA2c 50 °C
	boiler_b509_phone_number:       {0xFF},       // no BCD digit
}

func newControllerlessSite(t *testing.T, timing controllerlessTiming) *controllerlessSite {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	site := &controllerlessSite{
		t:        t,
		ctx:      ctx,
		timing:   timing,
		clock:    &controllerlessClock{base: time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)},
		provider: graphql.NewLiveSemanticProvider(),
		deadline: ebusgateway.DefaultConfig().BootLiveTimeout,
		devices:  map[byte]controllerlessDevice{},
	}
	site.clock.onAdvance = site.observePhase
	site.observePhase(0)
	return site
}

// observePhase records phase changes and applies the boot-live deadline. The
// production timer (graphql.StartBootFSM) starts with the semantic runtime at
// process start and moves any phase other than LIVE_READY to DEGRADED when it
// fires; the scenario fires it at the first simulated instant at or after the
// deadline.
func (s *controllerlessSite) observePhase(at time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	controllerlessNote(&s.phases, at, string(s.provider.StartupPhase()))
	if s.deadlinePassed || at < s.deadline {
		return
	}
	s.deadlinePassed = true
	s.phaseAtDeadline = s.provider.StartupPhase()
	if s.phaseAtDeadline == graphql.SemanticStartupPhaseLiveReady {
		return
	}
	s.provider.StartBootFSM(s.ctx, time.Nanosecond, func(format string, args ...any) {
		s.transitionMu.Lock()
		s.transitions = append(s.transitions, fmt.Sprintf(format, args...))
		s.transitionMu.Unlock()
	})
	limit := time.Now().Add(5 * time.Second)
	for s.provider.StartupPhase() != graphql.SemanticStartupPhaseDegraded {
		if time.Now().After(limit) {
			s.t.Errorf("boot-live timer did not move phase %s to DEGRADED", s.provider.StartupPhase())
			return
		}
		time.Sleep(time.Millisecond)
	}
	controllerlessNote(&s.phases, s.deadline, string(graphql.SemanticStartupPhaseDegraded))
}

// degradedTransition returns the logged transition to DEGRADED. The timer
// logs it right after the transition, so it may trail the phase briefly.
func (s *controllerlessSite) degradedTransition() string {
	limit := time.Now().Add(5 * time.Second)
	for {
		s.transitionMu.Lock()
		for _, line := range s.transitions {
			if strings.Contains(line, " to=DEGRADED ") {
				s.transitionMu.Unlock()
				return line
			}
		}
		s.transitionMu.Unlock()
		if time.Now().After(limit) {
			return ""
		}
		time.Sleep(time.Millisecond)
	}
}

// controllerlessReasonValue names the startup reason in a series; "none" when
// no reason applies.
func controllerlessReasonValue(provider *graphql.LiveSemanticProvider) string {
	return controllerlessReasonValueOf(provider.StartupReason())
}

func controllerlessReasonValueOf(reason string) string {
	if reason == "" {
		return "none"
	}
	return reason
}

func (s *controllerlessSite) phaseSeries() []controllerlessObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]controllerlessObservation(nil), s.phases...)
}

func (s *controllerlessSite) addDevice(target byte, device controllerlessDevice) {
	s.mu.Lock()
	s.devices[target] = device
	s.mu.Unlock()
}

func controllerlessFrameKind(frame protocol.Frame) string {
	switch {
	case frame.Primary == 0x07 && frame.Secondary == 0x04:
		return controllerlessIdentify
	case frame.Primary == vaillantB509Primary && frame.Secondary == vaillantB509Secondary && len(frame.Data) > 0:
		switch op := frame.Data[0]; {
		case op >= 0x24 && op <= 0x27 && len(frame.Data) == 1:
			return controllerlessSerialWindow
		case op == vaillantB509OpcodeRead && len(frame.Data) == 3:
			return controllerlessB509Read
		case op == vaillantB509OpcodeWrite:
			return controllerlessWriteFrame
		}
	case frame.Primary == vaillantExtRegisterPrimary && frame.Secondary == vaillantExtRegisterSecondary:
		if len(frame.Data) > 1 && frame.Data[1] == vaillantB524OpWrite {
			return controllerlessWriteFrame
		}
		return controllerlessB524Frame
	}
	return controllerlessOtherFrame
}

// frameCost is the simulated bus time of one request: arbitration (one SYN),
// QQ ZZ PB SB NN, data and CRC, then either the answered exchange (the
// target's ACK, NN, data and CRC, the initiator's ACK and the closing SYN:
// five fixed symbols plus the data) or, when nobody answers, one more symbol
// in wire timing or the rest of the attempt's timeout in timeout timing.
func (s *controllerlessSite) frameCost(ctx context.Context, frame protocol.Frame, answered bool, replyLen int) time.Duration {
	symbols := 1 + 6 + len(frame.Data)
	if answered {
		symbols += 5 + replyLen
	} else {
		symbols++
	}
	wire := time.Duration(symbols) * controllerlessSymbol
	if answered || s.timing == controllerlessWireTiming {
		return wire
	}
	if deadline, ok := ctx.Deadline(); ok {
		if rest := time.Until(deadline).Round(100 * time.Millisecond); rest > wire {
			return rest
		}
	}
	return wire
}

// Send is the recording bus: every frame of the scenario arrives here.
func (s *controllerlessSite) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	kind := controllerlessFrameKind(frame)
	event := controllerlessEvent{at: s.clock.elapsed(), kind: kind, target: frame.Target, data: append([]byte(nil), frame.Data...)}
	if kind == controllerlessB509Read {
		event.register = uint16(frame.Data[1])<<8 | uint16(frame.Data[2])
	}
	s.mu.Lock()
	s.events = append(s.events, event)
	device, present := s.devices[frame.Target]
	s.mu.Unlock()
	if s.onFrame != nil {
		s.onFrame(event)
	}

	var reply []byte
	answered := false
	if present {
		switch kind {
		case controllerlessIdentify:
			reply = append(append([]byte{0xB5}, []byte(device.deviceID)...), 0x01, 0x00, 0x01, 0x00)
			answered = true
		case controllerlessSerialWindow:
			if device.serialRaw != "" {
				padded := device.serialRaw + strings.Repeat(" ", 32-len(device.serialRaw))
				start := int(frame.Data[0]-0x24) * 8
				reply = append([]byte{0x00}, padded[start:start+8]...)
				answered = true
			}
		case controllerlessB509Read:
			if device.answersB509 {
				value, ok := controllerlessHeatSourceRegisters[event.register]
				if !ok {
					value = []byte{0x00, 0x00}
				}
				reply = append([]byte{vaillantB509OpcodeRead, frame.Data[1], frame.Data[2]}, value...)
				answered = true
			}
		}
	}
	s.clock.advance(s.frameCost(ctx, frame, answered, len(reply)))
	if kind == controllerlessIdentify {
		s.clock.advance(controllerlessScanPacing)
	}
	if !answered {
		return nil, fmt.Errorf("synthetic bus: no answer from 0x%02X: %w", frame.Target, ebuserrors.ErrTimeout)
	}
	return &protocol.Frame{Source: frame.Target, Target: frame.Source, Primary: frame.Primary, Secondary: frame.Secondary, Data: reply}, nil
}

// pause keeps the gateway's own pauses on the simulated clock.
func (s *controllerlessSite) pause(ctx context.Context, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	s.clock.advance(d)
	return true
}

func (s *controllerlessSite) eventsOf(kind string) []controllerlessEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []controllerlessEvent
	for _, event := range s.events {
		if event.kind == kind {
			out = append(out, event)
		}
	}
	return out
}

func (s *controllerlessSite) countByTarget(kind string, from time.Duration) map[byte]int {
	counts := make(map[byte]int)
	for _, event := range s.eventsOf(kind) {
		if event.at >= from {
			counts[event.target]++
		}
	}
	return counts
}

// controllerlessCatalogPart selects a ten-digit part number that the public
// product catalogue classifies with role and as a non-controller.
func controllerlessCatalogPart(t *testing.T, role string) string {
	t.Helper()
	return controllerlessCatalogPartWith(t, role, productids.ControllerNone)
}

// controllerlessCatalogPartWith selects a ten-digit part number that the
// public product catalogue classifies with role and capability.
func controllerlessCatalogPartWith(t *testing.T, role string, capability productids.ControllerCapability) string {
	t.Helper()
	catalog, err := productids.LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog() error = %v", err)
	}
	for _, record := range catalog.All {
		part := strings.TrimSpace(record.PartNumber)
		if !strings.EqualFold(strings.TrimSpace(record.Role), role) || len(part) != 10 || !isAllDigits(part) {
			continue
		}
		if catalog.ControllerCapability(part) == capability {
			return part
		}
	}
	t.Fatalf("product catalogue has no %q part number with capability %v", role, capability)
	return ""
}

// controllerlessSerialRaw builds the synthetic 28-character serial the 24..27
// windows return: zero date, plant and sequence fields around part.
func controllerlessSerialRaw(part string) string {
	return "000000" + part + "0000000000" + "N0"
}

// controllerlessSerial is the registry form of controllerlessSerialRaw.
func controllerlessSerial(part string) string {
	return "00-00-00-" + part + "-0000-000000-N0"
}

// controllerlessSilentTransport counts writes to the gateway's own protocol
// bus. Every frame of this scenario must take the recording bus instead.
type controllerlessSilentTransport struct {
	*transport.Loopback
	writes atomic.Int64
}

func (t *controllerlessSilentTransport) Write(payload []byte) (int, error) {
	t.writes.Add(1)
	return t.Loopback.Write(payload)
}

type controllerlessStartup struct {
	gateway             *ebusgateway.Gateway
	silent              *controllerlessSilentTransport
	cfg                 ebusgateway.Config
	passes              int
	activeProbePassed   bool
	admissionFailed     bool
	identityEnrichCalls int
	delayedRetryCalls   int
	barrierAt           time.Duration
}

// runControllerlessStartupScan runs the passive warm-up and the production
// startup scan loop in the add-on default admission (adapter-direct, automatic
// source with its companion) or with an explicitly configured source. The
// seams stay installed until the test ends so the poller's per-address
// identity probe reaches the same bus. observed lists target addresses the
// passive warm-up offered; without them the structural targets are probed.
func runControllerlessStartupScan(t *testing.T, site *controllerlessSite, explicitSource bool, observed ...byte) *controllerlessStartup {
	t.Helper()
	silent := &controllerlessSilentTransport{Loopback: transport.NewLoopback()}
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{Transport: silent})
	if err != nil {
		t.Fatalf("ebusgateway.New() error = %v", err)
	}
	busCtx, stopBus := context.WithCancel(context.Background())
	go gateway.Start(busCtx)
	t.Cleanup(func() {
		stopBus()
		if err := gateway.Close(); err != nil {
			t.Errorf("gateway.Close() error = %v", err)
		}
	})

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	// Wall-clock spacing of loop passes; the simulated clock charges the
	// production interval instead.
	productionScanInterval := cfg.ScanInterval
	cfg.ScanInterval = 10 * time.Millisecond
	cfg.TransportConfig = ebusgateway.TransportConfig{Protocol: ebusgateway.TransportAdapterDirect, Network: "tcp", Address: "adapter.invalid:9999"}
	cfg.ScanSource = controllerlessSource
	selection := protocol.SourceAddressSelection{Source: controllerlessSource}
	selection.Metrics.ObservedProbableTargets = observed
	if explicitSource {
		source := controllerlessSource
		cfg.StartupSource.Source = &source
	} else {
		cfg.StartupCompanionTarget = controllerlessCompanion
		selection.Companion = controllerlessCompanion
	}
	cfg.StartupProbeTargets = startupProbeTargetsForSelection(selection)

	startup := &controllerlessStartup{gateway: gateway, silent: silent, cfg: cfg}
	var mu sync.Mutex
	savedScan, savedDirected := registryScanFn, registryScanDirectedFn
	savedProbe, savedExit := startupScanB524ProbeFn, startupScanLoopExitFn
	savedIdentity, savedSerials := enrichVaillantIdentityFn, enrichSerialsFromEbusdFn
	savedB524Identity, savedDelayed := enrichB524InterfaceIdentityFn, postStartupIdentityRetryFn
	savedTargets, savedInfos := ebusdScanResultTargetsFn, ebusdScanResultInfosFn
	t.Cleanup(func() {
		registryScanFn, registryScanDirectedFn = savedScan, savedDirected
		startupScanB524ProbeFn, startupScanLoopExitFn = savedProbe, savedExit
		enrichVaillantIdentityFn, enrichSerialsFromEbusdFn = savedIdentity, savedSerials
		enrichB524InterfaceIdentityFn, postStartupIdentityRetryFn = savedB524Identity, savedDelayed
		ebusdScanResultTargetsFn, ebusdScanResultInfosFn = savedTargets, savedInfos
	})

	// The production directed scan runs unchanged; only the bus beneath the
	// loop's counting scan bus is the recording one.
	registryScanDirectedFn = func(ctx context.Context, scanBus registry.ScanBus, reg *registry.DeviceRegistry, source byte, targets []byte) ([]registry.DeviceEntry, error) {
		if stats, ok := scanBus.(*statsBus); ok {
			mu.Lock()
			startup.passes++
			pass := startup.passes
			mu.Unlock()
			if pass > 1 {
				site.clock.advance(productionScanInterval)
			}
			stats.bus = &timeoutBus{bus: site, timeout: cfg.ScanRequestTimeout}
			return registry.ScanDirected(ctx, stats, reg, source, targets)
		}
		return registry.ScanDirected(ctx, site, reg, source, targets)
	}
	registryScanFn = func(context.Context, registry.ScanBus, *registry.DeviceRegistry, byte, []byte) ([]registry.DeviceEntry, error) {
		t.Error("adapter-direct admission must use the directed scan")
		return nil, nil
	}
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		t.Error("ebusd scan result queried on an adapter-direct transport")
		return nil, nil
	}
	ebusdScanResultInfosFn = func(context.Context, ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		t.Error("ebusd scan result queried on an adapter-direct transport")
		return nil, nil
	}
	// The startup root check builds its own poller over the gateway bus; this
	// gives the same production probe the recording bus and simulated pauses.
	startupScanB524ProbeFn = func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
		probe := &vaillantSemanticPoller{bus: gateway.Bus, sendFrameFn: site.Send, pauseFn: site.pause,
			source: cfg.ScanSource, requestTimeout: cfg.SemanticRequestTimeout}
		return probe.probeB524Register(ctx, target, opcode, group, instance, addr)
	}
	// Identity enrichment re-reads the same 24..27 windows the scan already
	// read; with this fixture its outcome is the scan's, so only calls count.
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {
		mu.Lock()
		startup.identityEnrichCalls++
		mu.Unlock()
	}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {
		t.Error("ebusd serial enrichment on an adapter-direct transport")
	}
	enrichB524InterfaceIdentityFn = func(ctx context.Context, reg *registry.DeviceRegistry, _ registry.ScanBus, source, companion byte) {
		enrichB524InterfaceIdentity(ctx, reg, site, source, companion)
	}
	postStartupIdentityRetryFn = func(context.Context, *ebusgateway.Gateway, *graphql.Builder, ebusgateway.Config, *ebusgateway.TransportConfig) {
		mu.Lock()
		startup.delayedRetryCalls++
		mu.Unlock()
	}
	startupScanLoopExitFn = nil

	// Source selection listens passively before the first scan pass.
	site.clock.advance(ebusgateway.DefaultStartupAdmissionSourceSelectionConfig().ListenWarmup)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	signals := startDiscoveryScanLoop(ctx, cfg, gateway, nil)
	for _, wait := range []struct {
		name string
		ch   <-chan struct{}
	}{{"semantic bootstrap", signals.semanticBootstrapReady}, {"scan loop exit", signals.done}} {
		select {
		case <-wait.ch:
		case <-time.After(30 * time.Second):
			t.Fatalf("%s not signalled: the startup scan never finished on the controller-less bus", wait.name)
		}
	}
	select {
	case <-signals.activeProbePassed:
		startup.activeProbePassed = true
	default:
	}
	select {
	case <-signals.admissionFailed:
		startup.admissionFailed = true
	default:
	}
	startup.barrierAt = site.clock.elapsed()
	mu.Lock()
	defer mu.Unlock()
	return startup
}

type controllerlessRun struct {
	pollStart                time.Duration
	absence                  []controllerlessObservation
	capability               []controllerlessObservation
	reasons                  []controllerlessObservation
	recurringFamilies        int
	maxTasksPerBusyPeriod    int
	liveReadyAt, liveWarmAt  time.Duration
	degradedAt, capabilityAt time.Duration
	controllerAt             time.Duration
}

// runControllerlessPolling starts the production poller over the startup
// registry when the barrier opens and drives its production schedule on the
// simulated clock for horizon. A script entry runs at its polling-relative
// instant before the tickers due at the same instant.
func runControllerlessPolling(t *testing.T, site *controllerlessSite, startup *controllerlessStartup, horizon time.Duration, script map[time.Duration]func(*vaillantSemanticPoller)) *controllerlessRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	site.stopPolling = cancel
	clock := site.clock
	run := &controllerlessRun{pollStart: clock.elapsed(), liveReadyAt: -1, liveWarmAt: -1, degradedAt: -1, capabilityAt: -1, controllerAt: -1}

	poller := newVaillantSemanticPoller(startup.cfg, startup.gateway, site.provider, nil, nil)
	poller.nowFn = clock.Now
	poller.tasks.now = clock.Now
	poller.scheduler.SetClock(clock.Now)
	// The constructor's shadow cache, rebuilt with the simulated clock.
	shadow := ebusgateway.NewShadowCache(ebusgateway.ShadowCacheOptions{FeatureFlags: normalizeObserveFirstFeatureFlagsForPoller(startup.cfg), Now: clock.Now})
	poller.shadow = shadow
	poller.scheduler.SetShadowCache(shadow)
	poller.sendFrameFn = site.Send
	poller.pauseFn = site.pause

	observe := func() {
		at := clock.elapsed()
		site.observePhase(at)
		poller.mu.Lock()
		state, controller := poller.regAbsenceState, poller.controller
		poller.mu.Unlock()
		if controller != 0 && run.controllerAt < 0 {
			run.controllerAt = at
		}
		controllerlessNote(&run.absence, at, string(state))
		controllerlessNote(&run.capability, at, string(site.provider.VaillantRegulatorCapability()))
		controllerlessNote(&run.reasons, at, controllerlessReasonValue(site.provider))
	}

	schedule := poller.pollSchedule()
	run.recurringFamilies = len(schedule.recurring)
	// A disabled task (interval zero or negative) never ticks, as the
	// production loop returns without a ticker; -1 marks it.
	next := make([]time.Duration, len(schedule.recurring))
	for i, task := range schedule.recurring {
		next[i] = -1
		if task.interval > 0 {
			next[i] = run.pollStart + task.interval
		}
	}
	var stepTimes []time.Duration
	for at := range script {
		stepTimes = append(stepTimes, at)
	}
	sort.Slice(stepTimes, func(i, j int) bool { return stepTimes[i] < stepTimes[j] })

	observe()
	for _, task := range schedule.initial {
		poller.enqueueTask(task.key, task.priority, task.refresh)
	}
	end := run.pollStart + horizon
	queue := poller.tasks
	busy, sameInstant := 0, 0
	lastAt := clock.elapsed()
	for {
		now := clock.elapsed()
		for len(stepTimes) > 0 && run.pollStart+stepTimes[0] <= now {
			script[stepTimes[0]](poller)
			stepTimes = stepTimes[1:]
			observe()
		}
		// Production tickers enqueue on their own goroutines, also while a
		// task runs; the queue coalesces a family that is still pending.
		for i, task := range schedule.recurring {
			for next[i] >= 0 && next[i] <= now {
				poller.enqueueTask(task.key, task.priority, task.refresh)
				next[i] += task.interval
			}
		}
		queue.mu.Lock()
		pending := len(queue.queue)
		queue.mu.Unlock()
		if pending > 0 {
			task := queue.nextTask(ctx)
			if task == nil {
				t.Fatal("task queue stopped during the simulation")
			}
			task.run(ctx)
			queue.taskDone(task)
			observe()
			if ctx.Err() != nil {
				break
			}
			busy++
			if clock.elapsed() == lastAt {
				sameInstant++
				if sameInstant > controllerlessTaskLimit {
					t.Fatalf("tight loop: more than %d tasks without simulated time advancing at %s", controllerlessTaskLimit, lastAt)
				}
			} else {
				sameInstant, lastAt = 0, clock.elapsed()
			}
			continue
		}
		if busy > run.maxTasksPerBusyPeriod {
			run.maxTasksPerBusyPeriod = busy
		}
		busy = 0
		nextAt := end + 1
		for _, due := range next {
			if due >= 0 && due < nextAt {
				nextAt = due
			}
		}
		if len(stepTimes) > 0 && run.pollStart+stepTimes[0] < nextAt {
			nextAt = run.pollStart + stepTimes[0]
		}
		if !site.deadlinePassedNow() && site.deadline < nextAt {
			nextAt = site.deadline
		}
		if nextAt > end {
			break
		}
		clock.advanceTo(nextAt)
		lastAt = clock.elapsed()
		observe()
	}

	for _, phase := range site.phaseSeries() {
		switch graphql.SemanticStartupPhase(phase.value) {
		case graphql.SemanticStartupPhaseLiveWarmup:
			if run.liveWarmAt < 0 {
				run.liveWarmAt = phase.at
			}
		case graphql.SemanticStartupPhaseLiveReady:
			if run.liveReadyAt < 0 {
				run.liveReadyAt = phase.at
			}
		case graphql.SemanticStartupPhaseDegraded:
			if run.degradedAt < 0 {
				run.degradedAt = phase.at
			}
		}
	}
	for _, observation := range run.capability {
		if observation.value != string(graphql.VaillantRegulatorCapabilityUnknown) {
			run.capabilityAt = observation.at
			break
		}
	}
	return run
}

func (s *controllerlessSite) deadlinePassedNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deadlinePassed
}

// controllerlessAbsentMinute is the polling minute at which the absence state
// becomes ABSENT with default intervals: the first 60 s recheck enters
// ABSENCE_GRACE, the recheck five minutes after it enters ABSENT.
const controllerlessAbsentMinute = 6

// controllerlessDiscoveryPlan is the expected number of root probe calls per
// polling minute, each pass probing every candidate once; extra adds
// event-driven calls. One pass runs at polling start. Up to and including
// absentMinute, the state task (60 s, high priority) and the configuration
// task (5 min, medium priority) each run a fallback pass. At absentMinute the
// regulator recheck that enters ABSENT is a low-priority task, so the state
// task (and at a 5 min mark the configuration task) runs before it, still in
// ABSENCE_GRACE, and its pass happens. From the next minute on no controller is
// known and the state is ABSENT, so both fallbacks are skipped; the discovery
// task (10 min, low priority) runs its pass throughout.
func controllerlessDiscoveryPlan(candidates func(minute int) int, extra map[int]int, absentMinute int) map[int]int {
	plan := map[int]int{0: candidates(0)}
	for minute := 1; minute <= int(controllerlessHorizon/time.Minute); minute++ {
		passes := 0
		if minute <= absentMinute {
			passes++
			if minute%5 == 0 {
				passes++
			}
		}
		if minute%10 == 0 {
			passes++
		}
		if calls := passes*candidates(minute) + extra[minute]; calls > 0 {
			plan[minute] = calls
		}
	}
	return plan
}

// controllerlessProbeFrames returns the B524 frames sent after from and fails
// on any frame that is not the root probe's regulator-register read.
func controllerlessProbeFrames(t *testing.T, site *controllerlessSite, from time.Duration) []controllerlessEvent {
	t.Helper()
	probe := b524CapabilityProbes[0]
	want := buildB524ReadSelector(probe.opcode, probe.group, probe.instance, probe.addr)
	var frames []controllerlessEvent
	for _, event := range site.eventsOf(controllerlessB524Frame) {
		if event.at < from {
			continue
		}
		if !reflect.DeepEqual(event.data, want) {
			t.Errorf("B524 frame to 0x%02X at %s has selector % X; want only the root probe read % X", event.target, event.at, event.data, want)
		}
		frames = append(frames, event)
	}
	return frames
}

func controllerlessGraphQL(t *testing.T, provider *graphql.LiveSemanticProvider) map[string]any {
	t.Helper()
	builder := graphql.NewBuilder(nil, nil)
	builder.SetSemanticProvider(provider)
	schema, err := graphql.NewQuerySchema(builder)
	if err != nil {
		t.Fatalf("NewQuerySchema() error = %v", err)
	}
	result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: `{
		vaillant_regulator_capability
		zones { __typename }
		dhw { __typename }
		circuits { __typename }
		radio_devices { __typename }
		system { __typename }
		schedules { __typename }
		cylinders { __typename }
		fm5_semantic_mode
		solar { collector_temperature_c return_temperature_c pump_active current_yield pump_hours solar_enabled function_mode }
		energy_totals { gas { dhw { today today_meta { provenance } } } }
		boiler_status {
			state { water_pressure_bar flame_active flow_temperature_c dhw_temperature_c dhw_target_temperature_c dhw_temp_desired_c }
			config { flowset_hc_max_c dhw_operating_mode }
		}
	}`})
	if len(result.Errors) != 0 {
		t.Fatalf("GraphQL errors = %v", result.Errors)
	}
	return result.Data.(map[string]any)
}

func controllerlessMCPCapability(t *testing.T, provider *graphql.LiveSemanticProvider) any {
	t.Helper()
	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("mcp.NewServer() error = %v", err)
	}
	server.SetStatusProvider(newMCPRuntimeStatusProvider(provider, nil))
	return mcpCallToolEnvelope(t, server.Handler(), "ebus_v1_runtime_status_get", `{}`)["data"].(map[string]any)["vaillant_regulator_capability"]
}

// controllerlessMCPEnergy returns the data member of the MCP energy totals
// envelope, served through the gateway's semantic adapter.
func controllerlessMCPEnergy(t *testing.T, provider *graphql.LiveSemanticProvider) (any, bool) {
	t.Helper()
	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("mcp.NewServer() error = %v", err)
	}
	server.SetSemanticProvider(newMCPSemanticProvider(provider))
	data, present := mcpCallToolEnvelope(t, server.Handler(), "ebus_v1_semantic_energy_totals_get", `{}`)["data"]
	return data, present
}

// controllerlessStartupBlocks returns the bus observability startup block as
// GraphQL bus_summary and the MCP bus summary tool serve it, both fed by the
// gateway's startup surface of provider.
func controllerlessStartupBlocks(t *testing.T, provider *graphql.LiveSemanticProvider) (map[string]any, map[string]any) {
	t.Helper()
	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = true
	store := ebusgateway.NewBusObservabilityStore(cfg)
	if store == nil {
		t.Fatal("NewBusObservabilityStore() = nil")
	}
	store.SetStartupSurfaceProvider(func() *ebusgateway.BusObservabilityStartup { return semanticStartupSurface(provider) })

	builder := graphql.NewBuilder(nil, nil)
	builder.SetBusObservabilityProvider(newGraphQLBusObservabilityProvider(store))
	schema, err := graphql.NewQuerySchema(builder)
	if err != nil {
		t.Fatalf("NewQuerySchema() error = %v", err)
	}
	result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: `{ bus_summary { status { startup { phase cache_epoch live_epoch reason } } } }`})
	if len(result.Errors) != 0 {
		t.Fatalf("GraphQL errors = %v", result.Errors)
	}
	graphQLStartup, _ := controllerlessPath(result.Data.(map[string]any), "bus_summary", "status", "startup").(map[string]any)

	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("mcp.NewServer() error = %v", err)
	}
	server.SetBusObservabilityProvider(newMCPBusObservabilityProvider(store))
	envelope := mcpCallToolEnvelope(t, server.Handler(), mcpToolBusSummaryGet, `{}`)
	mcpStartup, _ := controllerlessPath(envelope, "data", "status", "startup").(map[string]any)
	return graphQLStartup, mcpStartup
}

// controllerlessExpectStartupBlock asserts phase and reason of the startup
// block in GraphQL (reason null when none applies) and MCP (reason omitted).
func controllerlessExpectStartupBlock(t *testing.T, provider *graphql.LiveSemanticProvider, wantPhase graphql.SemanticStartupPhase, wantReason string) {
	t.Helper()
	graphQLStartup, mcpStartup := controllerlessStartupBlocks(t, provider)
	if graphQLStartup == nil || mcpStartup == nil {
		t.Fatalf("startup block GraphQL %#v, MCP %#v; want both", graphQLStartup, mcpStartup)
	}
	var wantGraphQLReason any
	if wantReason != "" {
		wantGraphQLReason = wantReason
	}
	if got := graphQLStartup["phase"]; got != string(wantPhase) {
		t.Errorf("GraphQL startup.phase = %v; want %s", got, wantPhase)
	}
	if got := graphQLStartup["reason"]; got != wantGraphQLReason {
		t.Errorf("GraphQL startup.reason = %#v; want %#v", got, wantGraphQLReason)
	}
	if got := mcpStartup["phase"]; got != string(wantPhase) {
		t.Errorf("MCP startup.phase = %v; want %s", got, wantPhase)
	}
	got, present := mcpStartup["reason"]
	if wantReason == "" && present {
		t.Errorf("MCP startup.reason = %#v; want the member omitted", got)
	}
	if wantReason != "" && got != wantReason {
		t.Errorf("MCP startup.reason = %#v (present %v); want %q", got, present, wantReason)
	}
}

func controllerlessPath(data map[string]any, path ...string) any {
	var current any = data
	for _, key := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return fmt.Sprintf("<no object at %q>", key)
		}
		current = object[key]
	}
	return current
}

func controllerlessRegistryEntries(reg *registry.DeviceRegistry) map[byte]registry.DeviceEntrySnapshot {
	entries := make(map[byte]registry.DeviceEntrySnapshot)
	reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		entries[snap.PrimaryAddress] = snap
		return true
	})
	return entries
}

func controllerlessExpectSeries(t *testing.T, name string, got []controllerlessObservation, want []string) {
	t.Helper()
	values := make([]string, len(got))
	for i, observation := range got {
		values[i] = observation.value
	}
	if !reflect.DeepEqual(values, want) {
		t.Errorf("%s = %v; want %v", name, got, want)
	}
}

// controllerlessExpectProbes asserts the root discovery traffic after polling
// starts: per candidate and per polling minute, three production read attempts
// per probe call, and the first pass's candidate order.
func controllerlessExpectProbes(t *testing.T, site *controllerlessSite, run *controllerlessRun, wantCalls map[byte]int, wantPlan map[int]int) {
	t.Helper()
	frames := controllerlessProbeFrames(t, site, run.pollStart)
	byTarget := map[byte]int{}
	byMinute := map[int]int{}
	for _, frame := range frames {
		byTarget[frame.target]++
		byMinute[int((frame.at-run.pollStart)/time.Minute)]++
	}
	wantFrames := map[byte]int{}
	for target, calls := range wantCalls {
		wantFrames[target] = calls * controllerlessProbeAttempts
	}
	if !reflect.DeepEqual(byTarget, wantFrames) {
		t.Errorf("B524 root probe frames by target = %v; want %v (%d attempts per call, calls %v)", byTarget, wantFrames, controllerlessProbeAttempts, wantCalls)
	}
	wantByMinute := map[int]int{}
	for minute, calls := range wantPlan {
		wantByMinute[minute] = calls * controllerlessProbeAttempts
	}
	if !reflect.DeepEqual(byMinute, wantByMinute) {
		t.Errorf("B524 root probe frames per polling minute = %v; want %v", byMinute, wantByMinute)
	}
	order := []byte{controllerlessRegulator, controllerlessHeatSource, controllerlessModuleSlot}
	for i := 0; i < len(order)*controllerlessProbeAttempts && i < len(frames); i++ {
		if want := order[i/controllerlessProbeAttempts]; frames[i].target != want {
			t.Errorf("first discovery pass frame %d to 0x%02X; want 0x%02X (0x15 first, three attempts per candidate)", i, frames[i].target, want)
		}
	}
}

// controllerlessExpectCommon asserts the results every polling variant shares.
func controllerlessExpectCommon(t *testing.T, site *controllerlessSite, startup *controllerlessStartup, run *controllerlessRun, wantCapability string) map[string]any {
	t.Helper()
	for _, kind := range []string{controllerlessWriteFrame, controllerlessOtherFrame} {
		if got := site.countByTarget(kind, 0); len(got) != 0 {
			t.Errorf("%s frames = %v; want none on a controller-less bus", kind, got)
		}
	}
	if writes := startup.silent.writes.Load(); writes != 0 {
		t.Errorf("gateway protocol bus writes = %d; want 0 (every frame takes the recording bus)", writes)
	}
	for _, read := range site.eventsOf(controllerlessB509Read) {
		if read.target != controllerlessHeatSource {
			t.Errorf("B509 register read to 0x%02X at %s; want only the heat source", read.target, read.at)
		}
	}
	controllerlessExpectSeries(t, "absence states", run.absence, []string{string(regulatorAbsenceUnknown), string(regulatorAbsenceGrace), string(regulatorAbsent)})
	if len(run.absence) == 3 {
		grace, absent := run.absence[1].at-run.pollStart, run.absence[2].at-run.absence[1].at
		if grace < time.Minute || grace >= time.Minute+10*time.Second || absent < 5*time.Minute || absent >= 5*time.Minute+time.Minute {
			t.Errorf("ABSENCE_GRACE at %s after polling start and ABSENT %s after that; want the first 60 s recheck and the next recheck after the 5 min grace", grace, absent)
		}
	}
	if got := semanticRegulatorState.Value(); got != string(regulatorAbsent) {
		t.Errorf("semantic_regulator_state = %q; want %q", got, regulatorAbsent)
	}
	if run.maxTasksPerBusyPeriod > run.recurringFamilies {
		t.Errorf("most tasks run between idle periods = %d; want at most %d, every recurring family once", run.maxTasksPerBusyPeriod, run.recurringFamilies)
	}
	// The run sheet states 1.7 s after polling starts (wire timing) for the
	// first discovery pass.
	if at := run.capabilityAt - run.pollStart; run.capabilityAt < 0 || at < 1700*time.Millisecond || at >= 1750*time.Millisecond {
		t.Errorf("first published regulator capability at %s after polling start; want it from the first discovery pass, in [1.7s, 1.75s) at wire timing", at)
	}

	view := controllerlessGraphQL(t, site.provider)
	if got := view["vaillant_regulator_capability"]; got != wantCapability {
		t.Errorf("GraphQL vaillant_regulator_capability = %v; want %s", got, wantCapability)
	}
	if got := controllerlessMCPCapability(t, site.provider); got != wantCapability {
		t.Errorf("MCP vaillant_regulator_capability = %v; want %s", got, wantCapability)
	}
	for _, plane := range []string{"zones", "circuits", "radio_devices", "cylinders"} {
		if got, ok := view[plane].([]any); !ok || len(got) != 0 {
			t.Errorf("GraphQL %s = %#v; want an empty list", plane, view[plane])
		}
	}
	for _, plane := range []string{"dhw", "system", "schedules"} {
		if got := view[plane]; got != nil {
			t.Errorf("GraphQL %s = %#v; want null without a controller", plane, got)
		}
	}
	if got := view["fm5_semantic_mode"]; got != string(graphql.Fm5SemanticModeAbsent) {
		t.Errorf("GraphQL fm5_semantic_mode = %v; want %s", got, graphql.Fm5SemanticModeAbsent)
	}
	solar, ok := view["solar"].(map[string]any)
	if !ok {
		t.Errorf("GraphQL solar = %#v; want the absent-FM5 placeholder", view["solar"])
	}
	for field, value := range solar {
		if value != nil {
			t.Errorf("GraphQL solar.%s = %v; want null without a controller", field, value)
		}
	}
	// No energy point is ever accepted on this bus, so the energy plane is null
	// rather than a projection of 0/none placeholders.
	if got, present := view["energy_totals"]; !present || got != nil {
		t.Errorf("GraphQL energy_totals = %#v (present %v); want null before the first accepted point", got, present)
	}
	if got, present := controllerlessMCPEnergy(t, site.provider); !present || got != nil {
		t.Errorf("MCP energy totals data = %#v (present %v); want null before the first accepted point", got, present)
	}
	return view
}

// controllerlessReadSpacing returns the spacing between consecutive native
// reads of register after polling starts.
func controllerlessReadSpacing(site *controllerlessSite, run *controllerlessRun, register uint16) []time.Duration {
	var reads []time.Duration
	for _, event := range site.eventsOf(controllerlessB509Read) {
		if event.register == register && event.at >= run.pollStart {
			reads = append(reads, event.at)
		}
	}
	var spacing []time.Duration
	for i := 1; i < len(reads); i++ {
		spacing = append(spacing, reads[i]-reads[i-1])
	}
	return spacing
}

// controllerlessExpectBoilerStream asserts that the boiler stream carries the
// heat source's own B509 values, no controller-owned mirror field, and that
// each boiler tier re-reads its registers natively once the 30 s read cache
// has expired.
func controllerlessExpectBoilerStream(t *testing.T, site *controllerlessSite, run *controllerlessRun, view map[string]any) {
	t.Helper()
	pressure, _ := decodeDATA2b(controllerlessHeatSourceRegisters[boiler_b509_water_pressure])
	flowset, _ := decodeDATA2c(controllerlessHeatSourceRegisters[boiler_b509_flowset_hc_max_c])
	dhwDesired, _ := decodeDATA2c(controllerlessHeatSourceRegisters[boiler_b509_dhw_temp_desired_c])
	for path, want := range map[string]any{
		"state.water_pressure_bar":       pressure,
		"state.flame_active":             false,
		"state.dhw_temp_desired_c":       dhwDesired,
		"config.flowset_hc_max_c":        flowset,
		"state.flow_temperature_c":       nil, // Hypothesis mapping: no qualified binding in production
		"state.dhw_temperature_c":        nil, // controller DHW mirror
		"state.dhw_target_temperature_c": nil, // controller DHW mirror
		"config.dhw_operating_mode":      nil, // controller DHW mirror
	} {
		got := controllerlessPath(view, append([]string{"boiler_status"}, strings.Split(path, ".")...)...)
		if got != want {
			t.Errorf("boiler_status.%s = %v; want %v", path, got, want)
		}
	}
	// The fast tier ticks every 30 s, but on the half-minute tick its values
	// are at most 30 s old and served from the read cache; the minute tick
	// finds them expired. A tier's read sits behind whatever discovery passes
	// run ahead of it at that instant, so the one gap after the fallback passes
	// stop (ABSENT) is a few seconds shorter than the tier interval.
	for _, tier := range []struct {
		name     string
		register uint16
		reads    int
		spacing  time.Duration
	}{
		{"fast", boiler_b509_water_pressure, 31, time.Minute},
		{"medium", boiler_b509_flowset_hc_max_c, 7, 5 * time.Minute},
		{"slow", boiler_b509_central_heating_hours, 4, 10 * time.Minute},
	} {
		spacing := controllerlessReadSpacing(site, run, tier.register)
		if len(spacing) > 0 {
			t.Logf("%s-tier register 0x%04X: %d native reads, first spacing %s", tier.name, tier.register, len(spacing)+1, spacing[0])
		}
		if len(spacing)+1 != tier.reads {
			t.Errorf("%s-tier register 0x%04X read %d times in %s; want %d native reads", tier.name, tier.register, len(spacing)+1, controllerlessHorizon, tier.reads)
		}
		for _, gap := range spacing {
			if gap <= tier.spacing-10*time.Second || gap >= tier.spacing+10*time.Second {
				t.Errorf("%s-tier register 0x%04X reads spaced %v; want every %s", tier.name, tier.register, spacing, tier.spacing)
				break
			}
		}
	}
}

func TestControllerlessSiteStartupWithoutController(t *testing.T) {
	boilerPart := controllerlessCatalogPart(t, "Boiler")
	regulatorPart := controllerlessCatalogPartWith(t, "Regulator", productids.ControllerPresent)
	for _, tc := range []struct {
		name            string
		explicitSource  bool
		atController    *controllerlessDevice // identity answering at the controller address
		wantPasses      int
		wantActive      bool
		wantFailed      bool
		wantIdentify    map[byte]int
		wantSerialReads map[byte]int
		wantProbeFrames map[byte]int
		// B524 interface identity reads (selectors 00 06 00 and 00 07 00) that
		// startup enrichment sends to an identity at a regulator address.
		wantInterfaceReads map[byte]int
		wantDelayedRetries int
	}{
		{
			// Source selection admits on the first pass on which any device
			// answers; the silent controller and module-slot addresses are
			// retried by the registry scan three more times within that pass.
			name: "automatic source", wantPasses: 1, wantActive: true,
			wantIdentify:    map[byte]int{controllerlessHeatSource: 1, controllerlessRegulator: 4, controllerlessModuleSlot: 4},
			wantSerialReads: map[byte]int{controllerlessHeatSource: 4},
			wantProbeFrames: map[byte]int{controllerlessHeatSource: 1 * controllerlessProbeAttempts},
		},
		{
			// Without source selection the loop waits for a coherent B524 root.
			// The first root confirmation finds none and the catalog classifies
			// the only Vaillant identity as a non-controller, so active
			// confirmation admits on that pass, as with the automatic source.
			name: "explicit source", explicitSource: true, wantPasses: 1, wantActive: true,
			wantIdentify:    map[byte]int{controllerlessHeatSource: 1, controllerlessRegulator: 4, controllerlessModuleSlot: 4},
			wantSerialReads: map[byte]int{controllerlessHeatSource: 4},
			wantProbeFrames: map[byte]int{controllerlessHeatSource: 1 * controllerlessProbeAttempts},
		},
		{
			// An identity at the controller address whose serial cannot be read
			// is unclassified: root confirmation stays required, the loop gives
			// up after two passes and reports admission failure while still
			// releasing semantic polling. The missing serial also schedules the
			// delayed identity retry.
			name: "explicit source, unclassified identity", explicitSource: true,
			atController: &controllerlessDevice{deviceID: "CTL00"},
			wantPasses:   2, wantFailed: true,
			wantIdentify:       map[byte]int{controllerlessHeatSource: 2, controllerlessRegulator: 2, controllerlessModuleSlot: 8},
			wantSerialReads:    map[byte]int{controllerlessHeatSource: 8, controllerlessRegulator: 2},
			wantProbeFrames:    map[byte]int{controllerlessHeatSource: 2 * controllerlessProbeAttempts, controllerlessRegulator: 2 * controllerlessProbeAttempts},
			wantInterfaceReads: map[byte]int{controllerlessRegulator: 4},
			wantDelayedRetries: 1,
		},
		{
			// A catalogued regulator identity without a B524 root keeps root
			// confirmation required: two passes, then admission failure.
			name: "explicit source, regulator identity", explicitSource: true,
			atController: &controllerlessDevice{deviceID: "CTL00", serialRaw: controllerlessSerialRaw(regulatorPart)},
			wantPasses:   2, wantFailed: true,
			wantIdentify:       map[byte]int{controllerlessHeatSource: 2, controllerlessRegulator: 2, controllerlessModuleSlot: 8},
			wantSerialReads:    map[byte]int{controllerlessHeatSource: 8, controllerlessRegulator: 8},
			wantProbeFrames:    map[byte]int{controllerlessHeatSource: 2 * controllerlessProbeAttempts, controllerlessRegulator: 2 * controllerlessProbeAttempts},
			wantInterfaceReads: map[byte]int{controllerlessRegulator: 4},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			site := newControllerlessSite(t, controllerlessWireTiming)
			site.addDevice(controllerlessHeatSource, controllerlessDevice{deviceID: "BAI00", serialRaw: controllerlessSerialRaw(boilerPart), answersB509: true})
			if tc.atController != nil {
				site.addDevice(controllerlessRegulator, *tc.atController)
			}
			startup := runControllerlessStartupScan(t, site, tc.explicitSource)

			if startup.passes != tc.wantPasses {
				t.Errorf("scan passes before semantic polling = %d; want %d", startup.passes, tc.wantPasses)
			}
			if startup.activeProbePassed != tc.wantActive || startup.admissionFailed != tc.wantFailed {
				t.Errorf("active probe passed = %v, admission failed = %v; want %v, %v", startup.activeProbePassed, startup.admissionFailed, tc.wantActive, tc.wantFailed)
			}
			for kind, want := range map[string]map[byte]int{
				controllerlessIdentify:     tc.wantIdentify,
				controllerlessSerialWindow: tc.wantSerialReads,
			} {
				if got := site.countByTarget(kind, 0); !reflect.DeepEqual(got, want) {
					t.Errorf("%s attempts by target = %v; want %v", kind, got, want)
				}
			}
			probe := b524CapabilityProbes[0]
			rootProbe := buildB524ReadSelector(probe.opcode, probe.group, probe.instance, probe.addr)
			probeFrames, interfaceReads := map[byte]int{}, map[byte]int{}
			for _, event := range site.eventsOf(controllerlessB524Frame) {
				switch {
				case reflect.DeepEqual(event.data, rootProbe):
					probeFrames[event.target]++
				case reflect.DeepEqual(event.data, []byte{0x00, 0x06, 0x00}), reflect.DeepEqual(event.data, []byte{0x00, 0x07, 0x00}):
					interfaceReads[event.target]++
				default:
					t.Errorf("B524 frame to 0x%02X at %s has selector % X; want the root probe read or an interface identity read", event.target, event.at, event.data)
				}
			}
			if !reflect.DeepEqual(probeFrames, tc.wantProbeFrames) {
				t.Errorf("startup root confirmation frames by target = %v; want %v", probeFrames, tc.wantProbeFrames)
			}
			wantInterfaceReads := tc.wantInterfaceReads
			if wantInterfaceReads == nil {
				wantInterfaceReads = map[byte]int{}
			}
			if !reflect.DeepEqual(interfaceReads, wantInterfaceReads) {
				t.Errorf("startup interface identity reads by target = %v; want %v", interfaceReads, wantInterfaceReads)
			}
			for _, kind := range []string{controllerlessB509Read, controllerlessWriteFrame, controllerlessOtherFrame} {
				if got := site.countByTarget(kind, 0); len(got) != 0 {
					t.Errorf("%s during startup = %v; want none", kind, got)
				}
			}
			if startup.identityEnrichCalls != tc.wantPasses || startup.delayedRetryCalls != tc.wantDelayedRetries {
				t.Errorf("identity enrichment calls = %d, delayed retries = %d; want %d, %d", startup.identityEnrichCalls, startup.delayedRetryCalls, tc.wantPasses, tc.wantDelayedRetries)
			}
			if writes := startup.silent.writes.Load(); writes != 0 {
				t.Errorf("gateway protocol bus writes = %d; want 0", writes)
			}
			entries := controllerlessRegistryEntries(startup.gateway.Registry)
			heatSource, ok := entries[controllerlessHeatSource]
			wantEntries := 1
			if tc.atController != nil {
				wantEntries = 2
				if _, found := entries[controllerlessRegulator]; !found {
					t.Errorf("registry = %v; want the identity at 0x%02X", entries, controllerlessRegulator)
				}
			}
			if len(entries) != wantEntries || !ok {
				t.Fatalf("registry = %v; want the heat source and %d other entries", entries, wantEntries-1)
			}
			if !reflect.DeepEqual(heatSource.Addresses, []byte{controllerlessHeatSource, controllerlessHeatSourceInit}) ||
				heatSource.Manufacturer != "Vaillant" || heatSource.DeviceID != "BAI00" || heatSource.SerialNumber != controllerlessSerial(boilerPart) {
				t.Errorf("heat source entry = addresses %v, %q, %q, %q; want [8 3], Vaillant, BAI00, the synthetic serial",
					heatSource.Addresses, heatSource.Manufacturer, heatSource.DeviceID, heatSource.SerialNumber)
			}
			t.Logf("semantic barrier opens %s after process start (%s)", startup.barrierAt, site.timing)
		})
	}
}

// TestControllerlessSiteReadinessAgainstBootDeadline places LIVE_READY on the
// process timeline for a boiler-class heat source in both admissions and both
// timing profiles. The boot-live deadline counts from process start.
func TestControllerlessSiteReadinessAgainstBootDeadline(t *testing.T) {
	boilerPart := controllerlessCatalogPart(t, "Boiler")
	for _, tc := range []struct {
		name           string
		explicitSource bool
		unclassified   bool // an identity without a readable serial answers at the controller address
		timing         controllerlessTiming
		wantDegraded   bool
	}{
		{name: "automatic source, wire timing", timing: controllerlessWireTiming},
		{name: "automatic source, timeout timing", timing: controllerlessTimeoutTiming},
		{name: "explicit source, wire timing", explicitSource: true, timing: controllerlessWireTiming},
		// The explicit source admits on the first pass like the automatic one,
		// so one 5 s-per-attempt root confirmation precedes polling.
		{name: "explicit source, timeout timing", explicitSource: true, timing: controllerlessTimeoutTiming},
		// An unclassified identity keeps root confirmation required: two scan
		// passes with their 30 s interval and two 5 s-per-attempt root
		// confirmations push the first boiler reads past the 120 s deadline.
		{name: "explicit source, unclassified identity, timeout timing", explicitSource: true, unclassified: true, timing: controllerlessTimeoutTiming, wantDegraded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			site := newControllerlessSite(t, tc.timing)
			site.addDevice(controllerlessHeatSource, controllerlessDevice{deviceID: "BAI00", serialRaw: controllerlessSerialRaw(boilerPart), answersB509: true})
			if tc.unclassified {
				site.addDevice(controllerlessRegulator, controllerlessDevice{deviceID: "CTL00"})
			}
			startup := runControllerlessStartupScan(t, site, tc.explicitSource)
			run := runControllerlessPolling(t, site, startup, 3*time.Minute, nil)

			want := []string{string(graphql.SemanticStartupPhaseBootInit), string(graphql.SemanticStartupPhaseLiveWarmup), string(graphql.SemanticStartupPhaseLiveReady)}
			if tc.wantDegraded {
				want = []string{string(graphql.SemanticStartupPhaseBootInit), string(graphql.SemanticStartupPhaseDegraded),
					string(graphql.SemanticStartupPhaseLiveWarmup), string(graphql.SemanticStartupPhaseLiveReady)}
			}
			phases := site.phaseSeries()
			controllerlessExpectSeries(t, "startup phases", phases, want)
			if run.liveReadyAt < 0 {
				t.Fatalf("LIVE_READY never reached; phases %v", phases)
			}
			if before := run.liveReadyAt < site.deadline; before == tc.wantDegraded {
				t.Errorf("LIVE_READY at %s, boot-live deadline at %s; want LIVE_READY before the deadline = %v", run.liveReadyAt, site.deadline, !tc.wantDegraded)
			}
			if tc.wantDegraded && run.degradedAt != site.deadline {
				t.Errorf("DEGRADED at %s; want at the deadline %s", run.degradedAt, site.deadline)
			}
			if tc.wantDegraded {
				if got, want := site.degradedTransition(), `reason="boot_live_timeout elapsed=1ns"`; !strings.Contains(got, want) {
					t.Errorf("DEGRADED transition = %q; want it to carry %s and no startup reason", got, want)
				}
			}
			controllerlessExpectSeries(t, "startup reason", run.reasons, []string{"none"})
			t.Logf("%s: barrier %s, polling start %s, LIVE_WARMUP %s, LIVE_READY %s, deadline %s, margin %s",
				tc.name, startup.barrierAt, run.pollStart, run.liveWarmAt, run.liveReadyAt, site.deadline, site.deadline-run.liveReadyAt)
		})
	}
}

func TestControllerlessSitePollingOverSimulatedTime(t *testing.T) {
	boilerPart := controllerlessCatalogPart(t, "Boiler")
	heatPumpPart := controllerlessCatalogPart(t, "Heat Pump")
	modulePart := controllerlessCatalogPart(t, "Gateway")
	// A catalogued part of neither heat-source class: with an unknown device
	// ID the heat source's class is unknown.
	unknownClassPart := controllerlessCatalogPart(t, "Module")
	threeCandidates := func(int) int { return 3 }
	moduleFromMinuteThree := func(minute int) int {
		if minute < 3 {
			return 3
		}
		return 4
	}
	liveReady := []string{string(graphql.SemanticStartupPhaseBootInit), string(graphql.SemanticStartupPhaseLiveWarmup), string(graphql.SemanticStartupPhaseLiveReady)}
	noneFromFirstDiscovery := []string{string(graphql.VaillantRegulatorCapabilityUnknown), string(graphql.VaillantRegulatorCapabilityNone)}
	noReason := []string{"none"}
	reasonFromFirstDiscovery := []string{"none", graphql.SemanticStartupReasonNoSupportedHeatSourcePath}

	for _, tc := range []struct {
		name           string
		heatSourceID   string
		heatSourcePart string
		boilerClass    bool
		// moduleSerial is used when the internet module is identified at two
		// minutes of polling; "-" identifies it without a readable serial.
		moduleSerial   string
		wantPhases     []string
		wantCapability []string
		wantReasons    []string // startup reason series, "none" when none applies
		wantCalls      map[byte]int
		wantPlan       map[int]int
	}{
		{
			name: "boiler-class heat source", heatSourceID: "BAI00", heatSourcePart: boilerPart, boilerClass: true,
			wantPhases: liveReady, wantCapability: noneFromFirstDiscovery, wantReasons: noReason,
			// 1 start + 6 state (minutes 1-6) + 1 config (minute 5) + 3
			// discovery (minutes 10, 20, 30) passes, three candidates each.
			wantCalls: map[byte]int{controllerlessRegulator: 11, controllerlessHeatSource: 11, controllerlessModuleSlot: 11},
			wantPlan:  controllerlessDiscoveryPlan(threeCandidates, nil, controllerlessAbsentMinute),
		},
		{
			// The module joins the candidates; the inventory change at its
			// identification adds one discovery pass at two minutes. The module
			// gets the inventory pass, the state passes of minutes 3-6, the
			// config pass of minute 5 and the three discovery-tick passes.
			name: "boiler-class heat source and internet module", heatSourceID: "BAI00", heatSourcePart: boilerPart, boilerClass: true,
			moduleSerial: controllerlessSerialRaw(modulePart),
			wantPhases:   liveReady, wantCapability: noneFromFirstDiscovery, wantReasons: noReason,
			wantCalls: map[byte]int{controllerlessRegulator: 12, controllerlessHeatSource: 12, controllerlessModuleSlot: 12, controllerlessInternetModule: 9},
			wantPlan:  controllerlessDiscoveryPlan(moduleFromMinuteThree, map[int]int{2: 4}, controllerlessAbsentMinute),
		},
		{
			// An identified Vaillant device without a catalogued serial keeps
			// the public result UNKNOWN; the absence state still settles.
			name: "boiler-class heat source and internet module without serial", heatSourceID: "BAI00", heatSourcePart: boilerPart, boilerClass: true,
			moduleSerial: "-",
			wantPhases:   liveReady, wantReasons: noReason,
			wantCapability: append(append([]string(nil), noneFromFirstDiscovery...),
				string(graphql.VaillantRegulatorCapabilityUnknown)),
			wantCalls: map[byte]int{controllerlessRegulator: 12, controllerlessHeatSource: 12, controllerlessModuleSlot: 12, controllerlessInternetModule: 9},
			wantPlan:  controllerlessDiscoveryPlan(moduleFromMinuteThree, map[int]int{2: 4}, controllerlessAbsentMinute),
		},
		{
			// The poller reads boiler status only from boiler-class identities;
			// a heat-pump-class heat source therefore publishes no live stream.
			// The first discovery pass names the cause on the startup surface.
			name: "heat-pump-class heat source", heatSourceID: "EHP00", heatSourcePart: heatPumpPart,
			wantPhases:     []string{string(graphql.SemanticStartupPhaseBootInit), string(graphql.SemanticStartupPhaseDegraded)},
			wantCapability: noneFromFirstDiscovery, wantReasons: reasonFromFirstDiscovery,
			wantCalls: map[byte]int{controllerlessRegulator: 11, controllerlessHeatSource: 11, controllerlessModuleSlot: 11},
			wantPlan:  controllerlessDiscoveryPlan(threeCandidates, nil, controllerlessAbsentMinute),
		},
		{
			// A heat source of unknown class is not treated as heat-pump class:
			// no reason, and the same DEGRADED as before.
			name: "heat source of unknown class", heatSourceID: "XYZ00", heatSourcePart: unknownClassPart,
			wantPhases:     []string{string(graphql.SemanticStartupPhaseBootInit), string(graphql.SemanticStartupPhaseDegraded)},
			wantCapability: noneFromFirstDiscovery, wantReasons: noReason,
			wantCalls: map[byte]int{controllerlessRegulator: 11, controllerlessHeatSource: 11, controllerlessModuleSlot: 11},
			wantPlan:  controllerlessDiscoveryPlan(threeCandidates, nil, controllerlessAbsentMinute),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			site := newControllerlessSite(t, controllerlessWireTiming)
			site.addDevice(controllerlessHeatSource, controllerlessDevice{deviceID: tc.heatSourceID, serialRaw: controllerlessSerialRaw(tc.heatSourcePart), answersB509: tc.boilerClass})
			startup := runControllerlessStartupScan(t, site, false)
			if startup.passes != 1 || !startup.activeProbePassed {
				t.Fatalf("startup passes = %d, active probe passed = %v; want 1, true", startup.passes, startup.activeProbePassed)
			}

			var script map[time.Duration]func(*vaillantSemanticPoller)
			if tc.moduleSerial != "" {
				serial := tc.moduleSerial
				if serial == "-" {
					serial = ""
				}
				script = map[time.Duration]func(*vaillantSemanticPoller){2 * time.Minute: func(p *vaillantSemanticPoller) {
					site.addDevice(controllerlessInternetModule, controllerlessDevice{deviceID: "NETX3", serialRaw: serial})
					p.EnqueueAddressIdentityProbe(controllerlessInternetModule)
				}}
			}
			run := runControllerlessPolling(t, site, startup, controllerlessHorizon, script)

			controllerlessExpectSeries(t, "startup phases", site.phaseSeries(), tc.wantPhases)
			if tc.boilerClass && (run.liveReadyAt < 0 || run.liveReadyAt >= site.deadline) {
				t.Errorf("LIVE_READY at %s; want before the boot-live deadline %s", run.liveReadyAt, site.deadline)
			}
			if !tc.boilerClass && run.degradedAt != site.deadline {
				t.Errorf("DEGRADED at %s; want at the boot-live deadline %s", run.degradedAt, site.deadline)
			}
			controllerlessExpectSeries(t, "regulator capability", run.capability, tc.wantCapability)
			controllerlessExpectSeries(t, "startup reason", run.reasons, tc.wantReasons)
			wantReason := ""
			if len(tc.wantReasons) > 1 {
				wantReason = tc.wantReasons[len(tc.wantReasons)-1]
				// Set by the same first discovery pass that publishes the
				// regulator capability (pinned in controllerlessExpectCommon).
				if at := run.reasons[1].at; at != run.capabilityAt {
					t.Errorf("startup reason set %s after polling start, capability published %s after; want both from the first discovery pass", at-run.pollStart, run.capabilityAt-run.pollStart)
				}
			}
			wantPhase := graphql.SemanticStartupPhaseLiveReady
			if !tc.boilerClass {
				wantPhase = graphql.SemanticStartupPhaseDegraded
				want := `reason="boot_live_timeout elapsed=1ns"`
				if wantReason != "" {
					want = `reason="boot_live_timeout elapsed=1ns startup_reason=` + wantReason + `"`
				}
				if got := site.degradedTransition(); !strings.Contains(got, want) {
					t.Errorf("DEGRADED transition = %q; want it to carry %s", got, want)
				}
			}
			controllerlessExpectStartupBlock(t, site.provider, wantPhase, wantReason)
			controllerlessExpectProbes(t, site, run, tc.wantCalls, tc.wantPlan)
			view := controllerlessExpectCommon(t, site, startup, run, tc.wantCapability[len(tc.wantCapability)-1])

			if tc.boilerClass {
				controllerlessExpectBoilerStream(t, site, run, view)
			} else {
				if got := view["boiler_status"]; got != nil {
					t.Errorf("GraphQL boiler_status = %#v; want null for a heat source that is not boiler class", got)
				}
				if got := site.countByTarget(controllerlessB509Read, 0); len(got) != 0 {
					t.Errorf("B509 register reads = %v; want none for a heat source that is not boiler class", got)
				}
			}

			if tc.moduleSerial != "" {
				wantSerialWindows := 4
				wantSerial := controllerlessSerial(modulePart)
				if tc.moduleSerial == "-" {
					wantSerialWindows, wantSerial = 1, ""
				}
				module, ok := controllerlessRegistryEntries(startup.gateway.Registry)[controllerlessInternetModule]
				if !ok || module.Manufacturer != "Vaillant" || module.DeviceID != "NETX3" || module.SerialNumber != wantSerial {
					t.Errorf("internet module entry = %+v (present %v); want Vaillant NETX3 with serial %q", module, ok, wantSerial)
				}
				if got := site.countByTarget(controllerlessIdentify, 0)[controllerlessInternetModule]; got != 1 {
					t.Errorf("internet module identification attempts = %d; want 1", got)
				}
				if got := site.countByTarget(controllerlessSerialWindow, 0)[controllerlessInternetModule]; got != wantSerialWindows {
					t.Errorf("internet module serial window reads = %d; want %d", got, wantSerialWindows)
				}
			}
			t.Logf("%s: polling start %s, LIVE_READY %s, capability published %s after polling start, absence %v",
				tc.name, run.pollStart, run.liveReadyAt, run.capabilityAt-run.pollStart, run.absence)
		})
	}
}

// TestControllerlessSiteControllerAppearsAfterAbsent shows how a controller
// that becomes reachable after polling started is found. The synthetic bus
// never answers B524 and must not act as a controller, so the only seam left
// is the root probe of 0x15 from the appearance on: it reports a coherent root
// there and hands every other probe to the production probe over the
// recording bus. No frame answers as a controller. The simulation stops at the
// first frame the poller addresses to the found controller other than a root
// probe, which follows the successful discovery pass without simulated delay.
func TestControllerlessSiteControllerAppearsAfterAbsent(t *testing.T) {
	boilerPart := controllerlessCatalogPart(t, "Boiler")
	regulatorPart := controllerlessCatalogPartWith(t, "Regulator", productids.ControllerPresent)
	afterAbsent := 12*time.Minute + 30*time.Second
	for _, tc := range []struct {
		name     string
		appear   time.Duration // polling-relative
		registry bool          // the registry gains a catalogue-classified regulator identity
		enqueue  bool          // a discovery refresh is enqueued (passive insertion or promotion)
		// noPeriodic disables the periodic discovery task (interval zero).
		noPeriodic bool
		wantState  regulatorAbsenceState
		// The find falls in [wantFrom, wantFrom+10 s) of polling time.
		wantFrom time.Duration
	}{
		// The next regulator recheck (13:00) sees the inventory change and
		// enqueues a discovery pass.
		{name: "registry gains a regulator identity", appear: afterAbsent, registry: true, wantState: regulatorAbsent, wantFrom: 13 * time.Minute},
		// An enqueued refresh runs at once.
		{name: "discovery refresh enqueued", appear: afterAbsent, enqueue: true, wantState: regulatorAbsent, wantFrom: afterAbsent},
		// No trigger: the 15:00 configuration tick and the state ticks skip
		// their fallback; the 20:00 discovery tick finds it.
		{name: "no event", appear: afterAbsent, wantState: regulatorAbsent, wantFrom: 20 * time.Minute},
		// With the periodic discovery task disabled the fallback is never
		// suspended, so the next state tick (13:00) finds it.
		{name: "no event, periodic discovery disabled", appear: afterAbsent, noPeriodic: true, wantState: regulatorAbsent, wantFrom: 13 * time.Minute},
		// Before ABSENT the state tick's fallback still finds it within a minute.
		{name: "no event before ABSENT", appear: 3*time.Minute + 30*time.Second, wantState: regulatorAbsenceGrace, wantFrom: 4 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			site := newControllerlessSite(t, controllerlessWireTiming)
			site.addDevice(controllerlessHeatSource, controllerlessDevice{deviceID: "BAI00", serialRaw: controllerlessSerialRaw(boilerPart), answersB509: true})
			startup := runControllerlessStartupScan(t, site, false)

			rootProbe := b524CapabilityProbes[0]
			rootSelector := buildB524ReadSelector(rootProbe.opcode, rootProbe.group, rootProbe.instance, rootProbe.addr)
			appeared, found := time.Duration(-1), time.Duration(-1)
			var seamCalls []time.Duration
			var stateAtAppear regulatorAbsenceState
			site.onFrame = func(event controllerlessEvent) {
				if appeared < 0 || found >= 0 || event.target != controllerlessRegulator {
					return
				}
				if event.kind == controllerlessB524Frame && reflect.DeepEqual(event.data, rootSelector) {
					return
				}
				found = event.at
				site.stopPolling()
			}
			script := map[time.Duration]func(*vaillantSemanticPoller){tc.appear: func(p *vaillantSemanticPoller) {
				appeared = site.clock.elapsed()
				p.mu.Lock()
				stateAtAppear = p.regAbsenceState
				p.mu.Unlock()
				p.b524ProbeFn = func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
					if target == controllerlessRegulator {
						seamCalls = append(seamCalls, site.clock.elapsed())
						return true
					}
					return p.probeB524Register(ctx, target, opcode, group, instance, addr)
				}
				if tc.registry {
					startup.gateway.Registry.Register(registry.DeviceInfo{Address: controllerlessRegulator, Manufacturer: "Vaillant", SerialNumber: controllerlessSerial(regulatorPart)})
				}
				if tc.enqueue {
					p.EnqueueDiscoveryRefresh()
				}
			}}
			if tc.noPeriodic {
				startup.cfg.SemanticDiscoveryInterval = 0
			}
			run := runControllerlessPolling(t, site, startup, tc.wantFrom+time.Minute, script)

			if stateAtAppear != tc.wantState {
				t.Errorf("absence state when the controller appears = %s; want %s", stateAtAppear, tc.wantState)
			}
			if found < 0 || run.controllerAt < 0 {
				t.Fatalf("controller appearing at %s was not found by %s of polling", tc.appear, tc.wantFrom+time.Minute)
			}
			if at := found - run.pollStart; at < tc.wantFrom || at >= tc.wantFrom+10*time.Second {
				t.Errorf("controller found at %s of polling; want in [%s, %s)", at, tc.wantFrom, tc.wantFrom+10*time.Second)
			}
			for _, at := range seamCalls {
				if at-run.pollStart < tc.wantFrom {
					t.Errorf("root probe of 0x15 at %s of polling, before the expected find at %s; the suspended fallback must not probe", at-run.pollStart, tc.wantFrom)
				}
			}
			if got := site.countByTarget(controllerlessWriteFrame, 0); len(got) != 0 {
				t.Errorf("write frames = %v; want none", got)
			}
			t.Logf("%s: appears at %s, found at %s of polling", tc.name, tc.appear, found-run.pollStart)
		})
	}
}

// TestControllerlessSiteHeatPumpClassUnitAdoptedAsRoot pins that root
// discovery is unchanged for a heat-pump-class heat source: a unit at 0x08
// that answers the root probe coherently is adopted as semantic root, and with
// a root the startup reason is clear. The synthetic bus never answers B524 and
// must not act as a controller, so the root probe of 0x08 is the only seam
// from the appearance on: it reports a coherent root there and hands every
// other probe to the production probe over the recording bus. No frame
// answers as a controller. The simulation stops at the first frame the poller
// addresses to the adopted root other than a root probe.
func TestControllerlessSiteHeatPumpClassUnitAdoptedAsRoot(t *testing.T) {
	heatPumpPart := controllerlessCatalogPart(t, "Heat Pump")
	reason := graphql.SemanticStartupReasonNoSupportedHeatSourcePath
	for _, tc := range []struct {
		name   string
		appear time.Duration // polling-relative
		// The adoption falls in [wantFrom, wantFrom+10 s) of polling time.
		wantFrom     time.Duration
		wantAtAppear string // startup reason when the unit starts answering
		wantReasons  []string
		wantDegraded bool // the boot-live deadline passes before the adoption
	}{
		// The first discovery pass adopts it: the reason is never set.
		{name: "answers from polling start", wantAtAppear: "none", wantReasons: []string{"none"}},
		// The first pass sets the reason, the boot-live deadline moves the
		// phase to DEGRADED, and the state tick's pass at 4:00 (before ABSENT)
		// adopts the root and clears the reason.
		{name: "answers after the boot-live deadline", appear: 3*time.Minute + 30*time.Second, wantFrom: 4 * time.Minute,
			wantAtAppear: reason, wantReasons: []string{"none", reason, "none"}, wantDegraded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			site := newControllerlessSite(t, controllerlessWireTiming)
			site.addDevice(controllerlessHeatSource, controllerlessDevice{deviceID: "EHP00", serialRaw: controllerlessSerialRaw(heatPumpPart)})
			startup := runControllerlessStartupScan(t, site, false)

			rootProbe := b524CapabilityProbes[0]
			rootSelector := buildB524ReadSelector(rootProbe.opcode, rootProbe.group, rootProbe.instance, rootProbe.addr)
			appeared, found := time.Duration(-1), time.Duration(-1)
			site.onFrame = func(event controllerlessEvent) {
				if appeared < 0 || found >= 0 || event.target != controllerlessHeatSource {
					return
				}
				if event.kind == controllerlessB524Frame && reflect.DeepEqual(event.data, rootSelector) {
					return
				}
				found = event.at
				site.stopPolling()
			}
			var reasonAtAppear string
			script := map[time.Duration]func(*vaillantSemanticPoller){tc.appear: func(p *vaillantSemanticPoller) {
				appeared = site.clock.elapsed()
				reasonAtAppear = site.provider.StartupReason()
				p.b524ProbeFn = func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
					if target == controllerlessHeatSource {
						return true
					}
					return p.probeB524Register(ctx, target, opcode, group, instance, addr)
				}
			}}
			run := runControllerlessPolling(t, site, startup, tc.wantFrom+time.Minute, script)

			if found < 0 || run.controllerAt < 0 {
				t.Fatalf("heat-pump-class unit answering the root probe from %s was not adopted by %s of polling", tc.appear, tc.wantFrom+time.Minute)
			}
			if at := found - run.pollStart; at < tc.wantFrom || at >= tc.wantFrom+10*time.Second {
				t.Errorf("root adopted at %s of polling; want in [%s, %s)", at, tc.wantFrom, tc.wantFrom+10*time.Second)
			}
			if got := controllerlessReasonValueOf(reasonAtAppear); got != tc.wantAtAppear {
				t.Errorf("startup reason when the unit starts answering = %q; want %q", got, tc.wantAtAppear)
			}
			controllerlessExpectSeries(t, "startup reason", run.reasons, tc.wantReasons)
			// The adopted root drives the phase as any root does; the reason
			// only names the cause while there is no path.
			if (run.degradedAt == site.deadline) != tc.wantDegraded {
				t.Errorf("DEGRADED at %s; want at the deadline %s = %v", run.degradedAt, site.deadline, tc.wantDegraded)
			}
			if tc.wantDegraded {
				if got, want := site.degradedTransition(), `startup_reason=`+reason; !strings.Contains(got, want) {
					t.Errorf("DEGRADED transition = %q; want it to carry %s", got, want)
				}
			}
			controllerlessExpectStartupBlock(t, site.provider, site.provider.StartupPhase(), "")
			for _, kind := range []string{controllerlessWriteFrame, controllerlessB509Read} {
				if got := site.countByTarget(kind, 0); len(got) != 0 {
					t.Errorf("%s frames = %v; want none", kind, got)
				}
			}
			t.Logf("%s: answers from %s, adopted at %s of polling, reasons %v", tc.name, tc.appear, found-run.pollStart, run.reasons)
		})
	}
}

// TestControllerlessSiteHeatPumpBusControllerAppearsClearsReason covers a
// controller that appears on a bus whose heat source is heat-pump class: the
// first discovery pass sets the startup reason, the boot-live deadline moves
// the phase to DEGRADED with that reason, and after ABSENT a controller
// becomes reachable at 0x15 while the registry gains a catalogue-classified
// regulator identity. The next regulator recheck (13:00) enqueues a discovery
// pass that finds the root, and the reason clears. As in
// TestControllerlessSiteControllerAppearsAfterAbsent, the root probe of 0x15
// is the only seam from the appearance on, no frame answers as a controller,
// and the simulation stops at the first frame the poller addresses to the
// found controller other than a root probe.
func TestControllerlessSiteHeatPumpBusControllerAppearsClearsReason(t *testing.T) {
	heatPumpPart := controllerlessCatalogPart(t, "Heat Pump")
	regulatorPart := controllerlessCatalogPartWith(t, "Regulator", productids.ControllerPresent)
	reason := graphql.SemanticStartupReasonNoSupportedHeatSourcePath
	appear := 12*time.Minute + 30*time.Second // polling-relative, after ABSENT
	wantFrom := 13 * time.Minute              // the find falls in [wantFrom, wantFrom+10 s)

	site := newControllerlessSite(t, controllerlessWireTiming)
	site.addDevice(controllerlessHeatSource, controllerlessDevice{deviceID: "EHP00", serialRaw: controllerlessSerialRaw(heatPumpPart)})
	startup := runControllerlessStartupScan(t, site, false)

	rootProbe := b524CapabilityProbes[0]
	rootSelector := buildB524ReadSelector(rootProbe.opcode, rootProbe.group, rootProbe.instance, rootProbe.addr)
	appeared, found := time.Duration(-1), time.Duration(-1)
	site.onFrame = func(event controllerlessEvent) {
		if appeared < 0 || found >= 0 || event.target != controllerlessRegulator {
			return
		}
		if event.kind == controllerlessB524Frame && reflect.DeepEqual(event.data, rootSelector) {
			return
		}
		found = event.at
		site.stopPolling()
	}
	var reasonAtAppear string
	var stateAtAppear regulatorAbsenceState
	script := map[time.Duration]func(*vaillantSemanticPoller){appear: func(p *vaillantSemanticPoller) {
		appeared = site.clock.elapsed()
		reasonAtAppear = site.provider.StartupReason()
		p.mu.Lock()
		stateAtAppear = p.regAbsenceState
		p.mu.Unlock()
		p.b524ProbeFn = func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
			if target == controllerlessRegulator {
				return true
			}
			return p.probeB524Register(ctx, target, opcode, group, instance, addr)
		}
		startup.gateway.Registry.Register(registry.DeviceInfo{Address: controllerlessRegulator, Manufacturer: "Vaillant", SerialNumber: controllerlessSerial(regulatorPart)})
	}}
	run := runControllerlessPolling(t, site, startup, wantFrom+time.Minute, script)

	if stateAtAppear != regulatorAbsent {
		t.Errorf("absence state when the controller appears = %s; want %s", stateAtAppear, regulatorAbsent)
	}
	if got := controllerlessReasonValueOf(reasonAtAppear); got != reason {
		t.Errorf("startup reason when the controller appears = %q; want %q", got, reason)
	}
	if run.degradedAt != site.deadline {
		t.Errorf("DEGRADED at %s; want at the boot-live deadline %s", run.degradedAt, site.deadline)
	}
	if got, want := site.degradedTransition(), `startup_reason=`+reason; !strings.Contains(got, want) {
		t.Errorf("DEGRADED transition = %q; want it to carry %s", got, want)
	}
	if found < 0 || run.controllerAt < 0 {
		t.Fatalf("controller appearing at %s was not found by %s of polling", appear, wantFrom+time.Minute)
	}
	if at := found - run.pollStart; at < wantFrom || at >= wantFrom+10*time.Second {
		t.Errorf("controller found at %s of polling; want in [%s, %s)", at, wantFrom, wantFrom+10*time.Second)
	}
	controllerlessExpectSeries(t, "startup reason", run.reasons, []string{"none", reason, "none"})
	if len(run.reasons) == 3 {
		if at := run.reasons[2].at - run.pollStart; at < wantFrom || at >= wantFrom+10*time.Second {
			t.Errorf("startup reason cleared at %s of polling; want in [%s, %s)", at, wantFrom, wantFrom+10*time.Second)
		}
	}
	controllerlessExpectStartupBlock(t, site.provider, site.provider.StartupPhase(), "")
	for _, kind := range []string{controllerlessWriteFrame, controllerlessB509Read} {
		if got := site.countByTarget(kind, 0); len(got) != 0 {
			t.Errorf("%s frames = %v; want none", kind, got)
		}
	}
	t.Logf("appears at %s, found at %s of polling, reasons %v", appear, found-run.pollStart, run.reasons)
}

// TestControllerlessSiteHeatSourceIdentifiedAfterAbsent covers a heat source
// that only becomes known after the absence state is ABSENT, when the state and
// configuration ticks no longer run a root discovery pass. Its address first
// enters the registry without an identity, as the passive address-table
// inserter adds it; the inventory change starts a discovery pass at the next
// regulator recheck, which cannot yet classify it. Its identity arrives
// afterwards through the per-address identity probe, which changes no device
// count. The next state tick must pick the boiler-class heat source from the
// registry and start its status reads, not the next discovery tick.
func TestControllerlessSiteHeatSourceIdentifiedAfterAbsent(t *testing.T) {
	boilerPart := controllerlessCatalogPart(t, "Boiler")
	modulePart := controllerlessCatalogPart(t, "Gateway")
	site := newControllerlessSite(t, controllerlessWireTiming)
	// The heat source is off at startup; the passive warm-up offered the
	// internet module's target address, which answers identification.
	site.addDevice(controllerlessInternetModule, controllerlessDevice{deviceID: "NETX3", serialRaw: controllerlessSerialRaw(modulePart)})
	startup := runControllerlessStartupScan(t, site, false, controllerlessInternetModule)
	if startup.passes != 1 || !startup.activeProbePassed {
		t.Fatalf("startup passes = %d, active probe passed = %v; want 1, true", startup.passes, startup.activeProbePassed)
	}
	reg := startup.gateway.Registry
	slotAt, identityAt := 10*time.Minute+30*time.Second, 11*time.Minute+30*time.Second
	devicesAtIdentity := -1
	var stateAtIdentity regulatorAbsenceState
	script := map[time.Duration]func(*vaillantSemanticPoller){
		slotAt: func(*vaillantSemanticPoller) {
			site.addDevice(controllerlessHeatSource, controllerlessDevice{deviceID: "BAI00", serialRaw: controllerlessSerialRaw(boilerPart), answersB509: true})
			reg.Register(registry.DeviceInfo{Address: controllerlessHeatSource})
		},
		identityAt: func(p *vaillantSemanticPoller) {
			devicesAtIdentity = countRegistryDevices(reg)
			p.mu.Lock()
			stateAtIdentity = p.regAbsenceState
			p.mu.Unlock()
			p.EnqueueAddressIdentityProbe(controllerlessHeatSource)
		},
	}
	run := runControllerlessPolling(t, site, startup, 13*time.Minute, script)

	if stateAtIdentity != regulatorAbsent {
		t.Errorf("absence state when the identity arrives = %s; want ABSENT", stateAtIdentity)
	}
	if got := countRegistryDevices(reg); got != devicesAtIdentity {
		t.Errorf("registry devices after the identity probe = %d, before = %d; want no inventory change", got, devicesAtIdentity)
	}
	if entry, ok := controllerlessRegistryEntries(reg)[controllerlessHeatSource]; !ok || entry.DeviceID != "BAI00" {
		t.Fatalf("heat source entry = %+v (present %v); want the identified boiler-class heat source", entry, ok)
	}
	firstRead := time.Duration(-1)
	for _, read := range site.eventsOf(controllerlessB509Read) {
		if read.target == controllerlessHeatSource {
			firstRead = read.at - run.pollStart
			break
		}
	}
	if firstRead < 12*time.Minute || firstRead >= 12*time.Minute+10*time.Second {
		t.Errorf("first heat-source status read at %s of polling; want on the state tick after the identity (12:00), not the discovery tick (20:00)", firstRead)
	}
	if site.provider.BoilerStatus() == nil {
		t.Error("boiler status not published after the heat source was identified")
	}
	t.Logf("slot at %s, identity at %s, first status read at %s of polling; phases %v", slotAt, identityAt, firstRead, site.phaseSeries())
}
