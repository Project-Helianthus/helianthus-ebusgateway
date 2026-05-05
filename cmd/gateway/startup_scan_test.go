package main

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func TestShouldStopDiscoveryScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		total                 int
		confirmationPending   bool
		confirmationSatisfied bool
		confirmationExhausted bool
		want                  bool
	}{
		{name: "no devices", total: 0, want: false},
		{name: "normal direct inventory stops", total: 1, want: true},
		{name: "imported inventory keeps scanning while confirmation is pending", total: 4, confirmationPending: true, want: false},
		{name: "imported inventory stops once confirmation resolves", total: 4, confirmationPending: true, confirmationSatisfied: true, want: true},
		{name: "bounded root-aware fallback exhaustion stops", total: 4, confirmationPending: true, confirmationExhausted: true, want: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldStopDiscoveryScan(test.total, test.confirmationPending, test.confirmationSatisfied, test.confirmationExhausted); got != test.want {
				t.Fatalf(
					"shouldStopDiscoveryScan(%d, pending=%v, satisfied=%v, exhausted=%v) = %v; want %v",
					test.total,
					test.confirmationPending,
					test.confirmationSatisfied,
					test.confirmationExhausted,
					got,
					test.want,
				)
			}
		})
	}
}

func TestShouldRetryDiscoveryWithFullRange(t *testing.T) {
	origProbeFn := startupScanB524ProbeFn
	t.Cleanup(func() { startupScanB524ProbeFn = origProbeFn })

	tests := []struct {
		name              string
		devices           []registry.DeviceInfo
		usedRestricted    bool
		retryingFullRange bool
		coherent          map[byte]bool
		canceledCtx       bool
		want              bool
		wantProbeCalls    bool
	}{
		{
			name: "non restricted pass never retries",
			devices: []registry.DeviceInfo{{
				Address:      0x08,
				Manufacturer: "Vaillant",
				DeviceID:     "BAI00",
			}},
			coherent:       map[byte]bool{0x08: false},
			want:           false,
			wantProbeCalls: false,
		},
		{
			name: "bounded full range retry does not recurse",
			devices: []registry.DeviceInfo{{
				Address:      0x08,
				Manufacturer: "Vaillant",
				DeviceID:     "BAI00",
			}},
			usedRestricted:    true,
			retryingFullRange: true,
			coherent:          map[byte]bool{0x08: false},
			want:              false,
			wantProbeCalls:    false,
		},
		{
			name: "non vaillant inventory does not broaden",
			devices: []registry.DeviceInfo{{
				Address:      0x01,
				Manufacturer: "Other",
				DeviceID:     "GENERIC",
			}},
			usedRestricted: true,
			coherent:       map[byte]bool{0x01: false},
			want:           false,
			wantProbeCalls: false,
		},
		{
			name: "mixed inventory broadens when vaillant root is not ready",
			devices: []registry.DeviceInfo{
				{Address: 0x01, Manufacturer: "Other", DeviceID: "GENERIC"},
				{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"},
			},
			usedRestricted: true,
			coherent:       map[byte]bool{0x01: false, 0x08: false},
			want:           true,
			wantProbeCalls: true,
		},
		{
			name: "mixed inventory stops broadening once root is ready",
			devices: []registry.DeviceInfo{
				{Address: 0x01, Manufacturer: "Other", DeviceID: "GENERIC"},
				{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"},
			},
			usedRestricted: true,
			coherent:       map[byte]bool{0x01: false, 0x26: true},
			want:           false,
			wantProbeCalls: true,
		},
		{
			name: "canceled context never broadens or probes",
			devices: []registry.DeviceInfo{{
				Address:      0x08,
				Manufacturer: "Vaillant",
				DeviceID:     "BAI00",
			}},
			usedRestricted: true,
			coherent:       map[byte]bool{0x08: false},
			canceledCtx:    true,
			want:           false,
			wantProbeCalls: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
				Transport: transport.NewLoopback(),
			})
			if err != nil {
				t.Fatalf("gateway.New error = %v", err)
			}
			t.Cleanup(func() {
				if err := gateway.Close(); err != nil {
					t.Fatalf("gateway.Close error = %v", err)
				}
			})

			for _, device := range test.devices {
				gateway.Registry.Register(device)
			}

			var (
				probeCalls       int
				sawCanceledProbe bool
			)
			startupScanB524ProbeFn = func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
				probeCalls++
				if err := ctx.Err(); err != nil {
					sawCanceledProbe = true
					return false
				}
				return test.coherent[target]
			}

			ctx := context.Background()
			if test.canceledCtx {
				canceledCtx, cancel := context.WithCancel(context.Background())
				cancel()
				ctx = canceledCtx
			}

			got := shouldRetryDiscoveryWithFullRange(ctx, ebusgateway.DefaultConfig(), gateway, test.usedRestricted, test.retryingFullRange)
			if got != test.want {
				t.Fatalf("shouldRetryDiscoveryWithFullRange(...) = %v; want %v", got, test.want)
			}
			if sawCanceledProbe {
				t.Fatal("shouldRetryDiscoveryWithFullRange probed with a canceled context")
			}
			if test.wantProbeCalls && probeCalls == 0 {
				t.Fatal("shouldRetryDiscoveryWithFullRange did not exercise real readiness probing")
			}
			if !test.wantProbeCalls && probeCalls != 0 {
				t.Fatalf("probe calls = %d; want 0", probeCalls)
			}
		})
	}
}

func TestResolveStartupScanSourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  ebusgateway.Config
		want byte
	}{
		{
			name: "proxy observe first auto resolves explicit startup source",
			cfg: func() ebusgateway.Config {
				cfg := ebusgateway.DefaultConfig()
				cfg.BroadcastListen = true
				cfg.ScanSource = 0x00
				cfg.ScanSourceAuto = true
				cfg.TransportConfig.Protocol = ebusgateway.TransportENS
				cfg.TransportConfig.Network = "tcp"
				cfg.TransportConfig.Address = "127.0.0.1:19001"
				return cfg
			}(),
			want: proxyObserveFirstStartupSource,
		},
		{
			name: "direct adapter auto remains dynamic",
			cfg: func() ebusgateway.Config {
				cfg := ebusgateway.DefaultConfig()
				cfg.BroadcastListen = true
				cfg.ScanSource = 0x00
				cfg.ScanSourceAuto = true
				cfg.TransportConfig.Protocol = ebusgateway.TransportENS
				cfg.TransportConfig.Network = "tcp"
				cfg.TransportConfig.Address = "192.168.100.2:9999"
				return cfg
			}(),
			want: 0x00,
		},
		{
			name: "proxy single ENS without broadcast resolves startup source",
			cfg: func() ebusgateway.Config {
				cfg := ebusgateway.DefaultConfig()
				cfg.BroadcastListen = false
				cfg.ScanSource = 0x00
				cfg.ScanSourceAuto = true
				cfg.TransportConfig.Protocol = ebusgateway.TransportENS
				cfg.TransportConfig.Network = "tcp"
				cfg.TransportConfig.Address = "127.0.0.1:19001"
				return cfg
			}(),
			want: proxyObserveFirstStartupSource,
		},
		{
			name: "proxy single ENH without broadcast resolves startup source",
			cfg: func() ebusgateway.Config {
				cfg := ebusgateway.DefaultConfig()
				cfg.BroadcastListen = false
				cfg.ScanSource = 0x00
				cfg.ScanSourceAuto = true
				cfg.TransportConfig.Protocol = ebusgateway.TransportENH
				cfg.TransportConfig.Network = "tcp"
				cfg.TransportConfig.Address = "127.0.0.1:19001"
				return cfg
			}(),
			want: proxyObserveFirstStartupSource,
		},
		{
			name: "explicit source is preserved",
			cfg: func() ebusgateway.Config {
				cfg := ebusgateway.DefaultConfig()
				cfg.BroadcastListen = true
				cfg.ScanSource = 0xF0
				cfg.ScanSourceAuto = false
				cfg.TransportConfig.Protocol = ebusgateway.TransportENS
				cfg.TransportConfig.Network = "tcp"
				cfg.TransportConfig.Address = "127.0.0.1:19001"
				return cfg
			}(),
			want: 0xF0,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := resolveStartupScanSourceConfig(test.cfg)
			if got.ScanSource != test.want {
				t.Fatalf("resolveStartupScanSourceConfig(...).ScanSource = 0x%02X; want 0x%02X", got.ScanSource, test.want)
			}
			if got.ScanSource == proxyObserveFirstStartupSource && got.ScanSourceAuto {
				t.Fatal("resolveStartupScanSourceConfig(...) left ScanSourceAuto=true for explicit proxy startup source")
			}
		})
	}
}

func TestStartupProbeTargetsFromSelection_SanitizesPromotedTargets(t *testing.T) {
	t.Parallel()

	targets := startupProbeTargetsFromSelection(protocol.SourceAddressSelection{
		Source: 0x7F,
		Metrics: protocol.SourceAddressSelectionMetrics{
			ObservedProbableTargets: []byte{0x15, 0x08, 0x7F, 0xFE, 0xAA, 0x08, 0x02},
		},
	})
	want := []byte{0x08, 0x15}
	if !reflect.DeepEqual(targets, want) {
		t.Fatalf("startup probe targets = % X; want % X", targets, want)
	}
}

func TestEbusdScanTargetCandidates(t *testing.T) {
	t.Parallel()

	t.Run("ebusd tcp transport returns configured endpoint only", func(t *testing.T) {
		t.Parallel()

		candidates := ebusdScanTargetCandidates(ebusgateway.TransportConfig{
			Protocol:     ebusgateway.TransportEbusdTCP,
			Network:      "tcp",
			Address:      "192.168.100.4:8888",
			DialTimeout:  5 * time.Second,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
		})
		if len(candidates) != 1 {
			t.Fatalf("candidate count = %d; want 1", len(candidates))
		}
		if got := candidates[0].Address; got != "192.168.100.4:8888" {
			t.Fatalf("candidate[0].Address = %q; want %q", got, "192.168.100.4:8888")
		}
	})

	t.Run("non ebusd transport returns no candidates", func(t *testing.T) {
		t.Parallel()

		candidates := ebusdScanTargetCandidates(ebusgateway.TransportConfig{
			Protocol:    ebusgateway.TransportENS,
			Network:     "tcp",
			Address:     "127.0.0.1:19001",
			DialTimeout: 500 * time.Millisecond,
		})
		if len(candidates) != 0 {
			t.Fatalf("candidate count = %d; want 0 (non-ebusd-tcp transport must not query ebusd)", len(candidates))
		}
	})

	t.Run("local endpoint preserves config dial timeout", func(t *testing.T) {
		t.Parallel()

		candidates := ebusdScanTargetCandidates(ebusgateway.TransportConfig{
			Protocol:    ebusgateway.TransportEbusdTCP,
			Network:     "tcp",
			Address:     "127.0.0.1:8888",
			DialTimeout: 750 * time.Millisecond,
		})
		if len(candidates) != 1 {
			t.Fatalf("candidate count = %d; want 1", len(candidates))
		}
		if got := candidates[0].DialTimeout; got != 750*time.Millisecond {
			t.Fatalf("dial timeout = %s; want 750ms", got)
		}
	})
}

func TestParseEbusdScanResultLine(t *testing.T) {
	t.Parallel()

	row, ok := parseEbusdScanResultLine("15;Vaillant;BASV2;0507;1704;21;21;34;0020262148;0082;014267;N7")
	if !ok {
		t.Fatalf("parseEbusdScanResultLine returned ok=false")
	}
	if row.Address != 0x15 {
		t.Fatalf("Address = 0x%02x; want 0x15", row.Address)
	}
	if row.Manufacturer != "Vaillant" {
		t.Fatalf("Manufacturer = %q; want %q", row.Manufacturer, "Vaillant")
	}
	if row.DeviceID != "BASV2" {
		t.Fatalf("DeviceID = %q; want %q", row.DeviceID, "BASV2")
	}
	if row.SoftwareVersion != "0507" {
		t.Fatalf("SoftwareVersion = %q; want %q", row.SoftwareVersion, "0507")
	}
	if row.HardwareVersion != "1704" {
		t.Fatalf("HardwareVersion = %q; want %q", row.HardwareVersion, "1704")
	}
	if row.SerialNumber != "21-21-34-0020262148-0082-014267-N7" {
		t.Fatalf("SerialNumber = %q; want %q", row.SerialNumber, "21-21-34-0020262148-0082-014267-N7")
	}
}

func TestStartDiscoveryScanLoop_ProxyObserveFirstAutoUsesExplicitStartupSource(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origRegistryScanDirectedFn := registryScanDirectedFn
	origTargetCandidatesFn := ebusdScanTargetCandidatesFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origLoopExitFn := startupScanLoopExitFn
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		registryScanDirectedFn = origRegistryScanDirectedFn
		ebusdScanTargetCandidatesFn = origTargetCandidatesFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		startupScanLoopExitFn = origLoopExitFn
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scanSourceCh := make(chan byte, 1)
	loopExited := make(chan struct{})
	registryScanFn = func(_ context.Context, _ registry.ScanBus, _ *registry.DeviceRegistry, source byte, _ []byte) ([]registry.DeviceEntry, error) {
		scanSourceCh <- source
		cancel()
		return nil, nil
	}
	registryScanDirectedFn = registryScanFn
	ebusdScanTargetCandidatesFn = func(cfg ebusgateway.TransportConfig) []ebusgateway.TransportConfig {
		return []ebusgateway.TransportConfig{cfg}
	}
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		return []byte{0x08}, nil
	}
	startupScanLoopExitFn = func() {
		close(loopExited)
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.ScanInterval = time.Hour
	cfg.BroadcastListen = true
	cfg.ScanSource = 0x00
	cfg.ScanSourceAuto = true
	cfg.TransportConfig.Protocol = ebusgateway.TransportENS
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "127.0.0.1:19001"

	signals := startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	select {
	case got := <-scanSourceCh:
		if got != proxyObserveFirstStartupSource {
			t.Fatalf("registry scan source = 0x%02X; want 0x%02X", got, proxyObserveFirstStartupSource)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("registry scan was not invoked")
	}

	select {
	case <-signals.firstPassDone:
	case <-time.After(2 * time.Second):
		t.Fatal("firstPassDone was not signaled")
	}

	select {
	case <-loopExited:
	case <-time.After(2 * time.Second):
		t.Fatal("startup scan loop did not exit after context cancellation")
	}
}

func TestStartDiscoveryScanLoop_ProxySingleENSWithoutBroadcastResolvesSource(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origRegistryScanDirectedFn := registryScanDirectedFn
	origTargetCandidatesFn := ebusdScanTargetCandidatesFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origLoopExitFn := startupScanLoopExitFn
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		registryScanDirectedFn = origRegistryScanDirectedFn
		ebusdScanTargetCandidatesFn = origTargetCandidatesFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		startupScanLoopExitFn = origLoopExitFn
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scanSourceCh := make(chan byte, 1)
	loopExited := make(chan struct{})
	registryScanFn = func(_ context.Context, _ registry.ScanBus, _ *registry.DeviceRegistry, source byte, _ []byte) ([]registry.DeviceEntry, error) {
		scanSourceCh <- source
		cancel()
		return nil, nil
	}
	registryScanDirectedFn = registryScanFn
	ebusdScanTargetCandidatesFn = func(cfg ebusgateway.TransportConfig) []ebusgateway.TransportConfig {
		return []ebusgateway.TransportConfig{cfg}
	}
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		return []byte{0x08}, nil
	}
	startupScanLoopExitFn = func() {
		close(loopExited)
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.ScanInterval = time.Hour
	cfg.BroadcastListen = false
	cfg.ScanSource = 0x00
	cfg.ScanSourceAuto = true
	cfg.TransportConfig.Protocol = ebusgateway.TransportENS
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "127.0.0.1:19001"

	signals := startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	select {
	case got := <-scanSourceCh:
		if got == 0x00 {
			t.Fatal("registry scan source = 0x00; proxy-single ENS must not use invalid initiator")
		}
		if got != proxyObserveFirstStartupSource {
			t.Fatalf("registry scan source = 0x%02X; want 0x%02X", got, proxyObserveFirstStartupSource)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("registry scan was not invoked")
	}

	select {
	case <-signals.firstPassDone:
	case <-time.After(2 * time.Second):
		t.Fatal("firstPassDone was not signaled")
	}

	select {
	case <-loopExited:
	case <-time.After(2 * time.Second):
		t.Fatal("startup scan loop did not exit after context cancellation")
	}
}

func TestParseEbusdScanResultLineRejectsInvalid(t *testing.T) {
	t.Parallel()

	cases := []string{
		"",
		"ERR: failed",
		"15;too;short",
		"ZZ;Vaillant;BASV2;0507;1704",
	}

	for _, sample := range cases {
		sample := sample
		t.Run(sample, func(t *testing.T) {
			t.Parallel()
			if _, ok := parseEbusdScanResultLine(sample); ok {
				t.Fatalf("parseEbusdScanResultLine(%q) returned ok=true; want false", sample)
			}
		})
	}
}

func TestStartDiscoveryScanLoop_RerunsPhysicalIdentityEnrichmentAfterNormalScan(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origRegistryScanDirectedFn := registryScanDirectedFn
	origTargetCandidatesFn := ebusdScanTargetCandidatesFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origResultInfosFn := ebusdScanResultInfosFn
	origEnrichVaillantIdentityFn := enrichVaillantIdentityFn
	origEnrichSerialsFromEbusdFn := enrichSerialsFromEbusdFn
	origPostStartupIdentityRetryFn := postStartupIdentityRetryFn
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		registryScanDirectedFn = origRegistryScanDirectedFn
		ebusdScanTargetCandidatesFn = origTargetCandidatesFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
		postStartupIdentityRetryFn = origPostStartupIdentityRetryFn
	})

	done := make(chan struct{}, 1)
	registryScanFn = func(_ context.Context, _ registry.ScanBus, reg *registry.DeviceRegistry, _ byte, _ []byte) ([]registry.DeviceEntry, error) {
		entry := reg.Register(registry.DeviceInfo{
			Address:         0x08,
			Manufacturer:    "Vaillant",
			DeviceID:        "BAI00",
			SoftwareVersion: "1201",
			HardwareVersion: "7603",
		})
		select {
		case done <- struct{}{}:
		default:
		}
		return []registry.DeviceEntry{entry}, nil
	}
	ebusdScanTargetCandidatesFn = func(cfg ebusgateway.TransportConfig) []ebusgateway.TransportConfig {
		return []ebusgateway.TransportConfig{cfg}
	}
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		return []byte{0x08}, nil
	}
	ebusdScanResultInfosFn = func(context.Context, ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		return nil, nil
	}

	var mu sync.Mutex
	var vaillantEnrichCalls int
	var ebusdEnrichCalls int
	retryScheduled := make(chan struct{}, 1)
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {
		mu.Lock()
		vaillantEnrichCalls++
		mu.Unlock()
	}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {
		mu.Lock()
		ebusdEnrichCalls++
		mu.Unlock()
	}
	postStartupIdentityRetryFn = func(context.Context, *ebusgateway.Gateway, *graphql.Builder, ebusgateway.Config, *ebusgateway.TransportConfig) {
		select {
		case retryScheduled <- struct{}{}:
		default:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.TransportConfig = ebusgateway.TransportConfig{
		Network: "tcp",
		Address: "127.0.0.1:8888",
	}

	startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not complete a pass")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		gotVaillant := vaillantEnrichCalls
		gotEbusd := ebusdEnrichCalls
		mu.Unlock()
		select {
		case <-retryScheduled:
			retryScheduled = nil
		default:
		}
		if gotVaillant >= 1 && gotEbusd >= 1 && retryScheduled == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("enrichment calls after normal scan = vaillant:%d ebusd:%d; want both >= 1 and delayed retry scheduled", gotVaillant, gotEbusd)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestStartDiscoveryScanLoop_FallbackIdentityEnrichmentRunsOnce(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origTargetCandidatesFn := ebusdScanTargetCandidatesFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origResultInfosFn := ebusdScanResultInfosFn
	origEnrichVaillantIdentityFn := enrichVaillantIdentityFn
	origEnrichSerialsFromEbusdFn := enrichSerialsFromEbusdFn
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		ebusdScanTargetCandidatesFn = origTargetCandidatesFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
	})

	done := make(chan struct{}, 1)
	registryScanFn = func(_ context.Context, scanBus registry.ScanBus, _ *registry.DeviceRegistry, _ byte, _ []byte) ([]registry.DeviceEntry, error) {
		statsBus, ok := scanBus.(*statsBus)
		if !ok {
			panic("startup scan bus missing stats wrapper")
		}
		statsBus.stats.timeouts = 1
		select {
		case done <- struct{}{}:
		default:
		}
		return nil, nil
	}
	ebusdScanTargetCandidatesFn = func(cfg ebusgateway.TransportConfig) []ebusgateway.TransportConfig {
		return []ebusgateway.TransportConfig{cfg}
	}
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		return []byte{0x08}, nil
	}
	ebusdScanResultInfosFn = func(context.Context, ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		return []registry.DeviceInfo{{
			Address:         0x08,
			Manufacturer:    "Vaillant",
			DeviceID:        "BAI00",
			SoftwareVersion: "1201",
			HardwareVersion: "7603",
		}}, nil
	}

	var mu sync.Mutex
	var vaillantEnrichCalls int
	var ebusdEnrichCalls int
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {
		mu.Lock()
		vaillantEnrichCalls++
		mu.Unlock()
	}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {
		mu.Lock()
		ebusdEnrichCalls++
		mu.Unlock()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.TransportConfig = ebusgateway.TransportConfig{
		Network: "tcp",
		Address: "127.0.0.1:8888",
	}

	startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan fallback did not execute")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		gotVaillant := vaillantEnrichCalls
		gotEbusd := ebusdEnrichCalls
		mu.Unlock()
		if gotVaillant >= 1 && gotEbusd >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fallback enrichment calls = vaillant:%d ebusd:%d; want both >= 1", gotVaillant, gotEbusd)
		}
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	gotVaillant := vaillantEnrichCalls
	gotEbusd := ebusdEnrichCalls
	mu.Unlock()
	if gotVaillant != 1 {
		t.Fatalf("fallback vaillant enrichment calls = %d; want 1", gotVaillant)
	}
	if gotEbusd != 1 {
		t.Fatalf("fallback ebusd enrichment calls = %d; want 1", gotEbusd)
	}

	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestStartDiscoveryScanLoop_EbusdPreloadKeepsScanningUntilControllerCandidate(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origTargetCandidatesFn := ebusdScanTargetCandidatesFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origResultInfosFn := ebusdScanResultInfosFn
	origProbeFn := startupScanB524ProbeFn
	origLoopExitFn := startupScanLoopExitFn
	origEnrichVaillantIdentityFn := enrichVaillantIdentityFn
	origEnrichSerialsFromEbusdFn := enrichSerialsFromEbusdFn
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		ebusdScanTargetCandidatesFn = origTargetCandidatesFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		startupScanB524ProbeFn = origProbeFn
		startupScanLoopExitFn = origLoopExitFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
	})

	var (
		mu                    sync.Mutex
		preloadRun            int
		scanRun               int
		lastTargets           []byte
		unexpectedPreloadRuns int
	)
	postScanEnrichDone := make(chan struct{}, 1)
	registryScanFn = func(_ context.Context, _ registry.ScanBus, reg *registry.DeviceRegistry, _ byte, targets []byte) ([]registry.DeviceEntry, error) {
		mu.Lock()
		scanRun++
		lastTargets = append([]byte(nil), targets...)
		mu.Unlock()

		entry := reg.Register(registry.DeviceInfo{
			Address:      0x15,
			Manufacturer: "Vaillant",
			DeviceID:     "BASV2",
		})
		return []registry.DeviceEntry{entry}, nil
	}
	ebusdScanTargetCandidatesFn = func(cfg ebusgateway.TransportConfig) []ebusgateway.TransportConfig {
		return []ebusgateway.TransportConfig{cfg}
	}
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		return []byte{0x04, 0x08}, nil
	}
	ebusdScanResultInfosFn = func(context.Context, ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		mu.Lock()
		defer mu.Unlock()
		preloadRun++
		if preloadRun == 1 {
			return []registry.DeviceInfo{
				{Address: 0x04, Manufacturer: "Vaillant", DeviceID: "NETX3"},
				{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"},
			}, nil
		}
		unexpectedPreloadRuns++
		return nil, nil
	}
	var probeCtxErr error
	startupScanB524ProbeFn = func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
		mu.Lock()
		defer mu.Unlock()
		if err := ctx.Err(); err != nil && probeCtxErr == nil {
			probeCtxErr = err
			return false
		}
		switch target {
		case 0x15:
			return true
		case 0x04, 0x08:
			return false
		default:
			return false
		}
	}
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {
		select {
		case postScanEnrichDone <- struct{}{}:
		default:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.ScanInterval = 10 * time.Millisecond
	cfg.TransportConfig = ebusgateway.TransportConfig{
		Protocol: ebusgateway.TransportEbusdTCP,
		Network:  "tcp",
		Address:  "127.0.0.1:8888",
	}

	startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if entry, ok := gateway.Registry.Lookup(0x15); ok && entry != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("startup discovery scan did not persist BASV2 after partial preload")
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	gotRuns := preloadRun
	gotScanRuns := scanRun
	gotLastTargets := append([]byte(nil), lastTargets...)
	gotUnexpectedPreloadRuns := unexpectedPreloadRuns
	gotProbeCtxErr := probeCtxErr
	mu.Unlock()
	if gotRuns != 1 {
		t.Fatalf("ebusd scan preload runs = %d; want 1 before bounded full-range retry", gotRuns)
	}
	if gotScanRuns != 1 {
		t.Fatalf("registry scan runs = %d; want 1 full-range recovery pass", gotScanRuns)
	}
	if gotLastTargets != nil {
		t.Fatalf("registry scan targets = %v; want nil for full-range retry", gotLastTargets)
	}
	if gotUnexpectedPreloadRuns != 0 {
		t.Fatalf("unexpected preload reruns = %d; want 0 after bounded full-range retry", gotUnexpectedPreloadRuns)
	}
	if gotProbeCtxErr != nil {
		t.Fatalf("startup scan readiness probe received canceled context: %v", gotProbeCtxErr)
	}

	if !startupScanHasCoherentVaillantRoot(context.Background(), cfg, gateway) {
		t.Fatal("startupScanHasCoherentVaillantRoot should succeed after bounded full-range recovery")
	}
	if shouldRetryDiscoveryWithFullRange(context.Background(), cfg, gateway, true, false) {
		t.Fatal("shouldRetryDiscoveryWithFullRange should stop retrying once recovery has produced a coherent root")
	}

	select {
	case <-postScanEnrichDone:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not complete post-scan serial enrichment before teardown")
	}

	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	finalPreloadRun := preloadRun
	finalScanRun := scanRun
	mu.Unlock()
	if finalPreloadRun != 1 {
		t.Fatalf("preload runs after recovery = %d; want prompt stop at 1", finalPreloadRun)
	}
	if finalScanRun != 1 {
		t.Fatalf("registry scan runs after recovery = %d; want prompt stop at 1", finalScanRun)
	}

	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestStartDiscoveryScanLoop_EbusdPreloadWithCoherentRootDoesNotRetryFullRange(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origTargetCandidatesFn := ebusdScanTargetCandidatesFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origResultInfosFn := ebusdScanResultInfosFn
	origProbeFn := startupScanB524ProbeFn
	origEnrichVaillantIdentityFn := enrichVaillantIdentityFn
	origEnrichSerialsFromEbusdFn := enrichSerialsFromEbusdFn
	origPostStartupIdentityRetryFn := postStartupIdentityRetryFn
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		ebusdScanTargetCandidatesFn = origTargetCandidatesFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		startupScanB524ProbeFn = origProbeFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
		postStartupIdentityRetryFn = origPostStartupIdentityRetryFn
	})

	var (
		mu                    sync.Mutex
		preloadRun            int
		scanRun               int
		probeCtxErr           error
		unexpectedPreloadRuns int
	)
	preloadEnrichDone := make(chan struct{}, 1)
	retryScheduled := make(chan *ebusgateway.TransportConfig, 1)
	registryScanFn = func(_ context.Context, _ registry.ScanBus, _ *registry.DeviceRegistry, _ byte, _ []byte) ([]registry.DeviceEntry, error) {
		mu.Lock()
		scanRun++
		mu.Unlock()
		return nil, nil
	}
	ebusdScanTargetCandidatesFn = func(cfg ebusgateway.TransportConfig) []ebusgateway.TransportConfig {
		return []ebusgateway.TransportConfig{cfg}
	}
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		return []byte{0x04, 0x08, 0x15}, nil
	}
	ebusdScanResultInfosFn = func(context.Context, ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		mu.Lock()
		defer mu.Unlock()
		preloadRun++
		if preloadRun > 1 {
			unexpectedPreloadRuns++
		}
		return []registry.DeviceInfo{
			{Address: 0x04, Manufacturer: "Vaillant", DeviceID: "NETX3"},
			{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"},
			{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"},
		}, nil
	}
	startupScanB524ProbeFn = func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
		mu.Lock()
		defer mu.Unlock()
		if err := ctx.Err(); err != nil && probeCtxErr == nil {
			probeCtxErr = err
			return false
		}
		return target == 0x15
	}
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {
		select {
		case preloadEnrichDone <- struct{}{}:
		default:
		}
	}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {}
	postStartupIdentityRetryFn = func(_ context.Context, _ *ebusgateway.Gateway, _ *graphql.Builder, _ ebusgateway.Config, targetConfig *ebusgateway.TransportConfig) {
		select {
		case retryScheduled <- targetConfig:
		default:
		}
	}
	loopExited := make(chan struct{}, 1)
	startupScanLoopExitFn = func() {
		select {
		case loopExited <- struct{}{}:
		default:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		select {
		case <-loopExited:
		case <-time.After(2 * time.Second):
			t.Fatal("startup discovery scan did not exit after cancellation")
		}
	}()

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.ScanInterval = 10 * time.Millisecond
	cfg.TransportConfig = ebusgateway.TransportConfig{
		Protocol: ebusgateway.TransportEbusdTCP,
		Network:  "tcp",
		Address:  "127.0.0.1:8888",
	}

	startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if ready := startupScanHasCoherentVaillantRoot(context.Background(), cfg, gateway); ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("startup discovery scan preload did not produce a coherent root")
		}
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case <-preloadEnrichDone:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not complete preload enrichment before teardown")
	}
	var gotRetryTarget *ebusgateway.TransportConfig
	select {
	case gotRetryTarget = <-retryScheduled:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not schedule delayed retry after preload-only identity enrichment")
	}
	if gotRetryTarget == nil || gotRetryTarget.Address != "127.0.0.1:8888" {
		t.Fatalf("delayed retry target config = %#v; want ebusd preload transport copy", gotRetryTarget)
	}

	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	finalPreloadRun := preloadRun
	finalScanRun := scanRun
	finalUnexpectedPreloadRuns := unexpectedPreloadRuns
	finalProbeCtxErr := probeCtxErr
	mu.Unlock()
	if finalPreloadRun != 1 {
		t.Fatalf("ebusd scan preload runs = %d; want 1", finalPreloadRun)
	}
	if finalScanRun != 0 {
		t.Fatalf("registry scan runs = %d; want 0 when preload already has a coherent root", finalScanRun)
	}
	if finalUnexpectedPreloadRuns != 0 {
		t.Fatalf("unexpected preload reruns = %d; want 0 when preload already has a coherent root", finalUnexpectedPreloadRuns)
	}
	if finalProbeCtxErr != nil {
		t.Fatalf("startup scan readiness probe received canceled context: %v", finalProbeCtxErr)
	}

}

func TestSchedulePostStartupIdentityRetryEnrichesMissingVaillantSerials(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origEnrichVaillantIdentityFn := enrichVaillantIdentityFn
	origEnrichSerialsFromEbusdFn := enrichSerialsFromEbusdFn
	origDelay := postStartupIdentityRetryDelay
	origAttempts := postStartupIdentityRetryAttempts
	t.Cleanup(func() {
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
		postStartupIdentityRetryDelay = origDelay
		postStartupIdentityRetryAttempts = origAttempts
	})

	gateway.Registry.Register(registry.DeviceInfo{
		Address:         0x08,
		Manufacturer:    "Vaillant",
		DeviceID:        "BAI00",
		SoftwareVersion: "1201",
		HardwareVersion: "7603",
	})

	postStartupIdentityRetryDelay = 10 * time.Millisecond
	postStartupIdentityRetryAttempts = 1

	done := make(chan struct{}, 1)
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {
		gateway.Registry.Register(registry.DeviceInfo{
			Address:         0x08,
			Manufacturer:    "Vaillant",
			DeviceID:        "BAI00",
			SoftwareVersion: "1201",
			HardwareVersion: "7603",
			SerialNumber:    "21-22-01-0010024604-0001-005034-N9",
		})
		select {
		case done <- struct{}{}:
		default:
		}
	}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schedulePostStartupIdentityRetry(ctx, gateway, nil, ebusgateway.DefaultConfig(), nil)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("delayed identity retry did not invoke enrichment")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		entry, ok := gateway.Registry.Lookup(0x08)
		if ok && entry != nil && entry.SerialNumber() == "21-22-01-0010024604-0001-005034-N9" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("delayed identity retry did not persist serial onto registry entry")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStartDiscoveryScanLoop_EbusdPreloadFailedRecoveryContinuesRestrictedScansUntilActiveSuccess(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origTargetCandidatesFn := ebusdScanTargetCandidatesFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origResultInfosFn := ebusdScanResultInfosFn
	origProbeFn := startupScanB524ProbeFn
	origLoopExitFn := startupScanLoopExitFn
	origEnrichVaillantIdentityFn := enrichVaillantIdentityFn
	origEnrichSerialsFromEbusdFn := enrichSerialsFromEbusdFn
	origPostStartupIdentityRetryFn := postStartupIdentityRetryFn
	restoreGlobals := func() {
		registryScanFn = origRegistryScanFn
		ebusdScanTargetCandidatesFn = origTargetCandidatesFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		startupScanB524ProbeFn = origProbeFn
		startupScanLoopExitFn = origLoopExitFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
		postStartupIdentityRetryFn = origPostStartupIdentityRetryFn
	}

	var (
		mu             sync.Mutex
		preloadRun     int
		scanRun        int
		scanCtxErr     error
		targetHistory  [][]byte
		retrySchedules int
	)
	activeSuccess := make(chan struct{}, 1)
	registryScanFn = func(scanCtx context.Context, _ registry.ScanBus, reg *registry.DeviceRegistry, _ byte, targets []byte) ([]registry.DeviceEntry, error) {
		if err := scanCtx.Err(); err != nil {
			mu.Lock()
			if scanCtxErr == nil {
				scanCtxErr = err
			}
			mu.Unlock()
			return nil, err
		}

		mu.Lock()
		scanRun++
		targetHistory = append(targetHistory, append([]byte(nil), targets...))
		currentRun := scanRun
		mu.Unlock()

		if currentRun == 1 {
			return nil, nil
		}
		entry := reg.Register(registry.DeviceInfo{
			Address:      0x15,
			Manufacturer: "Vaillant",
			DeviceID:     "BASV2",
		})
		select {
		case activeSuccess <- struct{}{}:
		default:
		}
		return []registry.DeviceEntry{entry}, nil
	}
	ebusdScanTargetCandidatesFn = func(cfg ebusgateway.TransportConfig) []ebusgateway.TransportConfig {
		return []ebusgateway.TransportConfig{cfg}
	}
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		return []byte{0x04, 0x08}, nil
	}
	ebusdScanResultInfosFn = func(context.Context, ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		mu.Lock()
		defer mu.Unlock()
		preloadRun++
		return []registry.DeviceInfo{
			{Address: 0x04, Manufacturer: "Vaillant", DeviceID: "NETX3"},
			{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"},
		}, nil
	}
	startupScanB524ProbeFn = func(_ context.Context, target, _opcode, _group, _instance byte, _addr uint16) bool {
		return target == 0x15
	}
	loopExited := make(chan struct{}, 1)
	startupScanLoopExitFn = func() {
		select {
		case loopExited <- struct{}{}:
		default:
		}
	}
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {}
	postStartupIdentityRetryFn = func(context.Context, *ebusgateway.Gateway, *graphql.Builder, ebusgateway.Config, *ebusgateway.TransportConfig) {
		mu.Lock()
		retrySchedules++
		mu.Unlock()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		select {
		case <-loopExited:
		case <-time.After(2 * time.Second):
		}
		restoreGlobals()
	}()

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.ScanInterval = 10 * time.Millisecond
	cfg.TransportConfig = ebusgateway.TransportConfig{
		Protocol: ebusgateway.TransportEbusdTCP,
		Network:  "tcp",
		Address:  "127.0.0.1:8888",
	}

	startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	select {
	case <-activeSuccess:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not continue after failed bounded recovery")
	}

	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	gotPreloadRun := preloadRun
	gotScanRun := scanRun
	gotScanCtxErr := scanCtxErr
	gotRetrySchedules := retrySchedules
	gotTargetHistory := make([][]byte, len(targetHistory))
	for i := range targetHistory {
		gotTargetHistory[i] = append([]byte(nil), targetHistory[i]...)
	}
	mu.Unlock()

	if gotPreloadRun != 2 {
		t.Fatalf("ebusd scan preload runs = %d; want 2 (initial preload plus later restricted scan check)", gotPreloadRun)
	}
	if gotScanRun != 2 {
		t.Fatalf("registry scan runs = %d; want 2 (failed full-range retry, then restricted success)", gotScanRun)
	}
	if gotRetrySchedules != 1 {
		t.Fatalf("delayed retry schedules = %d; want 1 for the entire preload-to-active confirmation flow", gotRetrySchedules)
	}
	if gotScanCtxErr != nil {
		t.Fatalf("registry scan received canceled context during follow-up active confirmation: %v", gotScanCtxErr)
	}

	wantTargetHistory := [][]byte{
		nil,
		{0x04, 0x08},
	}
	if !reflect.DeepEqual(gotTargetHistory, wantTargetHistory) {
		t.Fatalf("scan target history = %#v; want %#v", gotTargetHistory, wantTargetHistory)
	}

	cancel()
	select {
	case <-loopExited:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not exit after cancellation")
	}
}

func TestStartDiscoveryScanLoop_EbusdPreloadNonVaillantImportFallsThroughToRestrictedActiveScan(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origTargetCandidatesFn := ebusdScanTargetCandidatesFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origResultInfosFn := ebusdScanResultInfosFn
	origProbeFn := startupScanB524ProbeFn
	origLoopExitFn := startupScanLoopExitFn
	origEnrichVaillantIdentityFn := enrichVaillantIdentityFn
	origEnrichSerialsFromEbusdFn := enrichSerialsFromEbusdFn
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		ebusdScanTargetCandidatesFn = origTargetCandidatesFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		startupScanB524ProbeFn = origProbeFn
		startupScanLoopExitFn = origLoopExitFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
	})

	var (
		mu            sync.Mutex
		preloadRun    int
		scanRun       int
		scanCtxErr    error
		targetHistory [][]byte
	)
	activeSuccess := make(chan struct{}, 1)
	loopExited := make(chan struct{}, 1)
	registryScanFn = func(scanCtx context.Context, _ registry.ScanBus, reg *registry.DeviceRegistry, _ byte, targets []byte) ([]registry.DeviceEntry, error) {
		if err := scanCtx.Err(); err != nil {
			mu.Lock()
			if scanCtxErr == nil {
				scanCtxErr = err
			}
			mu.Unlock()
			return nil, err
		}

		mu.Lock()
		scanRun++
		targetHistory = append(targetHistory, append([]byte(nil), targets...))
		mu.Unlock()

		entry := reg.Register(registry.DeviceInfo{
			Address:      0x15,
			Manufacturer: "Vaillant",
			DeviceID:     "BASV2",
		})
		select {
		case activeSuccess <- struct{}{}:
		default:
		}
		return []registry.DeviceEntry{entry}, nil
	}
	ebusdScanTargetCandidatesFn = func(cfg ebusgateway.TransportConfig) []ebusgateway.TransportConfig {
		return []ebusgateway.TransportConfig{cfg}
	}
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		return []byte{0x04, 0x08}, nil
	}
	ebusdScanResultInfosFn = func(context.Context, ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		mu.Lock()
		defer mu.Unlock()
		preloadRun++
		return []registry.DeviceInfo{
			{Address: 0x04, Manufacturer: "Other", DeviceID: "GENERIC"},
		}, nil
	}
	startupScanB524ProbeFn = func(_ context.Context, target, _opcode, _group, _instance byte, _addr uint16) bool {
		return target == 0x15
	}
	startupScanLoopExitFn = func() {
		select {
		case loopExited <- struct{}{}:
		default:
		}
	}
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.ScanInterval = 10 * time.Millisecond
	cfg.TransportConfig = ebusgateway.TransportConfig{
		Protocol: ebusgateway.TransportEbusdTCP,
		Network:  "tcp",
		Address:  "127.0.0.1:8888",
	}

	startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	select {
	case <-activeSuccess:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not perform restricted active scan after non-Vaillant preload import")
	}

	select {
	case <-loopExited:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not stop after restricted active confirmation")
	}

	mu.Lock()
	gotPreloadRun := preloadRun
	gotScanRun := scanRun
	gotScanCtxErr := scanCtxErr
	gotTargetHistory := make([][]byte, len(targetHistory))
	for i := range targetHistory {
		gotTargetHistory[i] = append([]byte(nil), targetHistory[i]...)
	}
	mu.Unlock()

	if gotPreloadRun != 1 {
		t.Fatalf("ebusd scan preload runs = %d; want 1 before restricted active confirmation", gotPreloadRun)
	}
	if gotScanRun != 1 {
		t.Fatalf("registry scan runs = %d; want 1 restricted active confirmation pass", gotScanRun)
	}
	if gotScanCtxErr != nil {
		t.Fatalf("registry scan received canceled context during non-Vaillant preload confirmation flow: %v", gotScanCtxErr)
	}

	wantTargetHistory := [][]byte{
		{0x04, 0x08},
	}
	if !reflect.DeepEqual(gotTargetHistory, wantTargetHistory) {
		t.Fatalf("scan target history = %#v; want %#v", gotTargetHistory, wantTargetHistory)
	}
}

func TestStartDiscoveryScanLoop_NonEbusdTransportRejectsFullRangeScanByDefault(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origResultInfosFn := ebusdScanResultInfosFn
	origLoopExitFn := startupScanLoopExitFn
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		startupScanLoopExitFn = origLoopExitFn
	})

	var (
		mu                 sync.Mutex
		scanRun            int
		targetQueryHistory []string
		infoQueryHistory   []string
	)
	loopExited := make(chan struct{}, 1)
	registryScanFn = func(context.Context, registry.ScanBus, *registry.DeviceRegistry, byte, []byte) ([]registry.DeviceEntry, error) {
		mu.Lock()
		scanRun++
		mu.Unlock()
		return nil, nil
	}
	ebusdScanResultTargetsFn = func(_ context.Context, cfg ebusgateway.TransportConfig) ([]byte, error) {
		mu.Lock()
		targetQueryHistory = append(targetQueryHistory, cfg.Address)
		mu.Unlock()
		if cfg.Address != "127.0.0.1:8888" {
			return nil, nil
		}
		return []byte{0x04, 0x08}, nil
	}
	ebusdScanResultInfosFn = func(_ context.Context, cfg ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		mu.Lock()
		infoQueryHistory = append(infoQueryHistory, cfg.Address)
		mu.Unlock()
		if cfg.Address != "127.0.0.1:8888" {
			return nil, nil
		}
		return []registry.DeviceInfo{
			{Address: 0x04, Manufacturer: "Vaillant", DeviceID: "NETX3"},
			{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"},
		}, nil
	}
	startupScanLoopExitFn = func() {
		select {
		case loopExited <- struct{}{}:
		default:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.ScanInterval = 10 * time.Millisecond
	cfg.TransportConfig = ebusgateway.TransportConfig{
		Protocol: ebusgateway.TransportENS,
		Network:  "tcp",
		Address:  "127.0.0.1:19001",
	}

	signals := startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	mu.Lock()
	gotScanRun := scanRun
	gotTargetQueryHistory := append([]string(nil), targetQueryHistory...)
	gotInfoQueryHistory := append([]string(nil), infoQueryHistory...)
	mu.Unlock()

	if gotScanRun != 0 {
		t.Fatalf("registry scan runs = %d; want 0 because AD05 rejects default full-range on non-ebusd-tcp", gotScanRun)
	}

	if len(gotTargetQueryHistory) != 0 {
		t.Fatalf("scan-result target query history = %#v; want empty (non-ebusd-tcp must not query ebusd)", gotTargetQueryHistory)
	}

	if len(gotInfoQueryHistory) != 0 {
		t.Fatalf("scan-result info query history = %#v; want empty (non-ebusd-tcp must not query ebusd)", gotInfoQueryHistory)
	}

	select {
	case <-signals.firstPassDone:
	case <-time.After(2 * time.Second):
		t.Fatal("firstPassDone was not signaled after AD05 full-range rejection")
	}

	cancel()
	select {
	case <-loopExited:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan loop did not exit after cancellation")
	}
}

func TestStartDiscoveryScanLoop_ProxyObserveFirstKeepsSemanticBarrierUntilRootFallbackExhausted(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origTargetCandidatesFn := ebusdScanTargetCandidatesFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origResultInfosFn := ebusdScanResultInfosFn
	origProbeFn := startupScanB524ProbeFn
	origLoopExitFn := startupScanLoopExitFn
	origEnrichVaillantIdentityFn := enrichVaillantIdentityFn
	origEnrichSerialsFromEbusdFn := enrichSerialsFromEbusdFn
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		ebusdScanTargetCandidatesFn = origTargetCandidatesFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		startupScanB524ProbeFn = origProbeFn
		startupScanLoopExitFn = origLoopExitFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
	})

	var (
		mu            sync.Mutex
		scanRun       int
		targetHistory [][]byte
	)
	scanRuns := make(chan int, 3)
	loopExited := make(chan struct{}, 1)
	registryScanFn = func(_ context.Context, _ registry.ScanBus, reg *registry.DeviceRegistry, _ byte, targets []byte) ([]registry.DeviceEntry, error) {
		mu.Lock()
		scanRun++
		targetHistory = append(targetHistory, append([]byte(nil), targets...))
		currentRun := scanRun
		mu.Unlock()

		infos := []registry.DeviceInfo{
			{Address: 0x04, Manufacturer: "Vaillant", DeviceID: "NETX3"},
			{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"},
			{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"},
			{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"},
		}
		entries := make([]registry.DeviceEntry, 0, len(infos))
		for _, info := range infos {
			entries = append(entries, reg.Register(info))
		}

		select {
		case scanRuns <- currentRun:
		default:
		}
		return entries, nil
	}
	registryScanDirectedFn = registryScanFn
	ebusdScanTargetCandidatesFn = func(cfg ebusgateway.TransportConfig) []ebusgateway.TransportConfig {
		return []ebusgateway.TransportConfig{cfg}
	}
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		return []byte{0x04, 0x08, 0x15, 0x26}, nil
	}
	ebusdScanResultInfosFn = func(context.Context, ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		return nil, nil
	}
	startupScanB524ProbeFn = func(_ context.Context, _target, _opcode, _group, _instance byte, _addr uint16) bool {
		return false
	}
	startupScanLoopExitFn = func() {
		select {
		case loopExited <- struct{}{}:
		default:
		}
	}
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.ScanInterval = 10 * time.Millisecond
	cfg.BroadcastListen = true
	cfg.TransportConfig = ebusgateway.TransportConfig{
		Protocol: ebusgateway.TransportENS,
		Network:  "tcp",
		Address:  "127.0.0.1:19001",
	}

	signals := startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	select {
	case got := <-scanRuns:
		if got != 1 {
			t.Fatalf("first scan run = %d; want 1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not execute first restricted pass")
	}

	select {
	case <-signals.firstPassDone:
	case <-time.After(2 * time.Second):
		t.Fatal("firstPassDone was not signaled after first restricted pass")
	}

	select {
	case <-signals.semanticBootstrapReady:
		t.Fatal("semantic barrier released before bounded full-range retry completed")
	default:
	}

	select {
	case got := <-scanRuns:
		if got != 2 {
			t.Fatalf("second scan run = %d; want 2", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not execute post-recovery restricted confirmation pass")
	}

	select {
	case <-signals.semanticBootstrapReady:
	case <-time.After(2 * time.Second):
		t.Fatal("semantic barrier did not release after bounded root-aware fallback exhaustion")
	}

	select {
	case <-loopExited:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not stop after bounded root-aware fallback exhaustion")
	}

	mu.Lock()
	gotScanRun := scanRun
	gotTargetHistory := make([][]byte, len(targetHistory))
	for i := range targetHistory {
		gotTargetHistory[i] = append([]byte(nil), targetHistory[i]...)
	}
	mu.Unlock()

	if gotScanRun != 2 {
		t.Fatalf("registry scan runs = %d; want 2 restricted passes; the rejected full-range cycle must not touch the scan stub", gotScanRun)
	}

	wantTargetHistory := [][]byte{
		{0x04, 0x08, 0x15, 0x26},
		{0x04, 0x08, 0x15, 0x26},
	}
	if !reflect.DeepEqual(gotTargetHistory, wantTargetHistory) {
		t.Fatalf("scan target history = %#v; want %#v", gotTargetHistory, wantTargetHistory)
	}
}

// TestStartDiscoveryScanLoop_DirectScanSignalsBootstrapAfterConfirmationRetries verifies
// that adapter-direct scans (non-ebusd-tcp, no restricted targets) signal
// semanticBootstrapReady after two consecutive confirmation failures instead of
// looping indefinitely.  This is the regression scenario from PR #481: the scan
// finds 4 Vaillant devices but the B524 coherent root probe fails under bus
// contention, leaving confirmationPending=true with no ebusd-specific fallback.
func TestStartDiscoveryScanLoop_DirectScanSignalsBootstrapAfterConfirmationRetries(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origResultInfosFn := ebusdScanResultInfosFn
	origProbeFn := startupScanB524ProbeFn
	origLoopExitFn := startupScanLoopExitFn
	origEnrichVaillantIdentityFn := enrichVaillantIdentityFn
	origEnrichSerialsFromEbusdFn := enrichSerialsFromEbusdFn
	origEvidenceFn := evidenceHasVaillantRootFn
	// Bypass AD05 full-range guard: the test mocks the scan response with
	// Vaillant devices, so the diagnostic-flag retry would be authorised
	// in practice. Override the evidenceHasVaillantRootFn to always return
	// true so adapter-direct → JoinCapable + nil targets does not block.
	evidenceHasVaillantRootFn = func(*registry.DeviceRegistry) bool { return true }
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		startupScanB524ProbeFn = origProbeFn
		startupScanLoopExitFn = origLoopExitFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
		evidenceHasVaillantRootFn = origEvidenceFn
	})

	var (
		mu      sync.Mutex
		scanRun int
	)
	loopExited := make(chan struct{}, 1)
	// Every scan pass returns 4 Vaillant devices (mimicking a scan that
	// finds devices before the ScanTimeout context expires).
	registryScanFn = func(_ context.Context, scanBus registry.ScanBus, reg *registry.DeviceRegistry, _ byte, _ []byte) ([]registry.DeviceEntry, error) {
		stats, ok := scanBus.(*statsBus)
		if ok {
			stats.stats.ok = 20
			stats.stats.timeouts = 173
		}
		mu.Lock()
		scanRun++
		mu.Unlock()

		infos := []registry.DeviceInfo{
			{Address: 0x04, Manufacturer: "Vaillant", DeviceID: "NETX3"},
			{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"},
			{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"},
			{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"},
		}
		entries := make([]registry.DeviceEntry, 0, len(infos))
		for _, info := range infos {
			entries = append(entries, reg.Register(info))
		}
		return entries, nil
	}
	// Non-ebusd-tcp: these should never be called.
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		t.Fatal("ebusd scan result targets should not be queried for direct transport")
		return nil, nil
	}
	ebusdScanResultInfosFn = func(context.Context, ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		t.Fatal("ebusd scan result infos should not be queried for direct transport")
		return nil, nil
	}
	// B524 probe always fails — simulates bus contention preventing
	// coherent root discovery.
	startupScanB524ProbeFn = func(context.Context, byte, byte, byte, byte, uint16) bool {
		return false
	}
	startupScanLoopExitFn = func() {
		select {
		case loopExited <- struct{}{}:
		default:
		}
	}
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.ScanInterval = 10 * time.Millisecond
	// Adapter-direct transport (not ebusd-tcp).
	cfg.TransportConfig = ebusgateway.TransportConfig{
		Protocol: ebusgateway.TransportAdapterDirect,
		Network:  "tcp",
		Address:  "127.0.0.1:9999",
	}
	// Enable AD05 diagnostic full-range retry: post-cruise-#20, adapter-
	// direct classifies as JoinCapable so nil-target scans require both
	// the diag flag AND evidenceHasVaillantRootFn=true (set above in
	// test setup) to bypass the guard. Without these, the test loop never
	// runs because the guard rejects.
	cfg.DiagnosticFullRangeRetry = true

	signals := startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	select {
	case <-signals.semanticBootstrapReady:
	case <-time.After(5 * time.Second):
		t.Fatal("semanticBootstrapReady was never signaled — direct-scan confirmation exhaustion did not fire")
	}

	select {
	case <-loopExited:
	case <-time.After(2 * time.Second):
		t.Fatal("scan loop did not exit after bootstrap signal")
	}

	mu.Lock()
	gotScanRun := scanRun
	mu.Unlock()

	// First pass discovers Vaillant devices, confirmation pending.
	// Second pass: still pending, directScanConfirmationRetries reaches 2, exhaustion fires.
	if gotScanRun < 2 {
		t.Fatalf("scan runs = %d; want >= 2 (need two passes for direct-scan confirmation exhaustion)", gotScanRun)
	}
	if gotScanRun > 3 {
		t.Fatalf("scan runs = %d; want <= 3 (should stop after confirmation exhaustion, not loop indefinitely)", gotScanRun)
	}
}

// TestStartDiscoveryScanLoop_DirectScanTimeoutStillSignalsBootstrap verifies
// that adapter-direct scans signal semanticBootstrapReady even when every scan
// pass returns (nil, context.DeadlineExceeded) — the production scenario where
// devices are registered during the scan but the scan times out before
// completing all targets, returning a nil device slice.  The existing
// DirectScanSignalsBootstrapAfterConfirmationRetries test uses a scan stub
// that returns non-nil entries; this test reproduces the exact production
// behavior where len(devices)==0 but total>0.
func TestStartDiscoveryScanLoop_DirectScanTimeoutStillSignalsBootstrap(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origResultInfosFn := ebusdScanResultInfosFn
	origProbeFn := startupScanB524ProbeFn
	origLoopExitFn := startupScanLoopExitFn
	origEnrichVaillantIdentityFn := enrichVaillantIdentityFn
	origEnrichSerialsFromEbusdFn := enrichSerialsFromEbusdFn
	origEvidenceFn := evidenceHasVaillantRootFn
	// Bypass AD05 full-range guard: the test mocks the scan response with
	// Vaillant devices, so the diagnostic-flag retry would be authorised
	// in practice. Override the evidenceHasVaillantRootFn to always return
	// true so adapter-direct → JoinCapable + nil targets does not block.
	evidenceHasVaillantRootFn = func(*registry.DeviceRegistry) bool { return true }
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		startupScanB524ProbeFn = origProbeFn
		startupScanLoopExitFn = origLoopExitFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
		evidenceHasVaillantRootFn = origEvidenceFn
	})

	var (
		mu      sync.Mutex
		scanRun int
	)
	loopExited := make(chan struct{}, 1)
	// Mimic production: scan registers devices in the registry but returns
	// (nil, context.DeadlineExceeded) — exactly what happens when the scan
	// timeout expires mid-way through the address range.
	registryScanFn = func(_ context.Context, scanBus registry.ScanBus, reg *registry.DeviceRegistry, _ byte, _ []byte) ([]registry.DeviceEntry, error) {
		stats, ok := scanBus.(*statsBus)
		if ok {
			stats.stats.ok = 20
			stats.stats.timeouts = 174
		}
		mu.Lock()
		scanRun++
		mu.Unlock()

		infos := []registry.DeviceInfo{
			{Address: 0x04, Manufacturer: "Vaillant", DeviceID: "NETX3"},
			{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"},
			{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"},
			{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"},
		}
		for _, info := range infos {
			reg.Register(info)
		}
		// Return nil entries + deadline error, exactly like production.
		return nil, context.DeadlineExceeded
	}
	// Non-ebusd-tcp: these should never be called.
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		t.Fatal("ebusd scan result targets should not be queried for direct transport")
		return nil, nil
	}
	ebusdScanResultInfosFn = func(context.Context, ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		t.Fatal("ebusd scan result infos should not be queried for direct transport")
		return nil, nil
	}
	// B524 probe always fails — simulates bus contention.
	startupScanB524ProbeFn = func(context.Context, byte, byte, byte, byte, uint16) bool {
		return false
	}
	startupScanLoopExitFn = func() {
		select {
		case loopExited <- struct{}{}:
		default:
		}
	}
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.ScanInterval = 10 * time.Millisecond
	cfg.TransportConfig = ebusgateway.TransportConfig{
		Protocol: ebusgateway.TransportAdapterDirect,
		Network:  "tcp",
		Address:  "127.0.0.1:9999",
	}
	// Enable AD05 diagnostic full-range retry: post-cruise-#20, adapter-
	// direct classifies as JoinCapable so nil-target scans require both
	// the diag flag AND evidenceHasVaillantRootFn=true (set above in
	// test setup) to bypass the guard. Without these, the test loop never
	// runs because the guard rejects.
	cfg.DiagnosticFullRangeRetry = true

	signals := startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	select {
	case <-signals.semanticBootstrapReady:
	case <-time.After(5 * time.Second):
		t.Fatal("semanticBootstrapReady was never signaled — direct-scan confirmation exhaustion did not fire when scan returns deadline-exceeded")
	}

	select {
	case <-loopExited:
	case <-time.After(2 * time.Second):
		t.Fatal("scan loop did not exit after bootstrap signal")
	}

	mu.Lock()
	gotScanRun := scanRun
	mu.Unlock()

	if gotScanRun < 2 {
		t.Fatalf("scan runs = %d; want >= 2 (need two passes for direct-scan confirmation exhaustion)", gotScanRun)
	}
	if gotScanRun > 3 {
		t.Fatalf("scan runs = %d; want <= 3 (should stop after confirmation exhaustion, not loop indefinitely)", gotScanRun)
	}
}

// TestStartDiscoveryScanLoop_SafetyNetForcesBootstrapAfterMaxUnconfirmedPasses
// exercises the scanPassesWithDevices safety net.  The B524 probe alternates
// between success and failure on consecutive passes — this prevents the
// directScanConfirmationRetries counter from reaching 2 because it resets on
// each satisfied pass.  The safety net must fire after
// startupScanMaxUnconfirmedPasses consecutive unsatisfied passes.
func TestStartDiscoveryScanLoop_SafetyNetForcesBootstrapAfterMaxUnconfirmedPasses(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origRegistryScanFn := registryScanFn
	origResultTargetsFn := ebusdScanResultTargetsFn
	origResultInfosFn := ebusdScanResultInfosFn
	origProbeFn := startupScanB524ProbeFn
	origLoopExitFn := startupScanLoopExitFn
	origEnrichVaillantIdentityFn := enrichVaillantIdentityFn
	origEnrichSerialsFromEbusdFn := enrichSerialsFromEbusdFn
	origEvidenceFn := evidenceHasVaillantRootFn
	// Bypass AD05 full-range guard: the test mocks the scan response with
	// Vaillant devices, so the diagnostic-flag retry would be authorised
	// in practice. Override the evidenceHasVaillantRootFn to always return
	// true so adapter-direct → JoinCapable + nil targets does not block.
	evidenceHasVaillantRootFn = func(*registry.DeviceRegistry) bool { return true }
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		startupScanB524ProbeFn = origProbeFn
		startupScanLoopExitFn = origLoopExitFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
		evidenceHasVaillantRootFn = origEvidenceFn
	})

	var (
		mu      sync.Mutex
		scanRun int
	)
	loopExited := make(chan struct{}, 1)
	registryScanFn = func(_ context.Context, scanBus registry.ScanBus, reg *registry.DeviceRegistry, _ byte, _ []byte) ([]registry.DeviceEntry, error) {
		stats, ok := scanBus.(*statsBus)
		if ok {
			stats.stats.ok = 20
			stats.stats.timeouts = 174
		}
		mu.Lock()
		scanRun++
		mu.Unlock()

		infos := []registry.DeviceInfo{
			{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"},
			{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"},
			{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"},
		}
		for _, info := range infos {
			reg.Register(info)
		}
		return nil, context.DeadlineExceeded
	}
	ebusdScanResultTargetsFn = func(context.Context, ebusgateway.TransportConfig) ([]byte, error) {
		t.Fatal("ebusd scan result targets should not be queried for direct transport")
		return nil, nil
	}
	ebusdScanResultInfosFn = func(context.Context, ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
		t.Fatal("ebusd scan result infos should not be queried for direct transport")
		return nil, nil
	}
	// B524 probe always fails.  This prevents confirmationSatisfied from
	// ever being true, so the directScanConfirmationRetries path fires
	// (retries >= 2).  The safety net at startupScanMaxUnconfirmedPasses
	// is a broader backstop but in this scenario the directScan path
	// fires first.
	startupScanB524ProbeFn = func(context.Context, byte, byte, byte, byte, uint16) bool {
		return false
	}
	startupScanLoopExitFn = func() {
		select {
		case loopExited <- struct{}{}:
		default:
		}
	}
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {}
	enrichSerialsFromEbusdFn = func(context.Context, *registry.DeviceRegistry, ebusgateway.TransportConfig) {}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanOnStart = true
	cfg.ScanInterval = 10 * time.Millisecond
	cfg.TransportConfig = ebusgateway.TransportConfig{
		Protocol: ebusgateway.TransportAdapterDirect,
		Network:  "tcp",
		Address:  "127.0.0.1:9999",
	}
	// Enable AD05 diagnostic full-range retry: post-cruise-#20, adapter-
	// direct classifies as JoinCapable so nil-target scans require both
	// the diag flag AND evidenceHasVaillantRootFn=true (set above in
	// test setup) to bypass the guard. Without these, the test loop never
	// runs because the guard rejects.
	cfg.DiagnosticFullRangeRetry = true

	signals := startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	select {
	case <-signals.semanticBootstrapReady:
	case <-time.After(5 * time.Second):
		t.Fatal("semanticBootstrapReady was never signaled — neither directScan exhaustion nor safety net fired")
	}

	select {
	case <-loopExited:
	case <-time.After(2 * time.Second):
		t.Fatal("scan loop did not exit after bootstrap signal")
	}

	mu.Lock()
	gotScanRun := scanRun
	mu.Unlock()

	// With the directScanConfirmationRetries path, it fires after 2 passes.
	// The safety net at 5 passes is a broader fallback.  In this test the
	// directScan path fires first, so we expect 2-3 passes.
	if gotScanRun < 2 {
		t.Fatalf("scan runs = %d; want >= 2", gotScanRun)
	}
	// If the directScan retry path did not fire and only the safety net
	// stopped the loop, gotScanRun would be >= startupScanMaxUnconfirmedPasses.
	// Either way, the loop must stop within startupScanMaxUnconfirmedPasses+1.
	if gotScanRun > startupScanMaxUnconfirmedPasses+1 {
		t.Fatalf("scan runs = %d; want <= %d (loop should stop before safety-net limit is far exceeded)",
			gotScanRun, startupScanMaxUnconfirmedPasses+1)
	}
}

// TestCoherentVaillantRootProbeTimeoutScalesWithCandidates verifies that the
// probe context timeout in startupScanHasCoherentVaillantRoot scales with the
// number of registry candidates rather than using a single per-request timeout
// for all probes combined.
func TestCoherentVaillantRootProbeTimeoutScalesWithCandidates(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("gateway.New error = %v", err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("gateway.Close error = %v", err)
		}
	})

	origProbeFn := startupScanB524ProbeFn
	t.Cleanup(func() { startupScanB524ProbeFn = origProbeFn })

	// Register 4 Vaillant devices.
	infos := []registry.DeviceInfo{
		{Address: 0x04, Manufacturer: "Vaillant", DeviceID: "NETX3"},
		{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI00"},
		{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"},
		{Address: 0x26, Manufacturer: "Vaillant", DeviceID: "VR_71"},
	}
	for _, info := range infos {
		gateway.Registry.Register(info)
	}

	// The probe sleeps slightly under the per-request timeout then returns
	// true only for 0x15.  With the old single-timeout approach, the first
	// candidate's slow probe would exhaust the outer context before 0x15
	// was reached.
	perRequest := 200 * time.Millisecond
	startupScanB524ProbeFn = func(ctx context.Context, target, _opcode, _group, _instance byte, _addr uint16) bool {
		delay := perRequest - 20*time.Millisecond
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
		}
		return target == 0x15
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.SemanticRequestTimeout = perRequest
	cfg.ScanRequestTimeout = 50 * time.Millisecond

	result := startupScanHasCoherentVaillantRoot(context.Background(), cfg, gateway)
	if !result {
		t.Fatal("startupScanHasCoherentVaillantRoot returned false; want true — probe timeout should scale with candidate count")
	}
}
