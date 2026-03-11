package main

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
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
		want                  bool
	}{
		{name: "no devices", total: 0, want: false},
		{name: "normal direct inventory stops", total: 1, want: true},
		{name: "imported inventory keeps scanning while confirmation is pending", total: 4, confirmationPending: true, want: false},
		{name: "imported inventory stops once confirmation resolves", total: 4, confirmationPending: true, confirmationSatisfied: true, want: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldStopDiscoveryScan(test.total, test.confirmationPending, test.confirmationSatisfied); got != test.want {
				t.Fatalf(
					"shouldStopDiscoveryScan(%d, pending=%v, satisfied=%v) = %v; want %v",
					test.total,
					test.confirmationPending,
					test.confirmationSatisfied,
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

func TestEbusdScanTargetCandidates(t *testing.T) {
	t.Parallel()

	t.Run("ebusd tcp transport prefers configured endpoint and adds local fallback", func(t *testing.T) {
		t.Parallel()

		candidates := ebusdScanTargetCandidates(ebusgateway.TransportConfig{
			Protocol:     ebusgateway.TransportEbusdTCP,
			Network:      "tcp",
			Address:      "192.168.100.4:8888",
			DialTimeout:  5 * time.Second,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
		})
		if len(candidates) != 2 {
			t.Fatalf("candidate count = %d; want 2", len(candidates))
		}
		if got := candidates[0].Address; got != "192.168.100.4:8888" {
			t.Fatalf("candidate[0].Address = %q; want %q", got, "192.168.100.4:8888")
		}
		if got := candidates[1].Address; got != "127.0.0.1:8888" {
			t.Fatalf("candidate[1].Address = %q; want %q", got, "127.0.0.1:8888")
		}
		if got := candidates[1].DialTimeout; got != 2*time.Second {
			t.Fatalf("fallback dial timeout = %s; want 2s", got)
		}
	})

	t.Run("non ebusd transport still includes local fallback", func(t *testing.T) {
		t.Parallel()

		candidates := ebusdScanTargetCandidates(ebusgateway.TransportConfig{
			Protocol:    ebusgateway.TransportENS,
			Network:     "tcp",
			Address:     "127.0.0.1:19001",
			DialTimeout: 500 * time.Millisecond,
		})
		if len(candidates) != 1 {
			t.Fatalf("candidate count = %d; want 1", len(candidates))
		}
		if got := candidates[0].Address; got != "127.0.0.1:8888" {
			t.Fatalf("candidate[0].Address = %q; want %q", got, "127.0.0.1:8888")
		}
		if got := candidates[0].DialTimeout; got != 500*time.Millisecond {
			t.Fatalf("fallback dial timeout = %s; want 500ms", got)
		}
	})

	t.Run("duplicate local endpoint is not appended", func(t *testing.T) {
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
	origLoopExitFn := startupScanLoopExitFn
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
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

	firstPassDone := startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	select {
	case got := <-scanSourceCh:
		if got != proxyObserveFirstStartupSource {
			t.Fatalf("registry scan source = 0x%02X; want 0x%02X", got, proxyObserveFirstStartupSource)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("registry scan was not invoked")
	}

	select {
	case <-firstPassDone:
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
		t.Fatal("startup discovery scan did not complete a pass")
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
			t.Fatalf("enrichment calls after normal scan = vaillant:%d ebusd:%d; want both >= 1", gotVaillant, gotEbusd)
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
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		ebusdScanTargetCandidatesFn = origTargetCandidatesFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		startupScanB524ProbeFn = origProbeFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
	})

	var (
		mu                    sync.Mutex
		preloadRun            int
		scanRun               int
		probeCtxErr           error
		unexpectedPreloadRuns int
	)
	preloadEnrichDone := make(chan struct{}, 1)
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
	restoreGlobals := func() {
		registryScanFn = origRegistryScanFn
		ebusdScanTargetCandidatesFn = origTargetCandidatesFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		startupScanB524ProbeFn = origProbeFn
		startupScanLoopExitFn = origLoopExitFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
	}

	var (
		mu            sync.Mutex
		preloadRun    int
		scanRun       int
		scanCtxErr    error
		targetHistory [][]byte
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
	origLoopExitFn := startupScanLoopExitFn
	origEnrichVaillantIdentityFn := enrichVaillantIdentityFn
	origEnrichSerialsFromEbusdFn := enrichSerialsFromEbusdFn
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		ebusdScanTargetCandidatesFn = origTargetCandidatesFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
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

func TestStartDiscoveryScanLoop_LocalFallbackImportRetriesFullRangeThenRestrictedConfirmation(t *testing.T) {
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
	t.Cleanup(func() {
		registryScanFn = origRegistryScanFn
		ebusdScanResultTargetsFn = origResultTargetsFn
		ebusdScanResultInfosFn = origResultInfosFn
		startupScanB524ProbeFn = origProbeFn
		startupScanLoopExitFn = origLoopExitFn
		enrichVaillantIdentityFn = origEnrichVaillantIdentityFn
		enrichSerialsFromEbusdFn = origEnrichSerialsFromEbusdFn
	})

	var (
		mu                 sync.Mutex
		scanRun            int
		scanCtxErr         error
		targetHistory      [][]byte
		targetQueryHistory []string
		infoQueryHistory   []string
	)
	activeSuccess := make(chan struct{}, 1)
	loopExited := make(chan struct{}, 1)
	registryScanFn = func(scanCtx context.Context, scanBus registry.ScanBus, reg *registry.DeviceRegistry, _ byte, targets []byte) ([]registry.DeviceEntry, error) {
		if err := scanCtx.Err(); err != nil {
			mu.Lock()
			if scanCtxErr == nil {
				scanCtxErr = err
			}
			mu.Unlock()
			return nil, err
		}

		stats, ok := scanBus.(*statsBus)
		if !ok {
			t.Fatalf("startup scan bus missing stats wrapper")
		}

		mu.Lock()
		scanRun++
		targetHistory = append(targetHistory, append([]byte(nil), targets...))
		currentRun := scanRun
		mu.Unlock()

		switch currentRun {
		case 1:
			stats.stats.timeouts = 1
			return nil, nil
		case 2:
			return nil, nil
		case 3:
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
		default:
			t.Fatalf("unexpected registry scan run %d", currentRun)
			return nil, nil
		}
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
		Protocol: ebusgateway.TransportENS,
		Network:  "tcp",
		Address:  "127.0.0.1:19001",
	}

	startDiscoveryScanLoop(ctx, cfg, gateway, nil)

	select {
	case <-activeSuccess:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not complete restricted follow-up confirmation")
	}

	select {
	case <-loopExited:
	case <-time.After(2 * time.Second):
		t.Fatal("startup discovery scan did not stop after restricted follow-up confirmation")
	}

	mu.Lock()
	gotScanRun := scanRun
	gotScanCtxErr := scanCtxErr
	gotTargetHistory := make([][]byte, len(targetHistory))
	for i := range targetHistory {
		gotTargetHistory[i] = append([]byte(nil), targetHistory[i]...)
	}
	gotTargetQueryHistory := append([]string(nil), targetQueryHistory...)
	gotInfoQueryHistory := append([]string(nil), infoQueryHistory...)
	mu.Unlock()

	if gotScanRun != 3 {
		t.Fatalf("registry scan runs = %d; want 3 (restricted timeout/import, bounded full-range retry, restricted success)", gotScanRun)
	}
	if gotScanCtxErr != nil {
		t.Fatalf("registry scan received canceled context during non-ebusd fallback confirmation flow: %v", gotScanCtxErr)
	}

	wantTargetHistory := [][]byte{
		{0x04, 0x08},
		nil,
		{0x04, 0x08},
	}
	if !reflect.DeepEqual(gotTargetHistory, wantTargetHistory) {
		t.Fatalf("scan target history = %#v; want %#v", gotTargetHistory, wantTargetHistory)
	}

	wantTargetQueryHistory := []string{"127.0.0.1:8888", "127.0.0.1:8888"}
	if !reflect.DeepEqual(gotTargetQueryHistory, wantTargetQueryHistory) {
		t.Fatalf("scan-result target query history = %#v; want %#v", gotTargetQueryHistory, wantTargetQueryHistory)
	}

	wantInfoQueryHistory := []string{"127.0.0.1:8888"}
	if !reflect.DeepEqual(gotInfoQueryHistory, wantInfoQueryHistory) {
		t.Fatalf("scan-result info query history = %#v; want %#v", gotInfoQueryHistory, wantInfoQueryHistory)
	}
}
