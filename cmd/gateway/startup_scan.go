package main

import (
	"bufio"
	"context"
	"errors"
	"expvar"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type timeoutBus struct {
	bus     registry.ScanBus
	timeout time.Duration
}

func (b *timeoutBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	if b == nil || b.bus == nil {
		return nil, fmt.Errorf("scan timeout bus missing")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if b.timeout <= 0 {
		return b.bus.Send(ctx, frame)
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= b.timeout {
			return b.bus.Send(ctx, frame)
		}
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	return b.bus.Send(ctxTimeout, frame)
}

type scanStats struct {
	ok         int
	timeouts   int
	collisions int
	nacks      int
	crcErrors  int
	otherErrs  int
}

type startupScanSignals struct {
	firstPassDone          <-chan struct{}
	semanticBootstrapReady <-chan struct{}
}

type ebusdScanResultRow struct {
	Address         byte
	Manufacturer    string
	DeviceID        string
	SoftwareVersion string
	HardwareVersion string
	SerialNumber    string
}

type statsBus struct {
	bus   registry.ScanBus
	stats scanStats
}

var (
	semanticBusCollisionsTotal  = expvar.NewInt("semantic_bus_collisions_total")
	registryScanFn              = registry.Scan
	ebusdScanTargetCandidatesFn = ebusdScanTargetCandidates
	ebusdScanResultTargetsFn    = ebusdScanResultTargets
	ebusdScanResultInfosFn      = ebusdScanResultInfos
	startupScanB524ProbeFn      func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool
	startupScanLoopExitFn       func()
	enrichVaillantIdentityFn    = enrichVaillantIdentity
	enrichSerialsFromEbusdFn    = enrichSerialsFromEbusd
	postStartupIdentityRetryFn  = schedulePostStartupIdentityRetry
)

const proxyObserveFirstStartupSource byte = 0xF7

var (
	postStartupIdentityRetryDelay    = 5 * time.Second
	postStartupIdentityRetryAttempts = 3
)

func (b *statsBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	if b == nil || b.bus == nil {
		return nil, fmt.Errorf("scan stats bus missing")
	}
	response, err := b.bus.Send(ctx, frame)
	if err == nil {
		b.stats.ok++
		return response, nil
	}

	switch {
	case errors.Is(err, ebuserrors.ErrTimeout) || errors.Is(err, context.DeadlineExceeded):
		b.stats.timeouts++
	case errors.Is(err, ebuserrors.ErrBusCollision):
		b.stats.collisions++
		semanticBusCollisionsTotal.Add(1)
		log.Printf("semantic_bus_collision total=%d", semanticBusCollisionsTotal.Value())
	case errors.Is(err, ebuserrors.ErrNACK):
		b.stats.nacks++
	case errors.Is(err, ebuserrors.ErrCRCMismatch):
		b.stats.crcErrors++
	default:
		b.stats.otherErrs++
	}
	return response, err
}

func startDiscoveryScanLoop(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, builder *graphql.Builder) startupScanSignals {
	firstPassDone := make(chan struct{})
	var firstPassOnce sync.Once
	signalFirstPassDone := func() {
		firstPassOnce.Do(func() {
			close(firstPassDone)
		})
	}
	semanticBootstrapReady := make(chan struct{})
	var semanticBootstrapReadyOnce sync.Once
	signalSemanticBootstrapReady := func() {
		semanticBootstrapReadyOnce.Do(func() {
			close(semanticBootstrapReady)
		})
	}

	if !cfg.ScanOnStart || gateway == nil || gateway.Bus == nil || gateway.Registry == nil {
		signalFirstPassDone()
		signalSemanticBootstrapReady()
		return startupScanSignals{
			firstPassDone:          firstPassDone,
			semanticBootstrapReady: semanticBootstrapReady,
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	startupCfg := resolveStartupScanSourceConfig(cfg)
	loopExitFn := startupScanLoopExitFn
	scanFn := registryScanFn
	targetCandidatesFn := ebusdScanTargetCandidatesFn
	resultTargetsFn := ebusdScanResultTargetsFn
	resultInfosFn := ebusdScanResultInfosFn
	enrichIdentityFn := enrichVaillantIdentityFn
	enrichSerialsFn := enrichSerialsFromEbusdFn

	interval := cfg.ScanInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	go func() {
		defer func() {
			signalFirstPassDone()
			signalSemanticBootstrapReady()
			if loopExitFn != nil {
				loopExitFn()
			}
		}()
		previousTotal := 0
		forceFullRangeNextPass := false
		confirmationPending := false
		fullRangeRecoveryAttempted := false
		restrictedConfirmationAfterRecoveryPending := false
		delayedIdentityRetryScheduled := false
		for {
			scanCtx := ctx
			cancel := func() {}
			if cfg.ScanTimeout > 0 {
				scanCtx, cancel = context.WithTimeout(ctx, cfg.ScanTimeout)
			}
			scanBus := &statsBus{
				bus: &timeoutBus{bus: gateway.Bus, timeout: cfg.ScanRequestTimeout},
			}
			targets := ([]byte)(nil)
			targetLabel := ""
			var targetConfig *ebusgateway.TransportConfig
			retryingFullRange := forceFullRangeNextPass
			forceFullRangeNextPass = false
			candidates := targetCandidatesFn(cfg.TransportConfig)
			if retryingFullRange {
				if len(candidates) > 0 {
					candidateCopy := candidates[0]
					targetConfig = &candidateCopy
				}
				log.Printf("startup scan: retrying with full target range after partial ebusd inventory")
			} else {
				for _, candidate := range candidates {
					scanTargets, err := resultTargetsFn(scanCtx, candidate)
					if err != nil || len(scanTargets) == 0 {
						continue
					}
					targets = scanTargets
					targetLabel = candidate.Address
					candidateCopy := candidate
					targetConfig = &candidateCopy
					break
				}
			}
			if len(targets) > 0 {
				log.Printf("startup scan: using %d target(s) from ebusd scan result at %s", len(targets), targetLabel)
			}
			usedRestrictedTargets := len(targets) > 0

			if cfg.TransportConfig.Protocol == ebusgateway.TransportEbusdTCP && targetConfig != nil && !retryingFullRange {
				infos, infoErr := resultInfosFn(scanCtx, *targetConfig)
				if infoErr != nil {
					log.Printf("startup scan preload error: %v", infoErr)
				} else if len(infos) > 0 {
					beforeTotal := countRegistryDevices(gateway.Registry)
					for _, info := range infos {
						gateway.Registry.Register(info)
					}
					total := countRegistryDevices(gateway.Registry)
					imported := total - beforeTotal
					if imported < 0 {
						imported = 0
					}
					log.Printf("startup scan preload: imported=%d total=%d (ebusd-tcp)", imported, total)

					enrichIdentityFn(ctx, gateway, startupCfg)
					if targetConfig != nil {
						enrichSerialsFn(ctx, gateway.Registry, *targetConfig)
					}
					delayedIdentityRetryScheduled = scheduleDelayedIdentityRetryIfNeeded(
						ctx,
						gateway,
						builder,
						startupCfg,
						targetConfig,
						delayedIdentityRetryScheduled,
					)

					if total > 0 && total != previousTotal {
						previousTotal = total
						gateway.RefreshRouterPlanes()
						if builder != nil {
							if err := builder.Rebuild(); err != nil {
								log.Printf("graphql schema rebuild failed after scan preload: %v", err)
							}
						}
					}

					requiresRootAwareConfirmation := startupScanHasVaillantInventory(gateway)
					confirmationSatisfied := startupScanConfirmationSatisfied(ctx, startupCfg, gateway, total, false)
					confirmationPending, fullRangeRecoveryAttempted = updateStartupScanConfirmationState(
						total,
						imported,
						confirmationPending,
						fullRangeRecoveryAttempted,
						confirmationSatisfied,
						requiresRootAwareConfirmation,
					)

					if confirmationPending && usedRestrictedTargets && !retryingFullRange && !fullRangeRecoveryAttempted &&
						shouldRetryDiscoveryWithFullRange(ctx, startupCfg, gateway, usedRestrictedTargets, retryingFullRange) {
						forceFullRangeNextPass = true
						fullRangeRecoveryAttempted = true
						restrictedConfirmationAfterRecoveryPending = true
						cancel()
						timer := time.NewTimer(interval)
						select {
						case <-ctx.Done():
							timer.Stop()
							return
						case <-timer.C:
						}
						continue
					} else if shouldStopDiscoveryScan(total, confirmationPending, confirmationSatisfied, false) {
						signalSemanticBootstrapReady()
						signalFirstPassDone()
						cancel()
						return
					} else if confirmationPending && usedRestrictedTargets && !retryingFullRange && !confirmationSatisfied {
						// Keep the preload inventory, but still allow a restricted active scan pass
						// whenever confirmation remains unresolved. This covers both:
						// - bounded recovery already consumed, and
						// - non-Vaillant preload imports that cannot justify a full-range retry.
					} else {
						signalFirstPassDone()
						cancel()
						timer := time.NewTimer(interval)
						select {
						case <-ctx.Done():
							timer.Stop()
							return
						case <-timer.C:
						}
						continue
					}
				}
			}

			devices, err := scanFn(scanCtx, scanBus, gateway.Registry, startupCfg.ScanSource, targets)

			if err != nil && ctx.Err() == nil {
				log.Printf("startup scan error: %v", err)
			}

			total := countRegistryDevices(gateway.Registry)
			imported := 0
			didIdentityEnrich := false
			if total == 0 && len(devices) == 0 && targetConfig != nil &&
				scanBus.stats.ok == 0 && (scanBus.stats.timeouts > 0 || scanBus.stats.collisions > 0) {
				infos, infoErr := resultInfosFn(scanCtx, *targetConfig)
				if infoErr != nil {
					log.Printf("startup scan fallback error: %v", infoErr)
				} else if len(infos) > 0 {
					for _, info := range infos {
						gateway.Registry.Register(info)
					}
					imported = len(infos)
					total = countRegistryDevices(gateway.Registry)
				}
			}
			if imported > 0 {
				log.Printf("startup scan fallback: imported %d device(s) from ebusd scan result", imported)
				enrichIdentityFn(ctx, gateway, startupCfg)
				didIdentityEnrich = true
			}
			log.Printf("startup scan: pass=%d device(s), total=%d", len(devices), total)
			log.Printf(
				"startup scan stats: ok=%d timeouts=%d collisions=%d nacks=%d crc=%d other=%d",
				scanBus.stats.ok,
				scanBus.stats.timeouts,
				scanBus.stats.collisions,
				scanBus.stats.nacks,
				scanBus.stats.crcErrors,
				scanBus.stats.otherErrs,
			)
			cancel()
			signalFirstPassDone()

			if total > 0 {
				// Normal direct scans can still miss B5.09 identity chunks on the first pass.
				// Retry the physical-device enrichment once before GraphQL/device consumers stabilize.
				if !didIdentityEnrich {
					enrichIdentityFn(ctx, gateway, startupCfg)
				}
				if targetConfig != nil {
					enrichSerialsFn(ctx, gateway.Registry, *targetConfig)
				}
				delayedIdentityRetryScheduled = scheduleDelayedIdentityRetryIfNeeded(
					ctx,
					gateway,
					builder,
					startupCfg,
					targetConfig,
					delayedIdentityRetryScheduled,
				)
			}

			if total > 0 && total != previousTotal {
				previousTotal = total
				gateway.RefreshRouterPlanes()
				if builder != nil {
					if err := builder.Rebuild(); err != nil {
						log.Printf("graphql schema rebuild failed after scan: %v", err)
					}
				}
			}

			activeConfirmed := len(devices) > 0 || scanBus.stats.ok > 0
			requiresRootAwareConfirmation := startupScanHasVaillantInventory(gateway)
			confirmationSatisfied := startupScanConfirmationSatisfied(ctx, startupCfg, gateway, total, activeConfirmed)
			confirmationPending, fullRangeRecoveryAttempted = updateStartupScanConfirmationState(
				total,
				imported,
				confirmationPending,
				fullRangeRecoveryAttempted,
				confirmationSatisfied,
				requiresRootAwareConfirmation,
			)
			confirmationFallbackExhausted := confirmationPending &&
				!confirmationSatisfied &&
				usedRestrictedTargets &&
				!retryingFullRange &&
				restrictedConfirmationAfterRecoveryPending
			if confirmationSatisfied || confirmationFallbackExhausted {
				restrictedConfirmationAfterRecoveryPending = false
			}

			if confirmationPending && usedRestrictedTargets && !retryingFullRange && !fullRangeRecoveryAttempted &&
				shouldRetryDiscoveryWithFullRange(ctx, startupCfg, gateway, usedRestrictedTargets, retryingFullRange) {
				forceFullRangeNextPass = true
				fullRangeRecoveryAttempted = true
				restrictedConfirmationAfterRecoveryPending = true
			} else if shouldStopDiscoveryScan(total, confirmationPending, confirmationSatisfied, confirmationFallbackExhausted) {
				signalSemanticBootstrapReady()
				return
			}

			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return startupScanSignals{
		firstPassDone:          firstPassDone,
		semanticBootstrapReady: semanticBootstrapReady,
	}
}

func startupScanHasMissingVaillantSerials(reg *registry.DeviceRegistry) bool {
	if reg == nil {
		return false
	}

	missing := false
	reg.Iterate(func(entry registry.DeviceEntry) bool {
		if entry == nil {
			return true
		}
		if !strings.EqualFold(entry.Manufacturer(), "Vaillant") {
			return true
		}
		if strings.TrimSpace(entry.SerialNumber()) != "" {
			return true
		}
		missing = true
		return false
	})
	return missing
}

func scheduleDelayedIdentityRetryIfNeeded(ctx context.Context, gateway *ebusgateway.Gateway, builder *graphql.Builder, cfg ebusgateway.Config, targetConfig *ebusgateway.TransportConfig, alreadyScheduled bool) bool {
	if alreadyScheduled || postStartupIdentityRetryFn == nil || gateway == nil || gateway.Registry == nil {
		return alreadyScheduled
	}
	if !startupScanHasMissingVaillantSerials(gateway.Registry) {
		return alreadyScheduled
	}

	var retryTargetConfig *ebusgateway.TransportConfig
	if targetConfig != nil {
		copyConfig := *targetConfig
		retryTargetConfig = &copyConfig
	}
	postStartupIdentityRetryFn(ctx, gateway, builder, cfg, retryTargetConfig)
	return true
}

func schedulePostStartupIdentityRetry(ctx context.Context, gateway *ebusgateway.Gateway, builder *graphql.Builder, cfg ebusgateway.Config, targetConfig *ebusgateway.TransportConfig) {
	if gateway == nil || gateway.Registry == nil {
		return
	}
	if !startupScanHasMissingVaillantSerials(gateway.Registry) {
		return
	}

	delay := postStartupIdentityRetryDelay
	if delay <= 0 {
		delay = 5 * time.Second
	}
	attempts := postStartupIdentityRetryAttempts
	if attempts <= 0 {
		attempts = 1
	}

	go func() {
		wait := time.NewTimer(delay)
		defer wait.Stop()

		select {
		case <-ctx.Done():
			return
		case <-wait.C:
		}

		for attempt := 1; attempt <= attempts; attempt++ {
			if ctx.Err() != nil || !startupScanHasMissingVaillantSerials(gateway.Registry) {
				return
			}

			log.Printf("startup scan delayed enrich: attempt=%d missing_vaillant_serials=true", attempt)
			enrichVaillantIdentityFn(ctx, gateway, cfg)
			if targetConfig != nil {
				enrichSerialsFromEbusdFn(ctx, gateway.Registry, *targetConfig)
			}

			if !startupScanHasMissingVaillantSerials(gateway.Registry) {
				gateway.RefreshRouterPlanes()
				if builder != nil {
					if err := builder.Rebuild(); err != nil {
						log.Printf("graphql schema rebuild failed after delayed identity enrich: %v", err)
					}
				}
				log.Printf("startup scan delayed enrich: attempt=%d complete", attempt)
				return
			}

			if attempt == attempts {
				log.Printf("startup scan delayed enrich: attempt=%d exhausted", attempt)
				return
			}

			retryTimer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				retryTimer.Stop()
				return
			case <-retryTimer.C:
			}
		}
	}()
}

func ebusdScanTargetCandidates(config ebusgateway.TransportConfig) []ebusgateway.TransportConfig {
	if config.Protocol != ebusgateway.TransportEbusdTCP {
		return nil
	}

	candidates := make([]ebusgateway.TransportConfig, 0, 2)
	if strings.EqualFold(config.Network, "tcp") {
		candidates = append(candidates, config)
	}

	fallback := ebusgateway.TransportConfig{
		Network:     "tcp",
		Address:     "127.0.0.1:8888",
		DialTimeout: config.DialTimeout,
		Dial:        config.Dial,
	}
	if fallback.DialTimeout <= 0 || fallback.DialTimeout > 2*time.Second {
		fallback.DialTimeout = 2 * time.Second
	}

	alreadyPresent := false
	for _, candidate := range candidates {
		if strings.EqualFold(strings.TrimSpace(candidate.Network), fallback.Network) &&
			strings.TrimSpace(candidate.Address) == fallback.Address {
			alreadyPresent = true
			break
		}
	}
	if !alreadyPresent {
		candidates = append(candidates, fallback)
	}

	return candidates
}

func shouldStopDiscoveryScan(total int, confirmationPending bool, confirmationSatisfied bool, confirmationFallbackExhausted bool) bool {
	if total == 0 {
		return false
	}
	if confirmationFallbackExhausted {
		return true
	}
	if confirmationPending && !confirmationSatisfied {
		return false
	}
	return true
}

func startupScanConfirmationSatisfied(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, total int, activeConfirmation bool) bool {
	if total == 0 {
		return false
	}
	if startupScanHasVaillantInventory(gateway) {
		return startupScanHasCoherentVaillantRoot(ctx, cfg, gateway)
	}
	return activeConfirmation
}

func updateStartupScanConfirmationState(total int, imported int, confirmationPending bool, fullRangeRecoveryAttempted bool, confirmationSatisfied bool, requiresRootAwareConfirmation bool) (bool, bool) {
	if total == 0 || confirmationSatisfied {
		return false, false
	}
	if imported > 0 || confirmationPending || requiresRootAwareConfirmation {
		return true, fullRangeRecoveryAttempted
	}
	return false, fullRangeRecoveryAttempted
}

func shouldRetryDiscoveryWithFullRange(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, usedRestrictedTargets bool, retryingFullRange bool) bool {
	if !usedRestrictedTargets || retryingFullRange || gateway == nil || gateway.Registry == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if !startupScanHasVaillantInventory(gateway) {
		return false
	}
	return !startupScanHasCoherentVaillantRoot(ctx, cfg, gateway)
}

func startupScanHasVaillantInventory(gateway *ebusgateway.Gateway) bool {
	if gateway == nil || gateway.Registry == nil {
		return false
	}
	hasVaillant := false
	gateway.Registry.Iterate(func(entry registry.DeviceEntry) bool {
		if entry != nil && strings.EqualFold(entry.Manufacturer(), "Vaillant") {
			hasVaillant = true
			return false
		}
		return true
	})
	return hasVaillant
}

func startupScanHasCoherentVaillantRoot(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway) bool {
	if gateway == nil || gateway.Bus == nil || gateway.Registry == nil {
		return false
	}
	cfg = resolveStartupScanSourceConfig(cfg)
	baseCtx := ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	timeout := cfg.SemanticRequestTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if cfg.ScanRequestTimeout > timeout {
		timeout = cfg.ScanRequestTimeout
	}
	probeCtx, cancel := context.WithTimeout(baseCtx, timeout)
	defer cancel()
	poller := &vaillantSemanticPoller{
		reg:            gateway.Registry,
		bus:            gateway.Bus,
		source:         cfg.ScanSource,
		requestTimeout: cfg.SemanticRequestTimeout,
	}
	if startupScanB524ProbeFn != nil {
		poller.b524ProbeFn = startupScanB524ProbeFn
	}
	_, err := poller.discoverB524Root(probeCtx)
	return err == nil
}

func resolveStartupScanSourceConfig(cfg ebusgateway.Config) ebusgateway.Config {
	if !cfg.ScanSourceAuto || cfg.ScanSource != 0x00 {
		return cfg
	}
	// Source 0x00 is not a valid eBUS initiator. On proxy-capable endpoints
	// (ENH/ENS through a proxy) the proxy cannot arbitrate with initiator=0x00.
	// Resolve to the dedicated proxy startup source regardless of broadcast
	// mode. Direct adapter endpoints (e.g. :9999) handle 0x00 internally via
	// firmware, so leave those unchanged.
	if ebusgateway.PassiveTransportSupported(cfg) {
		cfg.ScanSource = proxyObserveFirstStartupSource
		cfg.ScanSourceAuto = false
	}
	return cfg
}

func countRegistryDevices(reg *registry.DeviceRegistry) int {
	if reg == nil {
		return 0
	}
	count := 0
	reg.Iterate(func(entry registry.DeviceEntry) bool {
		count++
		return true
	})
	return count
}

func ebusdScanResultTargets(ctx context.Context, cfg ebusgateway.TransportConfig) ([]byte, error) {
	rows, err := ebusdScanResultRows(ctx, cfg)
	if err != nil {
		return nil, err
	}
	targets := make([]byte, 0, len(rows))
	for _, row := range rows {
		targets = append(targets, row.Address)
	}
	return targets, nil
}

func ebusdScanResultInfos(ctx context.Context, cfg ebusgateway.TransportConfig) ([]registry.DeviceInfo, error) {
	rows, err := ebusdScanResultRows(ctx, cfg)
	if err != nil {
		return nil, err
	}

	infos := make([]registry.DeviceInfo, 0, len(rows))
	for _, row := range rows {
		infos = append(infos, registry.DeviceInfo{
			Address:         row.Address,
			Manufacturer:    row.Manufacturer,
			DeviceID:        row.DeviceID,
			SoftwareVersion: row.SoftwareVersion,
			HardwareVersion: row.HardwareVersion,
			SerialNumber:    row.SerialNumber,
		})
	}
	return infos, nil
}

func ebusdScanResultRows(ctx context.Context, cfg ebusgateway.TransportConfig) ([]ebusdScanResultRow, error) {
	if cfg.Address == "" || cfg.Network != "tcp" {
		return nil, nil
	}

	dial := cfg.Dial
	if dial == nil {
		dial = dialContext
	}

	dialCtx := ctx
	cancel := func() {}
	if dialCtx == nil {
		dialCtx = context.Background()
	}
	if cfg.DialTimeout > 0 {
		dialCtx, cancel = context.WithTimeout(dialCtx, cfg.DialTimeout)
	}
	defer cancel()

	conn, err := dial(dialCtx, cfg.Network, cfg.Address, cfg.DialTimeout)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	if _, err := conn.Write([]byte("scan result\n")); err != nil {
		return nil, err
	}

	rows := make([]ebusdScanResultRow, 0)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, net.ErrClosed) {
				return rows, err
			}
			return rows, err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			break
		}
		row, ok := parseEbusdScanResultLine(line)
		if !ok {
			continue
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func parseEbusdScanResultLine(line string) (ebusdScanResultRow, bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(strings.ToLower(line), "err") {
		return ebusdScanResultRow{}, false
	}

	fields := strings.Split(line, ";")
	if len(fields) < 5 {
		return ebusdScanResultRow{}, false
	}

	rawAddress := strings.TrimSpace(fields[0])
	value, err := strconv.ParseUint(rawAddress, 16, 8)
	if err != nil {
		return ebusdScanResultRow{}, false
	}

	serialParts := make([]string, 0, len(fields)-5)
	for _, field := range fields[5:] {
		trimmed := strings.TrimSpace(field)
		if trimmed == "" {
			continue
		}
		serialParts = append(serialParts, trimmed)
	}

	return ebusdScanResultRow{
		Address:         byte(value),
		Manufacturer:    strings.TrimSpace(fields[1]),
		DeviceID:        strings.TrimSpace(fields[2]),
		SoftwareVersion: strings.TrimSpace(fields[3]),
		HardwareVersion: strings.TrimSpace(fields[4]),
		SerialNumber:    strings.Join(serialParts, "-"),
	}, true
}

func dialContext(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, network, address)
}

// enrichVaillantIdentity iterates registered Vaillant devices that lack a
// serial number and attempts a B5.09 identity read for each.  This fills in
// serial numbers that the ebusd-tcp preload cannot provide (ebusd's
// "scan result" only returns the 07 04 identification, not the Vaillant-
// specific B5.09 chunks).  Failures are logged but never block startup.
func enrichVaillantIdentity(ctx context.Context, gw *ebusgateway.Gateway, cfg ebusgateway.Config) {
	if gw == nil || gw.Bus == nil || gw.Registry == nil {
		return
	}

	const enrichTimeout = 30 * time.Second
	enrichCtx, cancel := context.WithTimeout(ctx, enrichTimeout)
	defer cancel()

	// Collect Vaillant devices that need enrichment before iterating,
	// so that we don't hold the registry read-lock while sending frames.
	type candidate struct {
		address      byte
		manufacturer string
		deviceID     string
		swVersion    string
		hwVersion    string
	}
	var candidates []candidate
	gw.Registry.Iterate(func(entry registry.DeviceEntry) bool {
		if entry.SerialNumber() != "" {
			return true
		}
		if !strings.EqualFold(entry.Manufacturer(), "Vaillant") {
			return true
		}
		candidates = append(candidates, candidate{
			address:      entry.Address(),
			manufacturer: entry.Manufacturer(),
			deviceID:     entry.DeviceID(),
			swVersion:    entry.SoftwareVersion(),
			hwVersion:    entry.HardwareVersion(),
		})
		return true
	})

	if len(candidates) == 0 {
		return
	}

	log.Printf("startup scan enrich: %d Vaillant device(s) missing serial, attempting B5.09 reads", len(candidates))

	scanBus := &timeoutBus{bus: gw.Bus, timeout: cfg.ScanRequestTimeout}
	enriched := 0
	for _, c := range candidates {
		if enrichCtx.Err() != nil {
			log.Printf("startup scan enrich: timeout reached, %d/%d enriched", enriched, len(candidates))
			break
		}

		serial, ok, err := registry.ReadVaillantScanID(enrichCtx, scanBus, cfg.ScanSource, c.address)
		if err != nil {
			log.Printf("startup scan enrich: B5.09 read for 0x%02X failed: %v", c.address, err)
			continue
		}
		if !ok || serial == "" {
			log.Printf("startup scan enrich: B5.09 read for 0x%02X returned no serial", c.address)
			continue
		}

		// Re-register the device with the discovered serial number.
		// All original fields are preserved to avoid data loss.
		gw.Registry.Register(registry.DeviceInfo{
			Address:         c.address,
			Manufacturer:    c.manufacturer,
			DeviceID:        c.deviceID,
			SoftwareVersion: c.swVersion,
			HardwareVersion: c.hwVersion,
			SerialNumber:    serial,
		})
		enriched++
		log.Printf("startup scan enrich: 0x%02X serial=%s", c.address, serial)
	}

	log.Printf("startup scan enrich: done, %d/%d enriched", enriched, len(candidates))
}

// enrichSerialsFromEbusd fills in missing serial numbers from ebusd's scan
// result.  ebusd performs its own B5.09 identity reads and caches the results;
// this function extracts those serials and applies them to devices in the
// registry that the gateway's own B5.09 reads failed to populate.
func enrichSerialsFromEbusd(ctx context.Context, reg *registry.DeviceRegistry, cfg ebusgateway.TransportConfig) {
	if reg == nil {
		return
	}

	rows, err := ebusdScanResultRows(ctx, cfg)
	if err != nil || len(rows) == 0 {
		return
	}

	// Build a lookup of ebusd rows by address for identity matching.
	ebusdByAddr := make(map[byte]ebusdScanResultRow, len(rows))
	for _, row := range rows {
		if row.SerialNumber != "" {
			ebusdByAddr[row.Address] = row
		}
	}
	if len(ebusdByAddr) == 0 {
		return
	}

	// Collect devices needing enrichment, verifying identity fields match.
	type candidate struct {
		address      byte
		manufacturer string
		deviceID     string
		swVersion    string
		hwVersion    string
		serial       string
	}
	var candidates []candidate
	reg.Iterate(func(entry registry.DeviceEntry) bool {
		if entry.SerialNumber() != "" {
			return true
		}
		row, ok := ebusdByAddr[entry.Address()]
		if !ok {
			return true
		}
		// Verify identity fields match to prevent stale cache misassignment.
		if !strings.EqualFold(entry.Manufacturer(), row.Manufacturer) {
			return true
		}
		if entry.DeviceID() != "" && row.DeviceID != "" &&
			!strings.EqualFold(entry.DeviceID(), row.DeviceID) {
			return true
		}
		candidates = append(candidates, candidate{
			address:      entry.Address(),
			manufacturer: entry.Manufacturer(),
			deviceID:     entry.DeviceID(),
			swVersion:    entry.SoftwareVersion(),
			hwVersion:    entry.HardwareVersion(),
			serial:       row.SerialNumber,
		})
		return true
	})

	if len(candidates) == 0 {
		return
	}

	enriched := 0
	for _, c := range candidates {
		reg.Register(registry.DeviceInfo{
			Address:         c.address,
			Manufacturer:    c.manufacturer,
			DeviceID:        c.deviceID,
			SoftwareVersion: c.swVersion,
			HardwareVersion: c.hwVersion,
			SerialNumber:    c.serial,
		})
		enriched++
	}

	log.Printf("startup scan ebusd enrich: %d/%d device(s) got serial from ebusd scan result", enriched, len(candidates))
}
