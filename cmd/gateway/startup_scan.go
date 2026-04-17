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

// scanAttemptLogCap bounds how many per-attempt diagnostic lines statsBus
// records per pass. 8 is enough to characterize the failure mode (which
// targets, which result class, which error string) without flooding the
// log when all 164 targets time out.
const scanAttemptLogCap = 8

// scanAttemptLog records a single scan attempt for bounded diagnostics.
type scanAttemptLog struct {
	source    byte
	target    byte
	resClass  string
	duration  time.Duration
	errStr    string // truncated, only for non-ok attempts
	probeKind string // e.g. "scan_07_04" — bounded set of probe names
	txnClass  string // adaptermux txn classification (if classifier wired)
}

// activeTxnClassifier is the optional interface statsBus queries after
// each probe to attach the adaptermux transaction classification to the
// scan attempt log. The adaptermux.Mux satisfies this via its
// LastTxnClass method. If the underlying bus does not implement it,
// txnClass is left empty — diagnostics degrade gracefully.
type activeTxnClassifier interface {
	LastTxnClass() string
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
	bus      registry.ScanBus
	stats    scanStats
	source   byte             // effective source address in use for this pass
	attempts []scanAttemptLog // bounded by scanAttemptLogCap
	total    int              // total send attempts this pass (including non-logged)
	// classifier is an optional adaptermux.Mux (or test fake) that
	// exposes the last-transaction classification via LastTxnClass.
	// When nil the txnClass field on each attempt is left empty.
	classifier activeTxnClassifier
}

// maxErrStrLen bounds logged error strings per attempt for diagnostics.
const maxErrStrLen = 96

func truncateErr(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > maxErrStrLen {
		return s[:maxErrStrLen] + "..."
	}
	return s
}

// scanProbeKind returns a bounded, human-readable label for the probe
// frame. Used in attempt diagnostics to distinguish 07 04 identification
// probes from future secondary probe types without logging payloads.
func scanProbeKind(frame protocol.Frame) string {
	switch {
	case frame.Primary == 0x07 && frame.Secondary == 0x04:
		return "scan_07_04"
	case frame.Primary == 0xB5 && frame.Secondary == 0x09:
		return "b509_identity"
	case frame.Primary == 0xB5 && frame.Secondary == 0x24:
		return "b524_register"
	default:
		return "other"
	}
}

func classifyScanErr(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ebuserrors.ErrTimeout) || errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, ebuserrors.ErrBusCollision):
		return "collision"
	case errors.Is(err, ebuserrors.ErrNACK):
		return "nack"
	case errors.Is(err, ebuserrors.ErrCRCMismatch):
		return "crc"
	default:
		return "other"
	}
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

// startupScanMaxUnconfirmedPasses is the number of consecutive scan passes
// where devices are present but confirmation remains unsatisfied before the
// safety-net forces semanticBootstrapReady.  This covers edge cases where the
// directScanConfirmationRetries counter cannot accumulate (e.g. intermittent
// B524 probe success that resets the counter before it reaches the threshold).
const startupScanMaxUnconfirmedPasses = 5

var (
	postStartupIdentityRetryDelay    = 5 * time.Second
	postStartupIdentityRetryAttempts = 3
)

func (b *statsBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	if b == nil || b.bus == nil {
		return nil, fmt.Errorf("scan stats bus missing")
	}
	b.total++
	start := time.Now()
	response, err := b.bus.Send(ctx, frame)
	dur := time.Since(start)

	class := classifyScanErr(err)
	switch class {
	case "ok":
		b.stats.ok++
	case "timeout":
		b.stats.timeouts++
	case "collision":
		b.stats.collisions++
		semanticBusCollisionsTotal.Add(1)
		log.Printf("semantic_bus_collision total=%d", semanticBusCollisionsTotal.Value())
	case "nack":
		b.stats.nacks++
	case "crc":
		b.stats.crcErrors++
	default:
		b.stats.otherErrs++
	}

	// Bounded per-attempt diagnostics: prioritize non-ok attempts so
	// the cap isn't filled with "ok" entries in mixed passes where
	// early targets succeed and later ones fail (which would hide the
	// timeout/collision/nack evidence operators actually need).
	//
	// Policy:
	//   - While slots remain, record anything (ok or non-ok).
	//   - Once full, if current is non-ok, evict the oldest "ok"
	//     entry (if any) to make room. If all entries are non-ok,
	//     keep the current set (first N non-ok attempts stay).
	//   - "ok" entries never displace anything after the cap is full.
	entry := scanAttemptLog{
		source:    b.source,
		target:    frame.Target,
		resClass:  class,
		duration:  dur,
		errStr:    truncateErr(err),
		probeKind: scanProbeKind(frame),
	}
	if b.classifier != nil {
		entry.txnClass = b.classifier.LastTxnClass()
	}
	if len(b.attempts) < scanAttemptLogCap {
		b.attempts = append(b.attempts, entry)
	} else if class != "ok" {
		for i, existing := range b.attempts {
			if existing.resClass == "ok" {
				b.attempts[i] = entry
				break
			}
		}
	}
	return response, err
}

// logPassDiagnostics emits bounded per-pass diagnostics: effective source,
// total attempts, aggregate stats, and the first-N attempt entries.
// Callers hold no locks.
func (b *statsBus) logPassDiagnostics(sourceMode string, targetCount int, passTimeout time.Duration) {
	if b == nil {
		return
	}
	log.Printf(
		"startup scan pass: source=0x%02X sourceMode=%s targets=%d passTimeout=%s attempts=%d",
		b.source, sourceMode, targetCount, passTimeout, b.total,
	)
	// First-N per-attempt lines — capped at scanAttemptLogCap.
	for i, a := range b.attempts {
		if a.resClass == "ok" {
			log.Printf(
				"startup scan attempt %d/%d: src=0x%02X tgt=0x%02X probe=%s result=ok dur=%s txnClass=%s",
				i+1, len(b.attempts), a.source, a.target, a.probeKind, a.duration, a.txnClass,
			)
		} else {
			log.Printf(
				"startup scan attempt %d/%d: src=0x%02X tgt=0x%02X probe=%s result=%s dur=%s txnClass=%s err=%q",
				i+1, len(b.attempts), a.source, a.target, a.probeKind, a.resClass, a.duration, a.txnClass, a.errStr,
			)
		}
	}
}

// startupScanClassifier is an unexported package-level override used by
// cmd/gateway wiring and tests to attach an adaptermux transaction
// classifier to each startup-scan pass. nil is safe — diagnostics simply
// omit the txnClass column.
var startupScanClassifier activeTxnClassifier

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
		directScanConfirmationRetries := 0
		scanPassesWithDevices := 0
		for {
			scanCtx := ctx
			cancel := func() {}
			if cfg.ScanTimeout > 0 {
				scanCtx, cancel = context.WithTimeout(ctx, cfg.ScanTimeout)
			}
			scanBus := &statsBus{
				bus:        &timeoutBus{bus: gateway.Bus, timeout: cfg.ScanRequestTimeout},
				source:     startupCfg.ScanSource,
				classifier: startupScanClassifier,
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
			// Bounded per-pass diagnostics: effective source, target count,
			// pass timeout, and first-N attempt entries.
			scanBus.logPassDiagnostics(
				startupScanSourceMode(cfg, startupCfg),
				len(targets),
				cfg.ScanTimeout,
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
			// Direct (non-ebusd-tcp) scans never set usedRestrictedTargets,
			// so the ebusd-specific fallback path above cannot fire.  The
			// !usedRestrictedTargets guard below correctly scopes this
			// counter to direct scans only (adapter-direct, ENH, ENS).
			// Track consecutive confirmation failures and treat them as
			// exhausted after two passes to prevent an infinite scan loop
			// when the B524 coherent-root probe fails under bus contention.
			if confirmationPending && !confirmationSatisfied && !usedRestrictedTargets && total > 0 {
				directScanConfirmationRetries++
				if directScanConfirmationRetries >= 2 {
					confirmationFallbackExhausted = true
					log.Printf("startup scan: direct-scan confirmation exhausted after %d retries, proceeding with bootstrap", directScanConfirmationRetries)
				}
			}
			// Safety net: count consecutive scan passes where devices exist
			// but confirmation remains unresolved.  If this counter exceeds
			// startupScanMaxUnconfirmedPasses the loop forces bootstrap
			// regardless of confirmation state.  This covers scenarios the
			// directScanConfirmationRetries path cannot reach (e.g. ebusd-tcp
			// preload with intermittent B524 probe success preventing the
			// retry counter from accumulating).
			if total > 0 && !confirmationSatisfied {
				scanPassesWithDevices++
			} else {
				scanPassesWithDevices = 0
			}
			if scanPassesWithDevices >= startupScanMaxUnconfirmedPasses && !confirmationFallbackExhausted {
				confirmationFallbackExhausted = true
				log.Printf("startup scan: unconfirmed passes=%d reached safety limit, proceeding with bootstrap", scanPassesWithDevices)
			}
			log.Printf(
				"startup scan: confirmation pending=%v satisfied=%v exhausted=%v vaillant=%v restricted=%v retries=%d passes=%d total=%d",
				confirmationPending, confirmationSatisfied, confirmationFallbackExhausted,
				requiresRootAwareConfirmation, usedRestrictedTargets, directScanConfirmationRetries, scanPassesWithDevices, total,
			)
			if confirmationSatisfied || confirmationFallbackExhausted {
				restrictedConfirmationAfterRecoveryPending = false
				directScanConfirmationRetries = 0
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
	if !strings.EqualFold(config.Network, "tcp") || config.Address == "" {
		return nil
	}
	return []ebusgateway.TransportConfig{config}
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
	perProbe := cfg.SemanticRequestTimeout
	if perProbe <= 0 {
		perProbe = 2 * time.Second
	}
	if cfg.ScanRequestTimeout > perProbe {
		perProbe = cfg.ScanRequestTimeout
	}
	// probeB524Register clamps its internal timeout to minB524ProbeTimeout
	// (5s). The outer context must use at least this value to avoid
	// expiring before the individual probe finishes.
	if perProbe < 5*time.Second {
		perProbe = 5 * time.Second
	}
	// Each candidate is tested with len(b524CapabilityProbes) serial probes,
	// and each probe retries up to 3 times with 200ms backoff. The outer
	// context must allow enough wall-clock time for the worst-case path:
	// every candidate × every probe × every retry.
	const probeRetryCount = 3
	numCandidates := countRegistryDevices(gateway.Registry)
	if numCandidates < 1 {
		numCandidates = 1
	}
	numProbes := len(b524CapabilityProbes)
	if numProbes < 1 {
		numProbes = 1
	}
	timeout := perProbe * time.Duration(numCandidates*numProbes*probeRetryCount+1)
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

// startupScanSourceMode reports which source-selection path was used
// for diagnostics: "auto-proxy" (auto-resolved to 0xF7 for proxy), "auto"
// (auto requested but kept), "configured" (explicit non-auto), or
// "default" (no config, ScanSource==0x00, ScanSourceAuto==false).
func startupScanSourceMode(original, resolved ebusgateway.Config) string {
	if original.ScanSourceAuto && original.ScanSource == 0x00 && resolved.ScanSource == proxyObserveFirstStartupSource {
		return "auto-proxy"
	}
	if original.ScanSourceAuto {
		return "auto"
	}
	if original.ScanSource != 0x00 {
		return "configured"
	}
	return "default"
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
	if cfg.Address == "" || !isEbusdTCPTransport(cfg) {
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

// throttledBus wraps a ScanBus with an inter-request delay to reduce bus
// contention during low-priority background scans.
type throttledBus struct {
	bus     registry.ScanBus
	delay   time.Duration
	timeout time.Duration
}

func (b *throttledBus) Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	if b == nil || b.bus == nil {
		return nil, fmt.Errorf("throttled bus missing")
	}
	if b.delay > 0 {
		select {
		case <-time.After(b.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if b.timeout > 0 {
		ctxTimeout, cancel := context.WithTimeout(ctx, b.timeout)
		defer cancel()
		return b.bus.Send(ctxTimeout, frame)
	}
	return b.bus.Send(ctx, frame)
}

const (
	backgroundScanInitialDelay = 5 * time.Minute
	backgroundScanThrottle     = 200 * time.Millisecond
)

var startBackgroundFullScanFn = startBackgroundFullScan

func startBackgroundFullScan(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, builder *graphql.Builder, ready <-chan struct{}) {
	if cfg.BackgroundScanInterval <= 0 || gateway == nil || gateway.Bus == nil || gateway.Registry == nil {
		return
	}

	select {
	case <-ready:
	case <-ctx.Done():
		return
	}

	initialDelay := time.NewTimer(backgroundScanInitialDelay)
	select {
	case <-initialDelay.C:
	case <-ctx.Done():
		initialDelay.Stop()
		return
	}

	scanCfg := resolveStartupScanSourceConfig(cfg)

	ticker := time.NewTicker(cfg.BackgroundScanInterval)
	defer ticker.Stop()

	for {
		runBackgroundFullScan(ctx, scanCfg, gateway, builder)

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func runBackgroundFullScan(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, builder *graphql.Builder) {
	if gateway == nil || gateway.Bus == nil || gateway.Registry == nil {
		return
	}

	scanBus := &throttledBus{
		bus:     gateway.Bus,
		delay:   backgroundScanThrottle,
		timeout: cfg.ScanRequestTimeout,
	}

	beforeTotal := countRegistryDevices(gateway.Registry)
	devices, err := registryScanFn(ctx, scanBus, gateway.Registry, cfg.ScanSource, nil)
	if err != nil && ctx.Err() == nil {
		log.Printf("background scan error: %v", err)
	}
	afterTotal := countRegistryDevices(gateway.Registry)

	if afterTotal > beforeTotal {
		enrichVaillantIdentityFn(ctx, gateway, cfg)
		gateway.RefreshRouterPlanes()
		if builder != nil {
			if err := builder.Rebuild(); err != nil {
				log.Printf("background scan: graphql rebuild failed: %v", err)
			}
		}
	}

	log.Printf("background scan: found=%d before=%d after=%d", len(devices), beforeTotal, afterTotal)
}
