package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
)

func wireAdapterDirect(ctx context.Context, cfg *ebusgateway.Config) (func() error, activeTxnClassifier, error) {
	return wireAdapterDirectWithConnectionLost(ctx, cfg, nil)
}

const adapterDirectENSProtocol ebusgateway.TransportProtocol = "adapter-direct-ens"

func wireAdapterDirectWithConnectionLost(ctx context.Context, cfg *ebusgateway.Config, onConnectionLost func()) (func() error, activeTxnClassifier, error) {
	normalized, err := ebusgateway.NormalizeEBusDriverTransportConfig(cfg.TransportConfig)
	if err != nil {
		return nil, nil, err
	}
	if normalized.Protocol != ebusgateway.TransportAdapterDirect && normalized.Protocol != adapterDirectENSProtocol {
		return nil, nil, nil
	}
	cfg.TransportConfig = normalized
	network := normalized.Network
	address := normalized.Address
	if address == "" {
		return nil, nil, fmt.Errorf("adapter-direct requires an address (e.g. adapter-direct://boiler.local:9999): %w", ebuserrors.ErrInvalidPayload)
	}

	// Determine ENH vs ENS sub-protocol. ENH is the default.
	// The shared normalizer preserves adapter-direct-ens as a private
	// canonical literal after stripping the URI.
	adapterProtocol := adapterDirectMuxProtocol(normalized.Protocol)

	muxCfg := adaptermux.Config{
		Protocol:     adapterProtocol,
		Network:      network,
		Address:      address,
		DialTimeout:  cfg.TransportConfig.DialTimeout,
		ReadTimeout:  cfg.TransportConfig.ReadTimeout,
		WriteTimeout: cfg.TransportConfig.WriteTimeout,
		// F-30 (batch-27, iter7, 2026-05-14; batch-24 round-5
		// parameterization, 2026-05-17): wire IsKnownInitiatorByte to
		// filter bit-arbitration phantom bytes from external session
		// forwarding. When the gateway loses arbitration to another
		// initiator, the adapter reports FAILED with the bit-wise-AND
		// result of the colliding initiators. This AND result is often
		// NOT a real initiator on the bus (e.g., 0x7F & 0xF1 = 0x71,
		// where no 0x71 initiator exists on the observed bus).
		//
		// Pre-F-30: phantom bytes were forwarded to ebusd as ENH_RES_FAILED
		// data; ebusd's bus reconstructor interpreted each phantom as a
		// real frame source and advanced its state machine to expect a
		// frame from that fictitious initiator. The next real wire bytes
		// (from the actual winning initiator) then mismatched, leaving
		// ebusd's state in bs_recvCmd/bs_skip — the "arbitration won in
		// invalid state" trigger when ebusd's NEXT grant arrives.
		//
		// Iter6 forensic measured 58 invalid-state events / 5 min
		// despite F-28+F-29 cooldowns reducing the cause-1 (timeout)
		// path. Cause-2 (state mismatch from phantom forwarding) is now
		// the dominant residual.
		//
		// Iter7 (hardcoded): rejected the SPECIFIC phantom 0x71 observed
		// on this bus. Round-5 (batch-24) generalizes via the
		// --phantom-initiator-reject-bytes CLI flag — default "0x71"
		// preserves iter7 behavior; operators on different buses should
		// set this explicitly (empty disables filtering, CSV extends
		// rejection to additional phantoms). The runtime_state-backed
		// lookup originally planned for iter8 is still on the table but
		// requires bus-membership convergence first.
		//
		// Returning false on the predicate routes the FAILED byte
		// through the gateway's suppression path (mux.go ~1523): the
		// byte is NOT delivered to external sessions, the per-session
		// notify gets the bidder's own initiator instead of the
		// phantom, and the passive emit / logging still fires for
		// observability.
	}
	phantomBytes, err := parseHexByteList(cfg.PhantomInitiatorRejectBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid -phantom-initiator-reject-bytes %q: %w", cfg.PhantomInitiatorRejectBytes, err)
	}
	if len(phantomBytes) == 0 {
		// Empty CSV disables filtering entirely — every byte is
		// "known", so the suppression path in mux.go never fires.
		muxCfg.IsKnownInitiatorByte = nil
	} else {
		// Copy the slice into the closure so subsequent mutations of
		// the caller's slice (none today, but defensive) can't shift
		// the predicate's behavior at runtime.
		pb := append([]byte(nil), phantomBytes...)
		muxCfg.IsKnownInitiatorByte = func(b byte) bool {
			for _, r := range pb {
				if r == b {
					return false
				}
			}
			return true
		}
	}
	if muxCfg.DialTimeout == 0 {
		muxCfg.DialTimeout = 5 * time.Second
	}
	// Mux read timeout controls how often the idle-timeout branch runs
	// (tryGrantAndStart) AND how quickly activeCh receives bytes from
	// the upstream ENH transport. 50ms matches the eBUS SYN interval so
	// waitForSyn (which needs 2 SYNs) completes within ~100ms instead
	// of ~400ms at 200ms, preventing scan-abort after collision retry.
	muxCfg.ReadTimeout = 50 * time.Millisecond
	if muxCfg.WriteTimeout == 0 {
		muxCfg.WriteTimeout = 5 * time.Second
	}

	// Phase 3 Step B3.7 (Codex round-1 MAJOR on PR #650): wire the
	// HELIANTHUS_V8_CLASSIFIER_MODE env var into the mux config.
	// Without this read, the v8 classifier is unreachable in
	// production — Config.V8ClassifierMode would always be the
	// zero-value (ModeOff) regardless of what operators set in
	// the addon options. ParseMode handles case-insensitive
	// "off|shadow|enforce" plus convenience synonyms ("disabled",
	// "false", etc.); an unset env var defaults to ModeOff (safe);
	// an unrecognized value fails loudly via the returned error.
	if envMode := os.Getenv(v8classifier.EnvVarName); envMode != "" {
		parsedMode, err := v8classifier.ParseMode(envMode)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid %s=%q (expected off|shadow|enforce): %w", v8classifier.EnvVarName, envMode, err)
		}
		muxCfg.V8ClassifierMode = parsedMode
		if parsedMode != v8classifier.ModeOff {
			log.Printf("v8 classifier enabled: mode=%s", parsedMode)
		}
	}

	mux := adaptermux.New(muxCfg)
	connectionLostOwner := onConnectionLost
	if onConnectionLost != nil {
		var connectionLostOnce sync.Once
		connectionLostOwner = func() { connectionLostOnce.Do(onConnectionLost) }
	}
	mux.SetConnectionLostCallback(connectionLostOwner)
	// Codex PR #502 P2: the instance-scoped classifier is returned to
	// run() and threaded explicitly as the 5th argument to
	// startDiscoveryScanLoopFn. No package-level closure captures this
	// mux — so a second wireAdapterDirect call (e.g. in the same process
	// across tests) sees its own classifier at the call site.

	// Create passive transport BEFORE Start() so the callback is wired.
	// Only create it when BroadcastListen is enabled — otherwise no
	// consumer reads from the passive channel and reset delivery blocks
	// readLoop (P0 deadlock fix).
	if cfg.BroadcastListen {
		cfg.PassiveTransport = mux.PassiveTransport()

		// Skip passive warmup in adapter-direct mode. The passive stream
		// only sees third-party traffic (gateway bytes are suppressed by
		// the mux), so warmup thresholds are never met organically. Zero
		// thresholds tell the store to promote immediately.
		cfg.ObserveFirstWarmupCompletedTransactions = 0
		cfg.ObserveFirstWarmupConnectedWindow = 0
		cfg.ObserveFirstWarmupPostResetTransactions = 0
		cfg.ObserveFirstWarmupPostResetWindow = 0
	}

	if err := mux.Start(ctx); err != nil {
		return nil, nil, fmt.Errorf("start multiplexer: %w", err)
	}

	log.Printf("adapter-direct: connected to %s/%s", network, address)

	// Start proxy listener if configured (exposes ENH endpoint for
	// external clients like ebusd).
	var proxyListener *adaptermux.ProxyListener
	if cfg.ProxyListenAddr != "" {
		onProxyFatal := adapterDirectProxyFatalCallback(mux, connectionLostOwner)
		pl, err := adaptermux.NewProxyListenerWithFatalCallback(ctx, mux, cfg.ProxyListenAddr, log.Default(), onProxyFatal)
		if err != nil {
			if cerr := mux.Close(); cerr != nil {
				log.Printf("adapter-direct: mux.Close after proxy-listener error: %v", cerr)
			}
			return nil, nil, fmt.Errorf("proxy listener: %w", err)
		}
		proxyListener = pl
		log.Printf("adapter-direct: proxy listener on %s", pl.Addr())
	}

	// Configure gateway transports.
	cfg.Transport = mux.ActiveTransport()

	// Return a closer that cleans up both the proxy listener (if any)
	// and the mux itself. This covers early run() failures where
	// gateway.Close() never runs (and thus activeTransport.Close
	// never calls mux.Close). On normal shutdown the gateway's
	// transport.Close calls mux.Close first — that is safe because
	// mux.Close is idempotent (sync.Once guarded).
	closer := func() error {
		if proxyListener != nil {
			if err := proxyListener.Close(); err != nil {
				log.Printf("adapter-direct: proxyListener.Close: %v", err)
			}
		}
		return mux.Close()
	}
	return closer, activeTxnClassifier(mux), nil
}

func adapterDirectProxyFatalCallback(mux *adaptermux.Mux, owner func()) func(error) {
	if mux == nil || owner == nil {
		return nil
	}
	return func(err error) {
		// Fatal listener loss is a generation boundary just like upstream loss.
		// Fence and synchronously retire every existing proxy session before
		// publishing failure; wireAdapterDirect's shared one-shot owner prevents
		// duplicate recovery with a concurrent upstream loss.
		mux.RetireManagedProxySessions()
		log.Printf("adapter-direct: proxy listener failed: %v", err)
		owner()
	}
}

func adapterDirectMuxProtocol(protocol ebusgateway.TransportProtocol) string {
	if protocol == adapterDirectENSProtocol {
		return "ens"
	}
	return "enh"
}

func adapterDirectProxyEnabled(config ebusgateway.TransportConfig) bool {
	return ebusgateway.EBusDriverTransportProtocol(config) == ebusgateway.TransportAdapterDirect
}

// v8AdminEventsResponse is the JSON envelope returned by
// /debug/v8/admin-events. Stable wire format — operator tooling
// (dashboards, classifier-tuning workflows) parse this.
//
// Fields:
//
//   - Events: copy of all events the ring buffer held at drain
//     time, in FIFO order (oldest first). Each event includes the
//     wire byte, FSM state at decision time, escape-decoded
//     provenance, and monotonic-clock timestamp.
//
//   - Dropped: number of events the ring buffer dropped (oldest-
//     first FIFO) since the last drain. Non-zero indicates the
//     consumer is polling too slowly for the production event
//     rate; saturate response is acceptable but the operator
//     should tighten the poll cadence.
//
//   - Kind / FSMState: JSON-encoded as their stable string labels
//     via the v8AdminEventJSON intermediate so the wire format
//     doesn't expose numeric enum values that could shift across
//     gateway versions.
