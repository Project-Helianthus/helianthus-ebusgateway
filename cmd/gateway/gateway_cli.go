package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func normalizeInstanceGUID(value string) (string, error) {
	normalized := strings.TrimSpace(strings.ToLower(value))
	if normalized == "" {
		return "", nil
	}
	if !instanceGUIDPattern.MatchString(normalized) {
		return "", fmt.Errorf("invalid instance-guid %q", value)
	}
	return normalized, nil
}

func gatewayMDNSText(cfg ebusgateway.Config) []string {
	text := []string{
		"path=" + cfg.GraphQLPath,
		"transport=http",
		"version=1",
	}
	if cfg.InstanceGUID != "" {
		text = append(text, "instance_guid="+cfg.InstanceGUID)
	}
	return text
}

func bindFlags(fs *flag.FlagSet, cfg *ebusgateway.Config) *gatewayFlagInputs {
	inputs := &gatewayFlagInputs{}
	if fs == nil || cfg == nil {
		return inputs
	}

	fs.StringVar((*string)(&cfg.TransportConfig.Protocol), "transport", string(cfg.TransportConfig.Protocol), "transport protocol: enh, ens, udp-plain, tcp-plain, or ebusd-tcp")
	fs.StringVar(&cfg.TransportConfig.Network, "network", cfg.TransportConfig.Network, "transport network: unix, tcp, or udp")
	fs.StringVar(&cfg.TransportConfig.Address, "address", cfg.TransportConfig.Address, "transport address (unix socket path or host:port)")
	fs.DurationVar(&cfg.TransportConfig.ReadTimeout, "read-timeout", cfg.TransportConfig.ReadTimeout, "transport read timeout")
	fs.DurationVar(&cfg.TransportConfig.WriteTimeout, "write-timeout", cfg.TransportConfig.WriteTimeout, "transport write timeout")
	fs.DurationVar(&cfg.TransportConfig.DialTimeout, "dial-timeout", cfg.TransportConfig.DialTimeout, "transport dial timeout")
	fs.BoolVar(&cfg.ModbusTCPConfig.Enabled, "modbus-tcp-enabled", cfg.ModbusTCPConfig.Enabled, "enable the read-only Modbus TCP sidecar")
	fs.StringVar(&cfg.ModbusTCPConfig.Endpoint, "modbus-tcp-endpoint", cfg.ModbusTCPConfig.Endpoint, "Modbus TCP endpoint URI (tcp://host:port)")
	fs.StringVar(&inputs.modbusEndpointFile, "modbus-tcp-endpoint-file", "", "path to an owner-only file containing the Modbus TCP endpoint URI")
	fs.DurationVar(&cfg.ModbusTCPConfig.DialTimeout, "modbus-tcp-dial-timeout", cfg.ModbusTCPConfig.DialTimeout, "Modbus TCP dial timeout")
	bindEEBusFlags(fs, cfg)
	bindM2MGraphQLFlags(fs, cfg)
	fs.BoolVar(
		&cfg.EvidenceOneShotEnabled,
		"synchronized-evidence-one-shot-enabled",
		cfg.EvidenceOneShotEnabled,
		"enable the fixed owner-only synchronized evidence one-shot control",
	)
	fs.IntVar(&cfg.QueueCapacity, "queue-capacity", cfg.QueueCapacity, "bus queue capacity (0 uses protocol default)")
	fs.BoolVar(&cfg.ScanOnStart, "scan", cfg.ScanOnStart, "scan bus on startup")
	fs.DurationVar(&cfg.ScanTimeout, "scan-timeout", cfg.ScanTimeout, "startup scan timeout")
	fs.DurationVar(&cfg.ScanRequestTimeout, "scan-request-timeout", cfg.ScanRequestTimeout, "startup scan per-request timeout")
	fs.DurationVar(&cfg.ScanInterval, "scan-interval", cfg.ScanInterval, "startup scan retry interval (when scan finds 0 devices)")
	fs.BoolVar(&cfg.DiagnosticFullRangeRetry, "diagnostic-full-range-retry", cfg.DiagnosticFullRangeRetry, "allow full-range retry on non-ebusd-tcp transports after a Vaillant root candidate is observed")
	fs.DurationVar(&cfg.BootLiveTimeout, "boot-live-timeout", cfg.BootLiveTimeout, "semantic startup timeout before entering degraded mode")
	fs.DurationVar(&cfg.SemanticDiscoveryInterval, "semantic-discovery-interval", cfg.SemanticDiscoveryInterval, "semantic discovery polling interval")
	fs.DurationVar(&cfg.SemanticConfigInterval, "semantic-config-interval", cfg.SemanticConfigInterval, "semantic config polling interval")
	fs.DurationVar(&cfg.SemanticStateInterval, "semantic-state-interval", cfg.SemanticStateInterval, "semantic state polling interval")
	fs.DurationVar(&cfg.SemanticEnergyInterval, "semantic-energy-interval", cfg.SemanticEnergyInterval, "semantic energy polling interval")
	fs.DurationVar(&cfg.SemanticRequestTimeout, "semantic-request-timeout", cfg.SemanticRequestTimeout, "semantic per-request timeout")
	fs.Func("semantic-read-breaker-failure-budget", "semantic read breaker consecutive failure budget (<=0 disables)", func(value string) error {
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("invalid semantic-read-breaker-failure-budget %q", value)
		}
		cfg.SemanticReadBreakerFailureBudget = parsed
		cfg.SemanticReadBreakerFailureBudgetSet = true
		return nil
	})
	fs.DurationVar(&cfg.SemanticReadBreakerOpenCooldown, "semantic-read-breaker-open-cooldown", cfg.SemanticReadBreakerOpenCooldown, "semantic read breaker open-state cooldown before probe")
	fs.IntVar(&cfg.SemanticReadBreakerHalfOpenProbeLimit, "semantic-read-breaker-half-open-probe-limit", cfg.SemanticReadBreakerHalfOpenProbeLimit, "semantic read breaker half-open probes per cooldown window")
	fs.IntVar(&cfg.SemanticZonePresenceMissThreshold, "semantic-zone-presence-miss-threshold", cfg.SemanticZonePresenceMissThreshold, "consecutive discovery misses required before a zone is marked absent")
	fs.IntVar(&cfg.SemanticZonePresenceHitThreshold, "semantic-zone-presence-hit-threshold", cfg.SemanticZonePresenceHitThreshold, "consecutive discovery hits required before an absent zone is marked present")
	fs.DurationVar(&cfg.SemanticDHWStaleTTL, "semantic-dhw-stale-ttl", cfg.SemanticDHWStaleTTL, "maximum age to keep DHW last-known state during cache-sourced/transient failures")
	fs.DurationVar(&cfg.SemanticRegulatorRecheckInterval, "semantic-regulator-recheck-interval", cfg.SemanticRegulatorRecheckInterval, "regulator capability re-evaluation interval")
	fs.DurationVar(&cfg.SemanticRegulatorAbsenceGrace, "semantic-regulator-absence-grace", cfg.SemanticRegulatorAbsenceGrace, "grace window before WARN_NO_REGULATOR after regulator disappears")
	fs.StringVar(&cfg.SemanticCachePath, "semantic-cache-path", cfg.SemanticCachePath, "semantic cache file path for startup preload and live persistence")
	fs.Func("semantic-interval", "DEPRECATED: semantic state polling interval", func(value string) error {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid semantic-interval %q", value)
		}
		cfg.SemanticInterval = duration
		cfg.SemanticStateInterval = duration
		return nil
	})
	fs.BoolVar(&cfg.BroadcastListen, "broadcast", cfg.BroadcastListen, "enable broadcast listener (separate connection)")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "http listen address (empty disables)")
	fs.StringVar(&cfg.GraphQLPath, "graphql-path", cfg.GraphQLPath, "graphql endpoint path")
	fs.StringVar(&cfg.SnapshotPath, "snapshot-path", cfg.SnapshotPath, "projection snapshot endpoint path")
	fs.StringVar(&cfg.SubscriptionPath, "subscription-path", cfg.SubscriptionPath, "graphql subscriptions path")
	fs.StringVar(&cfg.MCPPath, "mcp-path", cfg.MCPPath, "mcp endpoint path")
	fs.StringVar(&cfg.UIPath, "ui-path", cfg.UIPath, "portal ui path")
	fs.StringVar(&cfg.PortalPath, "portal-path", cfg.PortalPath, "dynamic portal path")
	fs.StringVar(&cfg.DumpUploadPath, "dump-upload-path", cfg.DumpUploadPath, "register dump upload endpoint path")
	fs.BoolVar(&cfg.MDNSAdvertise, "mdns", cfg.MDNSAdvertise, "advertise graphql endpoint via mdns")
	fs.StringVar(&cfg.MDNSInstance, "mdns-instance", cfg.MDNSInstance, "mdns instance name")
	fs.Func("instance-guid", "stable gateway instance UUIDv4 (lowercase)", func(value string) error {
		normalized, err := normalizeInstanceGUID(value)
		if err != nil {
			return err
		}
		cfg.InstanceGUID = normalized
		return nil
	})
	fs.StringVar(&cfg.InstanceGUIDSource, "instance-guid-source",
		cfg.InstanceGUIDSource,
		"AD27 provenance tag for -instance-guid (runtime_state | legacy_migrated | generated | cli-override); "+
			"empty defaults to cli-override with a deprecation log when -instance-guid is provided")
	fs.StringVar(&cfg.RuntimeStatePath, "runtime-state-path",
		cfg.RuntimeStatePath,
		"override /data/runtime_state.json path (empty uses runtimestate package default)")
	fs.StringVar(&cfg.DumpOutputDir, "dump-output-dir", cfg.DumpOutputDir, "unknown device dump output dir")
	fs.StringVar(&cfg.DumpUploadURL, "dump-upload-url", cfg.DumpUploadURL, "unknown device dump upload url (internal)")
	fs.BoolVar(&cfg.DumpIncludePII, "dump-include-pii", cfg.DumpIncludePII, "include identifiers in unknown device dumps")
	fs.BoolVar(&cfg.EnableStaticSeedTable, "enable-static-seed-table", cfg.EnableStaticSeedTable, "plant productids static seed entries (NETX3 0xF1 / 0xF6 / 0x04 / 0xFF, BASV2 0x15 / 0xEC) into registry at startup; default false")
	fs.BoolVar(&cfg.ObserveFirstEnabled, "observe-first-enabled", cfg.ObserveFirstEnabled, "enable observe-first runtime behavior gates")
	fs.BoolVar(&cfg.PassiveStateDirectApply, "passive-state-direct-apply", cfg.PassiveStateDirectApply, "allow passive state direct-apply when observe-first is enabled")
	fs.BoolVar(&cfg.PassiveConfigDirectApply, "passive-config-direct-apply", cfg.PassiveConfigDirectApply, "allow passive config direct-apply when state direct-apply is enabled")
	fs.Func("external-write-policy", "externally observed write policy: invalidate_only, record_only, or record_and_invalidate", func(value string) error {
		policy, err := ebusgateway.ParseObserveFirstExternalWritePolicy(value)
		if err != nil {
			return err
		}
		cfg.ExternalWritePolicy = policy
		return nil
	})

	fs.StringVar(&cfg.ProxyListenAddr, "proxy-listen", cfg.ProxyListenAddr, "TCP listen address for ENH proxy clients (e.g. :19001, empty disables)")

	fs.StringVar(&cfg.PhantomInitiatorRejectBytes, "phantom-initiator-reject-bytes",
		cfg.PhantomInitiatorRejectBytes,
		"comma-separated hex bytes treated as phantom AND-collision artifacts (e.g. 0x71,0xFD); empty disables filtering. Default 0x71 covers the gateway=0x7F & initiator=0xF1 collision case observed on the live HA bus. Operators on different buses should set this explicitly.")

	fs.Func("source-addr", "source address for scans/semantic reads (e.g. 0xf0, 0x00, or auto)", func(value string) error {
		value = strings.TrimSpace(strings.ToLower(value))
		if value == "" {
			return nil
		}
		if value == "auto" {
			cfg.ScanSource = 0x00
			cfg.ScanSourceAuto = true
			return nil
		}
		parsed, err := strconv.ParseUint(value, 0, 8)
		if err != nil {
			return fmt.Errorf("invalid source-addr %q", value)
		}
		cfg.ScanSource = byte(parsed)
		cfg.ScanSourceAuto = cfg.ScanSource == 0x00
		return nil
	})
	fs.Func("startup-probe-targets", "comma-separated explicit startup directed-probe targets (e.g. 0x08,0x15)", func(value string) error {
		targets, err := parseStartupProbeTargets(value)
		if err != nil {
			return err
		}
		cfg.StartupProbeTargets = targets
		return nil
	})
	fs.Func("startup-source-override", "override source address for source-selection-capable direct transports (e.g. 0xf0)", func(value string) error {
		value = strings.TrimSpace(strings.ToLower(value))
		if value == "" {
			cfg.StartupSource.Source = nil
			return nil
		}
		parsed, err := strconv.ParseUint(value, 0, 8)
		if err != nil {
			return fmt.Errorf("invalid startup-source-override %q", value)
		}
		source := uint8(parsed)
		if source == 0x00 {
			return fmt.Errorf("invalid startup-source-override %q: source 0x00 is not a valid active initiator", value)
		}
		cfg.StartupSource.Source = &source
		return nil
	})
	fs.BoolVar(&cfg.StartupSource.Validate, "startup-source-override-validate", cfg.StartupSource.Validate, "run source-address selector in advisory-only mode alongside startup-source-override")
	return inputs
}

// parseHexByteList parses a comma-separated list of hex byte literals
// (e.g. "0x71, 0xFD" or "71,FD") into []byte. Empty input returns a nil
// slice with no error. Whitespace around items is trimmed. Per-byte
// parsing accepts the standard Go base prefix (0x / 0X / no-prefix-hex),
// so "0x71", "0X71", and "71" all parse to 0x71. Invalid items return an
// error naming the offending substring.
//
// Used to materialize the --phantom-initiator-reject-bytes CLI flag into
// the IsKnownInitiatorByte predicate plumbed into adaptermux (Codex P2
// thread on PR #634, batch-24).
func parseHexByteList(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	out := make([]byte, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// strconv.ParseUint with base 0 accepts "0x71", "0X71", "71"
		// (treated as decimal!) — to keep "71" parsing as hex we
		// fall back to base 16 when no 0x prefix is present.
		var (
			parsed uint64
			err    error
		)
		lower := strings.ToLower(part)
		if strings.HasPrefix(lower, "0x") {
			parsed, err = strconv.ParseUint(part[2:], 16, 8)
		} else {
			parsed, err = strconv.ParseUint(part, 16, 8)
		}
		if err != nil {
			return nil, fmt.Errorf("invalid hex byte %q: %w", part, err)
		}
		out = append(out, byte(parsed))
	}
	return out, nil
}

func parseStartupProbeTargets(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	targets := make([]byte, 0, len(parts))
	seen := make(map[byte]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(strings.ToLower(part))
		if part == "" {
			continue
		}
		parsed, err := strconv.ParseUint(part, 0, 8)
		if err != nil {
			return nil, fmt.Errorf("invalid startup-probe-targets address %q", part)
		}
		target := byte(parsed)
		if target < 0x03 || target >= 0xFE || target == 0xAA || target == 0xA9 || isInitiatorCapableAddress(target) {
			return nil, fmt.Errorf("startup-probe-targets address 0x%02X outside target-capable range", target)
		}
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		targets = append(targets, target)
	}
	return targets, nil
}

// wireAdapterDirect creates and starts the adapter multiplexer if the
// transport protocol is adapter-direct. It configures both active and
// passive transports in cfg before gateway construction.
//
// Returns a closer function for the multiplexer, the instance-scoped
// activeTxnClassifier (the mux), or nils if not in adapter-direct mode.
// The classifier is threaded explicitly into startDiscoveryScanLoopFn
// at the call site in run() — never captured in a package-level closure —
// so classifier state is strictly instance-local (Codex PR #502 P2).
