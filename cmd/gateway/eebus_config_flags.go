package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func bindEEBusFlags(fs *flag.FlagSet, cfg *ebusgateway.Config) {
	fs.BoolVar(&cfg.EEBusConfig.Enabled, "eebus-enabled", cfg.EEBusConfig.Enabled, "record intent to activate the eeBUS runtime sidecar (M5A does not start it)")
	fs.Func("eebus-listen-port", "eeBUS SHIP listen port (0 remains unconfigured)", func(value string) error {
		parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 16)
		if err != nil {
			return fmt.Errorf("invalid eebus-listen-port %q", value)
		}
		cfg.EEBusConfig.ListenPort = uint16(parsed)
		return nil
	})
	fs.Func("eebus-interfaces", "comma-separated explicit eeBUS network interfaces", func(value string) error {
		cfg.EEBusConfig.Interfaces = normalizeEEBusInterfaces(value)
		return nil
	})
	fs.Func("eebus-subnets", "comma-separated explicit eeBUS network prefixes", func(value string) error {
		subnets, err := normalizeEEBusSubnets(value)
		if err != nil {
			return err
		}
		cfg.EEBusConfig.Subnets = subnets
		return nil
	})
	fs.Func("eebus-state-root", "eeBUS runtime state root", func(value string) error {
		path, err := normalizeEEBusPath("eebus-state-root", value)
		if err != nil {
			return err
		}
		cfg.EEBusConfig.StateRoot = path
		return nil
	})
	fs.BoolVar(&cfg.EEBusConfig.DiscoveryEnabled, "eebus-discovery-enabled", cfg.EEBusConfig.DiscoveryEnabled, "enable eeBUS discovery")
	fs.Func("eebus-certificate-path", "eeBUS certificate file path", func(value string) error {
		path, err := normalizeEEBusPath("eebus-certificate-path", value)
		if err != nil {
			return err
		}
		cfg.EEBusConfig.CertificatePath = path
		return nil
	})
	fs.Func("eebus-private-key-path", "eeBUS private-key file path", func(value string) error {
		path, err := normalizeEEBusPath("eebus-private-key-path", value)
		if err != nil {
			return err
		}
		cfg.EEBusConfig.PrivateKeyPath = path
		return nil
	})
	fs.Func("eebus-trust-store-path", "eeBUS trust-store file path", func(value string) error {
		path, err := normalizeEEBusPath("eebus-trust-store-path", value)
		if err != nil {
			return err
		}
		cfg.EEBusConfig.TrustStorePath = path
		return nil
	})
	fs.Func("eebus-remote-ski-allowlist", "comma-separated remote eeBUS SKI allowlist", func(value string) error {
		allowlist, err := normalizeEEBusRemoteSKIAllowlist(value)
		if err != nil {
			return err
		}
		cfg.EEBusConfig.RemoteSKIAllowlist = allowlist
		return nil
	})
	fs.Func("eebus-pairing-window-mode", "eeBUS pairing-window mode (M5A: closed only)", func(value string) error {
		mode := ebusgateway.EEBusPairingWindowMode(strings.ToLower(strings.TrimSpace(value)))
		if mode != ebusgateway.EEBusPairingWindowClosed {
			return fmt.Errorf("invalid eebus-pairing-window-mode %q", value)
		}
		cfg.EEBusConfig.PairingWindowMode = mode
		return nil
	})
}

func normalizeEEBusInterfaces(value string) []string {
	return normalizeEEBusList(value, false)
}

func normalizeEEBusSubnets(value string) ([]string, error) {
	items := normalizeEEBusList(value, false)
	seen := make(map[string]struct{}, len(items))
	normalized := make([]string, 0, len(items))
	for _, item := range items {
		prefix, err := netip.ParsePrefix(item)
		if err != nil {
			return nil, fmt.Errorf("invalid eebus-subnets prefix %q", item)
		}
		canonical := prefix.Masked().String()
		if _, ok := seen[canonical]; ok {
			continue
		}
		seen[canonical] = struct{}{}
		normalized = append(normalized, canonical)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func normalizeEEBusRemoteSKIAllowlist(value string) ([]string, error) {
	items := normalizeEEBusList(value, true)
	for _, item := range items {
		decoded, err := hex.DecodeString(item)
		if err != nil || len(decoded) != 20 {
			return nil, fmt.Errorf("invalid eebus-remote-ski-allowlist entry %q", item)
		}
	}
	sort.Strings(items)
	return items, nil
}

func normalizeEEBusPath(flagName, value string) (string, error) {
	if strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("invalid %s: path contains NUL", flagName)
	}
	return strings.TrimSpace(value), nil
}

func normalizeEEBusList(value string, lowercase bool) []string {
	parts := strings.Split(value, ",")
	seen := make(map[string]struct{}, len(parts))
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if lowercase {
			item = strings.ToLower(item)
		}
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		normalized = append(normalized, item)
	}
	return normalized
}
