package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"sort"
	"strings"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

type eebusInterfaceAddressResolver func(string) ([]netip.Addr, error)

func mapEEBusRuntimeConfig(cfg ebusgateway.EEBusConfig, resolve eebusInterfaceAddressResolver) (eebusruntime.Config, error) {
	if !cfg.Enabled {
		if eebusDisabledConfigIsInert(cfg) {
			return eebusruntime.Config{}, nil
		}
		return eebusruntime.Config{}, errors.New("disabled eeBUS configuration contains active fields")
	}

	stateRoot, err := validateEEBusStateRoot(cfg.StateRoot)
	if err != nil {
		return eebusruntime.Config{}, err
	}
	if cfg.ListenPort == 0 {
		return eebusruntime.Config{}, errors.New("enabled eeBUS configuration requires a non-zero listen port")
	}
	interfaceName, err := validateEEBusInterface(cfg.Interfaces)
	if err != nil {
		return eebusruntime.Config{}, err
	}
	prefixes, err := validateEEBusSubnets(cfg.Subnets)
	if err != nil {
		return eebusruntime.Config{}, err
	}
	if cfg.PairingWindowMode != ebusgateway.EEBusPairingWindowClosed {
		return eebusruntime.Config{}, errors.New("enabled eeBUS configuration requires closed pairing")
	}
	if cfg.CertificatePath != "" || cfg.PrivateKeyPath != "" || cfg.TrustStorePath != "" {
		return eebusruntime.Config{}, errors.New("enabled eeBUS configuration does not accept legacy credential paths")
	}
	remotes, err := mapEEBusRemotes(cfg.RemoteSKIAllowlist)
	if err != nil {
		return eebusruntime.Config{}, err
	}
	if resolve == nil {
		return eebusruntime.Config{}, errors.New("enabled eeBUS configuration requires an interface address resolver")
	}

	addresses, err := resolve(interfaceName)
	if err != nil {
		return eebusruntime.Config{}, fmt.Errorf("resolve eeBUS interface %q addresses: %w", interfaceName, err)
	}
	listenAddress, err := selectEEBusListenAddress(interfaceName, addresses, prefixes)
	if err != nil {
		return eebusruntime.Config{}, err
	}

	return eebusruntime.Config{
		Enabled:          true,
		StateRoot:        stateRoot,
		Interface:        interfaceName,
		ListenAddress:    netip.AddrPortFrom(listenAddress, cfg.ListenPort),
		DiscoveryEnabled: cfg.DiscoveryEnabled,
		Remotes:          remotes,
		PairingPolicy:    eebusruntime.PairingPolicyClosed,
	}, nil
}

func eebusDisabledConfigIsInert(cfg ebusgateway.EEBusConfig) bool {
	pairingIsInert := cfg.PairingWindowMode == "" || cfg.PairingWindowMode == ebusgateway.EEBusPairingWindowClosed
	return cfg.ListenPort == 0 &&
		len(cfg.Interfaces) == 0 &&
		len(cfg.Subnets) == 0 &&
		cfg.StateRoot == "" &&
		!cfg.DiscoveryEnabled &&
		cfg.CertificatePath == "" &&
		cfg.PrivateKeyPath == "" &&
		cfg.TrustStorePath == "" &&
		len(cfg.RemoteSKIAllowlist) == 0 &&
		pairingIsInert
}

func validateEEBusStateRoot(value string) (string, error) {
	if strings.ContainsRune(value, '\x00') {
		return "", errors.New("eeBUS state root contains NUL")
	}
	stateRootInput := strings.TrimSpace(value)
	for _, segment := range strings.Split(stateRootInput, string(filepath.Separator)) {
		if segment == ".." {
			return "", errors.New("eeBUS state root must not contain traversal")
		}
	}
	stateRoot := filepath.Clean(stateRootInput)
	if stateRoot == "." || stateRoot == "" {
		return "", errors.New("enabled eeBUS configuration requires a state root")
	}
	if !filepath.IsAbs(stateRoot) {
		return "", errors.New("eeBUS state root must be absolute")
	}
	volumeRoot := filepath.VolumeName(stateRoot) + string(filepath.Separator)
	if stateRoot == volumeRoot {
		return "", errors.New("eeBUS state root must not be the filesystem root")
	}
	return stateRoot, nil
}

func validateEEBusInterface(interfaces []string) (string, error) {
	if len(interfaces) != 1 {
		return "", errors.New("enabled eeBUS configuration requires exactly one interface")
	}
	interfaceName := strings.TrimSpace(interfaces[0])
	switch interfaceName {
	case "", "*", "0.0.0.0", "::", "[::]":
		return "", errors.New("eeBUS interface must be explicit")
	default:
		return interfaceName, nil
	}
}

func validateEEBusSubnets(subnets []string) ([]netip.Prefix, error) {
	if len(subnets) == 0 {
		return nil, errors.New("enabled eeBUS configuration requires at least one subnet")
	}
	prefixes := make([]netip.Prefix, 0, len(subnets))
	seen := make(map[netip.Prefix]struct{}, len(subnets))
	for _, value := range subnets {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("invalid eeBUS subnet %q", value)
		}
		prefix = prefix.Masked()
		if _, exists := seen[prefix]; exists {
			continue
		}
		seen[prefix] = struct{}{}
		prefixes = append(prefixes, prefix)
	}
	return prefixes, nil
}

func mapEEBusRemotes(allowlist []string) ([]eebusruntime.Remote, error) {
	var remotes []eebusruntime.Remote
	if allowlist != nil {
		remotes = make([]eebusruntime.Remote, 0, len(allowlist))
	}
	seen := make(map[string]struct{}, len(allowlist))
	for index, ski := range allowlist {
		if len(ski) != 40 {
			return nil, fmt.Errorf("invalid eeBUS remote SKI at index %d: must contain exactly 40 hexadecimal characters", index)
		}
		if _, err := hex.DecodeString(ski); err != nil {
			return nil, fmt.Errorf("invalid eeBUS remote SKI at index %d: must contain exactly 40 hexadecimal characters", index)
		}
		identity := strings.ToLower(ski)
		if _, exists := seen[identity]; exists {
			return nil, fmt.Errorf("duplicate eeBUS remote SKI at index %d", index)
		}
		seen[identity] = struct{}{}
		remotes = append(remotes, eebusruntime.Remote{SKI: identity})
	}
	sort.Slice(remotes, func(left, right int) bool {
		return remotes[left].SKI < remotes[right].SKI
	})
	return remotes, nil
}

func selectEEBusListenAddress(interfaceName string, addresses []netip.Addr, prefixes []netip.Prefix) (netip.Addr, error) {
	matches := make(map[netip.Addr]struct{})
	for _, address := range addresses {
		if !validEEBusListenAddress(interfaceName, address) {
			continue
		}
		membershipAddress := address.WithZone("")
		matched := false
		for _, prefix := range prefixes {
			if prefix.Contains(membershipAddress) && validEEBusListenAddressInPrefix(membershipAddress, prefix) {
				matched = true
				break
			}
		}
		if matched {
			matches[address] = struct{}{}
		}
	}
	if len(matches) != 1 {
		return netip.Addr{}, fmt.Errorf("eeBUS interface resolved %d unique valid addresses in configured subnets; want exactly one", len(matches))
	}
	for address := range matches {
		return address, nil
	}
	return netip.Addr{}, errors.New("eeBUS interface address selection failed")
}

func validEEBusListenAddress(interfaceName string, address netip.Addr) bool {
	if !address.IsValid() || address.IsUnspecified() || address.IsMulticast() || address.Is4In6() {
		return false
	}
	if address.Is6() {
		if address.IsLinkLocalUnicast() {
			return address.Zone() == interfaceName
		}
		return address.Zone() == ""
	}
	if !address.Is4() {
		return false
	}
	octets := address.As4()
	return octets[0] != 0 && octets != [4]byte{255, 255, 255, 255}
}

func validEEBusListenAddressInPrefix(address netip.Addr, prefix netip.Prefix) bool {
	if !address.Is4() || !prefix.Addr().Is4() || prefix.Bits() >= 31 {
		return true
	}
	octets := address.As4()
	addressValue := uint32(octets[0])<<24 | uint32(octets[1])<<16 | uint32(octets[2])<<8 | uint32(octets[3])
	networkOctets := prefix.Masked().Addr().As4()
	networkValue := uint32(networkOctets[0])<<24 | uint32(networkOctets[1])<<16 | uint32(networkOctets[2])<<8 | uint32(networkOctets[3])
	hostMask := ^uint32(0) >> prefix.Bits()
	return addressValue != networkValue && addressValue != networkValue|hostMask
}
