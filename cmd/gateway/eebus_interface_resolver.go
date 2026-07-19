package main

import (
	"fmt"
	"net"
	"net/netip"
)

var eebusInterfaceByNameFn = net.InterfaceByName

func resolveEEBusInterfaceAddresses(interfaceName string) ([]netip.Addr, error) {
	networkInterface, err := eebusInterfaceByNameFn(interfaceName)
	if err != nil {
		return nil, fmt.Errorf("look up eeBUS interface %q: %w", interfaceName, err)
	}
	if networkInterface == nil {
		return nil, fmt.Errorf("look up eeBUS interface %q: resolver returned nil", interfaceName)
	}
	addresses, err := networkInterface.Addrs()
	if err != nil {
		return nil, fmt.Errorf("list eeBUS interface %q addresses: %w", interfaceName, err)
	}
	return convertEEBusInterfaceAddresses(interfaceName, addresses)
}

func convertEEBusInterfaceAddresses(interfaceName string, addresses []net.Addr) ([]netip.Addr, error) {
	converted := make([]netip.Addr, 0, len(addresses))
	for index, source := range addresses {
		var ip net.IP
		switch address := source.(type) {
		case *net.IPNet:
			if address == nil {
				return nil, fmt.Errorf("eeBUS interface address %d is a nil *net.IPNet", index)
			}
			ip = address.IP
		case *net.IPAddr:
			if address == nil {
				return nil, fmt.Errorf("eeBUS interface address %d is a nil *net.IPAddr", index)
			}
			ip = address.IP
		default:
			return nil, fmt.Errorf("eeBUS interface address %d has unsupported type %T", index, source)
		}

		address, ok := netip.AddrFromSlice(ip)
		if !ok {
			return nil, fmt.Errorf("eeBUS interface address %d has invalid IP bytes", index)
		}
		address = address.Unmap().WithZone("")
		if address.Is6() && address.IsLinkLocalUnicast() {
			address = address.WithZone(interfaceName)
		}
		converted = append(converted, address)
	}
	return converted, nil
}
