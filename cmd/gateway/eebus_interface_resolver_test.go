package main

import (
	"errors"
	"net"
	"net/netip"
	"testing"
)

type msp05bUnknownAddr struct{}

func (msp05bUnknownAddr) Network() string { return "msp05b" }
func (msp05bUnknownAddr) String() string  { return "unsupported" }

func TestMSP05BConvertEEBusInterfaceAddresses(t *testing.T) {
	interfaceName := "fixture0"
	tests := []struct {
		name string
		addr net.Addr
		want netip.Addr
	}{
		{
			name: "IPNet IPv4 is unmapped and unzoned",
			addr: &net.IPNet{IP: net.ParseIP("192.0.2.42"), Mask: net.CIDRMask(24, 32)},
			want: netip.MustParseAddr("192.0.2.42"),
		},
		{
			name: "IPAddr IPv4 discards an inapplicable zone",
			addr: &net.IPAddr{IP: net.ParseIP("192.0.2.43"), Zone: "stale0"},
			want: netip.MustParseAddr("192.0.2.43"),
		},
		{
			name: "IPNet link local IPv6 gains the selected interface zone",
			addr: &net.IPNet{IP: net.ParseIP("fe80::1234"), Mask: net.CIDRMask(64, 128)},
			want: netip.MustParseAddr("fe80::1234%fixture0"),
		},
		{
			name: "IPAddr link local IPv6 replaces the reported zone",
			addr: &net.IPAddr{IP: net.ParseIP("fe80::5678"), Zone: "stale0"},
			want: netip.MustParseAddr("fe80::5678%fixture0"),
		},
		{
			name: "global IPv6 remains unzoned",
			addr: &net.IPAddr{IP: net.ParseIP("2001:db8::42"), Zone: "stale0"},
			want: netip.MustParseAddr("2001:db8::42"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := convertEEBusInterfaceAddresses(interfaceName, []net.Addr{test.addr})
			if err != nil {
				t.Fatalf("convert address: %v", err)
			}
			if len(got) != 1 || got[0] != test.want {
				t.Fatalf("converted addresses = %v; want [%v]", got, test.want)
			}
		})
	}
}

func TestMSP05BConvertEEBusInterfaceAddressesRejectsUnknownType(t *testing.T) {
	got, err := convertEEBusInterfaceAddresses("fixture0", []net.Addr{
		&net.IPAddr{IP: net.ParseIP("192.0.2.42")},
		msp05bUnknownAddr{},
	})
	if err == nil {
		t.Fatal("unknown net.Addr conversion error = nil; want rejection")
	}
	if got != nil {
		t.Fatalf("addresses after unknown type = %v; want nil fail-closed result", got)
	}
}

func TestMSP05BConvertEEBusInterfaceAddressesRejectsTypedNil(t *testing.T) {
	tests := []struct {
		name string
		addr net.Addr
	}{
		{name: "nil IPNet", addr: (*net.IPNet)(nil)},
		{name: "nil IPAddr", addr: (*net.IPAddr)(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := convertEEBusInterfaceAddresses("fixture0", []net.Addr{test.addr})
			if err == nil {
				t.Fatal("typed-nil conversion error = nil; want rejection")
			}
			if got != nil {
				t.Fatalf("addresses after typed nil = %v; want nil fail-closed result", got)
			}
		})
	}
}

func TestMSP05BResolveEEBusInterfaceAddressesUsesInterfaceByName(t *testing.T) {
	original := eebusInterfaceByNameFn
	t.Cleanup(func() { eebusInterfaceByNameFn = original })

	wantErr := errors.New("interface lookup failed")
	calledWith := ""
	eebusInterfaceByNameFn = func(name string) (*net.Interface, error) {
		calledWith = name
		return nil, wantErr
	}

	got, err := resolveEEBusInterfaceAddresses("fixture0")
	if !errors.Is(err, wantErr) {
		t.Fatalf("resolver error = %v; want interface lookup cause", err)
	}
	if got != nil {
		t.Fatalf("resolver addresses = %v; want nil", got)
	}
	if calledWith != "fixture0" {
		t.Fatalf("InterfaceByName argument = %q; want fixture0", calledWith)
	}
}
