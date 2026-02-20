package main

import (
	"testing"
	"time"

	"github.com/d3vi1/helianthus-ebusgateway"
)

func TestShouldStopDiscoveryScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		total int
		want  bool
	}{
		{name: "no devices", total: 0, want: false},
		{name: "some devices", total: 1, want: true},
		{name: "many devices", total: 7, want: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldStopDiscoveryScan(test.total); got != test.want {
				t.Fatalf("shouldStopDiscoveryScan(%d) = %v; want %v", test.total, got, test.want)
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
