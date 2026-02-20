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
