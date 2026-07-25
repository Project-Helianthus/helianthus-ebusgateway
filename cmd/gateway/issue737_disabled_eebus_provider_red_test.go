package main

import (
	"testing"
)

func TestIssue737DisabledEEBusAdapterProducesNilMCPProvider(t *testing.T) {
	var adapter *eebusRuntimeAdapter
	if provider := eebusMCPProvider(adapter); provider != nil {
		t.Fatalf("disabled eeBUS provider = %#v, want nil", provider)
	}
}

func TestIssue737EnabledEEBusAdapterPreservesMCPProvider(t *testing.T) {
	adapter := &eebusRuntimeAdapter{}
	provider := eebusMCPProvider(adapter)
	if provider == nil {
		t.Fatal("enabled eeBUS provider is nil")
	}
	if provider != adapter {
		t.Fatalf("enabled eeBUS provider = %#v, want original adapter", provider)
	}
}
