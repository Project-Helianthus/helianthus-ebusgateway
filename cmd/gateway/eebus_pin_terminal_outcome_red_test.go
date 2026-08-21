package main

import (
	"reflect"
	"testing"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

// The Portal must never manufacture a terminal PIN outcome from the immediate
// connection_started acknowledgement. A real terminal callback is required
// before a gateway-owned ActiveAction status can truthfully resolve.
func TestIssue848EEBusregExposesTerminalPINOutcomeCallback(t *testing.T) {
	admin := reflect.TypeOf((*eebusruntime.AdminV1)(nil)).Elem()
	for _, name := range []string{"SubscribeConnectTerminalOutcome", "SubscribePINConnectTerminalOutcome"} {
		if _, ok := admin.MethodByName(name); ok {
			return
		}
	}
	t.Fatalf("eebusreg AdminV1 lacks a public asynchronous terminal PIN outcome callback; Connect's immediate result is not a terminal pairing outcome")
}
