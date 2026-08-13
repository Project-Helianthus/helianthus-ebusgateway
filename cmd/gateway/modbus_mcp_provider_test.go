package main

import (
	"strings"
	"testing"
)

func TestNewGatewayModbusMCPProviderDisabledIsInert(t *testing.T) {
	if provider := newGatewayModbusMCPProvider(nil); provider != nil {
		t.Fatalf("nil adapter produced provider %T", provider)
	}
}

func TestEndpointReferenceDoesNotExposeEndpoint(t *testing.T) {
	endpoint := "tcp://192.0.2.10:502"
	reference := endpointReference(endpoint)
	if reference == endpoint || len(reference) != len("sha256:")+64 {
		t.Fatalf("endpoint reference = %q", reference)
	}
	if reference != endpointReference(endpoint) {
		t.Fatal("endpoint reference is nondeterministic")
	}
}

func TestRedactModbusEndpointsCoversNestedObservationProvenance(t *testing.T) {
	const endpoint = "tcp://operator:secret@192.0.2.10:502"
	observation := map[string]any{
		"endpoint": endpoint,
		"dependencies": []any{map[string]any{
			"view": map[string]any{"endpoint": endpoint, "words": []any{1.0, 2.0}},
		}},
	}

	redactModbusEndpoints(observation)
	if got := observation["endpoint"]; got != endpointReference(endpoint) {
		t.Fatalf("top-level endpoint = %q", got)
	}
	view := observation["dependencies"].([]any)[0].(map[string]any)["view"].(map[string]any)
	if got := view["endpoint"]; got != endpointReference(endpoint) {
		t.Fatalf("nested endpoint = %q", got)
	}
	if strings.Contains(view["endpoint"].(string), "secret") {
		t.Fatal("nested endpoint leaked credential material")
	}
}
