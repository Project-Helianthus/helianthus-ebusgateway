package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func TestWireObserveFirstObserversChainsIdentificationEvidence(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportEbusdTCP
	if _, _, err := wireObserveFirstObservers(&cfg); err != nil {
		t.Fatalf("wireObserveFirstObservers error = %v", err)
	}
	store := currentIdentificationEvidenceStore()
	if store == nil {
		t.Fatalf("identification evidence store not published by wiring")
	}
	err := cfg.BusConfig.Observer.OnBusEvent(protocol.BusEvent{
		Kind: protocol.BusEventAttemptComplete, HasRequest: true, HasResponse: true,
		Request:  protocol.Frame{Source: 0x71, Target: 0x08, Primary: 0x07, Secondary: 0x04},
		Response: protocol.Frame{Data: []byte{0xB5, 'B', 'A', 'I', '0', '0', 1, 2, 3, 4}},
	})
	if err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}
	if n := len(store.Snapshot().Exchanges); n != 1 {
		t.Fatalf("exchanges via chained observer = %d, want 1", n)
	}
}

func TestBusObservabilityAdaptersExposeIdentificationEvidence(t *testing.T) {
	store := ebusgateway.NewIdentificationEvidenceStore(nil)
	publishIdentificationEvidenceStore(store)
	_ = store.OnBusEvent(protocol.BusEvent{
		Kind: protocol.BusEventAttemptComplete, HasRequest: true, HasResponse: true,
		Request:  protocol.Frame{Source: 0x71, Target: 0x15, Primary: 0x07, Secondary: 0x04},
		Response: protocol.Frame{Data: []byte{0xB5}},
	})
	mcpEvidence, ok := newMCPBusObservabilityProvider(nil).(mcp.IdentificationEvidenceProvider)
	if !ok {
		t.Fatalf("mcp bus observability adapter does not expose identification evidence")
	}
	gqlEvidence, ok := newGraphQLBusObservabilityProvider(nil).(graphql.IdentificationEvidenceProvider)
	if !ok {
		t.Fatalf("graphql bus observability adapter does not expose identification evidence")
	}
	m, g := mcpEvidence.IdentificationEvidence(), gqlEvidence.IdentificationEvidence()
	if len(m.Exchanges) != 1 || len(g.Exchanges) != 1 || m.SessionID == "" || m.SessionID != g.SessionID {
		t.Fatalf("mcp=%+v graphql=%+v", m, g)
	}
	if string(m.Exchanges[0].Request) != string([]byte{0x71, 0x15, 0x07, 0x04, 0x00}) || string(g.Exchanges[0].Response) != string([]byte{0x01, 0xB5}) {
		t.Fatalf("bytes mismatch mcp=%+v graphql=%+v", m, g)
	}
	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetBusObservabilityProvider(newMCPBusObservabilityProvider(nil))
	handler := server.Handler()
	if !mcpToolsContain(mcpToolsList(t, handler), "ebus.v1.bus.identification_evidence.list") {
		t.Fatalf("identification evidence tool not registered through bus observability provider")
	}
	envelope := mcpCallToolEnvelope(t, handler, "ebus.v1.bus.identification_evidence.list", `{}`)
	data, _ := envelope["data"].(map[string]any)
	if data["evidence_class"] != "raw" || data["count"] != float64(1) {
		t.Fatalf("envelope data = %v", data)
	}
}
