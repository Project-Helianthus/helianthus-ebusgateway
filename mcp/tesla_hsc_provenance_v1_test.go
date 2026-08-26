package mcp

import (
	"bytes"
	"context"
	"testing"
)

type teslaHSCProvenanceV1FixtureProvider struct {
	calls   int
	records []TeslaHSCProvenanceV1Record
	err     error
}

func (provider *teslaHSCProvenanceV1FixtureProvider) TeslaHSCProvenanceV1(context.Context) ([]TeslaHSCProvenanceV1Record, error) {
	provider.calls++
	return provider.records, provider.err
}

func TestTeslaHSCProvenanceV1RuntimePreservesNativeTerminalRecords(t *testing.T) {
	provider := &teslaHSCProvenanceV1FixtureProvider{records: []TeslaHSCProvenanceV1Record{
		{Function: 101, Payload: []byte{}, Compatibility: "wc3_24_44_3", Provenance: "synthetic-replay", OutboundAllowed: true},
		{Function: 102, Payload: []byte{0x42, 0x99}, Compatibility: "wc3_24_44_3", Provenance: "synthetic-replay"},
	}}
	runtime, err := NewTeslaHSCProvenanceV1Runtime(provider)
	if err != nil {
		t.Fatal(err)
	}
	got, err := runtime.TeslaHSCProvenanceV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if provider.calls != 1 || len(got) != 2 {
		t.Fatalf("provider calls=%d, records=%#v", provider.calls, got)
	}
	for index, record := range got {
		if record.Function != provider.records[index].Function ||
			!bytes.Equal(record.Payload, provider.records[index].Payload) ||
			record.Compatibility != provider.records[index].Compatibility ||
			record.Provenance != provider.records[index].Provenance ||
			record.OutboundAllowed != provider.records[index].OutboundAllowed {
			t.Fatalf("native provenance[%d] = %#v", index, record)
		}
	}
	if got[0].Payload == nil {
		t.Fatalf("non-nil empty payload became nil: %#v", got[0])
	}
	provider.records[1].Payload[0] = 0
	if got[1].Payload[0] != 0x42 {
		t.Fatalf("native payload was not copied: %#v", got[1])
	}
}

func TestTeslaHSCProvenanceV1RuntimeRejectsInvalidNativeRecords(t *testing.T) {
	for _, record := range []TeslaHSCProvenanceV1Record{
		{Function: 100, Payload: []byte{}, Compatibility: "wc3_24_44_3", Provenance: "synthetic-replay"},
		{Function: 101, Payload: bytes.Repeat([]byte{1}, 253), Compatibility: "wc3_24_44_3", Provenance: "synthetic-replay"},
		{Function: 102, Payload: []byte{}, Provenance: "synthetic-replay"},
		{Function: 102, Payload: []byte{}, Compatibility: "wc3_24_44_3"},
	} {
		provider := &teslaHSCProvenanceV1FixtureProvider{records: []TeslaHSCProvenanceV1Record{record}}
		runtime, err := NewTeslaHSCProvenanceV1Runtime(provider)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.TeslaHSCProvenanceV1(context.Background()); err == nil {
			t.Fatalf("invalid provenance accepted: %#v", record)
		}
	}
}
