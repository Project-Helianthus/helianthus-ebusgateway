package mcp

import (
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

func TestTeslaHSCProvenanceV1RuntimeProjectsOnlyRedactedSelectedTerminalMetadata(t *testing.T) {
	provider := &teslaHSCProvenanceV1FixtureProvider{records: []TeslaHSCProvenanceV1Record{
		{Function: 101, PayloadLength: 0, PayloadDigest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", OutboundAllowed: true},
		{Function: 102, PayloadLength: 2, PayloadDigest: "9ee50aea7e52b0fd0c9dcae1a059ac08b94c21ad3e2fa7e6cbf9b6b5279d93d8"},
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
			record.PayloadLength != provider.records[index].PayloadLength ||
			record.PayloadDigest != provider.records[index].PayloadDigest ||
			record.OutboundAllowed {
			t.Fatalf("redacted provenance[%d] = %#v", index, record)
		}
	}
}

func TestTeslaHSCProvenanceV1RuntimeRejectsUnselectedOrMalformedMetadata(t *testing.T) {
	for _, record := range []TeslaHSCProvenanceV1Record{
		{Function: 100, PayloadLength: 0, PayloadDigest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{Function: 101, PayloadLength: 253, PayloadDigest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{Function: 102, PayloadLength: 1, PayloadDigest: "not-a-digest"},
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
