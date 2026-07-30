package syncevidence

import (
	"bytes"
	"encoding/json"
	"testing"
)

const (
	issue764HistoricalEEBusContract = "helianthus-eebus-mcp"
	issue764M625EEBusContract       = "helianthus.eebus.m625.public-redacted-evidence.v1"
)

func TestIssue764EEBusAuthoritiesAreKeyedByFullSourceTuple(t *testing.T) {
	historicalKey := sourceAuthorityKey{
		kind:     SourceEEBus,
		contract: issue764HistoricalEEBusContract,
		version:  1,
	}
	m625Key := sourceAuthorityKey{
		kind:     SourceEEBus,
		contract: issue764M625EEBusContract,
		version:  1,
	}

	if len(sourceAuthorities) != 6 {
		t.Fatalf("source authority count = %d, want 6", len(sourceAuthorities))
	}
	historical, ok := sourceAuthorities[historicalKey]
	if !ok {
		t.Fatalf("historical EEBUS authority missing for key %#v", historicalKey)
	}
	if historical.contract != issue764HistoricalEEBusContract ||
		historical.version != 1 ||
		historical.ownerCommit != "9819762a61c28eeceb11beb775aa2a91c83a68b6" ||
		historical.schemaSHA256 != "7f10fa6860e8ccee1af7f155e03d5ac208b5a6fb30518aa3145122a9a1dc0a1c" {
		t.Fatalf("historical EEBUS authority changed: %#v", historical)
	}
	m625, ok := sourceAuthorities[m625Key]
	if !ok {
		t.Fatalf("M6.25 EEBUS authority missing for key %#v", m625Key)
	}
	if m625.contract != issue764M625EEBusContract ||
		m625.version != 1 ||
		m625.ownerCommit != "a09e3a77153204bc3117e233c71e77ef1859834e" ||
		m625.schemaSHA256 != "0a2885d01d6703389541e246db59bcd845a332e7ed296abca2d49b4f8de31811" {
		t.Fatalf("M6.25 EEBUS authority = %#v", m625)
	}
}

func TestIssue764HistoricalEEBusRegistryEntryRemainsByteIdentical(t *testing.T) {
	var registry struct {
		Entries []json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(mustReadContract("synchronized-evidence-source-registry-v1.json"), &registry); err != nil {
		t.Fatalf("decode registry: %v", err)
	}

	var historical json.RawMessage
	for _, raw := range registry.Entries {
		var identity struct {
			SourceKind     SourceKind `json:"source_kind"`
			SourceContract string     `json:"source_contract"`
		}
		if err := json.Unmarshal(raw, &identity); err != nil {
			t.Fatalf("decode registry entry: %v", err)
		}
		if identity.SourceKind == SourceEEBus && identity.SourceContract == issue764HistoricalEEBusContract {
			historical = raw
			break
		}
	}
	if historical == nil {
		t.Fatal("historical EEBUS registry entry is absent")
	}

	want := []byte(`{
      "source_kind": "EEBUS",
      "source_contract": "helianthus-eebus-mcp",
      "source_schema_version": 1,
      "owner_repository": "Project-Helianthus/helianthus-docs-eebus",
      "owner_path": "api/_candidate/msp-06/helianthus.eebus.mcp.v1.schema.json",
      "owner_commit": "9819762a61c28eeceb11beb775aa2a91c83a68b6",
      "schema_sha256": "7f10fa6860e8ccee1af7f155e03d5ac208b5a6fb30518aa3145122a9a1dc0a1c",
      "embedded_schema": null
    }`)
	if !bytes.Equal(historical, want) {
		t.Fatalf("historical EEBUS registry entry changed byte-for-byte:\n got: %q\nwant: %q", historical, want)
	}
}
