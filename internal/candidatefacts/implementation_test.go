package candidatefacts

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestStrictJSONSyntaxPrecedesSchemaValidation(t *testing.T) {
	artifacts := PinnedArtifactsV1()
	for name, raw := range map[string][]byte{
		"duplicate key":  []byte(`{"x":1,"x":2}`),
		"float":          []byte(`{"x":1.0}`),
		"negative zero":  []byte(`{"x":-0}`),
		"unsafe integer": []byte(`{"x":9007199254740992}`),
		"malformed utf8": {'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'},
	} {
		t.Run(name, func(t *testing.T) {
			if err := Verify(raw, artifacts.SourceBundle, artifacts.SourceReplay); err == nil || err.Error() != "json.syntax" {
				t.Fatalf("Verify error = %v; want json.syntax", err)
			}
		})
	}
}

func TestSourceReplayBindingUsesCanonicalRegeneration(t *testing.T) {
	artifacts := PinnedArtifactsV1()
	var indented bytes.Buffer
	if err := json.Indent(&indented, bytes.TrimSpace(artifacts.SourceReplay), "", "  "); err != nil {
		t.Fatal(err)
	}
	indented.WriteByte('\n')
	if err := Verify(artifacts.PositiveGraph, artifacts.SourceBundle, indented.Bytes()); err != nil {
		t.Fatalf("Verify with semantically identical source replay: %v", err)
	}
}

func TestPinnedArtifactsAreReturnedAsIndependentCopies(t *testing.T) {
	first := PinnedArtifactsV1()
	first.Registry[0] ^= 0xff
	first.NegativeGraphs["unknown-field.json"][0] ^= 0xff
	second := PinnedArtifactsV1()
	if bytes.Equal(first.Registry, second.Registry) || bytes.Equal(first.NegativeGraphs["unknown-field.json"], second.NegativeGraphs["unknown-field.json"]) {
		t.Fatal("PinnedArtifactsV1 returned mutable shared storage")
	}
}
