package ebus_standard_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	estd "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
)

func TestEnvelope_ShapeMetaDataError(t *testing.T) {
	env := estd.NewEnvelope(map[string]any{"k": 1}, nil, time.Unix(0, 0).UTC())
	for _, key := range []string{"meta", "data", "error"} {
		if _, ok := env[key]; !ok {
			t.Fatalf("envelope missing key %q", key)
		}
	}
	meta, _ := env["meta"].(map[string]any)
	if _, ok := meta["data_hash"]; !ok {
		t.Fatal("meta.data_hash missing")
	}
	if _, ok := meta["data_timestamp"]; !ok {
		t.Fatal("meta.data_timestamp missing")
	}
}

func TestDataHash_DeterministicAcrossReorderedMaps(t *testing.T) {
	a := map[string]any{"a": 1, "b": 2, "c": []any{"x", "y"}}
	b := map[string]any{"c": []any{"x", "y"}, "b": 2, "a": 1}
	if estd.DataHash(a) != estd.DataHash(b) {
		t.Fatalf("hash differs on reordered maps: %s vs %s",
			estd.DataHash(a), estd.DataHash(b))
	}
}

func TestDataHash_DifferentForDifferentData(t *testing.T) {
	if estd.DataHash(1) == estd.DataHash(2) {
		t.Fatal("1 and 2 must hash differently")
	}
}

func TestDataHash_StableAcrossRepeatedCalls(t *testing.T) {
	data := map[string]any{"n": 42, "s": "hello", "a": []any{1, 2, 3}}
	h1 := estd.DataHash(data)
	h2 := estd.DataHash(data)
	if h1 != h2 {
		t.Fatalf("repeated call hash differs: %s vs %s", h1, h2)
	}
}

func TestEnvelope_ErrorCarriesClassification(t *testing.T) {
	env := estd.NewEnvelope(nil, errors.New("boom"), time.Unix(0, 0).UTC())
	errOut, ok := env["error"].(map[string]any)
	if !ok || errOut == nil {
		t.Fatal("envelope.error must be a populated object on non-nil err")
	}
	if _, ok := errOut["code"]; !ok {
		t.Fatal("error.code missing")
	}
	if _, ok := errOut["message"]; !ok {
		t.Fatal("error.message missing")
	}
}

func TestDataHash_HexOnly(t *testing.T) {
	h := estd.DataHash("x")
	if len(h) != 64 {
		t.Fatalf("hash length = %d, want 64", len(h))
	}
	for _, r := range h {
		if !strings.ContainsRune("0123456789abcdef", r) {
			t.Fatalf("non-hex char in hash: %q", r)
		}
	}
}
