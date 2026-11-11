package mcp

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

func TestMSP06CandidateJCSHashVectors(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		exclusions []string
		canonical  string
	}{
		{
			name:      "object-key-order",
			input:     `{"z":0,"a":1}`,
			canonical: `{"a":1,"z":0}`,
		},
		{
			name:      "unicode-key-order",
			input:     "{\"outer\":{\"\ue000\":\"bmp\",\"😀\":\"supplementary\"},\"\\r\":\"cr\",\"1\":\"one\",\"€\":\"euro\"}",
			canonical: "{\"\\r\":\"cr\",\"1\":\"one\",\"outer\":{\"😀\":\"supplementary\",\"\ue000\":\"bmp\"},\"€\":\"euro\"}",
		},
		{
			name:      "explicit-null",
			input:     `{"a":null}`,
			canonical: `{"a":null}`,
		},
		{
			name:      "omitted-field",
			input:     `{}`,
			canonical: `{}`,
		},
		{
			name:      "safe-integer-string",
			input:     `{"n":"9007199254740992"}`,
			canonical: `{"n":"9007199254740992"}`,
		},
		{
			name:       "token-substitution-a",
			input:      `{"data":{"snapshot_content_hash":"sha256:` + strings.Repeat("a", 64) + `","snapshot_ref":"opaque-a"},"tool":"eebus_v1_snapshot_capture"}`,
			exclusions: []string{"/data/snapshot_ref"},
			canonical:  `{"data":{"snapshot_content_hash":"sha256:` + strings.Repeat("a", 64) + `"},"tool":"eebus_v1_snapshot_capture"}`,
		},
		{
			name:       "token-substitution-b",
			input:      `{"data":{"snapshot_content_hash":"sha256:` + strings.Repeat("a", 64) + `","snapshot_ref":"opaque-b"},"tool":"eebus_v1_snapshot_capture"}`,
			exclusions: []string{"/data/snapshot_ref"},
			canonical:  `{"data":{"snapshot_content_hash":"sha256:` + strings.Repeat("a", 64) + `"},"tool":"eebus_v1_snapshot_capture"}`,
		},
		{
			name:      "payload-before",
			input:     `{"data":{"value":"before"},"tool":"eebus_v1_services_get"}`,
			canonical: `{"data":{"value":"before"},"tool":"eebus_v1_services_get"}`,
		},
		{
			name:      "payload-after",
			input:     `{"data":{"value":"after"},"tool":"eebus_v1_services_get"}`,
			canonical: `{"data":{"value":"after"},"tool":"eebus_v1_services_get"}`,
		},
		{
			name:       "error-message-invariant-a",
			input:      `{"backend_error":"implementation-detail-a","error":{"code":"backend_unavailable","message":"eeBUS runtime unavailable","retriable":true,"source_layer":"eebusruntime"}}`,
			exclusions: []string{"/backend_error"},
			canonical:  `{"error":{"code":"backend_unavailable","message":"eeBUS runtime unavailable","retriable":true,"source_layer":"eebusruntime"}}`,
		},
		{
			name:       "error-message-invariant-b",
			input:      `{"backend_error":"implementation-detail-b","error":{"code":"backend_unavailable","message":"eeBUS runtime unavailable","retriable":true,"source_layer":"eebusruntime"}}`,
			exclusions: []string{"/backend_error"},
			canonical:  `{"error":{"code":"backend_unavailable","message":"eeBUS runtime unavailable","retriable":true,"source_layer":"eebusruntime"}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			canonical, hash, err := eebusV1CanonicalHashJSON([]byte(test.input), test.exclusions...)
			if err != nil {
				t.Fatalf("eebusV1CanonicalHashJSON() error = %v", err)
			}
			wantHash := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(test.canonical)))
			if !bytes.Equal(canonical, []byte(test.canonical)) || hash != wantHash {
				t.Fatalf("canonical/hash = %s / %s, want %s / %s", canonical, hash, test.canonical, wantHash)
			}
		})
	}
}

func TestMSP06JCSRelationsAndExcludedPathsAreExact(t *testing.T) {
	_, tokenA, err := eebusV1CanonicalHashJSON(
		[]byte(`{"data":{"snapshot_content_hash":"sha256:`+strings.Repeat("a", 64)+`","snapshot_ref":"opaque-a"},"tool":"eebus_v1_snapshot_capture"}`),
		"/data/snapshot_ref",
	)
	if err != nil {
		t.Fatal(err)
	}
	_, tokenB, err := eebusV1CanonicalHashJSON(
		[]byte(`{"data":{"snapshot_content_hash":"sha256:`+strings.Repeat("a", 64)+`","snapshot_ref":"opaque-b"},"tool":"eebus_v1_snapshot_capture"}`),
		"/data/snapshot_ref",
	)
	if err != nil {
		t.Fatal(err)
	}
	if tokenA != tokenB {
		t.Fatalf("excluded opaque token changed hash: %s vs %s", tokenA, tokenB)
	}

	_, before, err := eebusV1CanonicalHashJSON([]byte(`{"data":{"value":"before"},"tool":"eebus_v1_services_get"}`))
	if err != nil {
		t.Fatal(err)
	}
	_, after, err := eebusV1CanonicalHashJSON([]byte(`{"data":{"value":"after"},"tool":"eebus_v1_services_get"}`))
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("payload mutation did not change hash")
	}

	_, explicitNull, err := eebusV1CanonicalHashJSON([]byte(`{"a":null}`))
	if err != nil {
		t.Fatal(err)
	}
	_, omitted, err := eebusV1CanonicalHashJSON([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if explicitNull == omitted {
		t.Fatal("explicit null and omitted field produced the same hash")
	}
}

func TestMSP06JCSRejectsNegativeZeroUnsafeNumbersAndInvalidJSON(t *testing.T) {
	for _, input := range []string{
		`{"n":-0}`,
		`{"n":9007199254740992}`,
		`{"n":-9007199254740992}`,
		`{"n":NaN}`,
		`{"n":Infinity}`,
		`{"unterminated":`,
	} {
		if canonical, hash, err := eebusV1CanonicalHashJSON([]byte(input)); err == nil {
			t.Fatalf("input %s accepted as canonical=%s hash=%s", input, canonical, hash)
		}
	}
}
