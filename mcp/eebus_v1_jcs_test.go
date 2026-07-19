package mcp

import (
	"bytes"
	"testing"
)

func TestMSP06CandidateJCSHashVectors(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		exclusions []string
		canonical  string
		hash       string
	}{
		{
			name:      "object-key-order",
			input:     `{"z":0,"a":1}`,
			canonical: `{"a":1,"z":0}`,
			hash:      "sha256:b55af27c4bd5f02ebeca8f901b84d2940b22e7bea7230e4d06f275d903bfdd72",
		},
		{
			name:      "unicode-key-order",
			input:     "{\"outer\":{\"\ue000\":\"bmp\",\"😀\":\"supplementary\"},\"\\r\":\"cr\",\"1\":\"one\",\"€\":\"euro\"}",
			canonical: "{\"\\r\":\"cr\",\"1\":\"one\",\"outer\":{\"😀\":\"supplementary\",\"\ue000\":\"bmp\"},\"€\":\"euro\"}",
			hash:      "sha256:fd16c106364021a01f7a014dbf9f6a2871051afc5eb7d313a5967f5346eb48f9",
		},
		{
			name:      "explicit-null",
			input:     `{"a":null}`,
			canonical: `{"a":null}`,
			hash:      "sha256:d091f9c83c091f79652fe8786375b3fe4ce0861a56f5bfbafedbe431877ff0e8",
		},
		{
			name:      "omitted-field",
			input:     `{}`,
			canonical: `{}`,
			hash:      "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
		},
		{
			name:      "safe-integer-string",
			input:     `{"n":"9007199254740992"}`,
			canonical: `{"n":"9007199254740992"}`,
			hash:      "sha256:bb80eb37329e0a7e980fe3638c9722c44ac3184f7488f20c28cf67ae0b5f4f96",
		},
		{
			name:       "token-substitution-a",
			input:      `{"data":{"snapshot_content_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","snapshot_ref":"opaque-a"},"tool":"eebus.v1.snapshot.capture"}`,
			exclusions: []string{"/data/snapshot_ref"},
			canonical:  `{"data":{"snapshot_content_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"tool":"eebus.v1.snapshot.capture"}`,
			hash:       "sha256:1dd1b393e6cd221850141f0fb4aa66e050abab7cd8fd32abffc8c3e8135b9555",
		},
		{
			name:       "token-substitution-b",
			input:      `{"data":{"snapshot_content_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","snapshot_ref":"opaque-b"},"tool":"eebus.v1.snapshot.capture"}`,
			exclusions: []string{"/data/snapshot_ref"},
			canonical:  `{"data":{"snapshot_content_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"tool":"eebus.v1.snapshot.capture"}`,
			hash:       "sha256:1dd1b393e6cd221850141f0fb4aa66e050abab7cd8fd32abffc8c3e8135b9555",
		},
		{
			name:      "payload-before",
			input:     `{"data":{"value":"before"},"tool":"eebus.v1.services.get"}`,
			canonical: `{"data":{"value":"before"},"tool":"eebus.v1.services.get"}`,
			hash:      "sha256:c88088abcd03a63a675b1b6886b67c9f44c4eff3e081fad3a7315dc8c4928ae9",
		},
		{
			name:      "payload-after",
			input:     `{"data":{"value":"after"},"tool":"eebus.v1.services.get"}`,
			canonical: `{"data":{"value":"after"},"tool":"eebus.v1.services.get"}`,
			hash:      "sha256:8ddc952deff2bd36eade164c45b2799d0b46851086f40a0acb0116f985c33395",
		},
		{
			name:       "error-message-invariant-a",
			input:      `{"backend_error":"implementation-detail-a","error":{"code":"backend_unavailable","message":"eeBUS runtime unavailable","retriable":true,"source_layer":"eebusruntime"}}`,
			exclusions: []string{"/backend_error"},
			canonical:  `{"error":{"code":"backend_unavailable","message":"eeBUS runtime unavailable","retriable":true,"source_layer":"eebusruntime"}}`,
			hash:       "sha256:4ab875e3987cc60dd0fdc382a3d0063b86742bc2349be5831d96e3bf05b7918e",
		},
		{
			name:       "error-message-invariant-b",
			input:      `{"backend_error":"implementation-detail-b","error":{"code":"backend_unavailable","message":"eeBUS runtime unavailable","retriable":true,"source_layer":"eebusruntime"}}`,
			exclusions: []string{"/backend_error"},
			canonical:  `{"error":{"code":"backend_unavailable","message":"eeBUS runtime unavailable","retriable":true,"source_layer":"eebusruntime"}}`,
			hash:       "sha256:4ab875e3987cc60dd0fdc382a3d0063b86742bc2349be5831d96e3bf05b7918e",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			canonical, hash, err := eebusV1CanonicalHashJSON([]byte(test.input), test.exclusions...)
			if err != nil {
				t.Fatalf("eebusV1CanonicalHashJSON() error = %v", err)
			}
			if !bytes.Equal(canonical, []byte(test.canonical)) || hash != test.hash {
				t.Fatalf("canonical/hash = %s / %s, want %s / %s", canonical, hash, test.canonical, test.hash)
			}
		})
	}
}

func TestMSP06JCSRelationsAndExcludedPathsAreExact(t *testing.T) {
	_, tokenA, err := eebusV1CanonicalHashJSON(
		[]byte(`{"data":{"snapshot_content_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","snapshot_ref":"opaque-a"},"tool":"eebus.v1.snapshot.capture"}`),
		"/data/snapshot_ref",
	)
	if err != nil {
		t.Fatal(err)
	}
	_, tokenB, err := eebusV1CanonicalHashJSON(
		[]byte(`{"data":{"snapshot_content_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","snapshot_ref":"opaque-b"},"tool":"eebus.v1.snapshot.capture"}`),
		"/data/snapshot_ref",
	)
	if err != nil {
		t.Fatal(err)
	}
	if tokenA != tokenB {
		t.Fatalf("excluded opaque token changed hash: %s vs %s", tokenA, tokenB)
	}

	_, before, err := eebusV1CanonicalHashJSON([]byte(`{"data":{"value":"before"},"tool":"eebus.v1.services.get"}`))
	if err != nil {
		t.Fatal(err)
	}
	_, after, err := eebusV1CanonicalHashJSON([]byte(`{"data":{"value":"after"},"tool":"eebus.v1.services.get"}`))
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
