package main

import (
	"bytes"
	"testing"
)

// TestParseHexByteList_Empty asserts that an empty / whitespace-only
// input returns a nil/empty slice with no error. Empty input is the
// "disable filtering" signal for --phantom-initiator-reject-bytes.
func TestParseHexByteList_Empty(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"", "   ", "\t", "\n"} {
		got, err := parseHexByteList(in)
		if err != nil {
			t.Fatalf("parseHexByteList(%q) returned error: %v", in, err)
		}
		if len(got) != 0 {
			t.Fatalf("parseHexByteList(%q) = %v, want empty", in, got)
		}
	}
}

// TestParseHexByteList_Single asserts the default-case single 0x71
// (live HA bus default) parses correctly.
func TestParseHexByteList_Single(t *testing.T) {
	t.Parallel()

	got, err := parseHexByteList("0x71")
	if err != nil {
		t.Fatalf("parseHexByteList(\"0x71\") returned error: %v", err)
	}
	want := []byte{0x71}
	if !bytes.Equal(got, want) {
		t.Fatalf("parseHexByteList(\"0x71\") = %v, want %v", got, want)
	}
}

// TestParseHexByteList_Multiple asserts CSV parsing of multiple hex
// bytes with various whitespace around items, plus both 0x and bare
// hex formats.
func TestParseHexByteList_Multiple(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  []byte
	}{
		{"two-with-space", "0x71, 0xFD", []byte{0x71, 0xFD}},
		{"two-no-space", "0x71,0xFD", []byte{0x71, 0xFD}},
		{"bare-hex", "71,FD", []byte{0x71, 0xFD}},
		{"mixed-case", "0X71,0xfd", []byte{0x71, 0xFD}},
		{"three", "0x71,0xFD,0xA0", []byte{0x71, 0xFD, 0xA0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseHexByteList(tc.input)
			if err != nil {
				t.Fatalf("parseHexByteList(%q) returned error: %v", tc.input, err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("parseHexByteList(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

// TestParseHexByteList_Invalid asserts that malformed input returns an
// error rather than silently dropping bad items. The CLI surface must
// fail fast so operators don't ship a misconfigured filter.
func TestParseHexByteList_Invalid(t *testing.T) {
	t.Parallel()

	cases := []string{
		"0xZZ",           // non-hex digits
		"0x71,garbage",   // second item invalid
		"0x71,0x100",     // value > 0xFF (8-bit overflow)
		"0x71;0xFD",      // wrong separator
		"not-a-hex-byte", // wholly bogus
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			_, err := parseHexByteList(in)
			if err == nil {
				t.Fatalf("parseHexByteList(%q) returned nil error, want error", in)
			}
		})
	}
}
